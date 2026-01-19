// Fast Dirty Page Tracker using soft-dirty bits
//
// High-performance Go implementation for tracking dirty pages.
// Compatible with the Python dirty_tracker output format.
//
// Usage:
//
//	./dirty_tracker -pid 1234 -interval 100 -duration 10 -output dirty_pattern.json
package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	PageSize         = 4096
	PagemapEntrySize = 8

	// Pagemap entry flags
	PagePresent = uint64(1) << 63
	PageSwapped = uint64(1) << 62
	SoftDirty   = uint64(1) << 55
)

// VMAInfo represents a Virtual Memory Area from /proc/[pid]/maps
type VMAInfo struct {
	Start    uint64
	End      uint64
	Perms    string
	Offset   uint64
	Device   string
	Inode    uint64
	Pathname string
}

func (v *VMAInfo) IsWritable() bool {
	return len(v.Perms) > 1 && v.Perms[1] == 'w'
}

func (v *VMAInfo) VMAType() string {
	switch v.Pathname {
	case "[heap]":
		return "heap"
	case "[stack]":
		return "stack"
	case "[vdso]", "[vvar]", "[vsyscall]":
		return "vdso"
	case "":
		return "anonymous"
	default:
		if strings.HasPrefix(v.Pathname, "/") {
			if strings.Contains(v.Perms, "x") {
				return "code"
			}
			return "data"
		}
		return "unknown"
	}
}

// DirtyPage represents a single dirty page
type DirtyPage struct {
	Addr     string `json:"addr"`
	VMAType  string `json:"vma_type"`
	VMAPerms string `json:"vma_perms"`
	Pathname string `json:"pathname"`
	Size     int    `json:"size"`
}

// DirtySample represents a single sampling point
type DirtySample struct {
	TimestampMs     float64     `json:"timestamp_ms"`
	DirtyPages      []DirtyPage `json:"dirty_pages"`
	DeltaDirtyCount int         `json:"delta_dirty_count"`
	PidsTracked     []int       `json:"pids_tracked"`
}

// DirtyRateEntry represents a point in the dirty rate timeline
type DirtyRateEntry struct {
	TimestampMs      float64 `json:"timestamp_ms"`
	RatePagesPerSec  float64 `json:"rate_pages_per_sec"`
	CumulativePages  int     `json:"cumulative_pages"`
	ProcessesTracked int     `json:"processes_tracked"`
}

// Summary contains aggregated statistics
type Summary struct {
	TotalUniquePages    int                `json:"total_unique_pages"`
	TotalDirtyEvents    int                `json:"total_dirty_events"`
	TotalDirtySizeBytes int                `json:"total_dirty_size_bytes"`
	AvgDirtyRatePerSec  float64            `json:"avg_dirty_rate_per_sec"`
	PeakDirtyRate       float64            `json:"peak_dirty_rate"`
	VMADistribution     map[string]float64 `json:"vma_distribution"`
	VMASizeDistribution map[string]int     `json:"vma_size_distribution"`
	SampleCount         int                `json:"sample_count"`
	IntervalMs          float64            `json:"interval_ms"`
	MaxProcessesTracked int                `json:"max_processes_tracked"`
	TotalPidsSeen       []int              `json:"total_pids_seen"`
}

// DirtyPattern is the main output structure (compatible with Python version)
type DirtyPattern struct {
	Workload           string           `json:"workload"`
	RootPid            int              `json:"root_pid"`
	TrackChildren      bool             `json:"track_children"`
	TrackingDurationMs float64          `json:"tracking_duration_ms"`
	PageSize           int              `json:"page_size"`
	Samples            []DirtySample    `json:"samples"`
	Summary            Summary          `json:"summary"`
	DirtyRateTimeline  []DirtyRateEntry `json:"dirty_rate_timeline"`
}

// ProcessTracker tracks dirty pages for a single process
type ProcessTracker struct {
	pid         int
	pagemapFd   int
	clearRefsFd int
	isOpen      bool
}

func NewProcessTracker(pid int) *ProcessTracker {
	return &ProcessTracker{pid: pid}
}

func (pt *ProcessTracker) Open() error {
	pagemapPath := fmt.Sprintf("/proc/%d/pagemap", pt.pid)
	clearRefsPath := fmt.Sprintf("/proc/%d/clear_refs", pt.pid)

	var err error
	pt.pagemapFd, err = syscall.Open(pagemapPath, syscall.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("open pagemap: %w", err)
	}

	pt.clearRefsFd, err = syscall.Open(clearRefsPath, syscall.O_WRONLY, 0)
	if err != nil {
		syscall.Close(pt.pagemapFd)
		return fmt.Errorf("open clear_refs: %w", err)
	}

	pt.isOpen = true
	return nil
}

func (pt *ProcessTracker) Close() {
	if pt.pagemapFd > 0 {
		syscall.Close(pt.pagemapFd)
	}
	if pt.clearRefsFd > 0 {
		syscall.Close(pt.clearRefsFd)
	}
	pt.isOpen = false
}

func (pt *ProcessTracker) IsAlive() bool {
	_, err := os.Stat(fmt.Sprintf("/proc/%d", pt.pid))
	return err == nil
}

func (pt *ProcessTracker) ClearSoftDirty() error {
	if !pt.isOpen {
		return nil
	}
	_, err := syscall.Seek(pt.clearRefsFd, 0, 0)
	if err != nil {
		return err
	}
	_, err = syscall.Write(pt.clearRefsFd, []byte("4"))
	return err
}

func (pt *ProcessTracker) ParseMaps() ([]VMAInfo, error) {
	mapsPath := fmt.Sprintf("/proc/%d/maps", pt.pid)
	data, err := os.ReadFile(mapsPath)
	if err != nil {
		return nil, err
	}

	var vmas []VMAInfo
	lines := strings.Split(string(data), "\n")

	for _, line := range lines {
		if line == "" {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}

		addrRange := strings.Split(fields[0], "-")
		if len(addrRange) != 2 {
			continue
		}

		start, err := strconv.ParseUint(addrRange[0], 16, 64)
		if err != nil {
			continue
		}
		end, err := strconv.ParseUint(addrRange[1], 16, 64)
		if err != nil {
			continue
		}

		offset, _ := strconv.ParseUint(fields[2], 16, 64)
		inode, _ := strconv.ParseUint(fields[4], 10, 64)

		pathname := ""
		if len(fields) > 5 {
			pathname = fields[5]
		}

		vmas = append(vmas, VMAInfo{
			Start:    start,
			End:      end,
			Perms:    fields[1],
			Offset:   offset,
			Device:   fields[3],
			Inode:    inode,
			Pathname: pathname,
		})
	}

	return vmas, nil
}

func (pt *ProcessTracker) ReadDirtyPages(uniqueAddrs map[uint64]struct{}) ([]DirtyPage, error) {
	if !pt.isOpen {
		return nil, nil
	}

	vmas, err := pt.ParseMaps()
	if err != nil {
		return nil, err
	}

	var dirtyPages []DirtyPage

	// Pre-allocate buffer for reading pagemap entries
	maxPages := 0
	for _, vma := range vmas {
		if vma.IsWritable() {
			numPages := int((vma.End - vma.Start) / PageSize)
			if numPages > maxPages {
				maxPages = numPages
			}
		}
	}
	buf := make([]byte, maxPages*PagemapEntrySize)

	for _, vma := range vmas {
		if !vma.IsWritable() {
			continue
		}

		startPage := vma.Start / PageSize
		numPages := (vma.End - vma.Start) / PageSize
		pagemapOffset := int64(startPage * PagemapEntrySize)

		_, err := syscall.Seek(pt.pagemapFd, pagemapOffset, 0)
		if err != nil {
			continue
		}

		readSize := int(numPages * PagemapEntrySize)
		n, err := syscall.Read(pt.pagemapFd, buf[:readSize])
		if err != nil || n == 0 {
			continue
		}

		actualPages := n / PagemapEntrySize
		vmaType := vma.VMAType()

		for i := 0; i < actualPages; i++ {
			entry := binary.LittleEndian.Uint64(buf[i*PagemapEntrySize : (i+1)*PagemapEntrySize])

			if entry&SoftDirty != 0 {
				addr := vma.Start + uint64(i)*PageSize
				dirtyPages = append(dirtyPages, DirtyPage{
					Addr:     fmt.Sprintf("0x%x", addr),
					VMAType:  vmaType,
					VMAPerms: vma.Perms,
					Pathname: vma.Pathname,
					Size:     PageSize,
				})
				uniqueAddrs[addr] = struct{}{}
			}
		}
	}

	return dirtyPages, nil
}

// DirtyPageTracker is the main tracker with child process support
type DirtyPageTracker struct {
	rootPid       int
	intervalMs    int
	trackChildren bool
	workloadName  string

	mu              sync.Mutex
	trackers        map[int]*ProcessTracker
	knownPids       map[int]struct{}
	deadPids        map[int]struct{}
	samples         []DirtySample
	uniqueAddrs     map[uint64]struct{}
	totalDirtyPages int

	stopCh    chan struct{}
	startTime time.Time
}

func NewDirtyPageTracker(rootPid, intervalMs int, trackChildren bool, workloadName string) *DirtyPageTracker {
	return &DirtyPageTracker{
		rootPid:       rootPid,
		intervalMs:    intervalMs,
		trackChildren: trackChildren,
		workloadName:  workloadName,
		trackers:      make(map[int]*ProcessTracker),
		knownPids:     make(map[int]struct{}),
		deadPids:      make(map[int]struct{}),
		uniqueAddrs:   make(map[uint64]struct{}),
		stopCh:        make(chan struct{}),
	}
}

func (dt *DirtyPageTracker) discoverDescendants(pid int) map[int]struct{} {
	descendants := make(map[int]struct{})
	toCheck := []int{pid}
	checked := make(map[int]struct{})

	for len(toCheck) > 0 {
		currentPid := toCheck[0]
		toCheck = toCheck[1:]

		if _, ok := checked[currentPid]; ok {
			continue
		}
		checked[currentPid] = struct{}{}

		childrenPath := fmt.Sprintf("/proc/%d/task/%d/children", currentPid, currentPid)
		data, err := os.ReadFile(childrenPath)
		if err != nil {
			continue
		}

		content := strings.TrimSpace(string(data))
		if content == "" {
			continue
		}

		for _, pidStr := range strings.Fields(content) {
			childPid, err := strconv.Atoi(pidStr)
			if err != nil {
				continue
			}
			if _, ok := descendants[childPid]; !ok {
				descendants[childPid] = struct{}{}
				toCheck = append(toCheck, childPid)
			}
		}
	}

	return descendants
}

func (dt *DirtyPageTracker) addProcessTracker(pid int) bool {
	if _, ok := dt.trackers[pid]; ok {
		return false
	}
	if _, ok := dt.deadPids[pid]; ok {
		return false
	}

	tracker := NewProcessTracker(pid)
	if err := tracker.Open(); err != nil {
		dt.deadPids[pid] = struct{}{}
		return false
	}

	dt.trackers[pid] = tracker
	dt.knownPids[pid] = struct{}{}
	tracker.ClearSoftDirty()
	return true
}

func (dt *DirtyPageTracker) removeDeadProcesses() {
	for pid, tracker := range dt.trackers {
		if !tracker.IsAlive() {
			tracker.Close()
			delete(dt.trackers, pid)
			dt.deadPids[pid] = struct{}{}
		}
	}
}

func (dt *DirtyPageTracker) Run(duration time.Duration) {
	dt.startTime = time.Now()
	interval := time.Duration(dt.intervalMs) * time.Millisecond

	// Initialize root process tracker
	if !dt.addProcessTracker(dt.rootPid) {
		fmt.Fprintf(os.Stderr, "Failed to open root process %d\n", dt.rootPid)
		return
	}

	deadline := time.Now().Add(duration)
	sampleCount := 0

	for {
		iterStart := time.Now()

		// Check stop conditions
		select {
		case <-dt.stopCh:
			goto cleanup
		default:
		}

		if time.Now().After(deadline) {
			goto cleanup
		}

		dt.mu.Lock()

		// Discover new child processes
		if dt.trackChildren {
			descendants := dt.discoverDescendants(dt.rootPid)
			for childPid := range descendants {
				if _, known := dt.knownPids[childPid]; !known {
					if _, dead := dt.deadPids[childPid]; !dead {
						if dt.addProcessTracker(childPid) {
							fmt.Fprintf(os.Stderr, "Tracking child process: %d\n", childPid)
						}
					}
				}
			}
		}

		// Remove dead processes
		dt.removeDeadProcesses()

		// Read dirty pages from all tracked processes
		var allDirtyPages []DirtyPage
		var trackedPids []int

		for pid, tracker := range dt.trackers {
			trackedPids = append(trackedPids, pid)
			dirtyPages, err := tracker.ReadDirtyPages(dt.uniqueAddrs)
			if err == nil {
				allDirtyPages = append(allDirtyPages, dirtyPages...)
			}
			tracker.ClearSoftDirty()
		}

		elapsedMs := float64(time.Since(dt.startTime).Microseconds()) / 1000.0

		sample := DirtySample{
			TimestampMs:     elapsedMs,
			DirtyPages:      allDirtyPages,
			DeltaDirtyCount: len(allDirtyPages),
			PidsTracked:     trackedPids,
		}
		dt.samples = append(dt.samples, sample)
		sampleCount++
		dt.totalDirtyPages += len(allDirtyPages)

		dt.mu.Unlock()

		if sampleCount%10 == 0 {
			fmt.Fprintf(os.Stderr, "Sample %d: %d dirty pages, %d processes\n",
				sampleCount, len(allDirtyPages), len(trackedPids))
		}

		// Sleep for remaining time to maintain accurate interval
		elapsed := time.Since(iterStart)
		if remaining := interval - elapsed; remaining > 0 {
			time.Sleep(remaining)
		}
	}

cleanup:
	dt.mu.Lock()
	for _, tracker := range dt.trackers {
		tracker.Close()
	}
	dt.mu.Unlock()
	fmt.Fprintf(os.Stderr, "Stopped tracking (total %d samples)\n", sampleCount)
}

func (dt *DirtyPageTracker) Stop() {
	close(dt.stopCh)
}

func (dt *DirtyPageTracker) GetDirtyPattern() DirtyPattern {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	if len(dt.samples) == 0 {
		return DirtyPattern{
			Workload:      dt.workloadName,
			RootPid:       dt.rootPid,
			TrackChildren: dt.trackChildren,
			PageSize:      PageSize,
		}
	}

	durationMs := dt.samples[len(dt.samples)-1].TimestampMs

	// Calculate VMA distribution
	vmaCounts := make(map[string]int)
	vmaSizes := make(map[string]int)

	for _, sample := range dt.samples {
		for _, page := range sample.DirtyPages {
			vmaCounts[page.VMAType]++
			vmaSizes[page.VMAType] += page.Size
		}
	}

	totalDirty := 0
	for _, count := range vmaCounts {
		totalDirty += count
	}

	vmaDistribution := make(map[string]float64)
	if totalDirty > 0 {
		for vmaType, count := range vmaCounts {
			vmaDistribution[vmaType] = float64(count) / float64(totalDirty)
		}
	}

	// Calculate dirty rate timeline
	var timeline []DirtyRateEntry
	cumulative := 0
	maxProcesses := 0
	allPidsSeen := make(map[int]struct{})

	var rates []float64

	for i, sample := range dt.samples {
		cumulative += sample.DeltaDirtyCount
		var rate float64

		if i > 0 {
			deltaTime := (sample.TimestampMs - dt.samples[i-1].TimestampMs) / 1000.0
			if deltaTime > 0 {
				rate = float64(sample.DeltaDirtyCount) / deltaTime
			}
		}

		numProcs := len(sample.PidsTracked)
		if numProcs > maxProcesses {
			maxProcesses = numProcs
		}
		for _, pid := range sample.PidsTracked {
			allPidsSeen[pid] = struct{}{}
		}

		timeline = append(timeline, DirtyRateEntry{
			TimestampMs:      sample.TimestampMs,
			RatePagesPerSec:  rate,
			CumulativePages:  cumulative,
			ProcessesTracked: numProcs,
		})

		if rate > 0 {
			rates = append(rates, rate)
		}
	}

	// Calculate average and peak rates
	var avgRate, peakRate float64
	if len(rates) > 0 {
		sum := 0.0
		for _, r := range rates {
			sum += r
			if r > peakRate {
				peakRate = r
			}
		}
		avgRate = sum / float64(len(rates))
	}

	// Convert allPidsSeen to slice
	var pidList []int
	for pid := range allPidsSeen {
		pidList = append(pidList, pid)
	}

	summary := Summary{
		TotalUniquePages:    len(dt.uniqueAddrs),
		TotalDirtyEvents:    dt.totalDirtyPages,
		TotalDirtySizeBytes: dt.totalDirtyPages * PageSize,
		AvgDirtyRatePerSec:  avgRate,
		PeakDirtyRate:       peakRate,
		VMADistribution:     vmaDistribution,
		VMASizeDistribution: vmaSizes,
		SampleCount:         len(dt.samples),
		IntervalMs:          float64(dt.intervalMs),
		MaxProcessesTracked: maxProcesses,
		TotalPidsSeen:       pidList,
	}

	return DirtyPattern{
		Workload:           dt.workloadName,
		RootPid:            dt.rootPid,
		TrackChildren:      dt.trackChildren,
		TrackingDurationMs: durationMs,
		PageSize:           PageSize,
		Samples:            dt.samples,
		Summary:            summary,
		DirtyRateTimeline:  timeline,
	}
}

func main() {
	pid := flag.Int("pid", 0, "Process ID to track (required)")
	intervalMs := flag.Int("interval", 100, "Sampling interval in milliseconds")
	durationSec := flag.Float64("duration", 10, "Tracking duration in seconds")
	outputFile := flag.String("output", "", "Output JSON file (default: stdout)")
	workload := flag.String("workload", "unknown", "Workload name")
	trackChildren := flag.Bool("children", true, "Track child processes")

	flag.Parse()

	if *pid == 0 {
		fmt.Fprintln(os.Stderr, "Error: -pid is required")
		flag.Usage()
		os.Exit(1)
	}

	tracker := NewDirtyPageTracker(*pid, *intervalMs, *trackChildren, *workload)

	// Handle Ctrl+C
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Fprintln(os.Stderr, "\nReceived interrupt, stopping...")
		tracker.Stop()
	}()

	fmt.Fprintf(os.Stderr, "Tracking PID %d for %.1f seconds (interval=%dms, children=%v)\n",
		*pid, *durationSec, *intervalMs, *trackChildren)

	tracker.Run(time.Duration(*durationSec * float64(time.Second)))

	pattern := tracker.GetDirtyPattern()

	jsonData, err := json.MarshalIndent(pattern, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error encoding JSON: %v\n", err)
		os.Exit(1)
	}

	if *outputFile != "" {
		// Create directory if needed
		dir := filepath.Dir(*outputFile)
		if dir != "" && dir != "." {
			os.MkdirAll(dir, 0755)
		}

		err = os.WriteFile(*outputFile, jsonData, 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error writing file: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "Output written to %s\n", *outputFile)
	} else {
		fmt.Println(string(jsonData))
	}
}
