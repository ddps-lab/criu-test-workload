#!/usr/bin/env python3
"""
Standalone Dirty Page Tracker

Track dirty pages in a running process using Linux soft-dirty mechanism.
This tool uses the same mechanism as CRIU pre-dump for accurate simulation.

Requirements:
- Linux kernel 3.11+ (soft-dirty bit support)
- CAP_SYS_ADMIN or root for /proc/[pid]/pagemap access
- Process owner or root for /proc/[pid]/clear_refs write access

Usage:
    # Track process for 30 seconds
    sudo python3 dirty_tracker.py --pid 12345 --duration 30

    # Track with custom interval
    sudo python3 dirty_tracker.py --pid 12345 --duration 30 --interval 50

    # Output to file
    sudo python3 dirty_tracker.py --pid 12345 --duration 30 --output dirty_pattern.json
"""

import argparse
import json
import os
import sys
import signal
import time
import struct
import threading
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Any, Optional, Set
from enum import Enum


class VMAType(Enum):
    """Virtual Memory Area types."""
    HEAP = "heap"
    STACK = "stack"
    ANONYMOUS = "anonymous"
    CODE = "code"
    DATA = "data"
    VDSO = "vdso"
    UNKNOWN = "unknown"


@dataclass
class VMAInfo:
    """Virtual Memory Area information."""
    start: int
    end: int
    perms: str
    offset: int
    device: str
    inode: int
    pathname: str

    @property
    def vma_type(self) -> VMAType:
        if self.pathname == '[heap]':
            return VMAType.HEAP
        elif self.pathname == '[stack]':
            return VMAType.STACK
        elif self.pathname in ('[vdso]', '[vvar]', '[vsyscall]'):
            return VMAType.VDSO
        elif self.pathname.startswith('/'):
            return VMAType.CODE if 'x' in self.perms else VMAType.DATA
        elif not self.pathname:
            return VMAType.ANONYMOUS
        return VMAType.UNKNOWN

    @property
    def is_writable(self) -> bool:
        return 'w' in self.perms


@dataclass
class DirtyPage:
    """Dirty page information."""
    addr: int
    vma_type: str
    vma_perms: str
    pathname: str
    size: int = 4096


@dataclass
class DirtySample:
    """Sample of dirty pages at a point in time."""
    timestamp_ms: float
    dirty_pages: List[DirtyPage] = field(default_factory=list)
    delta_dirty_count: int = 0
    pids_tracked: List[int] = field(default_factory=list)


class SingleProcessTracker:
    """Track dirty pages for a single process."""

    PAGE_PRESENT = 1 << 63
    PAGE_SWAPPED = 1 << 62
    SOFT_DIRTY = 1 << 55
    PAGE_SIZE = 4096
    PAGEMAP_ENTRY_SIZE = 8

    def __init__(self, pid: int):
        self.pid = pid
        self.vma_map: Dict[int, VMAInfo] = {}
        self._pagemap_fd: Optional[int] = None
        self._clear_refs_fd: Optional[int] = None
        self._is_open = False

    def open(self) -> bool:
        """Open /proc files for this process. Returns False if process doesn't exist."""
        pagemap_path = f'/proc/{self.pid}/pagemap'
        clear_refs_path = f'/proc/{self.pid}/clear_refs'

        if not os.path.exists(pagemap_path):
            return False

        try:
            self._pagemap_fd = os.open(pagemap_path, os.O_RDONLY)
            self._clear_refs_fd = os.open(clear_refs_path, os.O_WRONLY)
            self._is_open = True
            return True
        except OSError:
            self.close()
            return False

    def close(self):
        """Close /proc file handles."""
        if self._pagemap_fd is not None:
            try:
                os.close(self._pagemap_fd)
            except OSError:
                pass
            self._pagemap_fd = None

        if self._clear_refs_fd is not None:
            try:
                os.close(self._clear_refs_fd)
            except OSError:
                pass
            self._clear_refs_fd = None

        self._is_open = False

    def is_alive(self) -> bool:
        """Check if the process is still alive."""
        return os.path.exists(f'/proc/{self.pid}')

    def _parse_maps(self) -> Dict[int, VMAInfo]:
        """Parse /proc/[pid]/maps for VMA information."""
        vma_map = {}
        maps_path = f'/proc/{self.pid}/maps'

        try:
            with open(maps_path, 'r') as f:
                for line in f:
                    parts = line.split()
                    if len(parts) < 5:
                        continue

                    addr_range = parts[0].split('-')
                    start = int(addr_range[0], 16)
                    end = int(addr_range[1], 16)

                    vma = VMAInfo(
                        start=start,
                        end=end,
                        perms=parts[1],
                        offset=int(parts[2], 16),
                        device=parts[3],
                        inode=int(parts[4]),
                        pathname=parts[5] if len(parts) > 5 else ''
                    )
                    vma_map[start] = vma
        except (OSError, ValueError):
            pass

        return vma_map

    def clear_soft_dirty(self):
        """Clear soft-dirty bits for all pages."""
        if self._clear_refs_fd is None:
            return
        try:
            os.lseek(self._clear_refs_fd, 0, os.SEEK_SET)
            os.write(self._clear_refs_fd, b'4')
        except OSError:
            pass

    def read_dirty_pages(self, unique_addrs: Set[int]) -> List[DirtyPage]:
        """Read pages with soft-dirty bit set."""
        if self._pagemap_fd is None or not self._is_open:
            return []

        dirty_pages = []
        self.vma_map = self._parse_maps()

        for vma_start, vma in self.vma_map.items():
            if not vma.is_writable:
                continue

            try:
                start_page = vma.start // self.PAGE_SIZE
                num_pages = (vma.end - vma.start) // self.PAGE_SIZE
                pagemap_offset = start_page * self.PAGEMAP_ENTRY_SIZE

                os.lseek(self._pagemap_fd, pagemap_offset, os.SEEK_SET)
                data = os.read(self._pagemap_fd, num_pages * self.PAGEMAP_ENTRY_SIZE)

                actual_pages = len(data) // self.PAGEMAP_ENTRY_SIZE

                for i in range(actual_pages):
                    entry_data = data[i * self.PAGEMAP_ENTRY_SIZE:(i + 1) * self.PAGEMAP_ENTRY_SIZE]
                    if len(entry_data) < self.PAGEMAP_ENTRY_SIZE:
                        break

                    entry = struct.unpack('Q', entry_data)[0]

                    if entry & self.SOFT_DIRTY:
                        addr = vma.start + i * self.PAGE_SIZE
                        dirty_pages.append(DirtyPage(
                            addr=addr,
                            vma_type=vma.vma_type.value,
                            vma_perms=vma.perms,
                            pathname=vma.pathname
                        ))
                        unique_addrs.add(addr)

            except OSError:
                continue

        return dirty_pages


class DirtyPageTracker:
    """Track dirty pages using soft-dirty bits with child process support."""

    PAGE_SIZE = 4096

    def __init__(self, pid: int, interval_ms: int = 100, verbose: bool = False,
                 track_children: bool = True):
        self.root_pid = pid
        self.interval = interval_ms / 1000.0
        self.verbose = verbose
        self.track_children = track_children
        self.samples: List[DirtySample] = []
        self._process_trackers: Dict[int, SingleProcessTracker] = {}
        self._known_pids: Set[int] = set()
        self._dead_pids: Set[int] = set()
        self._tracking_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._start_time = 0
        self._total_dirty_pages = 0
        self._unique_dirty_addrs: Set[int] = set()
        self._lock = threading.Lock()

    def _log(self, msg: str):
        if self.verbose:
            print(f"[DirtyTracker] {msg}", file=sys.stderr)

    def _discover_all_descendants(self, pid: int) -> Set[int]:
        """Discover all descendant processes recursively."""
        descendants: Set[int] = set()
        to_check = [pid]
        checked: Set[int] = set()

        while to_check:
            current_pid = to_check.pop(0)
            if current_pid in checked:
                continue
            checked.add(current_pid)

            # Read children from /proc/{pid}/task/{pid}/children
            children_file = f'/proc/{current_pid}/task/{current_pid}/children'
            try:
                with open(children_file, 'r') as f:
                    content = f.read().strip()
                    if content:
                        children = [int(p) for p in content.split()]
                        for child in children:
                            if child not in descendants:
                                descendants.add(child)
                                to_check.append(child)
            except (OSError, ValueError):
                continue

        return descendants

    def _add_process_tracker(self, pid: int) -> bool:
        """Add a tracker for a process. Returns True if successful."""
        if pid in self._process_trackers or pid in self._dead_pids:
            return False

        tracker = SingleProcessTracker(pid)
        if tracker.open():
            self._process_trackers[pid] = tracker
            self._known_pids.add(pid)
            tracker.clear_soft_dirty()
            return True
        else:
            self._dead_pids.add(pid)
            return False

    def _remove_dead_processes(self):
        """Remove trackers for processes that have exited."""
        dead_pids = []
        for pid, tracker in self._process_trackers.items():
            if not tracker.is_alive():
                tracker.close()
                dead_pids.append(pid)
                self._dead_pids.add(pid)

        for pid in dead_pids:
            del self._process_trackers[pid]
            if dead_pids:
                self._log(f"Process {pid} exited, removed from tracking")

    def _track_loop(self):
        self._log(f"Starting tracking for PID {self.root_pid} (track_children={self.track_children})")
        self._start_time = time.time()

        # Initialize root process tracker
        if not self._add_process_tracker(self.root_pid):
            self._log(f"Failed to open root process {self.root_pid}")
            return

        sample_count = 0

        while not self._stop_event.is_set():
            self._stop_event.wait(self.interval)
            if self._stop_event.is_set():
                break

            with self._lock:
                # Discover new child processes
                if self.track_children:
                    current_descendants = self._discover_all_descendants(self.root_pid)
                    new_pids = current_descendants - self._known_pids - self._dead_pids

                    for new_pid in new_pids:
                        if self._add_process_tracker(new_pid):
                            self._log(f"Discovered and tracking child process: {new_pid}")

                # Remove dead processes
                self._remove_dead_processes()

                # Read dirty pages from all tracked processes
                all_dirty_pages: List[DirtyPage] = []
                tracked_pids = list(self._process_trackers.keys())

                for pid, tracker in list(self._process_trackers.items()):
                    dirty_pages = tracker.read_dirty_pages(self._unique_dirty_addrs)
                    all_dirty_pages.extend(dirty_pages)
                    tracker.clear_soft_dirty()

                delta_dirty = len(all_dirty_pages)
                elapsed_ms = (time.time() - self._start_time) * 1000

                sample = DirtySample(
                    timestamp_ms=elapsed_ms,
                    dirty_pages=all_dirty_pages,
                    delta_dirty_count=delta_dirty,
                    pids_tracked=tracked_pids
                )
                self.samples.append(sample)
                sample_count += 1
                self._total_dirty_pages += delta_dirty

            if sample_count % 10 == 0:
                rate = delta_dirty / self.interval if self.interval > 0 else 0
                num_procs = len(self._process_trackers)
                self._log(f"Sample {sample_count}: {delta_dirty} dirty, rate={rate:.1f}/sec, {num_procs} processes")

        # Close all trackers
        for tracker in self._process_trackers.values():
            tracker.close()
        self._process_trackers.clear()
        self._log(f"Stopped tracking ({sample_count} samples)")

    def start(self):
        if self._tracking_thread is not None:
            raise RuntimeError("Already tracking")

        self._stop_event.clear()
        self.samples = []
        self._unique_dirty_addrs = set()
        self._total_dirty_pages = 0
        self._known_pids = set()
        self._dead_pids = set()
        self._process_trackers = {}

        self._tracking_thread = threading.Thread(target=self._track_loop, daemon=True)
        self._tracking_thread.start()

    def stop(self):
        if self._tracking_thread is None:
            return
        self._stop_event.set()
        self._tracking_thread.join(timeout=5.0)
        self._tracking_thread = None

    def get_results(self, workload_name: str = "unknown") -> Dict[str, Any]:
        if not self.samples:
            return {
                'workload': workload_name,
                'root_pid': self.root_pid,
                'track_children': self.track_children,
                'tracking_duration_ms': 0,
                'samples': [],
                'summary': {}
            }

        duration_ms = self.samples[-1].timestamp_ms

        # VMA distribution
        vma_counts: Dict[str, int] = {}
        vma_sizes: Dict[str, int] = {}

        for sample in self.samples:
            for page in sample.dirty_pages:
                vma_counts[page.vma_type] = vma_counts.get(page.vma_type, 0) + 1
                vma_sizes[page.vma_type] = vma_sizes.get(page.vma_type, 0) + page.size

        total_dirty = sum(vma_counts.values())
        vma_distribution = {k: v / total_dirty for k, v in vma_counts.items()} if total_dirty > 0 else {}

        # Rate timeline
        dirty_rate_timeline = []
        cumulative = 0
        max_processes = 0
        all_pids_seen: Set[int] = set()

        for i, sample in enumerate(self.samples):
            cumulative += sample.delta_dirty_count
            if i > 0:
                delta_time = (sample.timestamp_ms - self.samples[i - 1].timestamp_ms) / 1000.0
                rate = sample.delta_dirty_count / delta_time if delta_time > 0 else 0
            else:
                rate = 0

            num_procs = len(sample.pids_tracked)
            if num_procs > max_processes:
                max_processes = num_procs
            all_pids_seen.update(sample.pids_tracked)

            dirty_rate_timeline.append({
                'timestamp_ms': sample.timestamp_ms,
                'rate_pages_per_sec': rate,
                'cumulative_pages': cumulative,
                'processes_tracked': num_procs
            })

        rates = [e['rate_pages_per_sec'] for e in dirty_rate_timeline if e['rate_pages_per_sec'] > 0]
        avg_rate = sum(rates) / len(rates) if rates else 0
        peak_rate = max(rates) if rates else 0

        # Convert samples
        samples_data = []
        for sample in self.samples:
            samples_data.append({
                'timestamp_ms': sample.timestamp_ms,
                'dirty_pages': [
                    {
                        'addr': hex(p.addr),
                        'vma_type': p.vma_type,
                        'vma_perms': p.vma_perms,
                        'pathname': p.pathname,
                        'size': p.size
                    }
                    for p in sample.dirty_pages
                ],
                'delta_dirty_count': sample.delta_dirty_count,
                'pids_tracked': sample.pids_tracked
            })

        return {
            'workload': workload_name,
            'root_pid': self.root_pid,
            'track_children': self.track_children,
            'tracking_duration_ms': duration_ms,
            'page_size': self.PAGE_SIZE,
            'samples': samples_data,
            'summary': {
                'total_unique_pages': len(self._unique_dirty_addrs),
                'total_dirty_events': self._total_dirty_pages,
                'total_dirty_size_bytes': self._total_dirty_pages * self.PAGE_SIZE,
                'avg_dirty_rate_per_sec': avg_rate,
                'peak_dirty_rate': peak_rate,
                'vma_distribution': vma_distribution,
                'vma_size_distribution': vma_sizes,
                'sample_count': len(self.samples),
                'interval_ms': self.interval * 1000,
                'max_processes_tracked': max_processes,
                'total_pids_seen': sorted(all_pids_seen)
            },
            'dirty_rate_timeline': dirty_rate_timeline
        }


def main():
    parser = argparse.ArgumentParser(
        description='Track dirty pages in a process using soft-dirty bits',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Track process for 30 seconds
  sudo python3 dirty_tracker.py --pid 12345 --duration 30

  # Track with 50ms interval, output to file
  sudo python3 dirty_tracker.py --pid 12345 --duration 30 --interval 50 --output dirty.json

  # Verbose output
  sudo python3 dirty_tracker.py --pid 12345 --duration 10 -v
"""
    )

    parser.add_argument('--pid', '-p', type=int, required=True,
                        help='Process ID to track')
    parser.add_argument('--duration', '-d', type=float, default=None,
                        help='Tracking duration in seconds (if not specified, runs until SIGTERM/SIGINT)')
    parser.add_argument('--interval', '-i', type=int, default=100,
                        help='Sampling interval in milliseconds (default: 100)')
    parser.add_argument('--output', '-o', type=str, default=None,
                        help='Output JSON file (default: stdout)')
    parser.add_argument('--workload', '-w', type=str, default='unknown',
                        help='Workload name for output')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Verbose output')
    parser.add_argument('--summary', '-s', action='store_true',
                        help='Print summary to stderr')
    parser.add_argument('--no-track-children', dest='track_children',
                        action='store_false', default=True,
                        help='Disable tracking of child processes (default: enabled)')

    args = parser.parse_args()

    # Check if running as root
    if os.geteuid() != 0:
        print("Warning: This tool typically requires root privileges to access /proc/[pid]/pagemap",
              file=sys.stderr)

    # Check if process exists
    if not os.path.exists(f'/proc/{args.pid}'):
        print(f"Error: Process {args.pid} not found", file=sys.stderr)
        sys.exit(1)

    tracker = DirtyPageTracker(args.pid, args.interval, args.verbose, args.track_children)

    # Flag for graceful shutdown
    stop_requested = threading.Event()

    # Handle Ctrl+C and SIGTERM
    def signal_handler(sig, frame):
        print("\nSignal received, stopping tracker...", file=sys.stderr)
        stop_requested.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    child_mode = "with children" if args.track_children else "single process"
    if args.duration:
        print(f"Tracking PID {args.pid} for {args.duration}s (interval: {args.interval}ms, {child_mode})...",
              file=sys.stderr)
    else:
        print(f"Tracking PID {args.pid} until signal (interval: {args.interval}ms, {child_mode})...",
              file=sys.stderr)

    tracker.start()

    try:
        if args.duration:
            # Fixed duration mode
            start_time = time.time()
            while time.time() - start_time < args.duration:
                if stop_requested.is_set():
                    break
                time.sleep(0.1)
        else:
            # Run until signal
            while not stop_requested.is_set():
                time.sleep(0.1)
    finally:
        tracker.stop()

    results = tracker.get_results(args.workload)

    # Output results
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"Results written to {args.output}", file=sys.stderr)
    else:
        print(json.dumps(results, indent=2))

    # Print summary
    if args.summary or args.verbose:
        summary = results['summary']
        print("\n=== Dirty Page Tracking Summary ===", file=sys.stderr)
        print(f"  Root PID: {args.pid}", file=sys.stderr)
        print(f"  Track children: {results['track_children']}", file=sys.stderr)
        print(f"  Duration: {results['tracking_duration_ms']:.1f} ms", file=sys.stderr)
        print(f"  Samples: {summary['sample_count']}", file=sys.stderr)
        print(f"  Max processes tracked: {summary.get('max_processes_tracked', 1)}", file=sys.stderr)
        total_pids = summary.get('total_pids_seen', [args.pid])
        print(f"  Total PIDs seen: {len(total_pids)} {total_pids}", file=sys.stderr)
        print(f"  Unique dirty pages: {summary['total_unique_pages']}", file=sys.stderr)
        print(f"  Total dirty events: {summary['total_dirty_events']}", file=sys.stderr)
        print(f"  Total dirty size: {summary['total_dirty_size_bytes'] / (1024*1024):.2f} MB", file=sys.stderr)
        print(f"  Avg dirty rate: {summary['avg_dirty_rate_per_sec']:.1f} pages/sec", file=sys.stderr)
        print(f"  Peak dirty rate: {summary['peak_dirty_rate']:.1f} pages/sec", file=sys.stderr)
        print(f"  VMA distribution:", file=sys.stderr)
        for vma_type, pct in summary.get('vma_distribution', {}).items():
            print(f"    {vma_type}: {pct*100:.1f}%", file=sys.stderr)


if __name__ == '__main__':
    main()
