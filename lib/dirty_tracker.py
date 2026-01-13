"""
Dirty Page Tracker using soft-dirty bits.

Tracks dirty pages in a running process using Linux soft-dirty mechanism.
This uses the same mechanism as CRIU pre-dump for accurate simulation.

Requirements:
- Linux kernel 3.11+ (soft-dirty bit support)
- CAP_SYS_ADMIN or root for /proc/[pid]/pagemap access
- Process owner or root for /proc/[pid]/clear_refs write access

Usage:
    tracker = DirtyPageTracker(pid, interval_ms=100)
    tracker.start()
    # ... wait for workload to run ...
    tracker.stop()
    pattern = tracker.get_dirty_pattern()
"""

import os
import struct
import threading
import time
import json
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Any, Optional, Tuple, Set
from pathlib import Path
from enum import Enum
import logging

logger = logging.getLogger(__name__)


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
    """Virtual Memory Area information from /proc/[pid]/maps."""
    start: int
    end: int
    perms: str  # e.g., 'rw-p'
    offset: int
    device: str
    inode: int
    pathname: str

    @property
    def vma_type(self) -> VMAType:
        """Classify VMA type based on pathname and permissions."""
        if self.pathname == '[heap]':
            return VMAType.HEAP
        elif self.pathname == '[stack]':
            return VMAType.STACK
        elif self.pathname in ('[vdso]', '[vvar]', '[vsyscall]'):
            return VMAType.VDSO
        elif self.pathname.startswith('/'):
            if 'x' in self.perms:
                return VMAType.CODE
            else:
                return VMAType.DATA
        elif not self.pathname:
            return VMAType.ANONYMOUS
        else:
            return VMAType.UNKNOWN

    @property
    def size(self) -> int:
        """Size of VMA in bytes."""
        return self.end - self.start

    @property
    def is_writable(self) -> bool:
        """Check if VMA is writable."""
        return 'w' in self.perms


@dataclass
class DirtyPage:
    """Information about a dirty page."""
    addr: int
    vma_type: str
    vma_perms: str
    pathname: str
    size: int = 4096  # Page size


@dataclass
class DirtySample:
    """A single dirty page sample at a point in time."""
    timestamp_ms: float
    dirty_pages: List[DirtyPage] = field(default_factory=list)
    delta_dirty_count: int = 0  # New dirty pages since last clear
    pids_tracked: List[int] = field(default_factory=list)  # PIDs tracked in this sample


@dataclass
class DirtyPattern:
    """Aggregated dirty page pattern for simulation."""
    workload: str
    pid: int  # Root PID
    tracking_duration_ms: float
    page_size: int = 4096
    track_children: bool = True
    samples: List[DirtySample] = field(default_factory=list)
    summary: Dict[str, Any] = field(default_factory=dict)
    dirty_rate_timeline: List[Dict[str, Any]] = field(default_factory=list)


class SingleProcessTracker:
    """
    Track dirty pages for a single process.

    This is an internal helper class used by DirtyPageTracker to manage
    tracking of individual processes (root or child).
    """

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
            logger.debug(f"Opened proc files for PID {self.pid}")
            return True
        except OSError as e:
            logger.debug(f"Failed to open proc files for PID {self.pid}: {e}")
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
        except (OSError, ValueError) as e:
            logger.debug(f"Error parsing maps for PID {self.pid}: {e}")

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
    """
    Track dirty pages using soft-dirty bits with child process support.

    Uses the same mechanism as CRIU pre-dump:
    1. Clear soft-dirty bits via /proc/[pid]/clear_refs
    2. Wait for interval
    3. Read dirty pages via /proc/[pid]/pagemap
    4. Repeat

    When track_children=True (default), automatically discovers and tracks
    child processes forked by the root process.
    """

    PAGE_SIZE = 4096

    def __init__(self, pid: int, interval_ms: int = 100, track_children: bool = True):
        """
        Initialize dirty page tracker.

        Args:
            pid: Process ID to track (root process)
            interval_ms: Sampling interval in milliseconds
            track_children: If True, automatically track child processes
        """
        self.root_pid = pid
        self.interval = interval_ms / 1000.0
        self.track_children = track_children
        self.samples: List[DirtySample] = []

        # Process management
        self._process_trackers: Dict[int, SingleProcessTracker] = {}
        self._known_pids: Set[int] = set()
        self._dead_pids: Set[int] = set()

        # Threading
        self._tracking_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._start_time: float = 0
        self._lock = threading.Lock()

        # Statistics
        self._total_dirty_pages: int = 0
        self._unique_dirty_addrs: Set[int] = set()

    # Backward compatibility property
    @property
    def pid(self) -> int:
        """Return root PID for backward compatibility."""
        return self.root_pid

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
            logger.debug(f"Added tracker for PID {pid}")
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
            logger.debug(f"Removed tracker for exited PID {pid}")

    def _track_loop(self):
        """Background tracking loop."""
        logger.info(f"Starting dirty page tracking for PID {self.root_pid} (track_children={self.track_children})")
        self._start_time = time.time()

        # Initialize root process tracker
        if not self._add_process_tracker(self.root_pid):
            logger.error(f"Failed to open root process {self.root_pid}")
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
                            logger.info(f"Discovered and tracking child process: {new_pid}")

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
                logger.debug(f"Sample {sample_count}: {delta_dirty} dirty, rate={rate:.1f}/sec, {num_procs} processes")

        # Close all trackers
        for tracker in self._process_trackers.values():
            tracker.close()
        self._process_trackers.clear()
        logger.info(f"Stopped dirty page tracking for PID {self.root_pid} ({sample_count} samples)")

    def start(self):
        """Start background dirty page tracking."""
        if self._tracking_thread is not None:
            raise RuntimeError("Tracking already started")

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
        """Stop tracking and wait for thread to finish."""
        if self._tracking_thread is None:
            return

        self._stop_event.set()
        self._tracking_thread.join(timeout=5.0)
        self._tracking_thread = None

    def get_dirty_pattern(self, workload_name: str = "unknown") -> DirtyPattern:
        """
        Return collected dirty page patterns for simulation.

        Args:
            workload_name: Name of the workload being tracked

        Returns:
            DirtyPattern with all collected data and statistics
        """
        if not self.samples:
            return DirtyPattern(
                workload=workload_name,
                pid=self.root_pid,
                tracking_duration_ms=0,
                track_children=self.track_children
            )

        # Calculate duration
        duration_ms = self.samples[-1].timestamp_ms if self.samples else 0

        # Calculate VMA distribution
        vma_counts: Dict[str, int] = {}
        vma_sizes: Dict[str, int] = {}

        for sample in self.samples:
            for page in sample.dirty_pages:
                vma_type = page.vma_type
                vma_counts[vma_type] = vma_counts.get(vma_type, 0) + 1
                vma_sizes[vma_type] = vma_sizes.get(vma_type, 0) + page.size

        total_dirty = sum(vma_counts.values())
        vma_distribution = {k: v / total_dirty for k, v in vma_counts.items()} if total_dirty > 0 else {}

        # Calculate dirty rate timeline and track process counts
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

        # Calculate average and peak rates
        rates = [entry['rate_pages_per_sec'] for entry in dirty_rate_timeline if entry['rate_pages_per_sec'] > 0]
        avg_rate = sum(rates) / len(rates) if rates else 0
        peak_rate = max(rates) if rates else 0

        # Build summary with child process info
        summary = {
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
        }

        return DirtyPattern(
            workload=workload_name,
            pid=self.root_pid,
            tracking_duration_ms=duration_ms,
            page_size=self.PAGE_SIZE,
            track_children=self.track_children,
            samples=self.samples,
            summary=summary,
            dirty_rate_timeline=dirty_rate_timeline
        )

    def export_to_json(self, output_file: str, workload_name: str = "unknown"):
        """
        Export dirty pattern to JSON file.

        Args:
            output_file: Output file path
            workload_name: Name of the workload
        """
        pattern = self.get_dirty_pattern(workload_name)

        # Convert to dict (handle dataclasses)
        def to_dict(obj):
            if hasattr(obj, '__dataclass_fields__'):
                return {k: to_dict(v) for k, v in asdict(obj).items()}
            elif isinstance(obj, list):
                return [to_dict(item) for item in obj]
            elif isinstance(obj, dict):
                return {k: to_dict(v) for k, v in obj.items()}
            elif isinstance(obj, Enum):
                return obj.value
            else:
                return obj

        output_data = to_dict(pattern)

        # Convert addresses to hex strings for readability
        for sample in output_data.get('samples', []):
            for page in sample.get('dirty_pages', []):
                if isinstance(page.get('addr'), int):
                    page['addr'] = hex(page['addr'])

        with open(output_file, 'w') as f:
            json.dump(output_data, f, indent=2)

        logger.info(f"Exported dirty pattern to {output_file}")


def track_process(pid: int, duration_sec: float, interval_ms: int = 100,
                  workload_name: str = "unknown", track_children: bool = True) -> DirtyPattern:
    """
    Convenience function to track a process for a specified duration.

    Args:
        pid: Process ID to track
        duration_sec: How long to track (seconds)
        interval_ms: Sampling interval (milliseconds)
        workload_name: Name of the workload
        track_children: If True, automatically track child processes (default: True)

    Returns:
        DirtyPattern with collected data
    """
    tracker = DirtyPageTracker(pid, interval_ms, track_children)
    tracker.start()

    try:
        time.sleep(duration_sec)
    finally:
        tracker.stop()

    return tracker.get_dirty_pattern(workload_name)
