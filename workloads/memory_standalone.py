#!/usr/bin/env python3
"""
Memory Allocation Standalone Workload

This script runs on workload nodes and progressively allocates memory.
It is designed to be checkpointed by CRIU during execution.

Usage:
    python3 memory_standalone.py --mb-size 256 --interval 5 --max-memory-mb 8192 --duration 300

Checkpoint Protocol:
    1. Script creates 'checkpoint_ready' file when ready for checkpointing
    2. Script checks for 'checkpoint_flag' file to detect restore completion
    3. When checkpoint_flag is removed, script exits gracefully

Lazy Loading Test:
    When --check-lazy-loading is True, after restore the script will
    touch every page to trigger page faults for lazy-pages testing.
"""

import time
import os
import sys
import argparse


def create_ready_signal(working_dir: str = '.'):
    """Create checkpoint ready signal file."""
    ready_path = os.path.join(working_dir, 'checkpoint_ready')
    with open(ready_path, 'w') as f:
        f.write(f'ready:{os.getpid()}\n')
    print(f"[Memory] Checkpoint ready signal created (PID: {os.getpid()})")


def check_restore_complete(working_dir: str = '.') -> bool:
    """Check if restore is complete (checkpoint_flag removed)."""
    flag_path = os.path.join(working_dir, 'checkpoint_flag')
    return not os.path.exists(flag_path)


def trigger_lazy_page_faults(memory_blocks: list):
    """
    Touch every page in memory blocks to trigger page faults.
    This is used to test lazy-pages performance.
    """
    print("[Memory] Triggering lazy page faults...")
    start_time = time.time()
    page_count = 0

    for block in memory_blocks:
        # Touch every 4KB page
        for i in range(0, len(block), 4096):
            _ = block[i]
            page_count += 1

    duration = time.time() - start_time
    print(f"[Memory] Touched {page_count} pages in {duration:.2f}s")


def run_memory_workload(
    mb_size: int = 256,
    interval: float = 5.0,
    max_memory_mb: int = 8192,
    duration: int = 0,
    check_lazy_loading: bool = False,
    working_dir: str = '.',
    keep_running: bool = True,
):
    """
    Main memory allocation workload.

    Args:
        mb_size: Size of each memory block in MB
        interval: Interval between allocations in seconds
        max_memory_mb: Maximum total memory to allocate in MB
        duration: Duration in seconds (0 = run until max_memory reached)
        check_lazy_loading: Whether to trigger page faults after restore
        working_dir: Working directory for signal files
    """
    current_memory = 0
    memory_blocks = []
    iteration = 0

    duration_str = f"{duration}s" if duration > 0 else f"until {max_memory_mb}MB"
    print(f"[Memory] Starting memory workload")
    print(f"[Memory] Config: block_size={mb_size}MB, interval={interval}s, max={max_memory_mb}MB, duration={duration_str}")
    print(f"[Memory] Working directory: {working_dir}")
    os.makedirs(working_dir, exist_ok=True)

    # Signal ready for checkpoint
    create_ready_signal(working_dir)

    start_time = time.time()

    while True:
        # Check if restore completed (checkpoint_flag removed)
        if not keep_running and check_restore_complete(working_dir):
            elapsed = time.time() - start_time
            print("[Memory] Restore detected - checkpoint_flag removed")
            print(f"[Memory] === STATE SUMMARY (lost on restart) ===")
            print(f"[Memory]   Allocated blocks: {len(memory_blocks)}")
            print(f"[Memory]   Total memory: {current_memory} MB")
            print(f"[Memory]   Elapsed time: {elapsed:.1f}s")
            print(f"[Memory]   ALL allocated memory LOST on restart")
            print(f"[Memory] ==========================================")

            if check_lazy_loading and memory_blocks:
                trigger_lazy_page_faults(memory_blocks)

            print("[Memory] Workload complete, exiting")
            sys.exit(0)

        # Duration check
        if duration > 0:
            elapsed = time.time() - start_time
            if elapsed >= duration:
                if keep_running:
                    print(f"[Memory] Duration {duration}s reached, exiting")
                    sys.exit(0)
                time.sleep(1)
                continue

        # Check if we've reached max memory
        if current_memory >= max_memory_mb:
            time.sleep(1)
            continue

        # Allocate new memory block
        iteration += 1
        block = ' ' * (mb_size * 1024 * 1024)  # Allocate mb_size MB
        memory_blocks.append(block)
        current_memory += mb_size

        print(f"[Memory] Iteration {iteration}: allocated {mb_size}MB, total={current_memory}MB")

        time.sleep(interval)


def main():
    parser = argparse.ArgumentParser(
        description="Memory allocation workload for CRIU checkpoint testing"
    )
    parser.add_argument(
        '--mb-size',
        type=int,
        default=256,
        help='Memory block size in MB (default: 256)'
    )
    parser.add_argument(
        '--interval',
        type=float,
        default=5.0,
        help='Interval between allocations in seconds (default: 5.0)'
    )
    parser.add_argument(
        '--max-memory-mb',
        type=int,
        default=8192,
        help='Maximum total memory to allocate in MB (default: 8192)'
    )
    parser.add_argument(
        '--duration',
        type=int,
        default=0,
        help='Duration in seconds (0 = run until max memory, default: 0)'
    )
    parser.add_argument(
        '--check-lazy-loading',
        action='store_true',
        help='Trigger page faults after restore for lazy-pages testing'
    )
    parser.add_argument(
        '--working_dir',
        type=str,
        default='.',
        help='Working directory for signal files (default: current directory)'
    )
    parser.add_argument(
        '--stop-on-restore',
        action='store_true',
        help='Stop when restore is detected (checkpoint_flag removed). Default: keep running.'
    )

    args = parser.parse_args()
    args.keep_running = not args.stop_on_restore

    run_memory_workload(
        mb_size=args.mb_size,
        interval=args.interval,
        max_memory_mb=args.max_memory_mb,
        duration=args.duration,
        check_lazy_loading=args.check_lazy_loading,
        working_dir=args.working_dir,
        keep_running=args.keep_running,
    )


if __name__ == '__main__':
    main()
