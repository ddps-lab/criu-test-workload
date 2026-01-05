#!/usr/bin/env python3
"""
Memory Allocation Standalone Workload

This script runs on workload nodes and progressively allocates memory.
It is designed to be checkpointed by CRIU during execution.

Usage:
    python3 memory_standalone.py --mb_size 256 --interval 5 --max_memory_mb 8192

Checkpoint Protocol:
    1. Script creates 'checkpoint_ready' file when ready for checkpointing
    2. Script checks for 'checkpoint_flag' file to detect restore completion
    3. When checkpoint_flag is removed, script exits gracefully

Lazy Loading Test:
    When --check_lazy_loading is True, after restore the script will
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
    check_lazy_loading: bool = False,
    working_dir: str = '.'
):
    """
    Main memory allocation workload.

    Args:
        mb_size: Size of each memory block in MB
        interval: Interval between allocations in seconds
        max_memory_mb: Maximum total memory to allocate in MB
        check_lazy_loading: Whether to trigger page faults after restore
        working_dir: Working directory for signal files
    """
    current_memory = 0
    memory_blocks = []
    iteration = 0

    print(f"[Memory] Starting memory workload")
    print(f"[Memory] Config: block_size={mb_size}MB, interval={interval}s, max={max_memory_mb}MB")
    print(f"[Memory] Working directory: {working_dir}")

    # Signal ready for checkpoint
    create_ready_signal(working_dir)

    while True:
        # Check if restore completed (checkpoint_flag removed)
        if check_restore_complete(working_dir):
            print("[Memory] Restore detected - checkpoint_flag removed")

            if check_lazy_loading and memory_blocks:
                trigger_lazy_page_faults(memory_blocks)

            print("[Memory] Workload complete, exiting")
            sys.exit(0)

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
        '--mb_size',
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
        '--max_memory_mb',
        type=int,
        default=8192,
        help='Maximum total memory to allocate in MB (default: 8192)'
    )
    parser.add_argument(
        '--check_lazy_loading',
        action='store_true',
        help='Trigger page faults after restore for lazy-pages testing'
    )
    parser.add_argument(
        '--working_dir',
        type=str,
        default='.',
        help='Working directory for signal files (default: current directory)'
    )

    args = parser.parse_args()

    run_memory_workload(
        mb_size=args.mb_size,
        interval=args.interval,
        max_memory_mb=args.max_memory_mb,
        check_lazy_loading=args.check_lazy_loading,
        working_dir=args.working_dir
    )


if __name__ == '__main__':
    main()
