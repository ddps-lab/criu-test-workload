"""
Memory Allocation Workload Wrapper

This module provides the control node wrapper for the memory allocation workload.
It handles deployment of the standalone script and provides the command interface.
"""

from typing import Dict, Any
from .base_workload import BaseWorkload, WorkloadFactory


# Inline the standalone script content for easy deployment
MEMORY_STANDALONE_SCRIPT = '''#!/usr/bin/env python3
"""
Memory Allocation Standalone Workload
Auto-generated - do not edit directly
"""

import time
import os
import sys
import argparse


def create_ready_signal(working_dir: str = '.'):
    """Create checkpoint ready signal file."""
    ready_path = os.path.join(working_dir, 'checkpoint_ready')
    with open(ready_path, 'w') as f:
        f.write(f'ready:{os.getpid()}\\n')
    print(f"[Memory] Checkpoint ready signal created (PID: {os.getpid()})")


def check_restore_complete(working_dir: str = '.') -> bool:
    """Check if restore is complete (checkpoint_flag removed)."""
    flag_path = os.path.join(working_dir, 'checkpoint_flag')
    return not os.path.exists(flag_path)


def trigger_lazy_page_faults(memory_blocks: list):
    """Touch every page in memory blocks to trigger page faults."""
    print("[Memory] Triggering lazy page faults...")
    start_time = time.time()
    page_count = 0

    for block in memory_blocks:
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
    current_memory = 0
    memory_blocks = []
    iteration = 0

    print(f"[Memory] Starting memory workload")
    print(f"[Memory] Config: block_size={mb_size}MB, interval={interval}s, max={max_memory_mb}MB")
    print(f"[Memory] Working directory: {working_dir}")

    create_ready_signal(working_dir)

    while True:
        if check_restore_complete(working_dir):
            print("[Memory] Restore detected - checkpoint_flag removed")

            if check_lazy_loading and memory_blocks:
                trigger_lazy_page_faults(memory_blocks)

            print("[Memory] Workload complete, exiting")
            sys.exit(0)

        if current_memory >= max_memory_mb:
            time.sleep(1)
            continue

        iteration += 1
        block = ' ' * (mb_size * 1024 * 1024)
        memory_blocks.append(block)
        current_memory += mb_size

        print(f"[Memory] Iteration {iteration}: allocated {mb_size}MB, total={current_memory}MB")

        time.sleep(interval)


def main():
    parser = argparse.ArgumentParser(
        description="Memory allocation workload for CRIU checkpoint testing"
    )
    parser.add_argument('--mb_size', type=int, default=256)
    parser.add_argument('--interval', type=float, default=5.0)
    parser.add_argument('--max_memory_mb', type=int, default=8192)
    parser.add_argument('--check_lazy_loading', action='store_true')
    parser.add_argument('--working_dir', type=str, default='.')

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
'''


class MemoryWorkload(BaseWorkload):
    """
    Memory allocation workload for CRIU checkpoint testing.

    This workload progressively allocates memory blocks at configurable intervals.
    It's useful for testing:
    - Memory checkpoint sizes at different usage levels
    - Pre-dump efficiency with incremental memory growth
    - Lazy-pages performance with large memory footprints
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize memory workload.

        Config options:
            mb_size: Size of each memory block in MB (default: 256)
            interval: Interval between allocations in seconds (default: 5.0)
            max_memory_mb: Maximum total memory to allocate in MB (default: 8192)
            check_lazy_loading: Trigger page faults after restore (default: False)
        """
        super().__init__(config)

        # Memory-specific configuration
        self.mb_size = config.get('mb_size', 256)
        self.interval = config.get('interval', 5.0)
        self.max_memory_mb = config.get('max_memory_mb', 8192)
        self.check_lazy_loading = config.get('check_lazy_loading', False)

    def get_standalone_script_name(self) -> str:
        """Get standalone script filename."""
        return 'memory_standalone.py'

    def get_standalone_script_content(self) -> str:
        """Get standalone script content."""
        return MEMORY_STANDALONE_SCRIPT

    def get_command(self) -> str:
        """
        Get command to start the workload.

        Returns:
            Command string with all configured parameters
        """
        cmd = f"python3 {self.get_standalone_script_name()}"
        cmd += f" --mb_size {self.mb_size}"
        cmd += f" --interval {self.interval}"
        cmd += f" --max_memory_mb {self.max_memory_mb}"
        cmd += f" --working_dir {self.working_dir}"

        if self.check_lazy_loading:
            cmd += " --check_lazy_loading"

        return cmd

    def get_dependencies(self) -> list[str]:
        """Memory workload has no external dependencies."""
        return []

    def validate_config(self) -> bool:
        """Validate memory workload configuration."""
        if self.mb_size <= 0:
            raise ValueError(f"mb_size must be positive, got {self.mb_size}")

        if self.interval <= 0:
            raise ValueError(f"interval must be positive, got {self.interval}")

        if self.max_memory_mb <= 0:
            raise ValueError(f"max_memory_mb must be positive, got {self.max_memory_mb}")

        if self.mb_size > self.max_memory_mb:
            raise ValueError(f"mb_size ({self.mb_size}) cannot exceed max_memory_mb ({self.max_memory_mb})")

        return True

    def estimate_checkpoint_size(self, elapsed_time: float) -> float:
        """
        Estimate checkpoint size based on elapsed time.

        Args:
            elapsed_time: Time since workload started in seconds

        Returns:
            Estimated memory usage in MB
        """
        iterations = int(elapsed_time / self.interval)
        estimated_memory = min(iterations * self.mb_size, self.max_memory_mb)
        return estimated_memory


# Register workload with factory
WorkloadFactory.register('memory', MemoryWorkload)
