#!/usr/bin/env python3
"""
Memory Write-Intensive Standalone Workload

Allocates a buffer and repeatedly writes to every page.
Designed to maximize dirty page count per iteration for overhead measurement.
Similar to OoH (SC22) synthetic benchmark.

Usage:
    python3 memwrite_standalone.py --buffer-mb 256 --duration 60 --working_dir /tmp/test
"""

import time
import os
import sys
import argparse
import ctypes


def create_ready_signal(working_dir):
    ready_path = os.path.join(working_dir, "checkpoint_ready")
    with open(ready_path, "w") as f:
        f.write(f"ready:{os.getpid()}\n")
    print(f"[MemWrite] Checkpoint ready signal created (PID: {os.getpid()})")


def check_restore_complete(working_dir):
    flag_path = os.path.join(working_dir, "checkpoint_flag")
    return not os.path.exists(flag_path)


def main():
    parser = argparse.ArgumentParser(description="Memory write-intensive workload")
    parser.add_argument("--buffer-mb", type=int, default=256, help="Buffer size in MB")
    parser.add_argument("--duration", type=int, default=60, help="Duration in seconds")
    parser.add_argument("--working_dir", type=str, default=".", help="Working directory")
    args = parser.parse_args()

    buffer_mb = args.buffer_mb
    duration = args.duration
    working_dir = args.working_dir
    os.makedirs(working_dir, exist_ok=True)

    num_pages = buffer_mb * 256  # 256 pages per MB
    buffer_size = num_pages * 4096

    print(f"[MemWrite] Starting write-intensive workload")
    print(f"[MemWrite] Config: buffer={buffer_mb}MB, pages={num_pages}, duration={duration}s")
    print(f"[MemWrite] Working directory: {working_dir}")

    # Allocate buffer as mutable bytearray
    print(f"[MemWrite] Allocating {buffer_mb}MB buffer...")
    buf = bytearray(buffer_size)

    # Initial touch to ensure pages are mapped
    for i in range(0, len(buf), 4096):
        buf[i] = 0

    print(f"[MemWrite] Buffer allocated and initialized")
    create_ready_signal(working_dir)

    iteration = 0
    start_time = time.time()
    metric_printed = False

    while True:
        if check_restore_complete(working_dir):
            elapsed = time.time() - start_time
            if iteration > 0 and not metric_printed:
                iter_per_sec = iteration / elapsed if elapsed > 0 else 0
                print(f"[METRIC] throughput {iter_per_sec:.2f} iter/s")
                metric_printed = True
            print(f"[MemWrite] Restore detected - exiting")
            print(f"[MemWrite] Iterations: {iteration}, Elapsed: {elapsed:.1f}s")
            sys.exit(0)

        elapsed = time.time() - start_time
        if duration > 0 and elapsed >= duration:
            if not metric_printed:
                iter_per_sec = iteration / elapsed if elapsed > 0 else 0
                print(f"[METRIC] throughput {iter_per_sec:.2f} iter/s")
                metric_printed = True
            time.sleep(1)
            continue

        # Write to every page (OoH-style synthetic)
        for i in range(0, len(buf), 4096):
            buf[i] = (iteration & 0xFF)

        iteration += 1

        if iteration % 10 == 0:
            elapsed = time.time() - start_time
            print(f"[MemWrite] Iteration {iteration}: {num_pages} pages written, elapsed={elapsed:.0f}s")


if __name__ == "__main__":
    main()
