#!/usr/bin/env python3
"""
7zip Compression Standalone Workload (CRIU Checkpoint with Process Tree)

This script runs 7z compression for CRIU checkpoint testing.
CRIU checkpoints this wrapper script, and with --tree option, the 7z
child process is also checkpointed together.

Uses real file compression (not 7z b benchmark) for stable long-running
operation suitable for CRIU checkpoint.

Usage:
    python3 sevenzip_standalone.py --compression-level 9 --input-size-mb 256 --duration 300

Checkpoint Protocol:
    1. Generates random data file
    2. Starts 7z compression as child process
    3. Creates 'checkpoint_ready' file with THIS script's PID (wrapper)
    4. CRIU with --tree option checkpoints: wrapper + 7z
    5. After restore, both processes resume together

Important:
    - CRIU checkpoints THIS script's PID with --tree option
    - 7z is automatically included as child process
    - Between compression runs, 7z child may not exist (wrapper always alive)

Dirty page pattern:
    - Dictionary + hash table (LZMA): working set proportional to dictionary size
    - Sliding window pattern (different from matmul's matrix pattern)
    - Buffer I/O: read/write buffers create periodic dirty bursts

Scenario:
    - Compression workloads (HeatSnap comparison)
    - Batch processing pipelines
    - Data archival jobs
"""

import time
import os
import sys
import argparse
import subprocess
import signal
import random


def create_ready_signal(working_dir: str, wrapper_pid: int, child_pid: int):
    """Create checkpoint ready signal file with wrapper PID."""
    ready_path = os.path.join(working_dir, 'checkpoint_ready')
    with open(ready_path, 'w') as f:
        f.write(f'ready:{wrapper_pid}\n')
    print(f"[7zip] Checkpoint ready signal created")
    print(f"[7zip] Wrapper PID: {wrapper_pid} (checkpoint target)")
    print(f"[7zip] 7z PID: {child_pid} (child, included via --tree)")


def check_restore_complete(working_dir: str) -> bool:
    """Check if restore is complete (checkpoint_flag removed)."""
    flag_path = os.path.join(working_dir, 'checkpoint_flag')
    return not os.path.exists(flag_path)


def check_7z_installed() -> bool:
    """Check if 7z is installed."""
    try:
        result = subprocess.run(['which', '7z'], capture_output=True)
        return result.returncode == 0
    except:
        return False


def generate_input_file(filepath: str, size_mb: int, seed: int = 42):
    """Generate a deterministic data file for compression.

    Uses numpy for fast generation with seed-based reproducibility.
    Mix of random and semi-structured data for realistic compression ratio.
    """
    print(f"[7zip] Generating {size_mb} MB input file (seed={seed})...")

    try:
        import numpy as np
        rng = np.random.RandomState(seed)
        chunk_size = 1024 * 1024  # 1MB

        with open(filepath, 'wb') as f:
            for i in range(size_mb):
                if i % 2 == 0:
                    # Random data (low compressibility)
                    data = rng.bytes(chunk_size)
                else:
                    # Repeated patterns (high compressibility)
                    pattern = rng.bytes(256)
                    data = pattern * (chunk_size // 256)
                f.write(data)
    except ImportError:
        # Fallback to stdlib random (slower but no numpy dependency)
        random.seed(seed)
        chunk_size = 1024 * 1024

        with open(filepath, 'wb') as f:
            for i in range(size_mb):
                if i % 2 == 0:
                    data = random.randbytes(chunk_size)
                else:
                    pattern = random.randbytes(256)
                    data = pattern * (chunk_size // 256)
                f.write(data)

    actual_mb = os.path.getsize(filepath) / (1024 * 1024)
    print(f"[7zip] Input file created: {filepath} ({actual_mb:.1f} MB)")
    return filepath


def start_compression(input_file: str, output_file: str,
                      compression_level: int, threads: int) -> subprocess.Popen:
    """Start 7z compression process."""
    # Remove existing output to avoid prompts
    if os.path.exists(output_file):
        os.remove(output_file)

    cmd = [
        '7z', 'a',
        f'-mx{compression_level}',
        f'-mmt{threads}',
        '-y',  # Assume yes on all queries
        output_file,
        input_file,
    ]

    print(f"[7zip] Starting compression: {' '.join(cmd)}")

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        # preexec_fn=os.setsid,  # removed: tracker needs child visibility
    )
    return process


def run_sevenzip_workload(
    compression_level: int = 9,
    threads: int = 1,
    input_size_mb: int = 256,
    seed: int = 42,
    duration: int = 0,
    working_dir: str = '.',
    keep_running: bool = False,
):
    """
    Main 7zip compression workload.

    Args:
        compression_level: 1-9 (9 = ultra compression)
        threads: Number of compression threads
        input_size_mb: Size of input file in MB
        seed: Random seed for input data generation
        duration: Duration in seconds (0 = one compression cycle)
        working_dir: Working directory for signal files
    """
    if not check_7z_installed():
        print("[7zip] ERROR: 7z not found. Install: sudo apt install p7zip-full")
        sys.exit(1)

    duration_str = f"{duration}s" if duration > 0 else "single cycle"
    print(f"[7zip] Starting 7zip compression workload")
    print(f"[7zip] Config: level={compression_level}, threads={threads}, "
          f"input={input_size_mb}MB, seed={seed}, duration={duration_str}")
    print(f"[7zip] Working directory: {working_dir}")
    os.makedirs(working_dir, exist_ok=True)

    # Create output directory
    output_dir = os.path.join(working_dir, '7zip_output')
    os.makedirs(output_dir, exist_ok=True)

    # Generate input file
    input_file = os.path.join(output_dir, 'input.dat')
    generate_input_file(input_file, input_size_mb, seed)

    output_file = os.path.join(output_dir, 'output.7z')

    # Start first compression
    z_process = start_compression(input_file, output_file, compression_level, threads)
    z_pid = z_process.pid

    # Brief wait for 7z to start
    time.sleep(0.2)

    # Check if 7z failed immediately (non-zero exit = error)
    ret = z_process.poll()
    if ret is not None and ret != 0:
        stderr = z_process.stderr.read().decode() if z_process.stderr else ''
        print(f"[7zip] ERROR: 7z failed (exit={ret}): {stderr}")
        sys.exit(1)

    # Signal ready (7z may already be running or finished for small files — both OK)
    wrapper_pid = os.getpid()
    create_ready_signal(working_dir, wrapper_pid, z_pid)

    print(f"[7zip]")
    print(f"[7zip] ====== READY FOR CHECKPOINT ======")
    print(f"[7zip] Wrapper PID: {wrapper_pid} (checkpoint this)")
    print(f"[7zip] 7z PID: {z_pid} (child process)")
    print(f"[7zip] To checkpoint: sudo criu dump -t {wrapper_pid} --tree -D <dir> --shell-job")
    print(f"[7zip] ===================================")
    print(f"[7zip]")

    start_time = time.time()
    last_report_time = start_time
    compression_cycles = 0

    try:
        while True:
            # Check restore
            if not keep_running and check_restore_complete(working_dir):
                elapsed = time.time() - start_time
                print(f"[7zip] Restore detected - checkpoint_flag removed")

                # Check if 7z is still running after restore
                z_running = z_process.poll() is None
                output_size = 0
                if os.path.exists(output_file):
                    output_size = os.path.getsize(output_file) / (1024 * 1024)

                print(f"[7zip] === STATE SUMMARY (lost on restart) ===")
                print(f"[7zip]   Compression cycles completed: {compression_cycles}")
                print(f"[7zip]   7z process running: {z_running}")
                print(f"[7zip]   Output size: {output_size:.1f} MB")
                print(f"[7zip]   Elapsed time: {elapsed:.1f}s")
                print(f"[7zip]   ALL compression state LOST on restart")
                print(f"[7zip] ==========================================")
                break

            # Check if 7z finished current cycle
            if z_process.poll() is not None:
                compression_cycles += 1
                exit_code = z_process.returncode

                if os.path.exists(output_file):
                    output_size = os.path.getsize(output_file) / (1024 * 1024)
                    print(f"[7zip] Compression cycle {compression_cycles} done "
                          f"(exit={exit_code}, output={output_size:.1f} MB)")
                else:
                    print(f"[7zip] Compression cycle {compression_cycles} done (exit={exit_code})")

                # Check if we should continue
                elapsed = time.time() - start_time
                if duration > 0 and elapsed < duration:
                    # Start another compression cycle
                    if os.path.exists(output_file):
                        os.remove(output_file)
                    z_process = start_compression(input_file, output_file,
                                                  compression_level, threads)
                    z_pid = z_process.pid
                    print(f"[7zip] Starting cycle {compression_cycles + 1} (7z PID: {z_pid})")
                else:
                    if keep_running:
                        elapsed = time.time() - start_time
                        print(f"[7zip] Compression done, exiting (cycles={compression_cycles}, elapsed={elapsed:.1f}s)")
                        break
                    # Duration reached or single cycle mode, wait for checkpoint
                    print(f"[7zip] Waiting for checkpoint_flag removal...")
                    while not check_restore_complete(working_dir):
                        time.sleep(1)
                    elapsed = time.time() - start_time
                    print(f"[7zip] Restore detected")
                    print(f"[7zip] === STATE SUMMARY ===")
                    print(f"[7zip]   Cycles: {compression_cycles}, elapsed={elapsed:.1f}s")
                    print(f"[7zip] =========================")
                    break

            # Progress report
            current_time = time.time()
            if current_time - last_report_time >= 5.0:
                elapsed = current_time - start_time
                z_running = z_process.poll() is None
                output_size = 0
                if os.path.exists(output_file):
                    output_size = os.path.getsize(output_file) / (1024 * 1024)
                remaining = f", remaining={duration - elapsed:.0f}s" if duration > 0 else ""
                print(f"[7zip] Cycle {compression_cycles + 1}: 7z={'running' if z_running else 'done'}, "
                      f"output={output_size:.1f}MB, elapsed={elapsed:.0f}s{remaining}")
                last_report_time = current_time

            time.sleep(1)

    except KeyboardInterrupt:
        print(f"[7zip] Interrupted")

    finally:
        # Clean shutdown
        if z_process.poll() is None:
            print(f"[7zip] Stopping 7z...")
            try:
                os.killpg(os.getpgid(z_process.pid), signal.SIGTERM)
                z_process.wait(timeout=5)
            except:
                z_process.kill()

    sys.exit(0)


def main():
    parser = argparse.ArgumentParser(
        description="7zip compression workload for CRIU checkpoint testing"
    )
    parser.add_argument(
        '--compression-level',
        type=int,
        default=9,
        choices=range(1, 10),
        help='Compression level 1-9 (default: 9, ultra)'
    )
    parser.add_argument(
        '--threads',
        type=int,
        default=1,
        help='Number of compression threads (default: 1)'
    )
    parser.add_argument(
        '--input-size-mb',
        type=int,
        default=256,
        help='Size of input file in MB (default: 256)'
    )
    parser.add_argument(
        '--seed',
        type=int,
        default=42,
        help='Random seed for input data generation (default: 42)'
    )
    parser.add_argument(
        '--duration',
        type=int,
        default=0,
        help='Duration in seconds (0 = single compression cycle, default: 0)'
    )
    parser.add_argument(
        '--working_dir',
        type=str,
        default='.',
        help='Working directory for signal files'
    )
    parser.add_argument(
        '--keep-running',
        action='store_true',
        help='Keep running after restore (ignore checkpoint_flag removal)'
    )

    args = parser.parse_args()

    run_sevenzip_workload(
        compression_level=args.compression_level,
        threads=args.threads,
        input_size_mb=args.input_size_mb,
        seed=args.seed,
        duration=args.duration,
        working_dir=args.working_dir,
        keep_running=args.keep_running,
    )


if __name__ == '__main__':
    main()
