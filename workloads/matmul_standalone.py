#!/usr/bin/env python3
"""
Matrix Multiplication Standalone Workload

This script performs continuous matrix multiplication operations using NumPy.
It simulates compute-intensive scientific computing workloads.

Usage:
    python3 matmul_standalone.py --matrix-size 2048 --iterations 100

Checkpoint Protocol:
    1. Creates 'checkpoint_ready' file when matrices are initialized
    2. Checks 'checkpoint_flag' to detect restore completion
    3. Exits gracefully when checkpoint_flag is removed

Scenario:
    - Scientific computing workloads
    - HPC batch jobs
    - Numerical simulations
"""

import time
import os
import sys
import argparse

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False


def create_ready_signal(working_dir: str = '.'):
    """Create checkpoint ready signal file."""
    ready_path = os.path.join(working_dir, 'checkpoint_ready')
    with open(ready_path, 'w') as f:
        f.write(f'ready:{os.getpid()}\n')
    print(f"[MatMul] Checkpoint ready signal created (PID: {os.getpid()})")


def check_restore_complete(working_dir: str = '.') -> bool:
    """Check if restore is complete (checkpoint_flag removed)."""
    flag_path = os.path.join(working_dir, 'checkpoint_flag')
    return not os.path.exists(flag_path)


def run_matmul_workload(
    matrix_size: int = 2048,
    iterations: int = 0,  # 0 = infinite
    interval: float = 1.0,
    working_dir: str = '.'
):
    """
    Main matrix multiplication workload.

    Args:
        matrix_size: Size of square matrices (NxN)
        iterations: Number of iterations (0 for infinite until checkpoint_flag removed)
        interval: Interval between iterations in seconds
        working_dir: Working directory for signal files
    """
    if not HAS_NUMPY:
        print("[MatMul] ERROR: NumPy not installed. Please install with: pip3 install numpy")
        sys.exit(1)

    print(f"[MatMul] Starting matrix multiplication workload")
    print(f"[MatMul] Config: matrix_size={matrix_size}x{matrix_size}, iterations={iterations or 'infinite'}")
    print(f"[MatMul] Working directory: {working_dir}")

    # Initialize matrices
    print(f"[MatMul] Initializing matrices...")
    np.random.seed(42)
    matrix_a = np.random.rand(matrix_size, matrix_size).astype(np.float64)
    matrix_b = np.random.rand(matrix_size, matrix_size).astype(np.float64)
    result = np.zeros((matrix_size, matrix_size), dtype=np.float64)

    memory_mb = (matrix_size * matrix_size * 8 * 3) / (1024 * 1024)
    print(f"[MatMul] Matrix memory usage: {memory_mb:.2f} MB")

    # Signal ready for checkpoint
    create_ready_signal(working_dir)

    iteration = 0
    total_flops = 0

    while True:
        # Check if restore completed
        if check_restore_complete(working_dir):
            print(f"[MatMul] Restore detected - checkpoint_flag removed")
            print(f"[MatMul] Completed {iteration} iterations, {total_flops / 1e9:.2f} GFLOPS total")
            print("[MatMul] Workload complete, exiting")
            sys.exit(0)

        # Check iteration limit
        if iterations > 0 and iteration >= iterations:
            time.sleep(1)
            continue

        iteration += 1
        start_time = time.time()

        # Perform matrix multiplication
        np.matmul(matrix_a, matrix_b, out=result)

        elapsed = time.time() - start_time
        flops = 2 * (matrix_size ** 3)  # Approximate FLOPS for matmul
        gflops = flops / elapsed / 1e9
        total_flops += flops

        print(f"[MatMul] Iteration {iteration}: {elapsed:.3f}s, {gflops:.2f} GFLOPS")

        # Accumulate result to prevent optimization
        matrix_a = (matrix_a + result * 0.0001) % 1.0

        if interval > elapsed:
            time.sleep(interval - elapsed)


def main():
    parser = argparse.ArgumentParser(
        description="Matrix multiplication workload for CRIU checkpoint testing"
    )
    parser.add_argument(
        '--matrix-size',
        type=int,
        default=2048,
        help='Size of square matrices NxN (default: 2048)'
    )
    parser.add_argument(
        '--iterations',
        type=int,
        default=0,
        help='Number of iterations, 0 for infinite (default: 0)'
    )
    parser.add_argument(
        '--interval',
        type=float,
        default=1.0,
        help='Minimum interval between iterations in seconds (default: 1.0)'
    )
    parser.add_argument(
        '--working_dir',
        type=str,
        default='.',
        help='Working directory for signal files (default: current directory)'
    )

    args = parser.parse_args()

    run_matmul_workload(
        matrix_size=args.matrix_size,
        iterations=args.iterations,
        interval=args.interval,
        working_dir=args.working_dir
    )


if __name__ == '__main__':
    main()
