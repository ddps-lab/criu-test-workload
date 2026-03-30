#!/usr/bin/env python3
"""
Matrix Computation Standalone Workload (Power Iteration Eigenvalue Solver)

This script performs iterative eigenvalue computation using the Power Iteration
method. It represents long-running HPC/scientific computing workloads where
convergence progress is accumulated in memory and lost on restart.

Usage:
    python3 matmul_standalone.py --matrix-size 2048 --duration 3600

Checkpoint Protocol:
    1. Creates 'checkpoint_ready' file when matrix is initialized
    2. Checks 'checkpoint_flag' to detect restore completion
    3. Exits gracefully when checkpoint_flag is removed

Scenario:
    - Scientific computing workloads (eigenvalue problems)
    - HPC batch jobs (iterative solvers)
    - Numerical simulations (convergence-based)
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
    interval: float = 0.1,
    duration: int = 0,  # 0 = infinite (use iterations limit)
    working_dir: str = '.',
    keep_running: bool = False,
):
    """
    Power Iteration eigenvalue solver workload.

    Computes the dominant eigenvalue and eigenvector of a symmetric matrix
    using the Power Iteration method. The convergence state (eigenvector,
    eigenvalue history, convergence deltas) accumulates in memory and is
    lost on restart.

    Args:
        matrix_size: Size of square matrix (NxN)
        iterations: Max iterations (0 for infinite)
        interval: Minimum interval between iterations in seconds
        duration: Duration in seconds (0 for infinite, use iterations as limit)
        working_dir: Working directory for signal files
    """
    if not HAS_NUMPY:
        print("[MatMul] ERROR: NumPy not installed. Please install with: pip3 install numpy")
        sys.exit(1)

    duration_str = f"{duration}s" if duration > 0 else "infinite"
    print(f"[MatMul] Starting Power Iteration eigenvalue solver")
    print(f"[MatMul] Config: matrix_size={matrix_size}x{matrix_size}, iterations={iterations or 'infinite'}, duration={duration_str}")
    print(f"[MatMul] Working directory: {working_dir}")
    os.makedirs(working_dir, exist_ok=True)

    # Initialize symmetric matrix (ensures real eigenvalues)
    print(f"[MatMul] Initializing symmetric matrix...")
    np.random.seed(42)
    A = np.random.rand(matrix_size, matrix_size).astype(np.float64)
    A = (A + A.T) / 2.0  # Make symmetric

    # Initial random vector
    v = np.random.rand(matrix_size).astype(np.float64)
    v = v / np.linalg.norm(v)

    # Convergence tracking (THIS is the state lost on restart)
    eigenvalue_history = []
    convergence_deltas = []
    prev_eigenvalue = 0.0

    memory_mb = (matrix_size * matrix_size * 8 + matrix_size * 8 * 2) / (1024 * 1024)
    print(f"[MatMul] Matrix memory usage: {memory_mb:.2f} MB")

    # Signal ready for checkpoint
    create_ready_signal(working_dir)

    iteration = 0
    metric_printed = False
    start_time = time.time()

    while True:
        # Check if restore completed
        if not keep_running and check_restore_complete(working_dir):
            elapsed = time.time() - start_time
            print(f"[MatMul] Restore detected - checkpoint_flag removed")
            print(f"[MatMul] === STATE SUMMARY (lost on restart) ===")
            print(f"[MatMul]   Iterations completed: {iteration}")
            if eigenvalue_history:
                print(f"[MatMul]   Current eigenvalue estimate: {eigenvalue_history[-1]:.8f}")
            print(f"[MatMul]   Convergence history length: {len(eigenvalue_history)}")
            if convergence_deltas:
                print(f"[MatMul]   Final convergence delta: {convergence_deltas[-1]:.2e}")
            print(f"[MatMul]   Eigenvector state: {matrix_size}-dim vector (ALL lost on restart)")
            print(f"[MatMul]   Elapsed time: {elapsed:.1f}s")
            print(f"[MatMul] ==========================================")
            iter_per_sec = iteration / elapsed if elapsed > 0 else 0
            print(f"[METRIC] throughput {iter_per_sec:.4f} iter/s")
            sys.exit(0)

        # Duration check
        elapsed = time.time() - start_time
        if duration > 0 and elapsed >= duration:
            if keep_running:
                iter_per_sec = iteration / elapsed if elapsed > 0 else 0
                print(f"[MatMul] Duration {duration}s reached, exiting")
                print(f"[METRIC] throughput {iter_per_sec:.4f} iter/s")
                sys.exit(0)
            if not metric_printed:
                iter_per_sec = iteration / elapsed if elapsed > 0 else 0
                print(f"[METRIC] throughput {iter_per_sec:.4f} iter/s")
                metric_printed = True
            time.sleep(1)
            continue

        # Iteration limit check
        if iterations > 0 and iteration >= iterations:
            time.sleep(1)
            continue

        iteration += 1
        iter_start = time.time()

        # Power iteration step: v_{k+1} = A*v_k / ||A*v_k||
        Av = np.dot(A, v)
        eigenvalue = np.dot(v, Av)
        v_new = Av / np.linalg.norm(Av)

        # Track convergence
        delta = abs(eigenvalue - prev_eigenvalue)
        eigenvalue_history.append(float(eigenvalue))
        convergence_deltas.append(float(delta))
        prev_eigenvalue = eigenvalue
        v = v_new

        iter_elapsed = time.time() - iter_start

        if iteration % 100 == 0 or iteration <= 5:
            total_elapsed = time.time() - start_time
            print(f"[MatMul] Iteration {iteration}: eigenvalue={eigenvalue:.8f}, "
                  f"delta={delta:.2e}, time={iter_elapsed:.3f}s, elapsed={total_elapsed:.0f}s")

        if interval > iter_elapsed:
            time.sleep(interval - iter_elapsed)


def main():
    parser = argparse.ArgumentParser(
        description="Power Iteration eigenvalue solver for CRIU checkpoint testing"
    )
    parser.add_argument(
        '--matrix-size',
        type=int,
        default=2048,
        help='Size of square matrix NxN (default: 2048)'
    )
    parser.add_argument(
        '--iterations',
        type=int,
        default=0,
        help='Max iterations, 0 for infinite (default: 0)'
    )
    parser.add_argument(
        '--interval',
        type=float,
        default=0.1,
        help='Minimum interval between iterations in seconds (default: 0.1)'
    )
    parser.add_argument(
        '--duration',
        type=int,
        default=0,
        help='Duration in seconds (0 for infinite, use --iterations as limit)'
    )
    parser.add_argument(
        '--working_dir',
        type=str,
        default='.',
        help='Working directory for signal files (default: current directory)'
    )
    parser.add_argument(
        '--keep-running',
        action='store_true',
        help='Keep running after restore (ignore checkpoint_flag removal)'
    )

    args = parser.parse_args()

    run_matmul_workload(
        matrix_size=args.matrix_size,
        iterations=args.iterations,
        interval=args.interval,
        duration=args.duration,
        working_dir=args.working_dir,
        keep_running=args.keep_running,
    )


if __name__ == '__main__':
    main()
