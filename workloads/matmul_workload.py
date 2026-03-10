"""
Matrix Multiplication Workload Wrapper

Control node wrapper for the matrix multiplication workload.
Simulates compute-intensive scientific computing scenarios.
"""

from typing import Dict, Any
from .base_workload import BaseWorkload, WorkloadFactory


MATMUL_STANDALONE_SCRIPT = '''#!/usr/bin/env python3
"""Power Iteration Eigenvalue Solver - Auto-generated"""

import time
import os
import sys
import argparse

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False


def create_ready_signal(working_dir='.'):
    ready_path = os.path.join(working_dir, 'checkpoint_ready')
    with open(ready_path, 'w') as f:
        f.write(f'ready:{os.getpid()}\\n')
    print(f"[MatMul] Checkpoint ready signal created (PID: {os.getpid()})")


def check_restore_complete(working_dir='.'):
    flag_path = os.path.join(working_dir, 'checkpoint_flag')
    return not os.path.exists(flag_path)


def run_matmul_workload(matrix_size=2048, iterations=0, interval=0.1, duration=0, working_dir='.'):
    if not HAS_NUMPY:
        print("[MatMul] ERROR: NumPy not installed")
        sys.exit(1)

    print(f"[MatMul] Starting Power Iteration eigenvalue solver")
    print(f"[MatMul] Config: matrix_size={matrix_size}x{matrix_size}, duration={duration}s")

    np.random.seed(42)
    A = np.random.rand(matrix_size, matrix_size).astype(np.float64)
    A = (A + A.T) / 2.0

    v = np.random.rand(matrix_size).astype(np.float64)
    v = v / np.linalg.norm(v)

    eigenvalue_history = []
    convergence_deltas = []
    prev_eigenvalue = 0.0

    memory_mb = (matrix_size * matrix_size * 8 + matrix_size * 8 * 2) / (1024 * 1024)
    print(f"[MatMul] Matrix memory usage: {memory_mb:.2f} MB")

    create_ready_signal(working_dir)

    iteration = 0
    start_time = time.time()

    while True:
        if check_restore_complete(working_dir):
            elapsed = time.time() - start_time
            print(f"[MatMul] Restore detected - checkpoint_flag removed")
            print(f"[MatMul] === STATE SUMMARY (lost on restart) ===")
            print(f"[MatMul]   Iterations completed: {iteration}")
            if eigenvalue_history:
                print(f"[MatMul]   Current eigenvalue estimate: {eigenvalue_history[-1]:.8f}")
            print(f"[MatMul]   Convergence history length: {len(eigenvalue_history)}")
            if convergence_deltas:
                print(f"[MatMul]   Final convergence delta: {convergence_deltas[-1]:.2e}")
            print(f"[MatMul]   Eigenvector state: {matrix_size}-dim vector (ALL lost)")
            print(f"[MatMul]   Elapsed time: {elapsed:.1f}s")
            print(f"[MatMul] ==========================================")
            sys.exit(0)

        elapsed = time.time() - start_time
        if duration > 0 and elapsed >= duration:
            time.sleep(1)
            continue

        if iterations > 0 and iteration >= iterations:
            time.sleep(1)
            continue

        iteration += 1
        iter_start = time.time()

        Av = np.dot(A, v)
        eigenvalue = np.dot(v, Av)
        v_new = Av / np.linalg.norm(Av)

        delta = abs(eigenvalue - prev_eigenvalue)
        eigenvalue_history.append(float(eigenvalue))
        convergence_deltas.append(float(delta))
        prev_eigenvalue = eigenvalue
        v = v_new

        iter_elapsed = time.time() - iter_start

        if iteration % 100 == 0 or iteration <= 5:
            total_elapsed = time.time() - start_time
            print(f"[MatMul] Iteration {iteration}: eigenvalue={eigenvalue:.8f}, delta={delta:.2e}, elapsed={total_elapsed:.0f}s")

        if interval > iter_elapsed:
            time.sleep(interval - iter_elapsed)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--matrix-size', type=int, default=2048)
    parser.add_argument('--iterations', type=int, default=0)
    parser.add_argument('--interval', type=float, default=0.1)
    parser.add_argument('--duration', type=int, default=0)
    parser.add_argument('--working_dir', type=str, default='.')

    args = parser.parse_args()
    run_matmul_workload(args.matrix_size, args.iterations, args.interval, args.duration, args.working_dir)


if __name__ == '__main__':
    main()
'''


class MatMulWorkload(BaseWorkload):
    """
    Matrix Multiplication workload for CRIU checkpoint testing.

    Simulates compute-intensive scientific computing workloads:
    - HPC batch jobs
    - Numerical simulations
    - Scientific computing applications

    Memory usage is predictable: ~3 * matrix_size^2 * 8 bytes
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.matrix_size = config.get('matrix_size', 2048)
        self.iterations = config.get('iterations', 0)
        self.interval = config.get('interval', 0.1)
        self.duration = config.get('duration', 0)

    def get_standalone_script_name(self) -> str:
        return 'matmul_standalone.py'

    def get_standalone_script_content(self) -> str:
        return MATMUL_STANDALONE_SCRIPT

    def get_command(self) -> str:
        cmd = f"python3 {self.get_standalone_script_name()}"
        cmd += f" --matrix-size {self.matrix_size}"
        cmd += f" --iterations {self.iterations}"
        cmd += f" --interval {self.interval}"
        cmd += f" --duration {self.duration}"
        cmd += f" --working_dir {self.working_dir}"
        return cmd

    def get_dependencies(self) -> list[str]:
        return ['numpy']

    def validate_config(self) -> bool:
        if self.matrix_size <= 0:
            raise ValueError(f"matrix_size must be positive, got {self.matrix_size}")
        if self.matrix_size > 16384:
            raise ValueError(f"matrix_size too large (max 16384), got {self.matrix_size}")
        return True

    def estimate_memory_mb(self) -> float:
        """Estimate memory usage in MB."""
        return (self.matrix_size * self.matrix_size * 8 * 3) / (1024 * 1024)


WorkloadFactory.register('matmul', MatMulWorkload)
