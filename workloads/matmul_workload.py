"""
Matrix Multiplication Workload Wrapper

Control node wrapper for the matrix multiplication workload.
Simulates compute-intensive scientific computing scenarios.
"""

from typing import Dict, Any
from .base_workload import BaseWorkload, WorkloadFactory


MATMUL_STANDALONE_SCRIPT = '''#!/usr/bin/env python3
"""
Matrix Multiplication Standalone Workload
Auto-generated - do not edit directly
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
    ready_path = os.path.join(working_dir, 'checkpoint_ready')
    with open(ready_path, 'w') as f:
        f.write(f'ready:{os.getpid()}\\n')
    print(f"[MatMul] Checkpoint ready signal created (PID: {os.getpid()})")


def check_restore_complete(working_dir: str = '.') -> bool:
    flag_path = os.path.join(working_dir, 'checkpoint_flag')
    return not os.path.exists(flag_path)


def run_matmul_workload(
    matrix_size: int = 2048,
    iterations: int = 0,
    interval: float = 1.0,
    working_dir: str = '.'
):
    if not HAS_NUMPY:
        print("[MatMul] ERROR: NumPy not installed")
        sys.exit(1)

    print(f"[MatMul] Starting matrix multiplication workload")
    print(f"[MatMul] Config: matrix_size={matrix_size}x{matrix_size}")

    np.random.seed(42)
    matrix_a = np.random.rand(matrix_size, matrix_size).astype(np.float64)
    matrix_b = np.random.rand(matrix_size, matrix_size).astype(np.float64)
    result = np.zeros((matrix_size, matrix_size), dtype=np.float64)

    memory_mb = (matrix_size * matrix_size * 8 * 3) / (1024 * 1024)
    print(f"[MatMul] Matrix memory usage: {memory_mb:.2f} MB")

    create_ready_signal(working_dir)

    iteration = 0
    total_flops = 0

    while True:
        if check_restore_complete(working_dir):
            print(f"[MatMul] Restore detected")
            print(f"[MatMul] Completed {iteration} iterations")
            sys.exit(0)

        if iterations > 0 and iteration >= iterations:
            time.sleep(1)
            continue

        iteration += 1
        start_time = time.time()

        np.matmul(matrix_a, matrix_b, out=result)

        elapsed = time.time() - start_time
        flops = 2 * (matrix_size ** 3)
        gflops = flops / elapsed / 1e9
        total_flops += flops

        print(f"[MatMul] Iteration {iteration}: {elapsed:.3f}s, {gflops:.2f} GFLOPS")

        matrix_a = (matrix_a + result * 0.0001) % 1.0

        if interval > elapsed:
            time.sleep(interval - elapsed)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--matrix-size', type=int, default=2048)
    parser.add_argument('--iterations', type=int, default=0)
    parser.add_argument('--interval', type=float, default=1.0)
    parser.add_argument('--working_dir', type=str, default='.')

    args = parser.parse_args()
    run_matmul_workload(
        matrix_size=args.matrix_size,
        iterations=args.iterations,
        interval=args.interval,
        working_dir=args.working_dir
    )


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
        self.interval = config.get('interval', 1.0)

    def get_standalone_script_name(self) -> str:
        return 'matmul_standalone.py'

    def get_standalone_script_content(self) -> str:
        return MATMUL_STANDALONE_SCRIPT

    def get_command(self) -> str:
        cmd = f"python3 {self.get_standalone_script_name()}"
        cmd += f" --matrix-size {self.matrix_size}"
        cmd += f" --iterations {self.iterations}"
        cmd += f" --interval {self.interval}"
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
