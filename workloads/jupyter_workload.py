"""
Jupyter Notebook Simulation Workload Wrapper

Control node wrapper for Jupyter notebook simulation.
Simulates interactive data science sessions with variable state.
"""

from typing import Dict, Any
from .base_workload import BaseWorkload, WorkloadFactory


JUPYTER_STANDALONE_SCRIPT = '''#!/usr/bin/env python3
"""Jupyter Notebook Simulation - Auto-generated standalone script"""

import time
import os
import sys
import argparse
import random

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False


def create_ready_signal(working_dir: str = '.'):
    ready_path = os.path.join(working_dir, 'checkpoint_ready')
    with open(ready_path, 'w') as f:
        f.write(f'ready:{os.getpid()}\\n')
    print(f"[Jupyter] Checkpoint ready (PID: {os.getpid()})")


def check_restore_complete(working_dir: str = '.') -> bool:
    return not os.path.exists(os.path.join(working_dir, 'checkpoint_flag'))


class NotebookState:
    def __init__(self):
        self.variables = {}
        self.execution_count = 0

    def execute_cell(self, cell_type: str, cell_id: int) -> dict:
        self.execution_count += 1
        start_time = time.time()
        result = {'cell_type': cell_type, 'success': True}

        if cell_type == 'import':
            self.variables['np'] = True
            result['output'] = 'Libraries imported'
        elif cell_type == 'data_load':
            if HAS_NUMPY:
                size = random.randint(1000, 10000)
                self.variables[f'data_{cell_id}'] = np.random.randn(size, 10)
                result['output'] = f'Loaded ({size}, 10)'
            else:
                self.variables[f'data_{cell_id}'] = [[0]*10 for _ in range(1000)]
                result['output'] = 'Loaded (1000, 10)'
        elif cell_type == 'computation':
            if HAS_NUMPY:
                size = random.randint(100, 500)
                matrix = np.random.randn(size, size)
                self.variables[f'result_{cell_id}'] = np.linalg.eigvals(matrix[:50, :50])
                result['output'] = f'Computed {size}x{size}'
            else:
                self.variables[f'result_{cell_id}'] = [random.random() for _ in range(100)]
                result['output'] = 'Computed'
        elif cell_type == 'visualization':
            if HAS_NUMPY:
                self.variables[f'fig_{cell_id}'] = np.random.randn(500, 500)
            result['output'] = 'Visualization created'
        elif cell_type == 'model':
            time.sleep(random.uniform(0.5, 1.5))
            self.variables[f'model_{cell_id}'] = {'acc': random.uniform(0.7, 0.99)}
            result['output'] = f"Model acc: {self.variables[f'model_{cell_id}']['acc']:.4f}"
        else:
            result['output'] = 'Markdown'

        result['duration'] = time.time() - start_time
        return result

    def estimate_memory_mb(self) -> float:
        total = 0
        for v in self.variables.values():
            if HAS_NUMPY and isinstance(v, np.ndarray):
                total += v.nbytes
            else:
                total += 100
        return total / (1024 * 1024)


def run_jupyter_workload(num_cells, cell_interval, working_dir):
    print(f"[Jupyter] Starting simulation")
    cell_types = ['import', 'markdown', 'data_load', 'computation', 'visualization', 'model']
    weights = [0.05, 0.15, 0.15, 0.35, 0.15, 0.15]

    notebook = NotebookState()
    notebook.execute_cell('import', 0)
    create_ready_signal(working_dir)

    cell_id = 1
    while True:
        if check_restore_complete(working_dir):
            print(f"[Jupyter] Restore detected")
            print(f"[Jupyter] Cells: {notebook.execution_count}, Memory: {notebook.estimate_memory_mb():.2f} MB")
            sys.exit(0)

        if num_cells > 0 and cell_id > num_cells:
            time.sleep(1)
            continue

        cell_type = random.choices(cell_types, weights=weights)[0]
        result = notebook.execute_cell(cell_type, cell_id)
        print(f"[Jupyter] Cell {cell_id} ({cell_type}): {result['output']}")
        cell_id += 1
        time.sleep(cell_interval)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--num-cells', type=int, default=0)
    parser.add_argument('--cell-interval', type=float, default=3.0)
    parser.add_argument('--working_dir', type=str, default='.')
    args = parser.parse_args()
    run_jupyter_workload(args.num_cells, args.cell_interval, args.working_dir)


if __name__ == '__main__':
    main()
'''


class JupyterWorkload(BaseWorkload):
    """
    Jupyter notebook simulation workload.

    Simulates interactive data science sessions:
    - Cell execution with state accumulation
    - Various cell types (import, data_load, computation, etc.)
    - Memory growth over time

    This is a simulation that represents typical Jupyter usage patterns.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.num_cells = config.get('num_cells', 0)  # 0 = infinite
        self.cell_interval = config.get('cell_interval', 3.0)

    def get_standalone_script_name(self) -> str:
        return 'jupyter_standalone.py'

    def get_standalone_script_content(self) -> str:
        return JUPYTER_STANDALONE_SCRIPT

    def get_command(self) -> str:
        cmd = f"python3 {self.get_standalone_script_name()}"
        cmd += f" --num-cells {self.num_cells}"
        cmd += f" --cell-interval {self.cell_interval}"
        cmd += f" --working_dir {self.working_dir}"
        return cmd

    def get_dependencies(self) -> list[str]:
        return ['numpy']

    def validate_config(self) -> bool:
        if self.cell_interval <= 0:
            raise ValueError("cell_interval must be positive")
        return True

    def estimate_memory_mb(self) -> float:
        # Rough estimate: ~10MB per 10 cells
        return max(50, self.num_cells * 1) if self.num_cells > 0 else 100


WorkloadFactory.register('jupyter', JupyterWorkload)
