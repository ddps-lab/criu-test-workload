#!/usr/bin/env python3
"""
Jupyter Notebook Cell Simulation Standalone Workload

This script simulates the execution pattern of Jupyter notebook cells.
It maintains state between cells and performs various computations,
representing interactive data science sessions.

Usage:
    python3 jupyter_standalone.py --num-cells 50 --cell-interval 3

Checkpoint Protocol:
    1. Creates 'checkpoint_ready' file after initial setup
    2. Checks 'checkpoint_flag' to detect restore completion
    3. Validates notebook state after restore
    4. Exits gracefully when checkpoint_flag is removed

Scenario:
    - Interactive Jupyter notebooks
    - Data science exploration sessions
    - Research computing environments
    - Educational computing labs
"""

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
    """Create checkpoint ready signal file."""
    ready_path = os.path.join(working_dir, 'checkpoint_ready')
    with open(ready_path, 'w') as f:
        f.write(f'ready:{os.getpid()}\n')
    print(f"[Jupyter] Checkpoint ready signal created (PID: {os.getpid()})")


def check_restore_complete(working_dir: str = '.') -> bool:
    """Check if restore is complete (checkpoint_flag removed)."""
    flag_path = os.path.join(working_dir, 'checkpoint_flag')
    return not os.path.exists(flag_path)


class NotebookState:
    """Simulates Jupyter notebook kernel state."""

    def __init__(self):
        self.variables = {}
        self.execution_count = 0
        self.history = []
        self.outputs = []

    def execute_cell(self, cell_type: str, cell_id: int) -> dict:
        """Execute a simulated notebook cell."""
        self.execution_count += 1
        start_time = time.time()

        result = {
            'cell_id': cell_id,
            'execution_count': self.execution_count,
            'cell_type': cell_type,
            'success': True,
        }

        if cell_type == 'import':
            self.variables['np'] = True
            self.variables['pd'] = True
            result['output'] = 'Libraries imported'

        elif cell_type == 'data_load':
            if HAS_NUMPY:
                data_size = random.randint(1000, 10000)
                self.variables[f'data_{cell_id}'] = np.random.randn(data_size, 10)
                result['output'] = f'Loaded data with shape ({data_size}, 10)'
            else:
                self.variables[f'data_{cell_id}'] = [[random.random() for _ in range(10)] for _ in range(1000)]
                result['output'] = 'Loaded data (1000, 10)'

        elif cell_type == 'computation':
            if HAS_NUMPY:
                size = random.randint(100, 1000)
                matrix = np.random.randn(size, size)
                eigenvalues = np.linalg.eigvals(matrix[:min(50, size), :min(50, size)])
                self.variables[f'result_{cell_id}'] = eigenvalues
                result['output'] = f'Computed eigenvalues for {size}x{size} matrix'
            else:
                self.variables[f'result_{cell_id}'] = [random.random() for _ in range(100)]
                result['output'] = 'Computed results'

        elif cell_type == 'visualization':
            if HAS_NUMPY:
                fig_data = np.random.randn(1000, 1000)
                self.variables[f'fig_{cell_id}'] = fig_data
                result['output'] = 'Created visualization (1000x1000 figure)'
            else:
                self.variables[f'fig_{cell_id}'] = [[0] * 100 for _ in range(100)]
                result['output'] = 'Created visualization'

        elif cell_type == 'model':
            time.sleep(random.uniform(0.5, 2.0))
            self.variables[f'model_{cell_id}'] = {
                'weights': [random.random() for _ in range(100)],
                'accuracy': random.uniform(0.7, 0.99)
            }
            result['output'] = f"Model trained, accuracy: {self.variables[f'model_{cell_id}']['accuracy']:.4f}"

        elif cell_type == 'markdown':
            result['output'] = 'Markdown rendered'

        result['duration'] = time.time() - start_time
        self.history.append(result)
        self.outputs.append(result['output'])

        return result

    def get_state_summary(self) -> dict:
        return {
            'execution_count': self.execution_count,
            'num_variables': len(self.variables),
            'variable_names': list(self.variables.keys())[:10],
            'history_length': len(self.history)
        }

    def estimate_memory_mb(self) -> float:
        total = 0
        for key, value in self.variables.items():
            if HAS_NUMPY and isinstance(value, np.ndarray):
                total += value.nbytes
            elif isinstance(value, list):
                total += len(str(value))
            else:
                total += 100
        return total / (1024 * 1024)


def run_jupyter_workload(
    num_cells: int = 50,
    cell_interval: float = 3.0,
    working_dir: str = '.'
):
    """Main Jupyter notebook simulation workload."""
    print(f"[Jupyter] Starting Jupyter notebook simulation")
    print(f"[Jupyter] Config: num_cells={num_cells or 'infinite'}, interval={cell_interval}s")
    print(f"[Jupyter] NumPy available: {HAS_NUMPY}")
    print(f"[Jupyter] Working directory: {working_dir}")

    cell_types = [
        ('import', 0.05),
        ('markdown', 0.15),
        ('data_load', 0.15),
        ('computation', 0.35),
        ('visualization', 0.15),
        ('model', 0.15),
    ]

    def get_random_cell_type():
        r = random.random()
        cumulative = 0
        for cell_type, prob in cell_types:
            cumulative += prob
            if r <= cumulative:
                return cell_type
        return 'computation'

    notebook = NotebookState()
    print(f"[Jupyter] Executing initial setup...")
    notebook.execute_cell('import', 0)

    create_ready_signal(working_dir)

    cell_id = 1

    while True:
        if check_restore_complete(working_dir):
            print(f"[Jupyter] Restore detected - checkpoint_flag removed")
            state = notebook.get_state_summary()
            print(f"[Jupyter] Notebook state:")
            print(f"[Jupyter]   Execution count: {state['execution_count']}")
            print(f"[Jupyter]   Variables: {state['num_variables']}")
            print(f"[Jupyter]   Memory: {notebook.estimate_memory_mb():.2f} MB")
            print("[Jupyter] Workload complete, exiting")
            sys.exit(0)

        if num_cells > 0 and cell_id > num_cells:
            time.sleep(1)
            continue

        cell_type = get_random_cell_type()
        result = notebook.execute_cell(cell_type, cell_id)

        print(f"[Jupyter] Cell [{cell_id}] ({cell_type}): {result['output']} ({result['duration']:.2f}s)")

        cell_id += 1
        time.sleep(cell_interval)


def main():
    parser = argparse.ArgumentParser(description="Jupyter notebook simulation workload")
    parser.add_argument('--num-cells', type=int, default=0, help='Number of cells, 0 for infinite')
    parser.add_argument('--cell-interval', type=float, default=3.0, help='Interval between cells')
    parser.add_argument('--working_dir', type=str, default='.', help='Working directory')

    args = parser.parse_args()
    run_jupyter_workload(args.num_cells, args.cell_interval, args.working_dir)


if __name__ == '__main__':
    main()
