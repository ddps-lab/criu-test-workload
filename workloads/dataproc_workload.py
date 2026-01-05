"""
Data Processing Workload Wrapper (Pandas-like)

Control node wrapper for the data processing workload.
Simulates ETL pipelines and batch analytics jobs.
"""

from typing import Dict, Any
from .base_workload import BaseWorkload, WorkloadFactory


DATAPROC_STANDALONE_SCRIPT = '''#!/usr/bin/env python3
"""
Data Processing Standalone Workload
Auto-generated - do not edit directly
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
    ready_path = os.path.join(working_dir, 'checkpoint_ready')
    with open(ready_path, 'w') as f:
        f.write(f'ready:{os.getpid()}\\n')
    print(f"[DataProc] Checkpoint ready signal created (PID: {os.getpid()})")


def check_restore_complete(working_dir: str = '.') -> bool:
    flag_path = os.path.join(working_dir, 'checkpoint_flag')
    return not os.path.exists(flag_path)


class DataFrameSimulator:
    def __init__(self, num_rows, num_cols):
        self.num_rows = num_rows
        self.num_cols = num_cols
        if HAS_NUMPY:
            self.data = np.random.randn(num_rows, num_cols)
            self.string_col = np.array([f'str_{i % 1000}' for i in range(num_rows)])
            self.category_col = np.array([f'cat_{i % 100}' for i in range(num_rows)])
        else:
            random.seed(42)
            self.data = [[random.gauss(0, 1) for _ in range(num_cols)] for _ in range(num_rows)]
            self.string_col = [f'str_{i % 1000}' for i in range(num_rows)]
            self.category_col = [f'cat_{i % 100}' for i in range(num_rows)]

    def memory_usage_mb(self):
        if HAS_NUMPY:
            return self.data.nbytes / (1024 * 1024)
        return (self.num_rows * self.num_cols * 8) / (1024 * 1024)


class DataProcessor:
    def __init__(self, df):
        self.df = df
        self.operation_count = 0
        self.results = {}
        self.temp_data = []

    def filter_rows(self, col_idx, threshold):
        start = time.time()
        if HAS_NUMPY:
            mask = self.df.data[:, col_idx] > threshold
            filtered_count = int(np.sum(mask))
        else:
            filtered_count = sum(1 for row in self.df.data if row[col_idx] > threshold)
        self.operation_count += 1
        return {'operation': 'filter', 'filtered_rows': filtered_count, 'duration': time.time() - start}

    def aggregate_column(self, col_idx):
        start = time.time()
        if HAS_NUMPY:
            col = self.df.data[:, col_idx]
            stats = {'mean': float(np.mean(col)), 'std': float(np.std(col)), 'sum': float(np.sum(col))}
        else:
            col = [row[col_idx] for row in self.df.data]
            stats = {'mean': sum(col) / len(col), 'sum': sum(col)}
        self.results['last_agg'] = stats
        self.operation_count += 1
        return {'operation': 'aggregate', 'duration': time.time() - start}

    def sort_by_column(self, col_idx):
        start = time.time()
        if HAS_NUMPY:
            indices = np.argsort(self.df.data[:, col_idx])
            self.temp_data.append(indices[:1000].copy())
        else:
            sorted_data = sorted(enumerate(self.df.data), key=lambda x: x[1][col_idx])
            self.temp_data.append([x[0] for x in sorted_data[:1000]])
        self.operation_count += 1
        return {'operation': 'sort', 'duration': time.time() - start}

    def group_aggregate(self):
        start = time.time()
        groups = {}
        for i in range(min(10000, self.df.num_rows)):
            cat = self.df.category_col[i]
            if cat not in groups:
                groups[cat] = []
            if HAS_NUMPY:
                groups[cat].append(self.df.data[i, 0])
            else:
                groups[cat].append(self.df.data[i][0])
        self.operation_count += 1
        return {'operation': 'group_aggregate', 'num_groups': len(groups), 'duration': time.time() - start}

    def transform_columns(self):
        start = time.time()
        if HAS_NUMPY:
            for i in range(min(10, self.df.num_cols)):
                col = self.df.data[:, i]
                mean, std = np.mean(col), np.std(col) + 1e-8
                self.temp_data.append(((col - mean) / std)[:1000].copy())
        self.operation_count += 1
        return {'operation': 'transform', 'duration': time.time() - start}


def run_dataproc_workload(num_rows, num_cols, operations, interval, working_dir):
    print(f"[DataProc] Starting data processing workload")
    print(f"[DataProc] Config: rows={num_rows}, cols={num_cols}")

    print(f"[DataProc] Generating dataset...")
    df = DataFrameSimulator(num_rows, num_cols)
    print(f"[DataProc] Memory: {df.memory_usage_mb():.2f} MB")

    processor = DataProcessor(df)
    operation_funcs = [
        lambda: processor.filter_rows(random.randint(0, num_cols-1), random.gauss(0, 1)),
        lambda: processor.aggregate_column(random.randint(0, num_cols-1)),
        lambda: processor.sort_by_column(random.randint(0, num_cols-1)),
        lambda: processor.group_aggregate(),
        lambda: processor.transform_columns(),
    ]

    create_ready_signal(working_dir)

    op_count = 0
    start_time = time.time()
    last_report_time = start_time

    while True:
        if check_restore_complete(working_dir):
            print(f"[DataProc] Restore detected")
            print(f"[DataProc] Operations completed: {op_count}")
            sys.exit(0)

        if operations > 0 and op_count >= operations:
            time.sleep(1)
            continue

        op_func = random.choice(operation_funcs)
        result = op_func()
        op_count += 1

        current_time = time.time()
        if current_time - last_report_time >= 5.0:
            elapsed = current_time - start_time
            print(f"[DataProc] Op {op_count} ({result['operation']}): {result['duration']:.3f}s, Rate: {op_count/elapsed:.1f} ops/s")
            last_report_time = current_time

        time.sleep(interval)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--num-rows', type=int, default=1000000)
    parser.add_argument('--num-cols', type=int, default=50)
    parser.add_argument('--operations', type=int, default=0)
    parser.add_argument('--interval', type=float, default=1.0)
    parser.add_argument('--working_dir', type=str, default='.')

    args = parser.parse_args()
    run_dataproc_workload(args.num_rows, args.num_cols, args.operations, args.interval, args.working_dir)


if __name__ == '__main__':
    main()
'''


class DataProcWorkload(BaseWorkload):
    """
    Data processing workload (Pandas-like).

    Simulates:
    - ETL pipelines
    - Data warehouse operations
    - Batch analytics jobs
    - Data transformation workflows

    Performs various DataFrame operations on synthetic data.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.num_rows = config.get('num_rows', 1000000)
        self.num_cols = config.get('num_cols', 50)
        self.operations = config.get('operations', 0)
        self.interval = config.get('interval', 1.0)

    def get_standalone_script_name(self) -> str:
        return 'dataproc_standalone.py'

    def get_standalone_script_content(self) -> str:
        return DATAPROC_STANDALONE_SCRIPT

    def get_command(self) -> str:
        cmd = f"python3 {self.get_standalone_script_name()}"
        cmd += f" --num-rows {self.num_rows}"
        cmd += f" --num-cols {self.num_cols}"
        cmd += f" --operations {self.operations}"
        cmd += f" --interval {self.interval}"
        cmd += f" --working_dir {self.working_dir}"
        return cmd

    def get_dependencies(self) -> list[str]:
        return ['numpy']  # pandas is optional

    def validate_config(self) -> bool:
        if self.num_rows <= 0:
            raise ValueError(f"num_rows must be positive")
        if self.num_cols <= 0:
            raise ValueError(f"num_cols must be positive")
        return True

    def estimate_memory_mb(self) -> float:
        # Approximate: each cell is 8 bytes (float64)
        return (self.num_rows * self.num_cols * 8) / (1024 * 1024)


WorkloadFactory.register('dataproc', DataProcWorkload)
