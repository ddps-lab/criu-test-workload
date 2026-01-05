#!/usr/bin/env python3
"""
Data Processing Standalone Workload (Pandas-like)

This script simulates data processing operations similar to pandas/spark.
It performs ETL operations on synthetic datasets, representing
batch data processing jobs.

Usage:
    python3 dataproc_standalone.py --num-rows 1000000 --num-cols 50 --operations 100

Checkpoint Protocol:
    1. Creates 'checkpoint_ready' file after initial data load
    2. Checks 'checkpoint_flag' to detect restore completion
    3. Validates data state after restore
    4. Exits gracefully when checkpoint_flag is removed

Scenario:
    - ETL pipelines
    - Data warehouse operations
    - Batch analytics jobs
    - Data transformation workflows
"""

import time
import os
import sys
import argparse
import random
import csv
import io

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False


def create_ready_signal(working_dir: str = '.'):
    """Create checkpoint ready signal file."""
    ready_path = os.path.join(working_dir, 'checkpoint_ready')
    with open(ready_path, 'w') as f:
        f.write(f'ready:{os.getpid()}\n')
    print(f"[DataProc] Checkpoint ready signal created (PID: {os.getpid()})")


def check_restore_complete(working_dir: str = '.') -> bool:
    """Check if restore is complete (checkpoint_flag removed)."""
    flag_path = os.path.join(working_dir, 'checkpoint_flag')
    return not os.path.exists(flag_path)


class DataFrameSimulator:
    """Simulates pandas DataFrame operations without pandas dependency."""

    def __init__(self, num_rows: int, num_cols: int):
        self.num_rows = num_rows
        self.num_cols = num_cols
        self.columns = [f'col_{i}' for i in range(num_cols)]

        if HAS_NUMPY:
            # Use numpy for efficient storage
            self.data = np.random.randn(num_rows, num_cols)
            self.string_col = np.array([f'str_{i % 1000}' for i in range(num_rows)])
            self.category_col = np.array([f'cat_{i % 100}' for i in range(num_rows)])
        else:
            # Fallback to lists
            random.seed(42)
            self.data = [[random.gauss(0, 1) for _ in range(num_cols)] for _ in range(num_rows)]
            self.string_col = [f'str_{i % 1000}' for i in range(num_rows)]
            self.category_col = [f'cat_{i % 100}' for i in range(num_rows)]

    def memory_usage_mb(self) -> float:
        """Estimate memory usage."""
        if HAS_NUMPY:
            return (self.data.nbytes + len(self.string_col) * 20 + len(self.category_col) * 20) / (1024 * 1024)
        else:
            return (self.num_rows * self.num_cols * 8 + self.num_rows * 40) / (1024 * 1024)


class DataProcessor:
    """Simulates various data processing operations."""

    def __init__(self, df: DataFrameSimulator):
        self.df = df
        self.operation_count = 0
        self.results = {}
        self.temp_data = []

    def filter_rows(self, col_idx: int, threshold: float) -> dict:
        """Filter rows where column value > threshold."""
        start = time.time()

        if HAS_NUMPY:
            mask = self.df.data[:, col_idx] > threshold
            filtered_count = np.sum(mask)
            self.results['last_filter'] = self.df.data[mask, :]
        else:
            filtered_count = sum(1 for row in self.df.data if row[col_idx] > threshold)

        self.operation_count += 1
        return {
            'operation': 'filter',
            'filtered_rows': int(filtered_count),
            'duration': time.time() - start
        }

    def aggregate_column(self, col_idx: int) -> dict:
        """Compute aggregations on a column."""
        start = time.time()

        if HAS_NUMPY:
            col = self.df.data[:, col_idx]
            stats = {
                'mean': float(np.mean(col)),
                'std': float(np.std(col)),
                'min': float(np.min(col)),
                'max': float(np.max(col)),
                'sum': float(np.sum(col))
            }
        else:
            col = [row[col_idx] for row in self.df.data]
            stats = {
                'mean': sum(col) / len(col),
                'min': min(col),
                'max': max(col),
                'sum': sum(col)
            }

        self.results['last_agg'] = stats
        self.operation_count += 1
        return {
            'operation': 'aggregate',
            'stats': stats,
            'duration': time.time() - start
        }

    def sort_by_column(self, col_idx: int) -> dict:
        """Sort data by column."""
        start = time.time()

        if HAS_NUMPY:
            indices = np.argsort(self.df.data[:, col_idx])
            # Store sorted subset
            self.temp_data.append(self.df.data[indices[:1000], :].copy())
        else:
            sorted_data = sorted(enumerate(self.df.data), key=lambda x: x[1][col_idx])
            self.temp_data.append([x[1] for x in sorted_data[:1000]])

        self.operation_count += 1
        return {
            'operation': 'sort',
            'duration': time.time() - start
        }

    def group_aggregate(self) -> dict:
        """Group by category and aggregate."""
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

        # Compute mean per group
        group_means = {}
        for cat, values in groups.items():
            if HAS_NUMPY:
                group_means[cat] = float(np.mean(values))
            else:
                group_means[cat] = sum(values) / len(values) if values else 0

        self.results['group_means'] = group_means
        self.operation_count += 1
        return {
            'operation': 'group_aggregate',
            'num_groups': len(groups),
            'duration': time.time() - start
        }

    def join_operation(self) -> dict:
        """Simulate a join operation."""
        start = time.time()

        # Create a lookup table
        lookup = {}
        for i in range(min(1000, len(self.df.string_col))):
            key = self.df.string_col[i]
            if HAS_NUMPY:
                lookup[key] = float(self.df.data[i, 0])
            else:
                lookup[key] = self.df.data[i][0]

        # Perform lookup join
        joined_count = 0
        for i in range(min(10000, len(self.df.string_col))):
            key = self.df.string_col[i]
            if key in lookup:
                joined_count += 1

        self.operation_count += 1
        return {
            'operation': 'join',
            'joined_rows': joined_count,
            'duration': time.time() - start
        }

    def transform_columns(self) -> dict:
        """Apply transformations to columns."""
        start = time.time()

        if HAS_NUMPY:
            # Normalize columns
            for i in range(min(10, self.df.num_cols)):
                col = self.df.data[:, i]
                mean = np.mean(col)
                std = np.std(col) + 1e-8
                normalized = (col - mean) / std
                self.temp_data.append(normalized[:1000].copy())
        else:
            # Simple transformation
            for i in range(min(10, self.df.num_cols)):
                col = [row[i] for row in self.df.data[:1000]]
                mean = sum(col) / len(col)
                self.temp_data.append([x - mean for x in col])

        self.operation_count += 1
        return {
            'operation': 'transform',
            'duration': time.time() - start
        }

    def window_operation(self) -> dict:
        """Perform window/rolling operations."""
        start = time.time()

        window_size = 100

        if HAS_NUMPY:
            col = self.df.data[:, 0]
            # Rolling mean using cumsum
            cumsum = np.cumsum(col)
            rolling_mean = (cumsum[window_size:] - cumsum[:-window_size]) / window_size
            self.results['rolling_mean'] = rolling_mean[:100].tolist()
        else:
            col = [row[0] for row in self.df.data]
            rolling_mean = []
            for i in range(window_size, min(200, len(col))):
                window = col[i-window_size:i]
                rolling_mean.append(sum(window) / window_size)
            self.results['rolling_mean'] = rolling_mean

        self.operation_count += 1
        return {
            'operation': 'window',
            'duration': time.time() - start
        }


def run_dataproc_workload(
    num_rows: int = 1000000,
    num_cols: int = 50,
    operations: int = 0,  # 0 = infinite
    interval: float = 1.0,
    working_dir: str = '.'
):
    """
    Main data processing workload.

    Args:
        num_rows: Number of rows in dataset
        num_cols: Number of columns in dataset
        operations: Number of operations to perform (0 for infinite)
        interval: Interval between operations
        working_dir: Working directory for signal files
    """
    print(f"[DataProc] Starting data processing workload")
    print(f"[DataProc] Config: rows={num_rows}, cols={num_cols}, operations={operations or 'infinite'}")
    print(f"[DataProc] NumPy available: {HAS_NUMPY}, Pandas available: {HAS_PANDAS}")
    print(f"[DataProc] Working directory: {working_dir}")

    # Initialize dataset
    print(f"[DataProc] Generating dataset...")
    init_start = time.time()
    df = DataFrameSimulator(num_rows, num_cols)
    init_duration = time.time() - init_start

    print(f"[DataProc] Dataset created in {init_duration:.2f}s")
    print(f"[DataProc] Memory usage: {df.memory_usage_mb():.2f} MB")

    # Initialize processor
    processor = DataProcessor(df)

    # Available operations
    operation_funcs = [
        lambda: processor.filter_rows(random.randint(0, num_cols-1), random.gauss(0, 1)),
        lambda: processor.aggregate_column(random.randint(0, num_cols-1)),
        lambda: processor.sort_by_column(random.randint(0, num_cols-1)),
        lambda: processor.group_aggregate(),
        lambda: processor.join_operation(),
        lambda: processor.transform_columns(),
        lambda: processor.window_operation(),
    ]

    # Signal ready for checkpoint
    create_ready_signal(working_dir)

    op_count = 0
    start_time = time.time()
    last_report_time = start_time

    while True:
        # Check if restore completed
        if check_restore_complete(working_dir):
            print(f"[DataProc] Restore detected - checkpoint_flag removed")
            total_time = time.time() - start_time
            print(f"[DataProc] Processing summary:")
            print(f"[DataProc]   Operations completed: {op_count}")
            print(f"[DataProc]   Total time: {total_time:.2f}s")
            print(f"[DataProc]   Ops/second: {op_count/max(1,total_time):.2f}")
            print("[DataProc] Workload complete, exiting")
            sys.exit(0)

        # Check operation limit
        if operations > 0 and op_count >= operations:
            time.sleep(1)
            continue

        # Execute random operation
        op_func = random.choice(operation_funcs)
        result = op_func()
        op_count += 1

        # Report every 5 seconds
        current_time = time.time()
        if current_time - last_report_time >= 5.0:
            elapsed = current_time - start_time
            ops_rate = op_count / elapsed
            print(f"[DataProc] Op {op_count} ({result['operation']}): {result['duration']:.3f}s, Rate: {ops_rate:.1f} ops/s")
            last_report_time = current_time

        # Wait between operations
        time.sleep(interval)


def main():
    parser = argparse.ArgumentParser(
        description="Data processing workload for CRIU checkpoint testing"
    )
    parser.add_argument(
        '--num-rows',
        type=int,
        default=1000000,
        help='Number of rows in dataset (default: 1000000)'
    )
    parser.add_argument(
        '--num-cols',
        type=int,
        default=50,
        help='Number of columns in dataset (default: 50)'
    )
    parser.add_argument(
        '--operations',
        type=int,
        default=0,
        help='Number of operations, 0 for infinite (default: 0)'
    )
    parser.add_argument(
        '--interval',
        type=float,
        default=1.0,
        help='Interval between operations in seconds (default: 1.0)'
    )
    parser.add_argument(
        '--working_dir',
        type=str,
        default='.',
        help='Working directory for signal files'
    )

    args = parser.parse_args()

    run_dataproc_workload(
        num_rows=args.num_rows,
        num_cols=args.num_cols,
        operations=args.operations,
        interval=args.interval,
        working_dir=args.working_dir
    )


if __name__ == '__main__':
    main()
