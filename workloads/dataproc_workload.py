"""
Data Processing Workload Wrapper (Streaming Aggregation)

Control node wrapper for the streaming data processing workload.
Simulates ETL pipelines and real-time analytics using Welford's algorithm.
"""

from typing import Dict, Any
from .base_workload import BaseWorkload, WorkloadFactory


DATAPROC_STANDALONE_SCRIPT = '''#!/usr/bin/env python3
"""
Streaming Data Processing Standalone Workload
Auto-generated - do not edit directly
"""

import time
import os
import sys
import argparse
import math

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False


def create_ready_signal(working_dir='.'):
    ready_path = os.path.join(working_dir, 'checkpoint_ready')
    with open(ready_path, 'w') as f:
        f.write(f'ready:{os.getpid()}\\n')
    print(f"[DataProc] Checkpoint ready signal created (PID: {os.getpid()})")


def check_restore_complete(working_dir='.'):
    flag_path = os.path.join(working_dir, 'checkpoint_flag')
    return not os.path.exists(flag_path)


class StreamingAggregator:
    def __init__(self, num_cols, hist_bins=50, hist_cols=10):
        self.num_cols = num_cols
        self.hist_bins = hist_bins
        self.hist_cols = min(hist_cols, num_cols)
        self.count = 0
        if HAS_NUMPY:
            self.mean = np.zeros(num_cols, dtype=np.float64)
            self.m2 = np.zeros(num_cols, dtype=np.float64)
            self.col_min = np.full(num_cols, np.inf, dtype=np.float64)
            self.col_max = np.full(num_cols, -np.inf, dtype=np.float64)
            self.co_moment = np.zeros((num_cols, num_cols), dtype=np.float64)
            self.hist_low = np.zeros(self.hist_cols, dtype=np.float64)
            self.hist_high = np.zeros(self.hist_cols, dtype=np.float64)
            self.histograms = np.zeros((self.hist_cols, hist_bins), dtype=np.int64)
        else:
            self.mean = [0.0] * num_cols
            self.m2 = [0.0] * num_cols
            self.col_min = [float('inf')] * num_cols
            self.col_max = [float('-inf')] * num_cols
            self.co_moment = [[0.0] * num_cols for _ in range(num_cols)]
            self.hist_low = [0.0] * self.hist_cols
            self.hist_high = [0.0] * self.hist_cols
            self.histograms = [[0] * hist_bins for _ in range(self.hist_cols)]
        self.hist_range_set = False
        self.window_snapshots = []
        self.batches_processed = 0

    def update_batch(self, batch):
        if HAS_NUMPY:
            self._update_batch_numpy(batch)
        else:
            self._update_batch_python(batch)
        self.batches_processed += 1
        if self.batches_processed % 10 == 0:
            self._take_snapshot()

    def _update_batch_numpy(self, batch):
        batch_size = batch.shape[0]
        for row_idx in range(batch_size):
            row = batch[row_idx]
            self.count += 1
            n = self.count
            delta = row - self.mean
            self.mean += delta / n
            delta2 = row - self.mean
            self.m2 += delta * delta2
            np.minimum(self.col_min, row, out=self.col_min)
            np.maximum(self.col_max, row, out=self.col_max)
            self.co_moment += np.outer(delta, delta2) * (n - 1) / n

        if not self.hist_range_set and self.count >= batch_size:
            for c in range(self.hist_cols):
                self.hist_low[c] = float(np.min(batch[:, c])) - 0.5
                self.hist_high[c] = float(np.max(batch[:, c])) + 0.5
            self.hist_range_set = True
        if self.hist_range_set:
            for c in range(self.hist_cols):
                low, high = self.hist_low[c], self.hist_high[c]
                bin_width = (high - low) / self.hist_bins
                if bin_width > 0:
                    indices = ((batch[:, c] - low) / bin_width).astype(np.int64)
                    indices = np.clip(indices, 0, self.hist_bins - 1)
                    for idx in indices:
                        self.histograms[c, idx] += 1

    def _update_batch_python(self, batch):
        for row in batch:
            self.count += 1
            n = self.count
            delta = [row[c] - self.mean[c] for c in range(self.num_cols)]
            for c in range(self.num_cols):
                self.mean[c] += delta[c] / n
            delta2 = [row[c] - self.mean[c] for c in range(self.num_cols)]
            for c in range(self.num_cols):
                self.m2[c] += delta[c] * delta2[c]
                self.col_min[c] = min(self.col_min[c], row[c])
                self.col_max[c] = max(self.col_max[c], row[c])
            top = min(self.hist_cols, self.num_cols)
            for i in range(top):
                for j in range(i, top):
                    self.co_moment[i][j] += delta[i] * delta2[j] * (n - 1) / n

        if not self.hist_range_set and self.count >= len(batch):
            for c in range(self.hist_cols):
                col_vals = [r[c] for r in batch]
                self.hist_low[c] = min(col_vals) - 0.5
                self.hist_high[c] = max(col_vals) + 0.5
            self.hist_range_set = True
        if self.hist_range_set:
            for c in range(self.hist_cols):
                low, high = self.hist_low[c], self.hist_high[c]
                bin_width = (high - low) / self.hist_bins
                if bin_width > 0:
                    for r in batch:
                        idx = int((r[c] - low) / bin_width)
                        idx = max(0, min(self.hist_bins - 1, idx))
                        self.histograms[c][idx] += 1

    def _take_snapshot(self):
        if HAS_NUMPY:
            variance = self.m2 / max(1, self.count - 1)
            snapshot = {
                'count': self.count, 'batches': self.batches_processed,
                'mean_norm': float(np.linalg.norm(self.mean)),
                'var_mean': float(np.mean(variance)),
            }
        else:
            variance = [self.m2[c] / max(1, self.count - 1) for c in range(self.num_cols)]
            snapshot = {
                'count': self.count, 'batches': self.batches_processed,
                'mean_norm': math.sqrt(sum(m * m for m in self.mean)),
                'var_mean': sum(variance) / len(variance),
            }
        self.window_snapshots.append(snapshot)

    def get_variance(self):
        if self.count < 2:
            return np.zeros(self.num_cols) if HAS_NUMPY else [0.0] * self.num_cols
        if HAS_NUMPY:
            return self.m2 / (self.count - 1)
        return [self.m2[c] / (self.count - 1) for c in range(self.num_cols)]

    def memory_usage_mb(self):
        if HAS_NUMPY:
            size = (self.mean.nbytes + self.m2.nbytes + self.col_min.nbytes +
                    self.col_max.nbytes + self.co_moment.nbytes + self.histograms.nbytes)
        else:
            size = (self.num_cols * 8 * 4 + self.num_cols * self.num_cols * 8 +
                    self.hist_cols * self.hist_bins * 8)
        size += len(self.window_snapshots) * 200
        return size / (1024 * 1024)


class DataSource:
    def __init__(self, num_rows, num_cols, batch_size=1000):
        self.num_rows = num_rows
        self.num_cols = num_cols
        self.batch_size = batch_size
        self.cursor = 0
        if HAS_NUMPY:
            np.random.seed(42)
            self.data = np.random.randn(num_rows, num_cols).astype(np.float64)
            for c in range(min(5, num_cols)):
                self.data[:, c] += np.linspace(0, 2, num_rows)
            if num_cols >= 4:
                self.data[:, 2] += 0.5 * self.data[:, 0]
                self.data[:, 3] += 0.3 * self.data[:, 1]
        else:
            import random
            random.seed(42)
            self.data = [[random.gauss(0, 1) for _ in range(num_cols)] for _ in range(num_rows)]

    def next_batch(self):
        end = min(self.cursor + self.batch_size, self.num_rows)
        batch = self.data[self.cursor:end]
        self.cursor = end
        if self.cursor >= self.num_rows:
            self.cursor = 0
        return batch

    def memory_usage_mb(self):
        if HAS_NUMPY:
            return self.data.nbytes / (1024 * 1024)
        return (self.num_rows * self.num_cols * 8) / (1024 * 1024)


def run_dataproc_workload(num_rows, num_cols, operations, batch_size, duration, working_dir):
    duration_str = f"{duration}s" if duration > 0 else "infinite"
    print(f"[DataProc] Starting streaming aggregation workload")
    print(f"[DataProc] Config: rows={num_rows}, cols={num_cols}, batch_size={batch_size}, duration={duration_str}")

    source = DataSource(num_rows, num_cols, batch_size)
    aggregator = StreamingAggregator(num_cols)
    print(f"[DataProc] Data source memory: {source.memory_usage_mb():.1f} MB")

    create_ready_signal(working_dir)

    start_time = time.time()
    last_report_time = start_time

    while True:
        if check_restore_complete(working_dir):
            elapsed = time.time() - start_time
            print(f"[DataProc] Restore detected - checkpoint_flag removed")
            print(f"[DataProc] === STATE SUMMARY (lost on restart) ===")
            print(f"[DataProc]   Records processed: {aggregator.count:,}")
            print(f"[DataProc]   Batches processed: {aggregator.batches_processed}")
            var = aggregator.get_variance()
            if HAS_NUMPY:
                print(f"[DataProc]   Running mean (first 5): {aggregator.mean[:5].tolist()}")
                print(f"[DataProc]   Running variance (first 5): {var[:5].tolist()}")
            else:
                print(f"[DataProc]   Running mean (first 5): {aggregator.mean[:5]}")
                print(f"[DataProc]   Running variance (first 5): {var[:5]}")
            print(f"[DataProc]   Correlation matrix: {num_cols}x{num_cols}")
            print(f"[DataProc]   Histograms: {aggregator.hist_cols} cols x {aggregator.hist_bins} bins")
            print(f"[DataProc]   Window snapshots: {len(aggregator.window_snapshots)}")
            print(f"[DataProc]   Aggregator memory: {aggregator.memory_usage_mb():.2f} MB")
            print(f"[DataProc]   ALL accumulated statistics LOST on restart")
            print(f"[DataProc] ==========================================")
            sys.exit(0)

        elapsed = time.time() - start_time
        if duration > 0 and elapsed >= duration:
            time.sleep(1)
            continue

        if operations > 0 and aggregator.batches_processed >= operations:
            time.sleep(1)
            continue

        batch = source.next_batch()
        aggregator.update_batch(batch)

        current_time = time.time()
        if current_time - last_report_time >= 5.0:
            elapsed = current_time - start_time
            rate = aggregator.count / elapsed
            var = aggregator.get_variance()
            if HAS_NUMPY:
                mean_norm = float(np.linalg.norm(aggregator.mean))
                var_mean = float(np.mean(var))
            else:
                mean_norm = math.sqrt(sum(m * m for m in aggregator.mean))
                var_mean = sum(var) / len(var)
            print(f"[DataProc] Batch {aggregator.batches_processed}: "
                  f"records={aggregator.count:,}, mean_norm={mean_norm:.4f}, "
                  f"var_mean={var_mean:.4f}, rate={rate:.0f} rec/s, elapsed={elapsed:.0f}s")
            last_report_time = current_time

        time.sleep(0.01)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--num-rows', type=int, default=1000000)
    parser.add_argument('--num-cols', type=int, default=50)
    parser.add_argument('--operations', type=int, default=0)
    parser.add_argument('--batch-size', type=int, default=1000)
    parser.add_argument('--duration', type=int, default=0)
    parser.add_argument('--working_dir', type=str, default='.')

    args = parser.parse_args()
    run_dataproc_workload(args.num_rows, args.num_cols, args.operations,
                          args.batch_size, args.duration, args.working_dir)


if __name__ == '__main__':
    main()
'''


class DataProcWorkload(BaseWorkload):
    """
    Streaming data processing workload.

    Simulates:
    - Streaming ETL pipelines (Flink/Spark Streaming)
    - Real-time analytics with online statistics
    - Continuous data quality monitoring

    Uses Welford's algorithm for numerically stable streaming aggregation.
    Accumulated state (running mean, variance, correlation matrix, histograms)
    is entirely in-memory and lost on restart.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.num_rows = config.get('num_rows', 1000000)
        self.num_cols = config.get('num_cols', 50)
        self.operations = config.get('operations', 0)
        self.batch_size = config.get('batch_size', 1000)
        self.duration = config.get('duration', 0)

    def get_standalone_script_name(self) -> str:
        return 'dataproc_standalone.py'

    def get_standalone_script_content(self) -> str:
        return DATAPROC_STANDALONE_SCRIPT

    def get_command(self) -> str:
        cmd = f"python3 {self.get_standalone_script_name()}"
        cmd += f" --num-rows {self.num_rows}"
        cmd += f" --num-cols {self.num_cols}"
        cmd += f" --operations {self.operations}"
        cmd += f" --batch-size {self.batch_size}"
        cmd += f" --duration {self.duration}"
        cmd += f" --working_dir {self.working_dir}"
        return cmd

    def get_dependencies(self) -> list[str]:
        return ['numpy']  # pandas is optional

    def validate_config(self) -> bool:
        if self.num_rows <= 0:
            raise ValueError(f"num_rows must be positive")
        if self.num_cols <= 0:
            raise ValueError(f"num_cols must be positive")
        if self.batch_size <= 0:
            raise ValueError(f"batch_size must be positive")
        return True

    def estimate_memory_mb(self) -> float:
        # Data source + aggregator state
        data_mb = (self.num_rows * self.num_cols * 8) / (1024 * 1024)
        agg_mb = (self.num_cols * self.num_cols * 8) / (1024 * 1024)  # correlation matrix
        return data_mb + agg_mb


WorkloadFactory.register('dataproc', DataProcWorkload)
