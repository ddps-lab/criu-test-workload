#!/usr/bin/env python3
"""
Streaming Data Processing Standalone Workload

This script performs streaming statistical aggregation on synthetic data,
simulating real-time ETL/analytics pipelines. State accumulates incrementally
using Welford's online algorithm and incremental correlation computation.

Usage:
    python3 dataproc_standalone.py --num-rows 1000000 --num-cols 50 --duration 3600

Checkpoint Protocol:
    1. Creates 'checkpoint_ready' file after initial data load
    2. Checks 'checkpoint_flag' to detect restore completion
    3. Exits gracefully when checkpoint_flag is removed

State accumulated in memory (lost on restart):
    - Running mean/variance per column (Welford's algorithm)
    - Incremental correlation matrix
    - Histogram bins per column
    - Window statistics history
    - Total records processed count

Scenario:
    - Streaming ETL pipelines (Flink/Spark Streaming)
    - Real-time analytics dashboards
    - Continuous data quality monitoring
    - Online statistics computation
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


class StreamingAggregator:
    """
    Streaming statistical aggregator using online algorithms.

    Accumulates statistics incrementally without storing raw data:
    - Welford's algorithm for numerically stable mean/variance
    - Incremental correlation matrix via co-moment updates
    - Fixed-bin histograms per column
    - Periodic window snapshots for trend analysis

    All state is in-memory and lost on restart.
    """

    def __init__(self, num_cols: int, hist_bins: int = 50, hist_cols: int = 10):
        self.num_cols = num_cols
        self.hist_bins = hist_bins
        self.hist_cols = min(hist_cols, num_cols)

        # Welford's online statistics (per column)
        self.count = 0
        if HAS_NUMPY:
            self.mean = np.zeros(num_cols, dtype=np.float64)
            self.m2 = np.zeros(num_cols, dtype=np.float64)
            self.col_min = np.full(num_cols, np.inf, dtype=np.float64)
            self.col_max = np.full(num_cols, -np.inf, dtype=np.float64)
        else:
            self.mean = [0.0] * num_cols
            self.m2 = [0.0] * num_cols
            self.col_min = [float('inf')] * num_cols
            self.col_max = [float('-inf')] * num_cols

        # Incremental correlation matrix (upper triangle of co-moment matrix)
        # C[i,j] = sum((x_i - mean_i) * (x_j - mean_j))
        if HAS_NUMPY:
            self.co_moment = np.zeros((num_cols, num_cols), dtype=np.float64)
        else:
            self.co_moment = [[0.0] * num_cols for _ in range(num_cols)]

        # Histograms for top columns (estimated range, fixed bins)
        # Range estimated from first batch, then fixed
        self.hist_range_set = False
        if HAS_NUMPY:
            self.hist_low = np.zeros(self.hist_cols, dtype=np.float64)
            self.hist_high = np.zeros(self.hist_cols, dtype=np.float64)
            self.histograms = np.zeros((self.hist_cols, hist_bins), dtype=np.int64)
        else:
            self.hist_low = [0.0] * self.hist_cols
            self.hist_high = [0.0] * self.hist_cols
            self.histograms = [[0] * hist_bins for _ in range(self.hist_cols)]

        # Window statistics history (snapshot every N batches)
        self.window_snapshots = []
        self.batches_processed = 0

    def update_batch(self, batch):
        """
        Update streaming statistics with a new batch of data.

        Uses Welford's online algorithm for numerically stable updates.
        batch: numpy array of shape (batch_size, num_cols) or list of lists
        """
        if HAS_NUMPY:
            self._update_batch_numpy(batch)
        else:
            self._update_batch_python(batch)

        self.batches_processed += 1

        # Snapshot every 10 batches
        if self.batches_processed % 10 == 0:
            self._take_snapshot()

    def _update_batch_numpy(self, batch):
        """NumPy-accelerated batch update."""
        batch_size = batch.shape[0]

        for row_idx in range(batch_size):
            row = batch[row_idx]
            self.count += 1
            n = self.count

            # Welford's update
            delta = row - self.mean
            self.mean += delta / n
            delta2 = row - self.mean
            self.m2 += delta * delta2

            # Min/max
            np.minimum(self.col_min, row, out=self.col_min)
            np.maximum(self.col_max, row, out=self.col_max)

            # Co-moment matrix update (Chan's parallel algorithm, one-sample case)
            # C[i,j] += (n-1)/n * delta_i * delta_j  (before mean update)
            # Simplified: C += outer(delta, delta2)
            self.co_moment += np.outer(delta, delta2) * (n - 1) / n

        # Histogram update
        if not self.hist_range_set and self.count >= batch_size:
            # Set histogram range from first batch statistics
            for c in range(self.hist_cols):
                col_data = batch[:, c]
                self.hist_low[c] = float(np.min(col_data)) - 0.5
                self.hist_high[c] = float(np.max(col_data)) + 0.5
            self.hist_range_set = True

        if self.hist_range_set:
            for c in range(self.hist_cols):
                col_data = batch[:, c]
                low, high = self.hist_low[c], self.hist_high[c]
                bin_width = (high - low) / self.hist_bins
                if bin_width > 0:
                    indices = ((col_data - low) / bin_width).astype(np.int64)
                    indices = np.clip(indices, 0, self.hist_bins - 1)
                    for idx in indices:
                        self.histograms[c, idx] += 1

    def _update_batch_python(self, batch):
        """Pure Python batch update (fallback)."""
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

            # Co-moment (simplified for top columns only due to O(n^2))
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
        """Take a window snapshot of current statistics."""
        if HAS_NUMPY:
            variance = self.m2 / max(1, self.count - 1)
            snapshot = {
                'count': self.count,
                'batches': self.batches_processed,
                'mean_norm': float(np.linalg.norm(self.mean)),
                'var_mean': float(np.mean(variance)),
                'var_std': float(np.std(variance)),
            }
        else:
            variance = [self.m2[c] / max(1, self.count - 1) for c in range(self.num_cols)]
            mean_norm = math.sqrt(sum(m * m for m in self.mean))
            snapshot = {
                'count': self.count,
                'batches': self.batches_processed,
                'mean_norm': mean_norm,
                'var_mean': sum(variance) / len(variance),
            }
        self.window_snapshots.append(snapshot)

    def get_variance(self):
        """Get current variance estimates."""
        if self.count < 2:
            if HAS_NUMPY:
                return np.zeros(self.num_cols)
            return [0.0] * self.num_cols
        if HAS_NUMPY:
            return self.m2 / (self.count - 1)
        return [self.m2[c] / (self.count - 1) for c in range(self.num_cols)]

    def get_correlation_matrix(self):
        """Compute correlation matrix from co-moment and variance."""
        if self.count < 2:
            if HAS_NUMPY:
                return np.eye(self.num_cols)
            return [[1.0 if i == j else 0.0 for j in range(self.num_cols)] for i in range(self.num_cols)]

        var = self.get_variance()
        if HAS_NUMPY:
            std = np.sqrt(var)
            std[std == 0] = 1.0
            corr = self.co_moment / np.outer(std, std) / max(1, self.count - 1)
            np.fill_diagonal(corr, 1.0)
            return corr
        else:
            std = [math.sqrt(v) if v > 0 else 1.0 for v in var]
            n = min(self.hist_cols, self.num_cols)
            corr = [[0.0] * n for _ in range(n)]
            for i in range(n):
                for j in range(n):
                    if i == j:
                        corr[i][j] = 1.0
                    elif j > i:
                        corr[i][j] = self.co_moment[i][j] / (std[i] * std[j]) / max(1, self.count - 1)
                    else:
                        corr[i][j] = corr[j][i]
            return corr

    def memory_usage_mb(self) -> float:
        """Estimate memory usage of accumulated state."""
        if HAS_NUMPY:
            size = (self.mean.nbytes + self.m2.nbytes +
                    self.col_min.nbytes + self.col_max.nbytes +
                    self.co_moment.nbytes + self.histograms.nbytes)
        else:
            size = (self.num_cols * 8 * 4 +  # mean, m2, min, max
                    self.num_cols * self.num_cols * 8 +  # co_moment
                    self.hist_cols * self.hist_bins * 8)  # histograms
        # Add snapshot history
        size += len(self.window_snapshots) * 200  # rough estimate per snapshot
        return size / (1024 * 1024)


class DataSource:
    """
    Synthetic data source that generates batches in a streaming fashion.

    Simulates a continuous data stream with deterministic but varying patterns.
    Uses a cursor to track position, wrapping around when data is exhausted.
    """

    def __init__(self, num_rows: int, num_cols: int, batch_size: int = 1000):
        self.num_rows = num_rows
        self.num_cols = num_cols
        self.batch_size = batch_size
        self.cursor = 0

        # Generate full dataset (represents data "on disk" or incoming stream)
        print(f"[DataProc] Generating data source ({num_rows} rows x {num_cols} cols)...")
        init_start = time.time()
        if HAS_NUMPY:
            np.random.seed(42)
            self.data = np.random.randn(num_rows, num_cols).astype(np.float64)
            # Add some structure: trends, correlations
            for c in range(min(5, num_cols)):
                self.data[:, c] += np.linspace(0, 2, num_rows)  # trend
            if num_cols >= 4:
                self.data[:, 2] += 0.5 * self.data[:, 0]  # correlation
                self.data[:, 3] += 0.3 * self.data[:, 1]
        else:
            import random
            random.seed(42)
            self.data = [[random.gauss(0, 1) for _ in range(num_cols)] for _ in range(num_rows)]

        init_time = time.time() - init_start
        mem_mb = (num_rows * num_cols * 8) / (1024 * 1024)
        print(f"[DataProc] Data source ready in {init_time:.2f}s ({mem_mb:.1f} MB)")

    def next_batch(self):
        """Get next batch of data, wrapping around at the end."""
        end = min(self.cursor + self.batch_size, self.num_rows)
        if HAS_NUMPY:
            batch = self.data[self.cursor:end]
        else:
            batch = self.data[self.cursor:end]
        self.cursor = end
        if self.cursor >= self.num_rows:
            self.cursor = 0  # wrap around
        return batch

    def memory_usage_mb(self) -> float:
        if HAS_NUMPY:
            return self.data.nbytes / (1024 * 1024)
        return (self.num_rows * self.num_cols * 8) / (1024 * 1024)


def run_dataproc_workload(
    num_rows: int = 1000000,
    num_cols: int = 50,
    operations: int = 0,  # 0 = infinite (use duration)
    batch_size: int = 1000,
    duration: int = 0,  # 0 = infinite (use operations limit)
    working_dir: str = '.'
):
    """
    Streaming data processing workload.

    Processes data in batches through a StreamingAggregator that accumulates
    statistics using Welford's online algorithm. All accumulated state
    (running mean, variance, correlation matrix, histograms) is in-memory
    and lost on restart.

    Args:
        num_rows: Number of rows in data source
        num_cols: Number of columns in data source
        operations: Max batch operations (0 for infinite)
        batch_size: Rows per batch
        duration: Duration in seconds (0 for infinite)
        working_dir: Working directory for signal files
    """
    if not HAS_NUMPY:
        print("[DataProc] WARNING: NumPy not available, using pure Python (slow)")

    duration_str = f"{duration}s" if duration > 0 else "infinite"
    print(f"[DataProc] Starting streaming aggregation workload")
    print(f"[DataProc] Config: rows={num_rows}, cols={num_cols}, batch_size={batch_size}, duration={duration_str}")
    print(f"[DataProc] Working directory: {working_dir}")

    # Initialize data source and aggregator
    source = DataSource(num_rows, num_cols, batch_size)
    aggregator = StreamingAggregator(num_cols)

    total_mem = source.memory_usage_mb()
    print(f"[DataProc] Data source memory: {total_mem:.1f} MB")

    # Signal ready for checkpoint
    create_ready_signal(working_dir)

    start_time = time.time()
    last_report_time = start_time

    while True:
        # Check if restore completed
        if check_restore_complete(working_dir):
            elapsed = time.time() - start_time
            print(f"[DataProc] Restore detected - checkpoint_flag removed")
            print(f"[DataProc] === STATE SUMMARY (lost on restart) ===")
            print(f"[DataProc]   Records processed: {aggregator.count:,}")
            print(f"[DataProc]   Batches processed: {aggregator.batches_processed}")
            print(f"[DataProc]   Data source cursor: {source.cursor}")

            var = aggregator.get_variance()
            if HAS_NUMPY:
                print(f"[DataProc]   Running mean (first 5): {aggregator.mean[:5].tolist()}")
                print(f"[DataProc]   Running variance (first 5): {var[:5].tolist()}")
                corr = aggregator.get_correlation_matrix()
                print(f"[DataProc]   Correlation matrix: {num_cols}x{num_cols} ({corr.nbytes / 1024:.1f} KB)")
            else:
                print(f"[DataProc]   Running mean (first 5): {aggregator.mean[:5]}")
                print(f"[DataProc]   Running variance (first 5): {var[:5]}")

            print(f"[DataProc]   Histogram bins: {aggregator.hist_cols} cols x {aggregator.hist_bins} bins")
            print(f"[DataProc]   Window snapshots: {len(aggregator.window_snapshots)}")
            print(f"[DataProc]   Aggregator memory: {aggregator.memory_usage_mb():.2f} MB")
            print(f"[DataProc]   Elapsed time: {elapsed:.1f}s")
            print(f"[DataProc]   ALL accumulated statistics LOST on restart")
            print(f"[DataProc] ==========================================")
            sys.exit(0)

        # Duration check
        elapsed = time.time() - start_time
        if duration > 0 and elapsed >= duration:
            time.sleep(1)
            continue

        # Operation limit check
        if operations > 0 and aggregator.batches_processed >= operations:
            time.sleep(1)
            continue

        # Process next batch
        batch = source.next_batch()
        aggregator.update_batch(batch)

        # Report every 5 seconds
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
                  f"records={aggregator.count:,}, "
                  f"mean_norm={mean_norm:.4f}, var_mean={var_mean:.4f}, "
                  f"rate={rate:.0f} rec/s, elapsed={elapsed:.0f}s")
            last_report_time = current_time

        # Small delay to control processing rate
        time.sleep(0.01)


def main():
    parser = argparse.ArgumentParser(
        description="Streaming data processing workload for CRIU checkpoint testing"
    )
    parser.add_argument(
        '--num-rows',
        type=int,
        default=1000000,
        help='Number of rows in data source (default: 1000000)'
    )
    parser.add_argument(
        '--num-cols',
        type=int,
        default=50,
        help='Number of columns in data source (default: 50)'
    )
    parser.add_argument(
        '--operations',
        type=int,
        default=0,
        help='Max batch operations, 0 for infinite (default: 0)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Rows per batch (default: 1000)'
    )
    parser.add_argument(
        '--duration',
        type=int,
        default=0,
        help='Duration in seconds (0 for infinite)'
    )
    parser.add_argument(
        '--working_dir',
        type=str,
        default='.',
        help='Working directory for signal files (default: current directory)'
    )

    args = parser.parse_args()

    run_dataproc_workload(
        num_rows=args.num_rows,
        num_cols=args.num_cols,
        operations=args.operations,
        batch_size=args.batch_size,
        duration=args.duration,
        working_dir=args.working_dir
    )


if __name__ == '__main__':
    main()
