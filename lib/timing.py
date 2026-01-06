"""
Metrics collection and timing utilities for CRIU experiments.
"""

import time
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field, asdict
import json


@dataclass
class TimingMetric:
    """Individual timing measurement."""
    name: str
    duration: float
    start_time: float
    end_time: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExperimentMetrics:
    """Complete metrics for a CRIU experiment."""
    experiment_name: str
    workload_type: str
    total_duration: float = 0.0
    pre_dump_iterations: List[TimingMetric] = field(default_factory=list)
    final_dump: Optional[TimingMetric] = None
    transfer: Optional[TimingMetric] = None
    restore: Optional[TimingMetric] = None
    custom_metrics: Dict[str, TimingMetric] = field(default_factory=dict)

    # Extended metadata
    timestamp: str = ""
    config: Dict[str, Any] = field(default_factory=dict)
    cli_args: Dict[str, Any] = field(default_factory=dict)
    nodes: Dict[str, str] = field(default_factory=dict)
    log_files: Dict[str, List[str]] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary."""
        result = asdict(self)
        # Convert TimingMetric objects to dicts
        result['pre_dump_iterations'] = [asdict(m) for m in self.pre_dump_iterations]
        if self.final_dump:
            result['final_dump'] = asdict(self.final_dump)
        if self.transfer:
            result['transfer'] = asdict(self.transfer)
        if self.restore:
            result['restore'] = asdict(self.restore)
        result['custom_metrics'] = {k: asdict(v) for k, v in self.custom_metrics.items()}
        return result

    def to_json(self, indent: int = 2) -> str:
        """Convert metrics to JSON string."""
        return json.dumps(self.to_dict(), indent=indent)

    def print_summary(self):
        """Print human-readable metrics summary."""
        print("\n" + "=" * 60)
        print(f"CRIU Experiment Metrics: {self.experiment_name}")
        print(f"Workload Type: {self.workload_type}")
        print("=" * 60)

        if self.pre_dump_iterations:
            total_predump_time = sum(m.duration for m in self.pre_dump_iterations)
            print(f"\nPre-dump Iterations: {len(self.pre_dump_iterations)}")
            print(f"  Total time: {total_predump_time:.2f}s")
            print(f"  Average time: {total_predump_time / len(self.pre_dump_iterations):.2f}s")
            for i, metric in enumerate(self.pre_dump_iterations, 1):
                print(f"  Iteration {i}: {metric.duration:.2f}s", end="")
                if 'rsync_duration' in metric.metadata:
                    print(f" (rsync: {metric.metadata['rsync_duration']:.2f}s)", end="")
                print()

        if self.final_dump:
            print(f"\nFinal Dump:")
            print(f"  Duration: {self.final_dump.duration:.2f}s")
            if 'rsync_duration' in self.final_dump.metadata:
                print(f"  Rsync: {self.final_dump.metadata['rsync_duration']:.2f}s")

        if self.transfer:
            print(f"\nTransfer:")
            print(f"  Method: {self.transfer.metadata.get('method', 'unknown')}")
            print(f"  Duration: {self.transfer.duration:.2f}s")
            if 'size_mb' in self.transfer.metadata:
                print(f"  Size: {self.transfer.metadata['size_mb']:.2f} MB")
                throughput = self.transfer.metadata['size_mb'] / self.transfer.duration
                print(f"  Throughput: {throughput:.2f} MB/s")

        if self.restore:
            print(f"\nRestore:")
            print(f"  Duration: {self.restore.duration:.2f}s")
            if 'lazy_pages' in self.restore.metadata:
                print(f"  Lazy pages: {self.restore.metadata['lazy_pages']}")

        if self.custom_metrics:
            print(f"\nCustom Metrics:")
            for name, metric in self.custom_metrics.items():
                print(f"  {name}: {metric.duration:.2f}s")

        print(f"\nTotal Experiment Duration: {self.total_duration:.2f}s")
        print("=" * 60 + "\n")


class MetricsCollector:
    """Collect and manage experiment metrics."""

    def __init__(self, experiment_name: str, workload_type: str):
        """
        Initialize metrics collector.

        Args:
            experiment_name: Name of the experiment
            workload_type: Type of workload being tested
        """
        from datetime import datetime

        self.metrics = ExperimentMetrics(
            experiment_name=experiment_name,
            workload_type=workload_type,
            timestamp=datetime.now().isoformat()
        )
        self.start_time = time.time()
        self._active_timers: Dict[str, float] = {}

    def set_config(self, config: Dict[str, Any]):
        """
        Store experiment configuration.

        Args:
            config: Full experiment configuration dictionary
        """
        # Store relevant config sections
        self.metrics.config = {
            'checkpoint': config.get('checkpoint', {}),
            'transfer': config.get('transfer', {}),
            'workload': config.get('workload', {}),
            'experiment': config.get('experiment', {})
        }

    def set_cli_args(self, args_dict: Dict[str, Any]):
        """
        Store CLI arguments used for the experiment.

        Args:
            args_dict: Dictionary of CLI arguments
        """
        # Filter out None values
        self.metrics.cli_args = {k: v for k, v in args_dict.items() if v is not None}

    def set_nodes(self, source: str, dest: str):
        """
        Store node information.

        Args:
            source: Source node IP
            dest: Destination node IP
        """
        self.metrics.nodes = {
            'source': source,
            'destination': dest
        }

    def set_log_files(self, log_result: Dict[str, Any]):
        """
        Store collected log file paths.

        Args:
            log_result: Result from collect_logs()
        """
        self.metrics.log_files = {
            'output_dir': log_result.get('output_dir', ''),
            'source': log_result.get('source', []),
            'dest': log_result.get('dest', [])
        }

    def start_timer(self, name: str):
        """
        Start a named timer.

        Args:
            name: Timer identifier
        """
        self._active_timers[name] = time.time()

    def stop_timer(self, name: str, metadata: Optional[Dict[str, Any]] = None) -> TimingMetric:
        """
        Stop a named timer and return the metric.

        Args:
            name: Timer identifier
            metadata: Additional metadata for this timing

        Returns:
            TimingMetric object

        Raises:
            KeyError: If timer was never started
        """
        if name not in self._active_timers:
            raise KeyError(f"Timer '{name}' was never started")

        start = self._active_timers.pop(name)
        end = time.time()
        duration = end - start

        return TimingMetric(
            name=name,
            duration=duration,
            start_time=start,
            end_time=end,
            metadata=metadata or {}
        )

    def record_pre_dump(self, iteration: int, duration: float, metadata: Optional[Dict[str, Any]] = None):
        """
        Record a pre-dump iteration.

        Args:
            iteration: Iteration number
            duration: Duration in seconds
            metadata: Additional metadata
        """
        metric = TimingMetric(
            name=f"pre_dump_{iteration}",
            duration=duration,
            start_time=time.time() - duration,
            end_time=time.time(),
            metadata=metadata or {}
        )
        self.metrics.pre_dump_iterations.append(metric)

    def record_final_dump(self, duration: float, metadata: Optional[Dict[str, Any]] = None):
        """
        Record final dump timing.

        Args:
            duration: Duration in seconds
            metadata: Additional metadata
        """
        self.metrics.final_dump = TimingMetric(
            name="final_dump",
            duration=duration,
            start_time=time.time() - duration,
            end_time=time.time(),
            metadata=metadata or {}
        )

    def record_transfer(self, duration: float, method: str, metadata: Optional[Dict[str, Any]] = None):
        """
        Record transfer timing.

        Args:
            duration: Duration in seconds
            method: Transfer method (rsync, s3, efs)
            metadata: Additional metadata
        """
        meta = metadata or {}
        meta['method'] = method
        self.metrics.transfer = TimingMetric(
            name="transfer",
            duration=duration,
            start_time=time.time() - duration,
            end_time=time.time(),
            metadata=meta
        )

    def record_restore(self, duration: float, metadata: Optional[Dict[str, Any]] = None):
        """
        Record restore timing.

        Args:
            duration: Duration in seconds
            metadata: Additional metadata
        """
        self.metrics.restore = TimingMetric(
            name="restore",
            duration=duration,
            start_time=time.time() - duration,
            end_time=time.time(),
            metadata=metadata or {}
        )

    def add_custom_metric(self, name: str, duration: float, metadata: Optional[Dict[str, Any]] = None):
        """
        Add a custom metric.

        Args:
            name: Metric name
            duration: Duration in seconds
            metadata: Additional metadata
        """
        self.metrics.custom_metrics[name] = TimingMetric(
            name=name,
            duration=duration,
            start_time=time.time() - duration,
            end_time=time.time(),
            metadata=metadata or {}
        )

    def finalize(self) -> ExperimentMetrics:
        """
        Finalize metrics collection and calculate total duration.

        Returns:
            Complete ExperimentMetrics object
        """
        self.metrics.total_duration = time.time() - self.start_time
        return self.metrics

    def save_to_file(self, filepath: str):
        """
        Save metrics to JSON file.

        Args:
            filepath: Output file path
        """
        self.finalize()
        with open(filepath, 'w') as f:
            f.write(self.metrics.to_json())


class Timer:
    """Context manager for timing code blocks."""

    def __init__(self, collector: MetricsCollector, name: str, metadata: Optional[Dict[str, Any]] = None):
        """
        Initialize timer context manager.

        Args:
            collector: MetricsCollector instance
            name: Timer name
            metadata: Additional metadata
        """
        self.collector = collector
        self.name = name
        self.metadata = metadata

    def __enter__(self):
        """Start timer."""
        self.collector.start_timer(self.name)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop timer and record metric."""
        metric = self.collector.stop_timer(self.name, self.metadata)
        self.collector.add_custom_metric(self.name, metric.duration, self.metadata)
