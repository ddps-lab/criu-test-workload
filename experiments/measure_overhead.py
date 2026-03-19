#!/usr/bin/env python3
"""
Dirty Tracking Overhead Measurement

Measures the performance overhead of UFFD-WP dirty tracking on workload
performance. Compares against soft-dirty clear_refs (OoH SC'22 baseline).

Usage:
    # Quick test: baseline only
    python3 experiments/measure_overhead.py --workload redis --duration 15 \
        --repeats 1 --configs baseline

    # Full comparison: UFFD-WP vs soft-dirty
    python3 experiments/measure_overhead.py --workload redis \
        --ycsb-workload a --record-count 100000 \
        --duration 60 --repeats 3 \
        --output /tmp/overhead_redis.json

Experiment Configurations:
    baseline          - No tracker (pure workload performance)
    uffd-wp-100ms     - UFFD-WP write-protect, 100ms scan interval
    uffd-wp-500ms     - UFFD-WP write-protect, 500ms scan interval
    uffd-wp-1000ms    - UFFD-WP write-protect, 1000ms scan interval
    uffd-wp-5000ms    - UFFD-WP write-protect, 5000ms scan interval
    sd-clear-500ms    - Soft-dirty clear_refs (OoH), 500ms interval
    sd-clear-1000ms   - Soft-dirty clear_refs (OoH), 1000ms interval
    uffd-wp-setup-only - UFFD-WP with 60s interval (setup overhead only)
"""

import os
import sys
import argparse
import json
import subprocess
import signal
import time
import logging
import re
import math
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dirty_track_only import (
    STANDALONE_SCRIPTS,
    wait_for_checkpoint_ready,
    build_workload_cmd,
    TRACKER_PATHS,
)

logger = logging.getLogger(__name__)

# ─── Experiment configurations ────────────────────────────────────────────────

EXPERIMENT_CONFIGS = {
    'baseline': {'tracker_mode': None},

    'uffd-wp-100ms':  {'tracker_mode': 'uffd-wp',  'interval_ms': 100},
    'uffd-wp-500ms':  {'tracker_mode': 'uffd-wp',  'interval_ms': 500},
    'uffd-wp-1000ms': {'tracker_mode': 'uffd-wp',  'interval_ms': 1000},
    'uffd-wp-5000ms': {'tracker_mode': 'uffd-wp',  'interval_ms': 5000},

    'sd-clear-500ms':  {'tracker_mode': 'sd-clear', 'interval_ms': 500},
    'sd-clear-1000ms': {'tracker_mode': 'sd-clear', 'interval_ms': 1000},

    'uffd-wp-setup-only': {'tracker_mode': 'uffd-wp', 'interval_ms': 60000},
    "uffd-wp-1ms":    {"tracker_mode": "uffd-wp",  "interval_ms": 1},
    "sd-only-1ms":    {"tracker_mode": "sd-only",  "interval_ms": 1},
    "uffd-sync-1ms":  {"tracker_mode": "uffd-sync", "interval_ms": 1},

    # OoH (SC'22) comparison modes
    'sd-only-1000ms':   {'tracker_mode': 'sd-only',   'interval_ms': 1000},
    'uffd-sync-1000ms': {'tracker_mode': 'uffd-sync', 'interval_ms': 1000},
}

DEFAULT_CONFIGS = [
    'baseline',
    'uffd-wp-1000ms',
    'sd-only-1000ms',
    'uffd-sync-1000ms',
]


# ─── Tracker management ──────────────────────────────────────────────────────

def find_c_tracker():
    """Find the C dirty tracker binary."""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    tracker_path = os.path.join(base_dir, TRACKER_PATHS['c'])
    if os.path.exists(tracker_path):
        return tracker_path
    raise FileNotFoundError(
        f"C tracker not found at {tracker_path}. "
        f"Build it: cd {os.path.dirname(tracker_path)} && make"
    )


def start_tracker_for_mode(tracker_path, mode, pid, interval_ms,
                           duration_sec, output_path):
    """Start C tracker with flags appropriate for the given mode.

    Returns subprocess.Popen or None (for baseline).
    """
    if mode is None:
        return None

    cmd = ['sudo', tracker_path,
           '-p', str(pid),
           '-i', str(interval_ms),
           '-d', str(duration_sec + 10),  # extra margin
           '-w', 'overhead_test',
           '-Q']  # --no-output: skip dirty page data (saves memory for long runs)

    if mode == 'uffd-wp':
        pass  # default mode
    elif mode == 'sd-clear':
        cmd.extend(['-D', '-S'])
    elif mode == 'sd-only':
        cmd.append('--sd-only')
    elif mode == 'uffd-sync':
        cmd.append('--uffd-sync')
    else:
        raise ValueError(f"Unknown tracker mode: {mode}")

    logger.info(f"Starting tracker ({mode}): {' '.join(cmd)}")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return proc


def stop_tracker(tracker_proc):
    """Stop tracker process gracefully."""
    if tracker_proc is None or tracker_proc.poll() is not None:
        return
    try:
        tracker_proc.send_signal(signal.SIGTERM)
        tracker_proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        tracker_proc.kill()
        tracker_proc.wait(timeout=5)


# ─── Metric parsing ──────────────────────────────────────────────────────────

def parse_workload_metrics(stdout_text):
    """Parse metrics from workload stdout.

    Supports:
    1. YCSB output: [OVERALL], Throughput(ops/sec), 12345.67
    2. Generic metric: [METRIC] throughput 205.8 iter/s
    """
    metrics = {}

    for line in stdout_text.split('\n'):
        # 1. YCSB format
        for tag in ['[OVERALL]', '[READ]', '[UPDATE]', '[INSERT]', '[SCAN]',
                    '[READ-MODIFY-WRITE]']:
            idx = line.find(tag)
            if idx < 0:
                continue

            # Parse from the tag position: [TAG], MetricName, Value
            part = line[idx:]
            fields = [f.strip() for f in part.split(',')]
            if len(fields) < 3:
                continue

            tag_name = fields[0].strip('[]')
            metric_name = fields[1]
            try:
                value = float(fields[2])
            except ValueError:
                continue

            if tag_name == 'OVERALL' and 'Throughput' in metric_name:
                metrics['throughput_ops'] = value
            elif tag_name == 'OVERALL' and 'RunTime' in metric_name:
                metrics['runtime_ms'] = value
            elif 'AverageLatency' in metric_name:
                key = f'{tag_name.lower()}_avg_latency_us'
                metrics[key] = value
            elif '95thPercentileLatency' in metric_name:
                key = f'{tag_name.lower()}_p95_latency_us'
                metrics[key] = value
            elif '99thPercentileLatency' in metric_name:
                key = f'{tag_name.lower()}_p99_latency_us'
                metrics[key] = value

        # 2. Generic [METRIC] format (e.g. from matmul, xgboost)
        if '[METRIC]' in line:
            idx = line.find('[METRIC]')
            part = line[idx + len('[METRIC]'):].strip()
            tokens = part.split()
            if len(tokens) >= 2 and tokens[0] == 'throughput':
                try:
                    metrics['throughput_ops'] = float(tokens[1])
                    if len(tokens) >= 3:
                        metrics['throughput_unit'] = tokens[2]
                except ValueError:
                    pass

    return metrics


# ─── Process discovery ───────────────────────────────────────────────────────

def _find_server_pid(workload_name, wrapper_pid):
    """Find the server process PID for Redis/Memcached.

    The server may be a child of the wrapper or started via setsid.
    Returns server PID or None.
    """
    if workload_name == 'redis':
        server_name = 'redis-server'
    elif workload_name == 'memcached':
        server_name = 'memcached'
    else:
        return None

    # Try pgrep to find the server process
    try:
        result = subprocess.run(
            ['pgrep', '-f', server_name],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            pids = [int(p) for p in result.stdout.strip().split('\n') if p.strip()]
            if len(pids) == 1:
                return pids[0]
            # Multiple PIDs: find the one most recently started (highest PID)
            if pids:
                return max(pids)
    except Exception:
        pass

    return None


# ─── Experiment execution ────────────────────────────────────────────────────

def cleanup_between_runs(working_dir, workload_name):
    """Clean up between experiment runs.

    Kills leftover workload processes and cleans signal files.
    """
    for fname in ['checkpoint_ready', 'checkpoint_flag']:
        path = os.path.join(working_dir, fname)
        try:
            os.remove(path)
        except OSError:
            pass

    # Kill leftover workload-specific processes
    if workload_name in ('redis',):
        # Shut down any redis via redis-cli, then ensure systemd service is stopped
        subprocess.run(['redis-cli', 'shutdown', 'nosave'],
                       capture_output=True, timeout=5, check=False)
        subprocess.run(['sudo', 'systemctl', 'stop', 'redis-server'],
                       capture_output=True, timeout=5, check=False)
    elif workload_name in ('memcached',):
        subprocess.run(['sudo', 'systemctl', 'stop', 'memcached'],
                       capture_output=True, timeout=5, check=False)

    # Kill leftover YCSB Java processes
    subprocess.run(['pkill', '-f', 'site.ycsb'],
                   capture_output=True, timeout=5, check=False)

    time.sleep(3)


def run_single_experiment(args, experiment_name, experiment_config,
                          duration, working_dir, tracker_path, repeat_idx):
    """Run a single experiment and return metrics.

    Returns dict with 'config_name', 'repeat', 'metrics', or None on failure.
    """
    run_dir = os.path.join(working_dir, f'{experiment_name}_r{repeat_idx}')
    os.makedirs(run_dir, exist_ok=True)

    # Clean previous state
    for fname in ['checkpoint_ready', 'checkpoint_flag']:
        try:
            os.remove(os.path.join(run_dir, fname))
        except OSError:
            pass

    # Create checkpoint_flag so workload keeps running
    flag_path = os.path.join(run_dir, 'checkpoint_flag')
    with open(flag_path, 'w') as f:
        f.write('flag')

    # Build workload command (reuse dirty_track_only's build_workload_cmd)
    # Set workload_duration = duration so YCSB maxexecutiontime matches measurement.
    # YCSB will finish naturally after duration seconds → print results.
    # Then we remove checkpoint_flag so the wrapper exits.
    saved_duration = getattr(args, 'workload_duration', None)
    args.workload_duration = duration
    try:
        cmd = build_workload_cmd(args, run_dir)
    except (ValueError, FileNotFoundError) as e:
        logger.error(f"Failed to build workload command: {e}")
        args.workload_duration = saved_duration
        return None
    args.workload_duration = saved_duration

    # Start workload with stdout redirected to file
    stdout_path = os.path.join(run_dir, 'workload_stdout.txt')
    stdout_file = open(stdout_path, 'w')

    logger.info(f"  Starting workload: {' '.join(cmd[:6])}...")
    workload_proc = subprocess.Popen(
        cmd,
        stdout=stdout_file,
        stderr=subprocess.PIPE,
        start_new_session=True
    )

    tracker_proc = None
    try:
        # Wait for workload to be ready
        try:
            workload_pid = wait_for_checkpoint_ready(run_dir, timeout=120)
        except TimeoutError as e:
            logger.error(f"  Workload failed to start: {e}")
            return None

        logger.info(f"  Workload ready (PID: {workload_pid})")

        # Start tracker (None for baseline)
        tracker_mode = experiment_config.get('tracker_mode')
        interval_ms = experiment_config.get('interval_ms', 1000)

        # For Redis/Memcached: track the server PID directly, not wrapper.
        # This avoids sd-clear crashing YCSB Java process.
        tracker_pid = workload_pid
        if args.workload in ('redis', 'memcached') and tracker_mode:
            server_pid = _find_server_pid(args.workload, workload_pid)
            if server_pid:
                tracker_pid = server_pid
                logger.info(f"  Tracking server PID {tracker_pid} (not wrapper {workload_pid})")

        if tracker_mode and tracker_path:
            tracker_output = os.path.join(run_dir, 'tracker_output.json')
            tracker_proc = start_tracker_for_mode(
                tracker_path, tracker_mode, tracker_pid,
                interval_ms, duration, tracker_output
            )
            # Give tracker time to set up (especially uffd-wp ptrace injection)
            time.sleep(3)

            # Check tracker is still alive
            if tracker_proc.poll() is not None:
                stderr = tracker_proc.stderr.read().decode()
                logger.error(f"  Tracker exited early: {stderr[:200]}")
                return None

        # Wait for YCSB to finish naturally (duration + margin for YCSB shutdown)
        # YCSB maxexecutiontime = duration, so it should finish around that time.
        # We wait extra to let the workload print results before we kill it.
        wait_time = duration + 15
        logger.info(f"  Measuring for {duration}s (waiting {wait_time}s for YCSB finish)...")
        start_time = time.time()
        while time.time() - start_time < wait_time:
            if workload_proc.poll() is not None:
                # Workload exited on its own (shouldn't happen while flag exists)
                break
            time.sleep(1)

        # Stop tracker before removing flag (so tracker covers full duration)
        stop_tracker(tracker_proc)

        # Now remove checkpoint_flag to let workload exit
        try:
            os.remove(flag_path)
        except OSError:
            pass

        # Wait for workload to finish
        try:
            workload_proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            logger.warning(f"  Workload did not exit, terminating...")
            workload_proc.terminate()
            try:
                workload_proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                workload_proc.kill()

        # Close stdout file and read it
        stdout_file.close()
        stdout_file = None

        with open(stdout_path, 'r') as f:
            stdout_text = f.read()

        # Parse metrics
        metrics = parse_workload_metrics(stdout_text)
        if not metrics:
            logger.warning(f"  No metrics found in output")
            # Log first few lines for debugging
            lines = stdout_text.strip().split('\n')
            for line in lines[:10]:
                logger.debug(f"    stdout: {line}")

        return {
            'config_name': experiment_name,
            'repeat': repeat_idx,
            'metrics': metrics,
        }

    except Exception as e:
        logger.error(f"  Experiment failed: {e}")
        return None

    finally:
        # Ensure cleanup
        if stdout_file and not stdout_file.closed:
            stdout_file.close()

        stop_tracker(tracker_proc)

        try:
            os.remove(flag_path)
        except OSError:
            pass

        if workload_proc.poll() is None:
            # Kill entire process group (workload + redis/memcached children)
            try:
                os.killpg(os.getpgid(workload_proc.pid), signal.SIGTERM)
            except (OSError, ProcessLookupError):
                pass
            try:
                workload_proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(os.getpgid(workload_proc.pid), signal.SIGKILL)
                except (OSError, ProcessLookupError):
                    pass
                workload_proc.kill()


# ─── Statistics ──────────────────────────────────────────────────────────────

def compute_statistics(results_by_config, baseline_key='baseline'):
    """Compute mean/std/overhead for each config.

    Args:
        results_by_config: dict of config_name -> list of metric dicts
        baseline_key: name of the baseline config

    Returns:
        dict of config_name -> {'stats': {metric: {mean, std, min, max}},
                                'overhead_pct': {metric: pct}}
    """
    stats = {}

    # Compute baseline stats first
    baseline_stats = {}
    if baseline_key in results_by_config:
        baseline_metrics = results_by_config[baseline_key]
        for key in _all_metric_keys(baseline_metrics):
            values = [m[key] for m in baseline_metrics if key in m]
            if values:
                baseline_stats[key] = {
                    'mean': sum(values) / len(values),
                    'std': _std(values),
                    'min': min(values),
                    'max': max(values),
                    'n': len(values),
                }

    for config_name, metrics_list in results_by_config.items():
        config_stats = {}
        overhead = {}

        for key in _all_metric_keys(metrics_list):
            values = [m[key] for m in metrics_list if key in m]
            if not values:
                continue
            config_stats[key] = {
                'mean': sum(values) / len(values),
                'std': _std(values),
                'min': min(values),
                'max': max(values),
                'n': len(values),
            }

            # Compute overhead vs baseline
            if key in baseline_stats and baseline_stats[key]['mean'] > 0:
                baseline_mean = baseline_stats[key]['mean']
                config_mean = config_stats[key]['mean']
                if 'throughput' in key:
                    # Higher is better → overhead = how much slower
                    overhead[key] = ((baseline_mean - config_mean) / baseline_mean) * 100
                elif 'latency' in key:
                    # Lower is better → overhead = how much higher
                    overhead[key] = ((config_mean - baseline_mean) / baseline_mean) * 100

        stats[config_name] = {
            'stats': config_stats,
            'overhead_pct': overhead,
        }

    return stats


def _all_metric_keys(metrics_list):
    """Get all numeric metric keys from a list of metric dicts."""
    keys = set()
    for m in metrics_list:
        for k, v in m.items():
            if isinstance(v, (int, float)):
                keys.add(k)
    return sorted(keys)


def _std(values):
    """Compute standard deviation."""
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
    return math.sqrt(variance)


# ─── Output formatting ──────────────────────────────────────────────────────

def format_results_table(stats, config_order, throughput_unit=None):
    """Format results as an ASCII table."""
    # Determine which metrics to show
    has_throughput = any(
        'throughput_ops' in stats[c].get('stats', {})
        for c in config_order if c in stats
    )
    has_read_latency = any(
        'read_avg_latency_us' in stats[c].get('stats', {})
        for c in config_order if c in stats
    )
    has_update_latency = any(
        'update_avg_latency_us' in stats[c].get('stats', {})
        for c in config_order if c in stats
    )

    lines = []
    lines.append("")
    lines.append("=" * 90)
    lines.append("Overhead Measurement Results")
    lines.append("=" * 90)

    # Header — use dynamic throughput unit if provided
    throughput_label = f"Throughput ({throughput_unit})" if throughput_unit else "Throughput (ops/s)"
    header = f"{'Configuration':<22}"
    if has_throughput:
        header += f"{throughput_label:>20}"
    if has_read_latency:
        header += f"{'Read Lat (μs)':>18}"
    if has_update_latency:
        header += f"{'Update Lat (μs)':>18}"
    header += f"{'Overhead':>12}"
    lines.append(header)
    lines.append("-" * 90)

    for config_name in config_order:
        if config_name not in stats:
            continue

        s = stats[config_name]
        config_stats = s.get('stats', {})
        overhead_pct = s.get('overhead_pct', {})

        row = f"{config_name:<22}"

        if has_throughput:
            if 'throughput_ops' in config_stats:
                t = config_stats['throughput_ops']
                row += f"{t['mean']:>12.0f} ± {t['std']:>5.0f}"
            else:
                row += f"{'N/A':>20}"

        if has_read_latency:
            if 'read_avg_latency_us' in config_stats:
                r = config_stats['read_avg_latency_us']
                row += f"{r['mean']:>11.0f} ± {r['std']:>4.0f}"
            else:
                row += f"{'N/A':>18}"

        if has_update_latency:
            if 'update_avg_latency_us' in config_stats:
                u = config_stats['update_avg_latency_us']
                row += f"{u['mean']:>11.0f} ± {u['std']:>4.0f}"
            else:
                row += f"{'N/A':>18}"

        if config_name == 'baseline':
            row += f"{'baseline':>12}"
        elif 'throughput_ops' in overhead_pct:
            pct = overhead_pct['throughput_ops']
            row += f"{pct:>+11.2f}%"
        else:
            row += f"{'N/A':>12}"

        lines.append(row)

    lines.append("=" * 90)
    lines.append("Positive overhead % = slower than baseline")
    lines.append("")

    return '\n'.join(lines)


# ─── Main ────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description='Measure dirty tracking overhead on workload performance',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    # Core options
    parser.add_argument('--workload', '-w', type=str, default='redis',
                        choices=list(STANDALONE_SCRIPTS.keys()),
                        help='Workload type (default: redis)')
    parser.add_argument('--duration', '-d', type=int, default=60,
                        help='Duration of each experiment run in seconds (default: 60)')
    parser.add_argument('--repeats', '-r', type=int, default=3,
                        help='Number of repetitions per configuration (default: 3)')
    parser.add_argument('--configs', type=str, default=None,
                        help='Comma-separated list of experiment configs to run '
                             f'(default: all). Available: {",".join(DEFAULT_CONFIGS)}')
    parser.add_argument('--output', '-o', type=str, default=None,
                        help='Output JSON file for results')
    parser.add_argument('--working-dir', type=str, default=None,
                        help='Working directory (default: /tmp/measure_overhead_<pid>)')

    # ─── Workload-specific options (copied from dirty_track_only.py) ──────

    wl_group = parser.add_argument_group('Workload Options')
    wl_group.add_argument('--workload-duration', type=int, default=None,
                          help='Workload internal duration (managed automatically)')

    # Memory
    wl_group.add_argument('--mb-size', type=int, default=None,
                          help='Memory block size in MB (memory workload)')
    wl_group.add_argument('--max-memory', type=int, default=None,
                          help='Max memory in MB (memory workload)')
    wl_group.add_argument('--interval', type=float, default=None,
                          help='Allocation interval in seconds (memory workload)')

    # MatMul
    wl_group.add_argument('--matrix-size', type=int, default=None,
                          help='Matrix size NxN (matmul workload)')
    wl_group.add_argument('--iterations', type=int, default=None,
                          help='Number of iterations (matmul/dataproc)')

    # Redis
    wl_group.add_argument('--redis-port', type=int, default=None,
                          help='Redis port (redis workload)')
    wl_group.add_argument('--num-keys', type=int, default=None,
                          help='Number of keys (redis workload)')
    wl_group.add_argument('--value-size', type=int, default=None,
                          help='Value size in bytes (redis workload)')

    # ML Training
    wl_group.add_argument('--model-size', type=str, choices=['small', 'medium', 'large'],
                          default=None, help='Model size (ml_training workload)')
    wl_group.add_argument('--batch-size', type=int, default=None,
                          help='Batch size (ml_training workload)')
    wl_group.add_argument('--epochs', type=int, default=None,
                          help='Number of epochs (ml_training workload)')
    wl_group.add_argument('--learning-rate', type=float, default=None,
                          help='Learning rate (ml_training workload)')
    wl_group.add_argument('--dataset-size', type=int, default=None,
                          help='Dataset size (ml_training workload)')

    # Video
    wl_group.add_argument('--resolution', type=str, default=None,
                          help='Video resolution WxH (video workload)')
    wl_group.add_argument('--fps', type=int, default=None,
                          help='Frames per second (video workload)')

    # DataProc
    wl_group.add_argument('--num-rows', type=int, default=None,
                          help='Number of rows (dataproc workload)')
    wl_group.add_argument('--num-cols', type=int, default=None,
                          help='Number of columns (dataproc workload)')
    wl_group.add_argument('--operations', type=int, default=None,
                          help='Number of operations (dataproc workload)')

    # YCSB (shared by redis and memcached)
    wl_group.add_argument('--ycsb-workload', type=str, default=None,
                          choices=['a', 'b', 'c', 'd', 'e', 'f'],
                          help='YCSB workload type (redis/memcached)')
    wl_group.add_argument('--ycsb-home', type=str, default=None,
                          help='YCSB installation path (default: /opt/ycsb)')
    wl_group.add_argument('--record-count', type=int, default=None,
                          help='Number of YCSB records (redis/memcached)')
    wl_group.add_argument('--ycsb-threads', type=int, default=None,
                          help='YCSB client threads (redis/memcached)')
    wl_group.add_argument('--target-throughput', type=int, default=None,
                          help='YCSB target ops/sec, 0=unlimited (redis/memcached)')

    # XGBoost
    wl_group.add_argument('--xgb-dataset', type=str, default=None,
                          choices=['synthetic', 'covtype', 'higgs'],
                          help='XGBoost dataset (xgboost workload)')
    wl_group.add_argument('--xgb-dataset-path', type=str, default=None,
                          help='Path to dataset file (xgboost workload)')
    wl_group.add_argument('--xgb-num-samples', type=int, default=None,
                          help='Number of samples for synthetic (xgboost workload)')
    wl_group.add_argument('--xgb-num-features', type=int, default=None,
                          help='Number of features for synthetic (xgboost workload)')
    wl_group.add_argument('--xgb-num-rounds', type=int, default=None,
                          help='Max boosting rounds (xgboost workload)')
    wl_group.add_argument('--xgb-max-depth', type=int, default=None,
                          help='Tree max depth (xgboost workload)')
    wl_group.add_argument('--xgb-num-threads', type=int, default=None,
                          help='Number of threads (xgboost workload)')
    wl_group.add_argument('--seed', type=int, default=None,
                          help='Random seed (xgboost/7zip workload)')

    # Memcached
    wl_group.add_argument('--memcached-port', type=int, default=None,
                          help='Memcached port (memcached workload)')
    wl_group.add_argument('--memcached-memory', type=int, default=None,
                          help='Memcached memory in MB (memcached workload)')

    # 7zip
    wl_group.add_argument('--compression-level', type=int, default=None,
                          help='Compression level 1-9 (7zip workload)')
    wl_group.add_argument('--sevenzip-threads', type=int, default=None,
                          help='Compression threads (7zip workload)')
    wl_group.add_argument('--input-size-mb', type=int, default=None,
                          help='Input file size in MB (7zip workload)')

    # Logging
    parser.add_argument('--log-level', type=str, default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        help='Logging level')

    return parser.parse_args()


def main():
    args = parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # Determine configs to run
    if args.configs:
        selected_configs = [c.strip() for c in args.configs.split(',')]
        for c in selected_configs:
            if c not in EXPERIMENT_CONFIGS:
                logger.error(f"Unknown config: {c}. Available: {', '.join(EXPERIMENT_CONFIGS.keys())}")
                return 1
    else:
        selected_configs = DEFAULT_CONFIGS

    # Working directory
    if args.working_dir:
        working_dir = args.working_dir
    else:
        working_dir = f'/tmp/measure_overhead_{os.getpid()}'
    os.makedirs(working_dir, exist_ok=True)

    # Find C tracker (needed for non-baseline configs)
    tracker_path = None
    needs_tracker = any(
        EXPERIMENT_CONFIGS[c].get('tracker_mode') is not None
        for c in selected_configs
    )
    if needs_tracker:
        try:
            tracker_path = find_c_tracker()
            logger.info(f"C tracker: {tracker_path}")
        except FileNotFoundError as e:
            logger.error(str(e))
            return 1

    # Print experiment plan
    logger.info(f"Workload: {args.workload}")
    logger.info(f"Duration: {args.duration}s per run")
    logger.info(f"Repeats: {args.repeats}")
    logger.info(f"Configs: {', '.join(selected_configs)}")
    total_runs = len(selected_configs) * args.repeats
    total_time = total_runs * (args.duration + 10)  # rough estimate
    logger.info(f"Total runs: {total_runs} (estimated ~{total_time // 60}min)")

    # Run experiments — interleave configs across repeats to avoid cold-start bias.
    # Order: config1_r0 → config2_r0 → ... → configN_r0 → config1_r1 → ...
    results_by_config = {c: [] for c in selected_configs}

    for repeat in range(args.repeats):
        for config_idx, config_name in enumerate(selected_configs):
            experiment_config = EXPERIMENT_CONFIGS[config_name]
            run_num = repeat * len(selected_configs) + config_idx + 1

            logger.info(f"\n{'='*60}")
            logger.info(f"Run [{run_num}/{total_runs}]: {config_name} "
                         f"(repeat {repeat+1}/{args.repeats})")
            logger.info(f"  mode={experiment_config.get('tracker_mode', 'none')}, "
                         f"interval={experiment_config.get('interval_ms', 'N/A')}ms")
            logger.info(f"{'='*60}")

            # Cleanup between runs
            cleanup_between_runs(working_dir, args.workload)

            result = run_single_experiment(
                args, config_name, experiment_config,
                args.duration, working_dir, tracker_path, repeat
            )

            if result and result.get('metrics'):
                metrics = result['metrics']
                results_by_config[config_name].append(metrics)
                # Print quick summary
                if 'throughput_ops' in metrics:
                    unit = metrics.get('throughput_unit', 'ops/s')
                    logger.info(f"  => throughput={metrics['throughput_ops']:.0f} {unit}")
                if 'read_avg_latency_us' in metrics:
                    logger.info(f"  => read_latency={metrics['read_avg_latency_us']:.0f}μs")
            else:
                logger.warning(f"  => No metrics collected")

    # Compute statistics
    stats = compute_statistics(results_by_config)

    # Extract throughput_unit from results (if any non-YCSB workload)
    throughput_unit = None
    for metrics_list in results_by_config.values():
        for m in metrics_list:
            if 'throughput_unit' in m:
                throughput_unit = m['throughput_unit']
                break
        if throughput_unit:
            break

    # Print results table
    table = format_results_table(stats, selected_configs, throughput_unit=throughput_unit)
    print(table)

    # Build output data
    output_data = {
        'metadata': {
            'workload': args.workload,
            'duration': args.duration,
            'repeats': args.repeats,
            'configs': selected_configs,
            'timestamp': datetime.now().isoformat(),
        },
        'results': {},
        'summary_table': table,
    }

    for config_name in selected_configs:
        config_data = {
            'raw': results_by_config.get(config_name, []),
        }
        if config_name in stats:
            config_data['stats'] = stats[config_name]['stats']
            config_data['overhead_pct'] = stats[config_name]['overhead_pct']
        output_data['results'][config_name] = config_data

    # Save JSON output
    if args.output:
        os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
        with open(args.output, 'w') as f:
            json.dump(output_data, f, indent=2)
        logger.info(f"Results saved to {args.output}")
    else:
        # Print JSON to stderr so table goes to stdout
        print(json.dumps(output_data, indent=2), file=sys.stderr)

    # Cleanup working directory if auto-created
    if not args.working_dir:
        import shutil
        try:
            shutil.rmtree(working_dir, ignore_errors=True)
        except Exception:
            pass

    return 0


if __name__ == '__main__':
    sys.exit(main())
