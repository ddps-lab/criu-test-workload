#!/usr/bin/env python3
"""
Dirty Page Tracking Only (No Migration)

Run a workload locally and track dirty pages without performing any
CRIU checkpoint/migration. Useful for analyzing workload memory behavior
independently.

Usage:
    # Track matmul workload for 30 seconds
    python3 experiments/dirty_track_only.py --workload matmul --duration 30

    # Track with custom interval and output
    python3 experiments/dirty_track_only.py --workload memory --duration 60 \
        --dirty-track-interval 200 --output dirty_memory.json

    # Use no-clear mode (accumulate dirty pages)
    python3 experiments/dirty_track_only.py --workload matmul --duration 30 \
        --dirty-no-clear --output dirty_matmul.json
"""

import os
import sys
import argparse
import json
import signal
import subprocess
import time
import threading
import logging

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.dirty_tracker import DirtyPageTracker

logger = logging.getLogger(__name__)

# External tracker binary paths (relative to criu_workload/)
TRACKER_PATHS = {
    'c': 'tools/dirty_tracker_c/dirty_tracker',
    'go': 'tools/dirty_tracker_go/dirty_tracker',
    'python': 'tools/dirty_tracker.py',
}

# Workload standalone script mapping
STANDALONE_SCRIPTS = {
    'memory': 'workloads/memory_standalone.py',
    'matmul': 'workloads/matmul_standalone.py',
    'redis': 'workloads/redis_standalone.py',
    'ml_training': 'workloads/ml_training_standalone.py',
    'video': 'workloads/video_standalone.py',
    'dataproc': 'workloads/dataproc_standalone.py',
    'xgboost': 'workloads/xgboost_standalone.py',
    'memcached': 'workloads/memcached_standalone.py',
    '7zip': 'workloads/sevenzip_standalone.py',
    'memwrite': 'workloads/memwrite_standalone.py',
}


def build_workload_cmd(args, working_dir: str) -> list:
    """Build the workload subprocess command from CLI args."""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    script = STANDALONE_SCRIPTS.get(args.workload)
    if not script:
        raise ValueError(f"Unknown workload: {args.workload}")

    script_path = os.path.join(base_dir, script)
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"Standalone script not found: {script_path}")

    cmd = [sys.executable, script_path, '--working_dir', working_dir]

    # Common args
    if args.workload_duration:
        cmd.extend(['--duration', str(args.workload_duration)])

    # Workload-specific args
    if args.workload == 'memory':
        if args.mb_size:
            cmd.extend(['--mb-size', str(args.mb_size)])
        if args.max_memory:
            cmd.extend(['--max-memory-mb', str(args.max_memory)])
        if args.interval:
            cmd.extend(['--interval', str(args.interval)])

    elif args.workload == 'matmul':
        if args.matrix_size:
            cmd.extend(['--matrix-size', str(args.matrix_size)])
        if args.iterations:
            cmd.extend(['--iterations', str(args.iterations)])

    elif args.workload == 'redis':
        if args.redis_port:
            cmd.extend(['--redis-port', str(args.redis_port)])
        if args.num_keys:
            cmd.extend(['--num-keys', str(args.num_keys)])
        if args.value_size:
            cmd.extend(['--value-size', str(args.value_size)])
        if args.ycsb_workload:
            cmd.extend(['--ycsb-workload', args.ycsb_workload])
        if args.ycsb_home:
            cmd.extend(['--ycsb-home', args.ycsb_home])
        if args.record_count:
            cmd.extend(['--record-count', str(args.record_count)])
        if args.ycsb_threads:
            cmd.extend(['--ycsb-threads', str(args.ycsb_threads)])
        if args.target_throughput:
            cmd.extend(['--target-throughput', str(args.target_throughput)])

    elif args.workload == 'ml_training':
        if args.model_size:
            cmd.extend(['--model-size', args.model_size])
        if args.batch_size:
            cmd.extend(['--batch-size', str(args.batch_size)])
        if args.epochs:
            cmd.extend(['--epochs', str(args.epochs)])
        if args.learning_rate:
            cmd.extend(['--learning-rate', str(args.learning_rate)])
        if args.dataset_size:
            cmd.extend(['--dataset-size', str(args.dataset_size)])

    elif args.workload == 'video':
        if args.resolution:
            cmd.extend(['--resolution', args.resolution])
        if args.fps:
            cmd.extend(['--fps', str(args.fps)])

    elif args.workload == 'dataproc':
        if args.num_rows:
            cmd.extend(['--num-rows', str(args.num_rows)])
        if args.num_cols:
            cmd.extend(['--num-cols', str(args.num_cols)])
        if args.operations:
            cmd.extend(['--operations', str(args.operations)])

    elif args.workload == 'xgboost':
        if args.xgb_dataset:
            cmd.extend(['--dataset', args.xgb_dataset])
        if args.xgb_dataset_path:
            cmd.extend(['--dataset-path', args.xgb_dataset_path])
        if args.xgb_num_samples:
            cmd.extend(['--num-samples', str(args.xgb_num_samples)])
        if args.xgb_num_features:
            cmd.extend(['--num-features', str(args.xgb_num_features)])
        if args.xgb_num_rounds:
            cmd.extend(['--num-rounds', str(args.xgb_num_rounds)])
        if args.xgb_max_depth:
            cmd.extend(['--max-depth', str(args.xgb_max_depth)])
        if args.xgb_num_threads:
            cmd.extend(['--num-threads', str(args.xgb_num_threads)])
        if args.seed:
            cmd.extend(['--seed', str(args.seed)])

    elif args.workload == 'memcached':
        if args.memcached_port:
            cmd.extend(['--port', str(args.memcached_port)])
        if args.memcached_memory:
            cmd.extend(['--memory-mb', str(args.memcached_memory)])
        if args.ycsb_workload:
            cmd.extend(['--ycsb-workload', args.ycsb_workload])
        if args.ycsb_home:
            cmd.extend(['--ycsb-home', args.ycsb_home])
        if args.record_count:
            cmd.extend(['--record-count', str(args.record_count)])
        if args.ycsb_threads:
            cmd.extend(['--ycsb-threads', str(args.ycsb_threads)])
        if args.target_throughput:
            cmd.extend(['--target-throughput', str(args.target_throughput)])

    elif args.workload == "memwrite":
        if hasattr(args, "buffer_mb") and args.buffer_mb:
            cmd.extend(["--buffer-mb", str(args.buffer_mb)])
    elif args.workload == '7zip':
        if args.compression_level:
            cmd.extend(['--compression-level', str(args.compression_level)])
        if args.sevenzip_threads:
            cmd.extend(['--threads', str(args.sevenzip_threads)])
        if args.input_size_mb:
            cmd.extend(['--input-size-mb', str(args.input_size_mb)])
        if args.seed:
            cmd.extend(['--seed', str(args.seed)])

    return cmd


def wait_for_checkpoint_ready(working_dir: str, timeout: float = 120) -> int:
    """Wait for checkpoint_ready file and return PID."""
    ready_path = os.path.join(working_dir, 'checkpoint_ready')
    start = time.time()

    while time.time() - start < timeout:
        if os.path.exists(ready_path):
            try:
                with open(ready_path, 'r') as f:
                    content = f.read().strip()
                # Format: "ready:PID"
                if content.startswith('ready:'):
                    pid = int(content.split(':')[1])
                    return pid
            except (ValueError, IndexError, OSError):
                pass
        time.sleep(0.1)

    raise TimeoutError(f"Workload did not become ready within {timeout}s")


def select_tracker(tracker_type: str) -> str:
    """Select the best available tracker. Returns 'c', 'go', or 'python'."""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    if tracker_type != 'auto':
        path = os.path.join(base_dir, TRACKER_PATHS[tracker_type])
        if tracker_type == 'python' or os.path.exists(path):
            return tracker_type
        logger.warning(f"Requested tracker '{tracker_type}' not found at {path}, falling back to auto")

    # Auto-select: C > Go > Python
    for t in ['c', 'go']:
        path = os.path.join(base_dir, TRACKER_PATHS[t])
        if os.path.exists(path):
            return t
    return 'python'


def start_external_tracker(tracker_type: str, pid: int, interval_ms: int,
                           duration_sec: int, workload_name: str,
                           output_file: str, no_clear: bool,
                           uffd_sync: bool = False, sd_only: bool = False) -> subprocess.Popen:
    """Start C or Go tracker as a subprocess. Requires sudo for pagemap access."""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    tracker_path = os.path.join(base_dir, TRACKER_PATHS[tracker_type])

    if tracker_type == 'c':
        cmd = ['sudo', tracker_path,
               '-p', str(pid), '-i', str(interval_ms), '-d', str(duration_sec),
               '-w', workload_name, '-o', output_file]
        if no_clear:
            cmd.append('-n')
        if uffd_sync:
            cmd.append('--uffd-sync')
        if sd_only:
            cmd.append('--sd-only')
    elif tracker_type == 'go':
        cmd = ['sudo', tracker_path,
               '-pid', str(pid), '-interval', str(interval_ms),
               '-duration', str(duration_sec), '-workload', workload_name,
               '-output', output_file]
        if no_clear:
            cmd.append('-no-clear')
    elif tracker_type == 'python':
        cmd = ['sudo', sys.executable, tracker_path,
               '--pid', str(pid), '--interval', str(interval_ms),
               '--duration', str(duration_sec), '--workload', workload_name,
               '--output', output_file]
        if no_clear:
            cmd.append('--no-clear')
    else:
        raise ValueError(f"Unknown tracker type: {tracker_type}")

    logger.info(f"Starting {tracker_type} tracker: {' '.join(cmd)}")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return proc


def parse_args():
    parser = argparse.ArgumentParser(
        description='Run a workload locally and track dirty pages (no migration)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    # Core options
    parser.add_argument('--workload', '-w', type=str, required=True,
                        choices=list(STANDALONE_SCRIPTS.keys()),
                        help='Workload type to run')
    parser.add_argument('--duration', '-d', type=int, default=30,
                        help='Dirty page tracking duration in seconds (default: 30)')
    parser.add_argument('--output', '-o', type=str, default=None,
                        help='Output JSON file (default: stdout)')
    parser.add_argument('--analyze', '-a', action='store_true', default=True,
                        help='Run analysis after tracking (default: enabled)')
    parser.add_argument('--no-analyze', dest='analyze', action='store_false',
                        help='Skip analysis after tracking')
    parser.add_argument('--working-dir', type=str, default=None,
                        help='Working directory (default: auto-created temp dir)')

    # Dirty tracker options
    dirty_group = parser.add_argument_group('Dirty Page Tracker')
    dirty_group.add_argument('--dirty-tracker', type=str, default='auto',
                             choices=['auto', 'c', 'go', 'python'],
                             help='Tracker backend: auto (C>Go>Python), c (PAGEMAP_SCAN, fastest), '
                                  'go (soft-dirty), python (soft-dirty, in-process). '
                                  'C and Go trackers require sudo. (default: auto)')
    dirty_group.add_argument('--dirty-track-interval', type=int, default=100,
                             help='Tracking interval in milliseconds (default: 100)')
    dirty_group.add_argument('--dirty-no-clear', action='store_true', default=False,
                             help='Don\'t clear dirty bits after each scan (accumulate mode)')
    dirty_group.add_argument('--no-track-children', dest='track_children',
                             action='store_false', default=True,
                             help='Disable tracking of child processes (python tracker only)')
    dirty_group.add_argument('--uffd-sync', action='store_true', default=False,
                             help='Use userfaultfd synchronous WP mode (C tracker only, OoH ufd comparison)')
    dirty_group.add_argument('--sd-only', action='store_true', default=False,
                             help='Use soft-dirty clear+read only, no uffd (C tracker only, OoH /proc comparison)')

    # Workload-specific options
    wl_group = parser.add_argument_group('Workload Options')
    wl_group.add_argument('--workload-duration', type=int, default=None,
                          help='Workload internal duration (default: runs until stopped)')

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
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Setup working directory
    if args.working_dir:
        working_dir = args.working_dir
    else:
        working_dir = os.path.join('/tmp', f'dirty_track_{args.workload}_{os.getpid()}')

    os.makedirs(working_dir, exist_ok=True)

    # Create checkpoint_flag (workload will run until this is removed)
    flag_path = os.path.join(working_dir, 'checkpoint_flag')
    with open(flag_path, 'w') as f:
        f.write('flag')

    # Build and start workload
    try:
        cmd = build_workload_cmd(args, working_dir)
    except (ValueError, FileNotFoundError) as e:
        logger.error(str(e))
        return 1

    logger.info(f"Starting workload: {' '.join(cmd)}")
    workload_log = open(os.path.join(working_dir, 'workload.log'), 'w')
    workload_proc = subprocess.Popen(
        cmd,
        stdout=workload_log,
        stderr=workload_log,
        start_new_session=True  # setsid: prevent process group kill from sudo tracker
    )

    tracker = None
    tracker_proc = None
    try:
        # Wait for workload to be ready
        logger.info("Waiting for workload to become ready...")
        try:
            workload_pid = wait_for_checkpoint_ready(working_dir, timeout=120)
        except TimeoutError as e:
            logger.error(str(e))
            workload_proc.terminate()
            return 1

        logger.info(f"Workload ready (PID: {workload_pid})")

        # Select tracker backend
        selected_tracker = select_tracker(args.dirty_tracker)
        clear_mode = "no-clear (accumulate)" if args.dirty_no_clear else "clear after scan"
        logger.info(f"Using {selected_tracker} tracker for {args.duration}s "
                     f"(interval={args.dirty_track_interval}ms, {clear_mode})")

        # Determine output file path for external trackers
        if args.output:
            tracker_output = args.output
        else:
            tracker_output = os.path.join(working_dir, 'dirty_result.json')

        use_external = (selected_tracker in ('c', 'go') or
                        (selected_tracker == 'python' and os.geteuid() != 0))
        tracker_proc = None

        if use_external:
            # External tracker (C/Go/Python standalone) via subprocess
            os.makedirs(os.path.dirname(os.path.abspath(tracker_output)), exist_ok=True)
            tracker_proc = start_external_tracker(
                selected_tracker, workload_pid,
                args.dirty_track_interval, args.duration,
                args.workload, tracker_output, args.dirty_no_clear,
                uffd_sync=args.uffd_sync, sd_only=args.sd_only
            )
        else:
            # In-process Python tracker
            tracker = DirtyPageTracker(
                pid=workload_pid,
                interval_ms=args.dirty_track_interval,
                track_children=args.track_children,
                no_clear=args.dirty_no_clear
            )
            tracker.start()

        # Wait for tracking duration (interruptible)
        stop_event = threading.Event()

        def signal_handler(sig, frame):
            logger.info("Signal received, stopping...")
            stop_event.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        start_time = time.time()
        while time.time() - start_time < args.duration:
            if stop_event.is_set():
                break
            # Check if workload is still alive
            if workload_proc.poll() is not None:
                logger.warning("Workload exited prematurely")
                break
            # Check if external tracker exited
            if tracker_proc and tracker_proc.poll() is not None:
                break
            time.sleep(0.5)

        # Stop tracker and collect results
        if use_external:
            if tracker_proc and tracker_proc.poll() is None:
                tracker_proc.send_signal(signal.SIGTERM)
                try:
                    tracker_proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    tracker_proc.kill()

            stderr_output = tracker_proc.stderr.read().decode() if tracker_proc else ''
            if stderr_output:
                logger.info(f"Tracker output:\n{stderr_output.strip()}")

            # Read results from output file
            try:
                with open(tracker_output, 'r') as f:
                    output_data = json.load(f)
            except (OSError, json.JSONDecodeError) as e:
                logger.error(f"Failed to read tracker output: {e}")
                return 1
        else:
            tracker.stop()

            # Export results
            pattern = tracker.get_dirty_pattern(args.workload)
            from dataclasses import asdict
            output_data = asdict(pattern)

            # Convert addresses to hex
            for sample in output_data.get('samples', []):
                for page in sample.get('dirty_pages', []):
                    if isinstance(page.get('addr'), int):
                        page['addr'] = hex(page['addr'])

        logger.info("Dirty page tracking stopped")

        # Write output (if in-process tracker or stdout requested)
        if not use_external:
            if args.output:
                os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
                with open(args.output, 'w') as f:
                    json.dump(output_data, f, indent=2)
                logger.info(f"Results written to {args.output}")
            else:
                print(json.dumps(output_data, indent=2))
        elif not args.output:
            # External tracker wrote to temp file, dump to stdout
            print(json.dumps(output_data, indent=2))

        # Run analysis
        if args.analyze and (args.output or not sys.stdout.isatty()):
            try:
                base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                sys.path.insert(0, os.path.join(base_dir, 'tools'))
                from analyze_dirty_rate import generate_analysis_report, print_analysis_summary

                report = generate_analysis_report(output_data)
                print_analysis_summary(report)
            except Exception as e:
                logger.warning(f"Analysis failed: {e}")
        elif args.analyze and not args.output:
            try:
                base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                sys.path.insert(0, os.path.join(base_dir, 'tools'))
                from analyze_dirty_rate import generate_analysis_report, print_analysis_summary

                report = generate_analysis_report(output_data)
                old_stdout = sys.stdout
                sys.stdout = sys.stderr
                print_analysis_summary(report)
                sys.stdout = old_stdout
            except Exception as e:
                logger.warning(f"Analysis failed: {e}")

        # Print summary to stderr
        summary = output_data.get('summary', {})
        if summary:
            print("\n=== Dirty Page Tracking Summary ===", file=sys.stderr)
            print(f"  Workload: {args.workload}", file=sys.stderr)
            print(f"  Tracker: {selected_tracker}", file=sys.stderr)
            print(f"  Root PID: {workload_pid}", file=sys.stderr)
            print(f"  PAGEMAP_SCAN: {output_data.get('pagemap_scan_used', False)}", file=sys.stderr)
            print(f"  Duration: {output_data.get('tracking_duration_ms', 0):.1f} ms", file=sys.stderr)
            print(f"  Samples: {summary.get('sample_count', 0)}", file=sys.stderr)
            print(f"  Unique dirty pages: {summary.get('total_unique_pages', 0)}", file=sys.stderr)
            print(f"  Total dirty events: {summary.get('total_dirty_events', 0)}", file=sys.stderr)
            total_bytes = summary.get('total_dirty_size_bytes', 0)
            print(f"  Total dirty size: {total_bytes / (1024*1024):.2f} MB", file=sys.stderr)
            print(f"  Avg dirty rate: {summary.get('avg_dirty_rate_per_sec', 0):.1f} pages/sec", file=sys.stderr)
            print(f"  Peak dirty rate: {summary.get('peak_dirty_rate', 0):.1f} pages/sec", file=sys.stderr)
            print(f"  Clear on scan: {output_data.get('clear_on_scan', True)}", file=sys.stderr)

            vma_dist = summary.get('vma_distribution', {})
            if vma_dist:
                print(f"  VMA distribution:", file=sys.stderr)
                for vma_type, pct in sorted(vma_dist.items(), key=lambda x: -x[1]):
                    print(f"    {vma_type}: {pct*100:.1f}%", file=sys.stderr)

        return 0

    finally:
        # Stop tracker if still running
        if tracker:
            try:
                tracker.stop()
            except Exception:
                pass

        # Stop external tracker if still running
        if tracker_proc and tracker_proc.poll() is None:
            tracker_proc.send_signal(signal.SIGTERM)
            try:
                tracker_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                tracker_proc.kill()

        # Stop workload: remove checkpoint_flag and wait
        try:
            if os.path.exists(flag_path):
                os.remove(flag_path)
        except OSError:
            pass

        # Give workload time to exit gracefully
        try:
            workload_proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            logger.warning("Workload did not exit gracefully, terminating...")
            workload_proc.terminate()
            try:
                workload_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                workload_proc.kill()

        # Close workload log file
        try:
            workload_log.close()
        except Exception:
            pass

        # Cleanup working directory
        if not args.working_dir:
            import shutil
            try:
                shutil.rmtree(working_dir, ignore_errors=True)
            except Exception:
                pass


if __name__ == '__main__':
    sys.exit(main())
