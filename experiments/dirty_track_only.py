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

# Workload standalone script mapping
STANDALONE_SCRIPTS = {
    'memory': 'workloads/memory_standalone.py',
    'matmul': 'workloads/matmul_standalone.py',
    'redis': 'workloads/redis_standalone.py',
    'ml_training': 'workloads/ml_training_standalone.py',
    'video': 'workloads/video_standalone.py',
    'dataproc': 'workloads/dataproc_standalone.py',
    'jupyter': 'workloads/jupyter_standalone.py',
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

    cmd = [sys.executable, script_path, '--working-dir', working_dir]

    # Common args
    if args.workload_duration:
        cmd.extend(['--duration', str(args.workload_duration)])

    # Workload-specific args
    if args.workload == 'memory':
        if args.mb_size:
            cmd.extend(['--mb-size', str(args.mb_size)])
        if args.max_memory:
            cmd.extend(['--max-memory', str(args.max_memory)])
        if args.interval:
            cmd.extend(['--interval', str(args.interval)])

    elif args.workload == 'matmul':
        if args.matrix_size:
            cmd.extend(['--matrix-size', str(args.matrix_size)])
        if args.iterations:
            cmd.extend(['--iterations', str(args.iterations)])

    elif args.workload == 'redis':
        if args.redis_port:
            cmd.extend(['--port', str(args.redis_port)])
        if args.num_keys:
            cmd.extend(['--num-keys', str(args.num_keys)])
        if args.value_size:
            cmd.extend(['--value-size', str(args.value_size)])

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
    dirty_group.add_argument('--dirty-track-interval', type=int, default=100,
                             help='Tracking interval in milliseconds (default: 100)')
    dirty_group.add_argument('--dirty-no-clear', action='store_true', default=False,
                             help='Don\'t clear dirty bits after each scan (accumulate mode)')
    dirty_group.add_argument('--no-track-children', dest='track_children',
                             action='store_false', default=True,
                             help='Disable tracking of child processes')

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
    workload_proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    tracker = None
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

        # Start dirty page tracker
        tracker = DirtyPageTracker(
            pid=workload_pid,
            interval_ms=args.dirty_track_interval,
            track_children=args.track_children,
            no_clear=args.dirty_no_clear
        )

        clear_mode = "no-clear (accumulate)" if args.dirty_no_clear else "clear after scan"
        logger.info(f"Starting dirty page tracking for {args.duration}s "
                     f"(interval={args.dirty_track_interval}ms, {clear_mode})")

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
            time.sleep(0.5)

        # Stop tracker
        tracker.stop()
        logger.info("Dirty page tracking stopped")

        # Export results
        pattern = tracker.get_dirty_pattern(args.workload)

        # Convert to dict
        from dataclasses import asdict
        output_data = asdict(pattern)

        # Convert addresses to hex
        for sample in output_data.get('samples', []):
            for page in sample.get('dirty_pages', []):
                if isinstance(page.get('addr'), int):
                    page['addr'] = hex(page['addr'])

        # Output
        if args.output:
            os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
            with open(args.output, 'w') as f:
                json.dump(output_data, f, indent=2)
            logger.info(f"Results written to {args.output}")
        else:
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
            # JSON went to stdout, run analysis on stderr
            try:
                base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                sys.path.insert(0, os.path.join(base_dir, 'tools'))
                from analyze_dirty_rate import generate_analysis_report, print_analysis_summary

                report = generate_analysis_report(output_data)
                # Redirect print to stderr
                import io
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
            print(f"  Root PID: {workload_pid}", file=sys.stderr)
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

        # Cleanup working directory
        if not args.working_dir:
            import shutil
            try:
                shutil.rmtree(working_dir, ignore_errors=True)
            except Exception:
                pass


if __name__ == '__main__':
    sys.exit(main())
