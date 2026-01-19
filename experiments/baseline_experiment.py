#!/usr/bin/env python3
"""
Baseline CRIU Migration Experiment

This script runs a complete CRIU checkpoint/migration experiment with configurable
workload and checkpoint strategy.

Usage:
    # Run with default configuration
    python3 baseline_experiment.py

    # Run with custom configuration file
    python3 baseline_experiment.py --config config/experiments/lazy_pages.yaml

    # Override specific settings
    python3 baseline_experiment.py --source-ip 10.0.1.10 --dest-ip 10.0.1.11

    # Use memory workload with custom settings
    python3 baseline_experiment.py --workload memory --mb-size 512 --max-memory 8192

Example from aws-lab bastion:
    export SOURCE_NODE_IP=$(echo $AZ_A_INSTANCES_IP | cut -d' ' -f1)
    export DEST_NODE_IP=$(echo $AZ_A_INSTANCES_IP | cut -d' ' -f2)
    python3 baseline_experiment.py
"""

import os
import sys
import argparse
import logging
import json
import time
import threading

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib import CRIUExperiment, ConfigLoader
from workloads import WorkloadFactory


def collect_dirty_pattern(host: str, remote_file: str, local_file: str, ssh_user: str = 'ubuntu') -> bool:
    """Collect dirty pattern JSON from remote host."""
    import subprocess

    cmd = f"scp -o StrictHostKeyChecking=no {ssh_user}@{host}:{remote_file} {local_file}"
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, timeout=30)
        return result.returncode == 0
    except Exception:
        return False


def setup_logging(level: str = "INFO"):
    """Configure logging."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run a baseline CRIU migration experiment",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    # Configuration
    parser.add_argument(
        '--config', '-c',
        type=str,
        default=None,
        help='Path to YAML configuration file'
    )

    # Node configuration
    parser.add_argument(
        '--source-ip',
        type=str,
        default=None,
        help='Source node IP (overrides config)'
    )
    parser.add_argument(
        '--dest-ip',
        type=str,
        default=None,
        help='Destination node IP (overrides config)'
    )
    parser.add_argument(
        '--ssh-user',
        type=str,
        default=None,
        help='SSH username (default: ubuntu)'
    )

    # Workload configuration
    parser.add_argument(
        '--workload', '-w',
        type=str,
        default='memory',
        choices=['memory', 'matmul', 'redis', 'ml_training', 'dataproc', 'video'],
        help='Workload type to run'
    )
    parser.add_argument(
        '--mb-size',
        type=int,
        default=None,
        help='Memory block size in MB (for memory workload)'
    )
    parser.add_argument(
        '--max-memory',
        type=int,
        default=None,
        help='Maximum memory in MB (for memory workload)'
    )
    parser.add_argument(
        '--interval',
        type=float,
        default=None,
        help='Interval between allocations in seconds'
    )

    # MatMul workload
    parser.add_argument(
        '--matrix-size',
        type=int,
        default=None,
        help='Matrix size NxN for matmul (default: 2048)'
    )
    parser.add_argument(
        '--iterations',
        type=int,
        default=None,
        help='Number of iterations, 0=infinite (matmul/dataproc)'
    )

    # Redis workload
    parser.add_argument(
        '--redis-port',
        type=int,
        default=None,
        help='Redis server port (default: 6379)'
    )
    parser.add_argument(
        '--num-keys',
        type=int,
        default=None,
        help='Number of keys for Redis (default: 100000)'
    )
    parser.add_argument(
        '--value-size',
        type=int,
        default=None,
        help='Redis value size in bytes (default: 1024)'
    )

    # Video workload
    parser.add_argument(
        '--resolution',
        type=str,
        default=None,
        help='Video resolution WxH (default: 1920x1080)'
    )
    parser.add_argument(
        '--fps',
        type=int,
        default=None,
        help='Frames per second (default: 30)'
    )
    parser.add_argument(
        '--duration',
        type=int,
        default=None,
        help='Duration in seconds for video (default: 300)'
    )
    parser.add_argument(
        '--video-mode',
        type=str,
        choices=['file', 'live'],
        default=None,
        help='Video output mode (default: live)'
    )

    # DataProc workload
    parser.add_argument(
        '--num-rows',
        type=int,
        default=None,
        help='Number of rows for dataproc (default: 1000000)'
    )
    parser.add_argument(
        '--num-cols',
        type=int,
        default=None,
        help='Number of columns for dataproc (default: 50)'
    )
    parser.add_argument(
        '--operations',
        type=int,
        default=None,
        help='Number of operations, 0=infinite (dataproc)'
    )

    # ML Training workload
    parser.add_argument(
        '--model-size',
        type=str,
        choices=['small', 'medium', 'large'],
        default=None,
        help='Model size for ml_training (default: medium)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=None,
        help='Training batch size (default: 64)'
    )
    parser.add_argument(
        '--epochs',
        type=int,
        default=None,
        help='Number of epochs, 0=infinite (ml_training)'
    )
    parser.add_argument(
        '--learning-rate',
        type=float,
        default=None,
        help='Learning rate for ml_training (default: 0.001)'
    )
    parser.add_argument(
        '--dataset-size',
        type=int,
        default=None,
        help='ML training dataset size (overrides model-size default)'
    )

    # Checkpoint strategy
    parser.add_argument(
        '--strategy',
        type=str,
        default=None,
        choices=['predump', 'full'],
        help='Checkpoint strategy'
    )
    parser.add_argument(
        '--predump-iterations',
        type=int,
        default=None,
        help='Number of pre-dump iterations'
    )
    parser.add_argument(
        '--predump-interval',
        type=int,
        default=None,
        help='Interval between pre-dumps in seconds'
    )
    # Note: --lazy-pages is deprecated, use --lazy-mode instead

    # Checkpoint trigger options (mutually exclusive)
    trigger_group = parser.add_mutually_exclusive_group()
    trigger_group.add_argument(
        '--wait-before-dump',
        type=int,
        default=None,
        help='Seconds to wait before full dump (time-based trigger)'
    )
    trigger_group.add_argument(
        '--target-memory-mb',
        type=int,
        default=None,
        help='Wait until process VmRSS reaches this MB before dump (memory-based trigger)'
    )

    # Transfer configuration
    parser.add_argument(
        '--transfer-method',
        type=str,
        default=None,
        choices=['rsync', 's3', 'efs', 'ebs'],
        help='Checkpoint transfer method'
    )

    # S3 Object Storage configuration
    s3_group = parser.add_argument_group('S3 Object Storage',
        'Options for S3-based checkpoint transfer and lazy restore')
    s3_group.add_argument(
        '--s3-type',
        type=str,
        default='standard',
        choices=['standard', 'cloudfront', 'express-one-zone'],
        help='S3 storage type (default: standard)'
    )
    # Lazy mode configuration (moved from checkpoint strategy to here for CLI convenience)
    s3_group.add_argument(
        '--lazy-mode',
        type=str,
        default='none',
        choices=['none', 'lazy', 'lazy-prefetch', 'live-migration', 'live-migration-prefetch'],
        help='Lazy restore mode: none (standard), lazy (local lazy-pages), lazy-prefetch (S3 async prefetch), '
             'live-migration (page-server), live-migration-prefetch (S3 + page-server)'
    )
    s3_group.add_argument(
        '--page-server-port',
        type=int,
        default=27,
        help='Page server port for live migration modes (default: 27)'
    )

    # S3 Upload settings
    s3_group.add_argument(
        '--s3-upload-bucket',
        type=str,
        default=None,
        help='S3 bucket for uploading checkpoint (required for S3 transfer)'
    )
    s3_group.add_argument(
        '--s3-prefix',
        type=str,
        default='',
        help='S3 object prefix (e.g., "checkpoints/exp1")'
    )
    s3_group.add_argument(
        '--s3-region',
        type=str,
        default=None,
        help='AWS region for S3'
    )

    # S3 Download settings (for CRIU object storage)
    s3_group.add_argument(
        '--s3-download-endpoint',
        type=str,
        default=None,
        help='CRIU object storage endpoint URL (e.g., s3.us-east-1.amazonaws.com, d1234.cloudfront.net)'
    )
    s3_group.add_argument(
        '--s3-download-bucket',
        type=str,
        default=None,
        help='Bucket for CRIU download (default: same as upload bucket)'
    )

    # Express One Zone specific
    s3_group.add_argument(
        '--s3-access-key',
        type=str,
        default=None,
        help='AWS access key (required for express-one-zone)'
    )
    s3_group.add_argument(
        '--s3-secret-key',
        type=str,
        default=None,
        help='AWS secret key (required for express-one-zone)'
    )

    # Async prefetch settings (used when lazy-mode is lazy-prefetch or live-migration-prefetch)
    s3_group.add_argument(
        '--prefetch-workers',
        type=int,
        default=4,
        help='Number of prefetch worker threads for lazy-prefetch modes (default: 4)'
    )

    # Output
    parser.add_argument(
        '--output', '-o',
        type=str,
        default=None,
        help='Output file for metrics JSON'
    )
    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level'
    )

    # Process lifecycle
    parser.add_argument(
        '--no-cleanup',
        action='store_true',
        help='Skip process cleanup after experiment (keep restored process running)'
    )

    # Log collection
    parser.add_argument(
        '--collect-logs',
        action='store_true',
        help='Collect CRIU log files from nodes after experiment'
    )
    parser.add_argument(
        '--logs-dir',
        type=str,
        default='./results',
        help='Directory to save collected logs (default: ./results)'
    )
    parser.add_argument(
        '--name', '-n',
        type=str,
        default=None,
        help='Experiment name (used for log directory naming, e.g., "my_exp" -> "my_exp_20240101_120000")'
    )

    # Dirty page tracking (for simulation)
    dirty_group = parser.add_argument_group('Dirty Page Tracking',
        'Options for tracking dirty pages during workload execution (for simulation analysis)')
    dirty_group.add_argument(
        '--track-dirty-pages',
        action='store_true',
        help='Enable dirty page tracking during workload execution using soft-dirty bits'
    )
    dirty_group.add_argument(
        '--dirty-track-interval',
        type=int,
        default=100,
        help='Dirty page tracking interval in milliseconds (default: 100)'
    )
    dirty_group.add_argument(
        '--dirty-track-duration',
        type=int,
        default=None,
        help='Dirty page tracking duration in seconds (default: until checkpoint)'
    )

    return parser.parse_args()


def build_overrides(args) -> dict:
    """Build configuration overrides from command line arguments."""
    overrides = {}

    # Node overrides
    if args.source_ip:
        overrides['nodes.source.ip'] = args.source_ip
    if args.dest_ip:
        overrides['nodes.destination.ip'] = args.dest_ip
    if args.ssh_user:
        overrides['nodes.ssh_user'] = args.ssh_user

    # Experiment name override
    if args.name:
        overrides['experiment.name'] = args.name

    # Workload overrides
    if args.workload:
        overrides['experiment.workload_type'] = args.workload

    # Memory workload
    if args.mb_size:
        overrides['workload.mb_size'] = args.mb_size
    if args.max_memory:
        overrides['workload.max_memory_mb'] = args.max_memory
    if args.interval:
        overrides['workload.interval'] = args.interval

    # MatMul workload
    if args.matrix_size:
        overrides['workload.matrix_size'] = args.matrix_size
    if args.iterations is not None:
        overrides['workload.iterations'] = args.iterations

    # Redis workload
    if args.redis_port:
        overrides['workload.redis_port'] = args.redis_port
    if args.num_keys:
        overrides['workload.num_keys'] = args.num_keys
    if args.value_size:
        overrides['workload.value_size'] = args.value_size

    # Video workload
    if args.resolution:
        overrides['workload.resolution'] = args.resolution
    if args.fps:
        overrides['workload.fps'] = args.fps
    if args.duration:
        overrides['workload.duration'] = args.duration
    if args.video_mode:
        overrides['workload.mode'] = args.video_mode

    # DataProc workload
    if args.num_rows:
        overrides['workload.num_rows'] = args.num_rows
    if args.num_cols:
        overrides['workload.num_cols'] = args.num_cols
    if args.operations is not None:
        overrides['workload.operations'] = args.operations

    # ML Training workload
    if args.model_size:
        overrides['workload.model_size'] = args.model_size
    if args.batch_size:
        overrides['workload.batch_size'] = args.batch_size
    if args.epochs is not None:
        overrides['workload.epochs'] = args.epochs
    if args.learning_rate:
        overrides['workload.learning_rate'] = args.learning_rate
    if args.dataset_size:
        overrides['workload.dataset_size'] = args.dataset_size

    # Checkpoint strategy overrides
    if args.strategy:
        overrides['checkpoint.strategy.mode'] = args.strategy
    if args.predump_iterations:
        overrides['checkpoint.strategy.predump_iterations'] = args.predump_iterations
    if args.predump_interval:
        overrides['checkpoint.strategy.predump_interval'] = args.predump_interval
    if args.wait_before_dump is not None:
        overrides['checkpoint.strategy.wait_before_dump'] = args.wait_before_dump
    if args.target_memory_mb is not None:
        overrides['checkpoint.strategy.target_memory_mb'] = args.target_memory_mb

    # Lazy mode configuration (now unified across all transfer methods)
    if args.lazy_mode:
        overrides['checkpoint.strategy.lazy_mode'] = args.lazy_mode
    if args.page_server_port:
        overrides['checkpoint.strategy.page_server_port'] = args.page_server_port
    if args.prefetch_workers:
        overrides['checkpoint.strategy.prefetch_workers'] = args.prefetch_workers

    # Transfer overrides
    if args.transfer_method:
        overrides['transfer.method'] = args.transfer_method

    # S3 configuration overrides
    if args.s3_type:
        overrides['s3.type'] = args.s3_type
    if args.s3_upload_bucket:
        overrides['s3.upload_bucket'] = args.s3_upload_bucket
    if args.s3_prefix:
        overrides['s3.prefix'] = args.s3_prefix
    if args.s3_region:
        overrides['s3.region'] = args.s3_region
    if args.s3_download_endpoint:
        overrides['s3.download_endpoint'] = args.s3_download_endpoint
    if args.s3_download_bucket:
        overrides['s3.download_bucket'] = args.s3_download_bucket
    if args.s3_access_key:
        overrides['s3.access_key'] = args.s3_access_key
    if args.s3_secret_key:
        overrides['s3.secret_key'] = args.s3_secret_key

    # Output overrides
    if args.output:
        overrides['experiment.metrics_file'] = args.output
        overrides['experiment.save_metrics'] = True

    # Dirty page tracking overrides
    if args.track_dirty_pages:
        overrides['experiment.track_dirty_pages'] = True
    if args.dirty_track_interval:
        overrides['experiment.dirty_track_interval'] = args.dirty_track_interval
    if args.dirty_track_duration:
        overrides['experiment.dirty_track_duration'] = args.dirty_track_duration

    return overrides


def main():
    """Run the baseline experiment."""
    args = parse_args()

    # Setup logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)

    # Determine config file
    if args.config:
        config_file = args.config
    else:
        # Use default config
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_file = os.path.join(base_dir, 'config', 'default.yaml')

    logger.info(f"Using configuration: {config_file}")

    # Build overrides from command line
    overrides = build_overrides(args)

    if overrides:
        logger.info(f"Configuration overrides: {overrides}")

    try:
        # Create experiment
        experiment = CRIUExperiment(
            config_file=config_file,
            config_overrides=overrides
        )

        # Store CLI args in metrics
        experiment.metrics.set_cli_args(vars(args))

        # Create workload
        workload_type = experiment.config.get('experiment', {}).get('workload_type', 'memory')
        workload_config = experiment.config.get('workload', {})
        workload_config['working_dir'] = experiment.checkpoint_config['dirs']['working_dir']
        workload_config['ssh_user'] = experiment.nodes_config.get('ssh_user', 'ubuntu')

        workload = WorkloadFactory.create(workload_type, workload_config)
        workload.validate_config()

        # Deploy workload to source node
        logger.info(f"Deploying {workload_type} workload to source node...")
        if not workload.prepare(experiment.source_host):
            logger.error("Failed to deploy workload to source node")
            sys.exit(1)

        # Also deploy to destination (for restore)
        logger.info(f"Deploying {workload_type} workload to destination node...")
        if not workload.prepare(experiment.dest_host):
            logger.error("Failed to deploy workload to destination node")
            sys.exit(1)

        # Set workload on experiment
        experiment.set_workload(workload)

        # Run experiment (dirty tracking is handled automatically inside run() if enabled)
        logger.info("Starting experiment...")
        result = experiment.run()

        if result['success']:
            logger.info("Experiment completed successfully!")
            print("\n" + "=" * 60)
            print("EXPERIMENT COMPLETED SUCCESSFULLY")
            print("=" * 60)

            # Print key metrics
            metrics = result['metrics']
            print(f"Total duration: {metrics['total_duration']:.2f}s")

            if metrics.get('pre_dump_iterations'):
                pre_dump_total = sum(m['duration'] for m in metrics['pre_dump_iterations'])
                print(f"Pre-dump total: {pre_dump_total:.2f}s ({len(metrics['pre_dump_iterations'])} iterations)")

            if metrics.get('final_dump'):
                print(f"Final dump: {metrics['final_dump']['duration']:.2f}s")

            if metrics.get('transfer'):
                print(f"Transfer: {metrics['transfer']['duration']:.2f}s")

            if metrics.get('restore'):
                print(f"Restore: {metrics['restore']['duration']:.2f}s")

            # Handle lazy-pages completion tracking
            if metrics.get('lazy_pages_completion'):
                lp = metrics['lazy_pages_completion']
                if lp.get('completed'):
                    print(f"Lazy-pages: {lp['duration']:.2f}s")
                else:
                    print(f"Lazy-pages: timeout ({lp.get('error', 'unknown')})")

            print("=" * 60)

            # Track output directory for logs and dirty pattern
            log_result = None

            # Collect logs if requested
            if args.collect_logs:
                logger.info("Collecting CRIU logs from nodes...")
                log_result = experiment.checkpoint_mgr.collect_logs(
                    experiment.source_host,
                    experiment.dest_host,
                    args.logs_dir,
                    experiment.nodes_config.get('ssh_user', 'ubuntu'),
                    experiment_name=args.name
                )
                # Store log paths in metrics
                experiment.metrics.set_log_files(log_result)

                print(f"Logs collected: {log_result['output_dir']}")
                print(f"  Source: {len(log_result['source'])} files")
                print(f"  Dest: {len(log_result['dest'])} files")

                # Also save metrics alongside logs
                metrics_file = f"{log_result['output_dir']}/metrics.json"
                experiment.metrics.save_to_file(metrics_file)
                print(f"  Metrics: {metrics_file}")

            # Handle dirty page tracking results if requested
            if args.track_dirty_pages:
                logger.info("Collecting dirty page tracking results...")
                ssh_user = experiment.nodes_config.get('ssh_user', 'ubuntu')
                remote_dirty_file = '/tmp/dirty_pattern.json'

                # Use the same output directory as logs if --collect-logs was used
                if log_result is not None:
                    local_output_dir = log_result['output_dir']
                else:
                    local_output_dir = args.logs_dir if args.logs_dir else './results'
                    os.makedirs(local_output_dir, exist_ok=True)

                local_dirty_file = os.path.join(local_output_dir, 'dirty_pattern.json')

                # Try to collect dirty pattern from source node
                if collect_dirty_pattern(experiment.source_host, remote_dirty_file,
                                        local_dirty_file, ssh_user):
                    print(f"Dirty pattern collected: {local_dirty_file}")
                    print(f"  To analyze: python3 tools/analyze_dirty_rate.py {local_dirty_file}")
                else:
                    logger.warning("Failed to collect dirty pattern from source node")

            # Cleanup processes unless --no-cleanup specified
            if not args.no_cleanup:
                logger.info("Cleaning up processes...")
                experiment.checkpoint_mgr.cleanup_processes(
                    experiment.dest_host,
                    workload_type,
                    experiment.nodes_config.get('ssh_user', 'ubuntu')
                )
                # Also cleanup source node
                experiment.checkpoint_mgr.cleanup_processes(
                    experiment.source_host,
                    workload_type,
                    experiment.nodes_config.get('ssh_user', 'ubuntu')
                )
            else:
                logger.info("Skipping cleanup (--no-cleanup specified)")

            return 0

        else:
            logger.error(f"Experiment failed: {result.get('error')}")

            # Collect logs even on failure
            if args.collect_logs:
                logger.info("Collecting CRIU logs from nodes (experiment failed)...")
                try:
                    log_result = experiment.checkpoint_mgr.collect_logs(
                        experiment.source_host,
                        experiment.dest_host,
                        args.logs_dir,
                        experiment.nodes_config.get('ssh_user', 'ubuntu'),
                        experiment_name=f"{args.name}_failed" if args.name else "failed"
                    )
                    print(f"Logs collected: {log_result['output_dir']}")
                    print(f"  Source: {len(log_result['source'])} files")
                    print(f"  Dest: {len(log_result['dest'])} files")
                except Exception as log_err:
                    logger.warning(f"Failed to collect logs: {log_err}")

            return 1

    except FileNotFoundError as e:
        logger.error(f"Configuration file not found: {e}")
        return 1
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Experiment failed with exception: {e}", exc_info=True)

        # Collect logs on exception if possible
        if args.collect_logs:
            try:
                logger.info("Collecting CRIU logs from nodes (exception occurred)...")
                log_result = experiment.checkpoint_mgr.collect_logs(
                    experiment.source_host,
                    experiment.dest_host,
                    args.logs_dir,
                    experiment.nodes_config.get('ssh_user', 'ubuntu'),
                    experiment_name=f"{args.name}_exception" if args.name else "exception"
                )
                print(f"Logs collected: {log_result['output_dir']}")
            except Exception as log_err:
                logger.warning(f"Failed to collect logs: {log_err}")

        return 1


if __name__ == '__main__':
    sys.exit(main())
