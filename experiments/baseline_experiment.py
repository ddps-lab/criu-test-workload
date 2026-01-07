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

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib import CRIUExperiment, ConfigLoader
from workloads import WorkloadFactory


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
    parser.add_argument(
        '--lazy-pages',
        action='store_true',
        help='Enable lazy-pages mode'
    )

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
    if args.lazy_pages:
        overrides['checkpoint.strategy.lazy_pages'] = True
    if args.wait_before_dump is not None:
        overrides['checkpoint.strategy.wait_before_dump'] = args.wait_before_dump
    if args.target_memory_mb is not None:
        overrides['checkpoint.strategy.target_memory_mb'] = args.target_memory_mb

    # Transfer overrides
    if args.transfer_method:
        overrides['transfer.method'] = args.transfer_method

    # Output overrides
    if args.output:
        overrides['experiment.metrics_file'] = args.output
        overrides['experiment.save_metrics'] = True

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

        # Run experiment
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
            return 1

    except FileNotFoundError as e:
        logger.error(f"Configuration file not found: {e}")
        return 1
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Experiment failed with exception: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
