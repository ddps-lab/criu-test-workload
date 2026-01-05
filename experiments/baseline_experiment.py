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
from workloads.memory_workload import MemoryWorkload, WorkloadFactory


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
        choices=['memory'],  # Add more as implemented
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

    # Workload overrides
    if args.workload:
        overrides['experiment.workload_type'] = args.workload
    if args.mb_size:
        overrides['workload.mb_size'] = args.mb_size
    if args.max_memory:
        overrides['workload.max_memory_mb'] = args.max_memory
    if args.interval:
        overrides['workload.interval'] = args.interval

    # Checkpoint strategy overrides
    if args.strategy:
        overrides['checkpoint.strategy.mode'] = args.strategy
    if args.predump_iterations:
        overrides['checkpoint.strategy.predump_iterations'] = args.predump_iterations
    if args.predump_interval:
        overrides['checkpoint.strategy.predump_interval'] = args.predump_interval
    if args.lazy_pages:
        overrides['checkpoint.strategy.lazy_pages'] = True

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

            print("=" * 60)

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
