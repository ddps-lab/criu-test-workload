"""
Main CRIU experiment orchestrator.

Coordinates workload execution, checkpoint, transfer, and restore operations.
"""

import time
import logging
from typing import Dict, Any, Optional
from pathlib import Path

from .config import ConfigLoader, ConfigValidator
from .checkpoint import CheckpointManager
from .transfer import TransferManager
from .timing import MetricsCollector

logger = logging.getLogger(__name__)


class CRIUExperiment:
    """
    Main orchestrator for CRIU checkpoint/migration experiments.

    Handles the complete lifecycle:
    1. Load and validate configuration
    2. Start workload on source node
    3. Execute pre-dump iterations (if configured)
    4. Perform final dump
    5. Transfer checkpoint data
    6. Restore on destination node
    7. Collect and report metrics
    """

    def __init__(self, config_file: Optional[str] = None, config_overrides: Optional[Dict[str, Any]] = None):
        """
        Initialize CRIU experiment.

        Args:
            config_file: Path to YAML configuration file
            config_overrides: Dictionary of configuration overrides
        """
        # Load configuration
        self.config_loader = ConfigLoader(config_file, config_overrides)
        self.config = self.config_loader.load()

        # Validate configuration
        ConfigValidator.validate(self.config)

        # Extract configuration sections
        self.experiment_config = self.config['experiment']
        self.checkpoint_config = self.config['checkpoint']
        self.transfer_config = self.config['transfer']
        self.nodes_config = self.config['nodes']
        self.workload_config = self.config.get('workload', {})

        # Initialize managers
        self.checkpoint_mgr = CheckpointManager(self.checkpoint_config)
        self.transfer_mgr = TransferManager(self.transfer_config)
        self.metrics = MetricsCollector(
            experiment_name=self.experiment_config['name'],
            workload_type=self.experiment_config['workload_type']
        )

        # Workload instance (will be created by subclass or factory)
        self.workload = None
        self.source_host = self.nodes_config['source']['ip']
        self.dest_host = self.nodes_config['destination']['ip']
        self.ssh_user = self.nodes_config.get('ssh_user', 'ubuntu')

        # Experiment state
        self.workload_pid: Optional[str] = None
        self.checkpoint_iteration = 0

    def set_workload(self, workload):
        """
        Set the workload instance.

        Args:
            workload: Workload instance (BaseWorkload subclass)
        """
        self.workload = workload

    def run(self) -> Dict[str, Any]:
        """
        Execute the complete experiment.

        Returns:
            Dictionary with experiment results and metrics
        """
        try:
            logger.info(f"Starting experiment: {self.experiment_config['name']}")
            logger.info(f"Workload: {self.experiment_config['workload_type']}")
            logger.info(f"Source: {self.source_host}, Destination: {self.dest_host}")

            # Step 1: Prepare nodes
            self._prepare_nodes()

            # Step 2: Start workload
            self._start_workload()

            # Step 3: Run checkpoint strategy
            strategy_mode = self.checkpoint_config['strategy']['mode']
            if strategy_mode == 'predump':
                self._run_predump_strategy()
            elif strategy_mode == 'full':
                self._run_full_dump_strategy()
            else:
                raise ValueError(f"Unknown checkpoint strategy: {strategy_mode}")

            # Step 4: Transfer checkpoint
            self._transfer_checkpoint()

            # Step 5: Restore on destination
            self._restore()

            # Step 6: Finalize metrics
            final_metrics = self.metrics.finalize()
            final_metrics.print_summary()

            # Save metrics to file if configured
            if self.experiment_config.get('save_metrics'):
                output_file = self.experiment_config.get('metrics_file', 'experiment_metrics.json')
                self.metrics.save_to_file(output_file)
                logger.info(f"Metrics saved to {output_file}")

            return {
                'success': True,
                'metrics': final_metrics.to_dict()
            }

        except Exception as e:
            logger.error(f"Experiment failed: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e)
            }

        finally:
            # Clean up SSH connections
            self.checkpoint_mgr.close_all_connections()

    def _prepare_nodes(self):
        """Prepare source and destination nodes."""
        logger.info("Preparing nodes...")

        # Clean up and prepare working directories
        self.checkpoint_mgr.cleanup_and_prepare(self.source_host, self.ssh_user)
        self.checkpoint_mgr.cleanup_and_prepare(self.dest_host, self.ssh_user)

        logger.info("Nodes prepared")

    def _start_workload(self):
        """Start workload process on source node."""
        if self.workload is None:
            raise RuntimeError("Workload not set. Call set_workload() before run()")

        logger.info("Preparing workload...")

        # Deploy workload script and install dependencies
        if not self.workload.prepare(self.source_host):
            raise RuntimeError(f"Failed to prepare workload on {self.source_host}")

        logger.info("Starting workload...")

        # Get workload command
        workload_command = self.workload.get_command()

        # Start workload on source host
        self.workload_pid = self.checkpoint_mgr.start_workload(
            self.source_host,
            workload_command,
            self.ssh_user
        )

        # Wait for workload to be ready
        ready_file = self.workload_config.get('readiness', {}).get('file_path', 'checkpoint_ready')
        ready_timeout = self.workload_config.get('readiness', {}).get('timeout', 300)

        self.checkpoint_mgr.wait_for_ready(
            self.source_host,
            ready_file,
            ready_timeout,
            self.ssh_user
        )

        logger.info(f"Workload started with PID {self.workload_pid}")

    def _run_predump_strategy(self):
        """Execute pre-dump checkpoint strategy."""
        strategy = self.checkpoint_config['strategy']
        num_iterations = strategy.get('predump_iterations', 8)
        interval = strategy.get('predump_interval', 10)

        logger.info(f"Running {num_iterations} pre-dump iterations with {interval}s interval")

        for i in range(1, num_iterations + 1):
            iteration_start = time.time()

            # Pre-dump
            self.metrics.start_timer(f'pre_dump_{i}')
            result = self.checkpoint_mgr.pre_dump(
                self.source_host,
                self.workload_pid,
                i,
                self.ssh_user
            )

            if not result['success']:
                raise RuntimeError(f"Pre-dump {i} failed: {result.get('error')}")

            pre_dump_metric = self.metrics.stop_timer(f'pre_dump_{i}')

            # Optionally sync to transfer medium (e.g., EBS, EFS)
            transfer_method = self.transfer_config.get('method')
            rsync_duration = 0

            if transfer_method in ['ebs', 'efs'] and strategy.get('sync_after_predump', False):
                self.metrics.start_timer(f'pre_dump_{i}_sync')
                sync_result = self._sync_to_medium(result['checkpoint_dir'])
                rsync_metric = self.metrics.stop_timer(f'pre_dump_{i}_sync')
                rsync_duration = rsync_metric.duration

            # Record pre-dump metrics
            self.metrics.record_pre_dump(
                i,
                pre_dump_metric.duration,
                {'rsync_duration': rsync_duration}
            )

            self.checkpoint_iteration = i

            # Sleep to maintain interval
            elapsed = time.time() - iteration_start
            if elapsed < interval and i < num_iterations:
                time.sleep(interval - elapsed)

        logger.info(f"Completed {num_iterations} pre-dump iterations")

        # Final dump
        self._run_final_dump()

    def _run_full_dump_strategy(self):
        """Execute full dump (no pre-dumps)."""
        logger.info("Running full dump strategy (no pre-dumps)")

        # Optional: wait before dump
        wait_time = self.checkpoint_config['strategy'].get('wait_before_dump', 0)
        if wait_time > 0:
            logger.info(f"Waiting {wait_time}s before dump...")
            time.sleep(wait_time)

        self._run_final_dump()

    def _run_final_dump(self):
        """Perform final CRIU dump."""
        strategy = self.checkpoint_config['strategy']
        lazy_pages = strategy.get('lazy_pages', False)
        page_server_port = strategy.get('page_server_port', 22222)

        logger.info(f"Performing final dump (lazy_pages={lazy_pages})")

        self.metrics.start_timer('final_dump')

        result = self.checkpoint_mgr.final_dump(
            self.source_host,
            self.workload_pid,
            self.checkpoint_iteration,
            lazy_pages,
            page_server_port,
            self.ssh_user
        )

        if not result['success']:
            raise RuntimeError(f"Final dump failed: {result.get('error')}")

        final_dump_metric = self.metrics.stop_timer('final_dump')

        # Sync to transfer medium if needed
        transfer_method = self.transfer_config.get('method')
        rsync_duration = 0

        if transfer_method in ['ebs', 'efs']:
            self.metrics.start_timer('final_dump_sync')
            sync_result = self._sync_to_medium(result['checkpoint_dir'])
            rsync_metric = self.metrics.stop_timer('final_dump_sync')
            rsync_duration = rsync_metric.duration

        self.metrics.record_final_dump(
            final_dump_metric.duration,
            {'lazy_pages': lazy_pages, 'rsync_duration': rsync_duration}
        )

        self.checkpoint_iteration = result['iteration']
        self.final_checkpoint_dir = result['checkpoint_dir']

        logger.info(f"Final dump completed in {final_dump_metric.duration:.2f}s")

    def _sync_to_medium(self, checkpoint_dir: str) -> Dict[str, Any]:
        """
        Sync checkpoint to transfer medium (EBS/EFS).

        Args:
            checkpoint_dir: Checkpoint directory to sync

        Returns:
            Sync result dictionary
        """
        # This is a placeholder - actual implementation depends on transfer method
        # For EBS: rsync to /mnt/ebs_test
        # For EFS: already on shared filesystem
        return {'success': True}

    def _transfer_checkpoint(self):
        """Transfer checkpoint data to destination."""
        logger.info("Transferring checkpoint data...")

        self.metrics.start_timer('transfer')

        result = self.transfer_mgr.transfer(
            self.source_host,
            self.dest_host,
            self.final_checkpoint_dir
        )

        transfer_metric = self.metrics.stop_timer('transfer')

        if not result.get('success', True):
            raise RuntimeError(f"Transfer failed: {result.get('error')}")

        self.metrics.record_transfer(
            transfer_metric.duration,
            self.transfer_config['method'],
            result
        )

        logger.info(f"Transfer completed in {transfer_metric.duration:.2f}s")

    def _restore(self):
        """Restore process on destination node."""
        strategy = self.checkpoint_config['strategy']
        lazy_pages = strategy.get('lazy_pages', False)
        page_server_port = strategy.get('page_server_port', 22222)

        # Determine checkpoint directory on destination
        # This depends on transfer method
        transfer_method = self.transfer_config.get('method')

        if transfer_method == 'efs':
            # Same path on EFS
            dest_checkpoint_dir = self.final_checkpoint_dir
        elif transfer_method == 'ebs':
            # On EBS mount
            ebs_mount = self.transfer_config.get('ebs_mount', '/mnt/ebs_test')
            dest_checkpoint_dir = f"{ebs_mount}/{Path(self.checkpoint_mgr.working_dir).name}/{self.checkpoint_iteration}"
        else:
            # rsync or S3 - use destination working dir
            dest_checkpoint_dir = f"{self.checkpoint_mgr.working_dir}/{self.checkpoint_iteration}"

        logger.info(f"Restoring from {dest_checkpoint_dir} on {self.dest_host}")

        self.metrics.start_timer('restore')

        result = self.checkpoint_mgr.restore(
            self.dest_host,
            dest_checkpoint_dir,
            lazy_pages,
            self.source_host if lazy_pages else None,
            page_server_port,
            self.ssh_user
        )

        restore_metric = self.metrics.stop_timer('restore')

        if not result['success']:
            raise RuntimeError(f"Restore failed: {result.get('error')}")

        self.metrics.record_restore(
            restore_metric.duration,
            {'lazy_pages': lazy_pages}
        )

        logger.info(f"Restore completed in {restore_metric.duration:.2f}s")
