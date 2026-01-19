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
from .s3_config import S3Config, S3Type
from .lazy_mode import LazyMode, LazyConfig
from .dirty_tracker import DirtyPageTracker

logger = logging.getLogger(__name__)


class RemoteDirtyTracker:
    """
    Helper class to manage dirty page tracking on a remote host via SSH.

    Supports multiple tracker backends:
    - 'c': C PAGEMAP_SCAN tracker (fastest, requires kernel 6.7+)
    - 'go': Go soft-dirty tracker (cross-platform)
    - 'python': Python soft-dirty tracker (fallback)

    The tracker is auto-selected based on availability, preferring C > Go > Python.
    """

    # Tracker binary paths (relative to /opt/criu_workload)
    TRACKER_PATHS = {
        'c': 'criu_workload/tools/dirty_tracker_c/dirty_tracker',
        'go': 'criu_workload/tools/dirty_tracker_go/dirty_tracker',
        'python': 'tools/dirty_tracker.py',
    }

    def __init__(self, host: str, ssh_user: str = 'ubuntu', tracker_type: str = 'auto'):
        """
        Initialize RemoteDirtyTracker.

        Args:
            host: Remote host IP or hostname
            ssh_user: SSH username (default: ubuntu)
            tracker_type: 'auto', 'c', 'go', or 'python'
        """
        self.host = host
        self.ssh_user = ssh_user
        self.tracker_pid: Optional[int] = None
        self.output_file = '/tmp/dirty_pattern.json'
        self.tracker_type = tracker_type
        self._selected_tracker: Optional[str] = None

    def _check_tracker_exists(self, tracker_type: str) -> bool:
        """Check if a tracker binary exists on the remote host."""
        import subprocess

        base_path = '/opt/criu_workload'
        tracker_path = f"{base_path}/{self.TRACKER_PATHS[tracker_type]}"

        cmd = f"ssh -o StrictHostKeyChecking=no {self.ssh_user}@{self.host} 'test -x {tracker_path} && echo exists'"
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=5)
            return 'exists' in result.stdout
        except Exception:
            return False

    def _select_tracker(self) -> Optional[str]:
        """Auto-select the best available tracker."""
        if self.tracker_type != 'auto':
            if self._check_tracker_exists(self.tracker_type):
                return self.tracker_type
            logger.warning(f"Requested tracker '{self.tracker_type}' not found, falling back to auto-select")

        # Try in order of preference: C (PAGEMAP_SCAN) > Go > Python
        for tracker in ['c', 'go', 'python']:
            if self._check_tracker_exists(tracker):
                logger.info(f"Selected dirty tracker: {tracker}")
                return tracker

        logger.warning("No dirty tracker found on remote host")
        return None

    def start(self, target_pid: int, interval_ms: int = 100, workload_name: str = 'unknown',
              duration_sec: int = 3600) -> bool:
        """
        Start dirty page tracking on remote host.

        Args:
            target_pid: PID of the process to track
            interval_ms: Sampling interval in milliseconds
            workload_name: Name of the workload (for output metadata)
            duration_sec: Maximum tracking duration in seconds (default: 1 hour)

        Returns:
            True if tracking started successfully
        """
        import subprocess

        self._selected_tracker = self._select_tracker()
        if self._selected_tracker is None:
            return False

        base_path = '/opt/criu_workload'
        tracker_path = f"{base_path}/{self.TRACKER_PATHS[self._selected_tracker]}"

        # Build command based on tracker type
        if self._selected_tracker == 'c':
            # C tracker: ./dirty_tracker -p PID -i INTERVAL -d DURATION -w WORKLOAD -o OUTPUT
            tracker_cmd = (
                f"sudo {tracker_path} "
                f"-p {target_pid} -i {interval_ms} -d {duration_sec} "
                f"-w {workload_name} -o {self.output_file}"
            )
        elif self._selected_tracker == 'go':
            # Go tracker: ./dirty_tracker -pid PID -interval INTERVAL -duration DURATION -workload WORKLOAD -output OUTPUT
            tracker_cmd = (
                f"sudo {tracker_path} "
                f"-pid {target_pid} -interval {interval_ms} -duration {duration_sec} "
                f"-workload {workload_name} -output {self.output_file}"
            )
        else:
            # Python tracker: python3 dirty_tracker.py --pid PID --interval INTERVAL --workload WORKLOAD --output OUTPUT
            tracker_cmd = (
                f"sudo python3 {tracker_path} "
                f"--pid {target_pid} --interval {interval_ms} --workload {workload_name} "
                f"--output {self.output_file}"
            )

        cmd = (
            f"ssh -o StrictHostKeyChecking=no {self.ssh_user}@{self.host} "
            f"'nohup {tracker_cmd} > /tmp/dirty_tracker.log 2>&1 & echo $!'"
        )

        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
            if result.returncode == 0 and result.stdout.strip():
                self.tracker_pid = int(result.stdout.strip())
                logger.info(f"Started {self._selected_tracker} dirty tracking on {self.host} "
                           f"(tracker PID: {self.tracker_pid}, interval: {interval_ms}ms)")
                return True
            else:
                logger.warning(f"Failed to start dirty tracking: {result.stderr}")
                return False
        except Exception as e:
            logger.warning(f"Failed to start dirty tracking: {e}")
            return False

    def stop(self) -> bool:
        """Stop dirty page tracking on remote host."""
        if self.tracker_pid is None:
            return True

        import subprocess

        # Send SIGTERM to allow graceful shutdown and JSON output
        cmd = f"ssh -o StrictHostKeyChecking=no {self.ssh_user}@{self.host} 'sudo kill -TERM {self.tracker_pid} 2>/dev/null || true'"
        try:
            subprocess.run(cmd, shell=True, capture_output=True, timeout=10)
            logger.info(f"Stopped dirty tracking on {self.host} (was using {self._selected_tracker} tracker)")
            # Give it time to write output
            import time
            time.sleep(1)
            return True
        except Exception as e:
            logger.warning(f"Failed to stop dirty tracking: {e}")
            return False

    def collect_results(self, local_file: str) -> bool:
        """Collect dirty pattern results from remote host."""
        import subprocess

        cmd = f"scp -o StrictHostKeyChecking=no {self.ssh_user}@{self.host}:{self.output_file} {local_file}"
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, timeout=30)
            if result.returncode == 0:
                logger.info(f"Collected dirty pattern to {local_file}")
                return True
            else:
                logger.warning(f"Failed to collect dirty pattern: {result.stderr}")
                return False
        except Exception as e:
            logger.warning(f"Failed to collect dirty pattern: {e}")
            return False

    @property
    def selected_tracker(self) -> Optional[str]:
        """Return the currently selected tracker type."""
        return self._selected_tracker


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

        # Store config and node info in metrics
        self.metrics.set_config(self.config)

        # Workload instance (will be created by subclass or factory)
        self.workload = None
        self.source_host = self.nodes_config['source']['ip']
        self.dest_host = self.nodes_config['destination']['ip']
        self.ssh_user = self.nodes_config.get('ssh_user', 'ubuntu')

        # Store node info in metrics
        self.metrics.set_nodes(self.source_host, self.dest_host)

        # Experiment state
        self.workload_pid: Optional[str] = None
        self.checkpoint_iteration = 0

        # Dirty page tracking state
        self._dirty_tracker: Optional[RemoteDirtyTracker] = None
        self._dirty_tracking_enabled = self.experiment_config.get('track_dirty_pages', False)
        self._dirty_track_interval = self.experiment_config.get('dirty_track_interval', 100)
        logger.info(f"Dirty tracking config: enabled={self._dirty_tracking_enabled}, interval={self._dirty_track_interval}ms")

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

            # Step 2.5: Start dirty page tracking if enabled
            logger.info(f"Dirty tracking check: enabled={self._dirty_tracking_enabled}, pid={self.workload_pid}")
            if self._dirty_tracking_enabled and self.workload_pid:
                self._start_dirty_tracking()
            else:
                logger.info(f"Dirty tracking not started (enabled={self._dirty_tracking_enabled})")

            # Step 3: Run checkpoint strategy
            strategy_mode = self.checkpoint_config['strategy']['mode']
            if strategy_mode == 'predump':
                self._run_predump_strategy()
            elif strategy_mode == 'full':
                self._run_full_dump_strategy()
            else:
                raise ValueError(f"Unknown checkpoint strategy: {strategy_mode}")

            # Step 3.5: Stop dirty page tracking before transfer
            if self._dirty_tracker is not None:
                self._stop_dirty_tracking()

            # Step 4: Transfer checkpoint
            self._transfer_checkpoint()

            # Step 5: Restore on destination
            self._restore()

            # Step 6: Finalize metrics
            final_metrics = self.metrics.finalize()
            final_metrics.print_summary()

            # Save metrics to file if configured
            if self.experiment_config.get('save_metrics'):
                output_file = self.experiment_config.get('metrics_file', 'metrics.json')
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
            # Stop dirty tracking if still running
            if self._dirty_tracker is not None:
                self._stop_dirty_tracking()

            # Clean up SSH connections
            self.checkpoint_mgr.close_all_connections()

    def _prepare_nodes(self):
        """Prepare source and destination nodes."""
        logger.info("Preparing nodes...")

        # Clean up and prepare working directories
        self.checkpoint_mgr.cleanup_and_prepare(self.source_host, self.ssh_user)
        self.checkpoint_mgr.cleanup_and_prepare(self.dest_host, self.ssh_user)

        logger.info("Nodes prepared")

    def _start_dirty_tracking(self):
        """Start dirty page tracking on source node."""
        if not self.workload_pid:
            logger.warning("Cannot start dirty tracking: no workload PID")
            return

        logger.info(f"Starting dirty page tracking (interval: {self._dirty_track_interval}ms)")

        self._dirty_tracker = RemoteDirtyTracker(self.source_host, self.ssh_user)
        workload_name = self.experiment_config.get('workload_type', 'unknown')

        if not self._dirty_tracker.start(
            int(self.workload_pid),
            self._dirty_track_interval,
            workload_name
        ):
            logger.warning("Failed to start dirty tracking, continuing without it")
            self._dirty_tracker = None

    def _stop_dirty_tracking(self):
        """Stop dirty page tracking and record results."""
        if self._dirty_tracker is None:
            return

        logger.info("Stopping dirty page tracking...")
        self._dirty_tracker.stop()

        # Note: Results are collected later by baseline_experiment.py via collect_dirty_pattern()
        # The tracker writes to /tmp/dirty_pattern.json on the remote host

        self._dirty_tracker = None

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

        # Get workload type for CRIU flags
        workload_type = self.experiment_config.get('workload_type', 'memory')

        for i in range(1, num_iterations + 1):
            iteration_start = time.time()

            # Pre-dump
            self.metrics.start_timer(f'pre_dump_{i}')
            result = self.checkpoint_mgr.pre_dump(
                self.source_host,
                self.workload_pid,
                i,
                self.ssh_user,
                workload_type=workload_type
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

        strategy = self.checkpoint_config['strategy']

        # Check for memory-based or time-based trigger
        target_memory_mb = strategy.get('target_memory_mb')
        wait_time = strategy.get('wait_before_dump', 0)

        if target_memory_mb is not None:
            # Memory-based trigger: wait until process memory reaches target
            logger.info(f"Waiting for process memory to reach {target_memory_mb} MB...")
            if not self._wait_for_target_memory(target_memory_mb):
                logger.warning("Timeout waiting for target memory, proceeding with dump anyway")
        elif wait_time > 0:
            # Time-based trigger: wait fixed duration
            logger.info(f"Waiting {wait_time}s before dump...")
            time.sleep(wait_time)

        self._run_final_dump()

    def _wait_for_target_memory(self, target_mb: int, timeout: int = 600) -> bool:
        """
        Wait until process memory (VmRSS) reaches target size.

        Args:
            target_mb: Target memory in MB
            timeout: Maximum wait time in seconds

        Returns:
            True if target reached, False if timeout
        """
        start_time = time.time()
        target_kb = target_mb * 1024
        check_interval = 2.0

        ssh_client = self.checkpoint_mgr.get_ssh_client(self.source_host, self.ssh_user)

        while time.time() - start_time < timeout:
            # Get VmRSS from /proc/{pid}/status
            cmd = f"grep VmRSS /proc/{self.workload_pid}/status | awk '{{print $2}}'"
            stdout, stderr, status = ssh_client.execute(cmd)

            if status == 0 and stdout.strip():
                try:
                    current_kb = int(stdout.strip())
                    current_mb = current_kb / 1024
                    logger.info(f"[Memory Monitor] Current: {current_mb:.1f} MB / Target: {target_mb} MB")

                    if current_kb >= target_kb:
                        logger.info(f"Target memory reached: {current_mb:.1f} MB")
                        return True
                except ValueError:
                    pass

            time.sleep(check_interval)

        logger.warning(f"Timeout after {timeout}s waiting for target memory ({target_mb} MB)")
        return False

    def _run_final_dump(self):
        """Perform final CRIU dump."""
        strategy = self.checkpoint_config['strategy']

        # Build LazyConfig from strategy settings
        lazy_mode_str = strategy.get('lazy_mode', 'none')
        lazy_config = LazyConfig(
            mode=LazyMode(lazy_mode_str),
            page_server_port=strategy.get('page_server_port', 27),
            prefetch_workers=strategy.get('prefetch_workers', 4),
        )

        # Get workload type for CRIU flags
        workload_type = self.experiment_config.get('workload_type', 'memory')

        # Capture workload log before dump
        self.checkpoint_mgr.capture_workload_log(
            self.source_host, 'pre_dump', self.ssh_user
        )

        logger.info(f"Performing final dump (lazy_mode={lazy_config.mode.value})")

        self.metrics.start_timer('final_dump')

        result = self.checkpoint_mgr.final_dump(
            self.source_host,
            self.workload_pid,
            self.checkpoint_iteration,
            lazy_config=lazy_config,
            username=self.ssh_user,
            workload_type=workload_type
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
            {'lazy_config': lazy_config.to_dict(), 'rsync_duration': rsync_duration}
        )

        self.checkpoint_iteration = result['iteration']
        self.final_checkpoint_dir = result['checkpoint_dir']
        self.lazy_config = lazy_config  # Store for restore phase

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
        # Use lazy_config from dump phase (set in _run_final_dump)
        lazy_config = getattr(self, 'lazy_config', LazyConfig(mode=LazyMode.NONE))

        # Get workload type for CRIU flags
        workload_type = self.experiment_config.get('workload_type', 'memory')

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

        logger.info(f"Restoring from {dest_checkpoint_dir} on {self.dest_host} (lazy_mode={lazy_config.mode.value})")

        self.metrics.start_timer('restore')

        # Choose restore method based on whether S3 is needed
        if lazy_config.requires_s3() and transfer_method == 's3':
            # Use S3-based restore for LAZY_PREFETCH and LIVE_MIGRATION_PREFETCH
            s3_config = S3Config.from_dict(self.config.get('s3', {}))
            result = self.checkpoint_mgr.restore_with_s3(
                self.dest_host,
                dest_checkpoint_dir,
                s3_config=s3_config,
                lazy_config=lazy_config,
                page_server_host=self.source_host if lazy_config.requires_page_server() else None,
                username=self.ssh_user,
                workload_type=workload_type
            )
        else:
            # Use standard restore for NONE, LAZY, LIVE_MIGRATION
            result = self.checkpoint_mgr.restore(
                self.dest_host,
                dest_checkpoint_dir,
                lazy_config=lazy_config,
                page_server_host=self.source_host if lazy_config.requires_page_server() else None,
                username=self.ssh_user,
                workload_type=workload_type
            )

        restore_metric = self.metrics.stop_timer('restore')

        if not result['success']:
            raise RuntimeError(f"Restore failed: {result.get('error')}")

        # Verify restored process and capture post-restore log
        verify_result = self.checkpoint_mgr.verify_restored_process(
            self.dest_host, workload_type, wait_time=3.0, username=self.ssh_user
        )

        self.metrics.record_restore(
            restore_metric.duration,
            {
                'lazy_config': lazy_config.to_dict(),
                'process_running': verify_result['is_running'],
                'restored_pids': verify_result['pids']
            }
        )

        if not verify_result['is_running']:
            logger.warning("Restored process exited early - check post_restore log for details")

        logger.info(f"Restore completed in {restore_metric.duration:.2f}s")
