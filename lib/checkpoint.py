"""
CRIU checkpoint and restore operations with SSH-based remote execution.
"""

import subprocess
import time
import paramiko
from typing import Dict, Any, Optional, List
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class SSHClient:
    """Wrapper for SSH connections to remote hosts."""

    def __init__(self, host: str, username: str = 'ubuntu', key_filename: str = '~/.ssh/id_ed25519'):
        """
        Initialize SSH client.

        Args:
            host: Remote host IP/hostname
            username: SSH username
            key_filename: Path to SSH private key
        """
        self.host = host
        self.username = username
        self.key_filename = str(Path(key_filename).expanduser())
        self.client: Optional[paramiko.SSHClient] = None

    def connect(self):
        """Establish SSH connection."""
        if self.client is None:
            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            self.client.connect(
                hostname=self.host,
                username=self.username,
                key_filename=self.key_filename
            )
            logger.info(f"Connected to {self.host}")
        except Exception as e:
            logger.error(f"Failed to connect to {self.host}: {e}")
            raise

    def execute(self, command: str, timeout: Optional[int] = None) -> tuple[str, str, int]:
        """
        Execute command on remote host.

        Args:
            command: Shell command to execute
            timeout: Command timeout in seconds

        Returns:
            Tuple of (stdout, stderr, exit_status)
        """
        if self.client is None:
            self.connect()

        logger.debug(f"Executing on {self.host}: {command}")

        try:
            stdin, stdout, stderr = self.client.exec_command(command, timeout=timeout)
            exit_status = stdout.channel.recv_exit_status()

            stdout_str = stdout.read().decode('utf-8')
            stderr_str = stderr.read().decode('utf-8')

            if exit_status != 0 and stderr_str.strip():
                logger.debug(f"Command exited with code {exit_status}: {stderr_str}")

            return stdout_str, stderr_str, exit_status

        except Exception as e:
            logger.error(f"Command execution failed: {e}")
            raise

    def execute_background(self, command: str):
        """
        Execute command in background (non-blocking).

        Args:
            command: Shell command to execute
        """
        if self.client is None:
            self.connect()

        logger.debug(f"Executing in background on {self.host}: {command}")
        self.client.exec_command(command)

    def download_file(self, remote_path: str, local_path: str) -> bool:
        """
        Download file from remote host using SFTP.

        Args:
            remote_path: Path on remote host
            local_path: Local destination path

        Returns:
            True if successful, False otherwise
        """
        if self.client is None:
            self.connect()

        try:
            sftp = self.client.open_sftp()
            sftp.get(remote_path, local_path)
            sftp.close()
            logger.debug(f"Downloaded {remote_path} to {local_path}")
            return True
        except Exception as e:
            logger.warning(f"Failed to download {remote_path}: {e}")
            return False

    def download_directory(self, remote_dir: str, local_dir: str, pattern: str = "*.log") -> List[str]:
        """
        Download files matching pattern from remote directory.

        Args:
            remote_dir: Remote directory path
            local_dir: Local destination directory
            pattern: Glob pattern for files to download (default: *.log)

        Returns:
            List of downloaded file paths
        """
        if self.client is None:
            self.connect()

        downloaded = []
        try:
            sftp = self.client.open_sftp()

            # Create local directory if needed
            Path(local_dir).mkdir(parents=True, exist_ok=True)

            # List remote directory
            try:
                files = sftp.listdir(remote_dir)
            except FileNotFoundError:
                logger.warning(f"Remote directory not found: {remote_dir}")
                sftp.close()
                return downloaded

            # Download matching files
            import fnmatch
            for filename in files:
                if fnmatch.fnmatch(filename, pattern):
                    remote_path = f"{remote_dir}/{filename}"
                    local_path = f"{local_dir}/{filename}"
                    try:
                        sftp.get(remote_path, local_path)
                        downloaded.append(local_path)
                        logger.debug(f"Downloaded {remote_path}")
                    except Exception as e:
                        logger.warning(f"Failed to download {remote_path}: {e}")

            sftp.close()

        except Exception as e:
            logger.error(f"Failed to download from {remote_dir}: {e}")

        return downloaded

    def close(self):
        """Close SSH connection."""
        if self.client:
            self.client.close()
            self.client = None
            logger.info(f"Disconnected from {self.host}")


class CheckpointManager:
    """Manage CRIU checkpoint and restore operations."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize checkpoint manager.

        Args:
            config: Checkpoint configuration dictionary
        """
        self.config = config
        self.strategy = config.get('strategy', {})
        self.dirs = config.get('dirs', {})
        self.working_dir = self.dirs.get('working_dir', '/tmp/criu_checkpoint')
        self.ssh_clients: Dict[str, SSHClient] = {}

    def get_ssh_client(self, host: str, username: str = 'ubuntu') -> SSHClient:
        """
        Get or create SSH client for host.

        Args:
            host: Host IP/hostname
            username: SSH username

        Returns:
            SSHClient instance
        """
        if host not in self.ssh_clients:
            self.ssh_clients[host] = SSHClient(host, username)

        return self.ssh_clients[host]

    def cleanup_and_prepare(self, host: str, username: str = 'ubuntu'):
        """
        Clean up and prepare working directory on remote host.

        Args:
            host: Remote host IP
            username: SSH username
        """
        client = self.get_ssh_client(host, username)

        commands = [
            f"rm -rf {self.working_dir}",
            f"mkdir -p {self.working_dir}",
        ]

        for cmd in commands:
            stdout, stderr, status = client.execute(cmd)
            if status != 0:
                logger.warning(f"Cleanup command failed: {cmd}, error: {stderr}")

    def start_workload(self, host: str, command: str, username: str = 'ubuntu') -> str:
        """
        Start workload process on remote host and return PID.

        Args:
            host: Remote host IP
            command: Command to start workload
            username: SSH username

        Returns:
            Process PID as string
        """
        client = self.get_ssh_client(host, username)

        # Create checkpoint flag file
        client.execute(f"touch {self.working_dir}/checkpoint_flag")

        # Start workload in background
        # Output goes to /dev/null to avoid file descriptor dependencies in CRIU
        # Workload logs are captured separately via capture_workload_output() before dump
        full_command = f"cd {self.working_dir} && {command} > /dev/null 2>&1 &"
        client.execute_background(full_command)

        # Give process time to start
        time.sleep(2)

        # Get PID
        pid_cmd = f"ps -ef | grep '{command}' | grep -v grep | awk '{{print $2}}' | tail -n 1"
        stdout, stderr, status = client.execute(pid_cmd)

        if status != 0:
            raise RuntimeError(f"Failed to get workload PID: {stderr}")

        pid = stdout.strip()
        if not pid:
            raise RuntimeError(f"No PID found for command: {command}")

        logger.info(f"Started workload with PID {pid} on {host}")
        return pid

    def wait_for_ready(self, host: str, ready_file: str = 'checkpoint_ready', timeout: int = 300, username: str = 'ubuntu'):
        """
        Wait for workload to signal ready.

        Args:
            host: Remote host IP
            ready_file: Ready signal file path
            timeout: Timeout in seconds
            username: SSH username
        """
        client = self.get_ssh_client(host, username)
        ready_path = f"{self.working_dir}/{ready_file}"

        start_time = time.time()
        while time.time() - start_time < timeout:
            stdout, stderr, status = client.execute(f"test -f {ready_path} && echo exists")
            if 'exists' in stdout:
                logger.info(f"Workload ready on {host}")
                return

            time.sleep(0.5)

        raise TimeoutError(f"Workload not ready after {timeout}s")

    def pre_dump(self, host: str, pid: str, iteration: int, username: str = 'ubuntu',
                 workload_type: str = None) -> Dict[str, Any]:
        """
        Perform CRIU pre-dump.

        Args:
            host: Remote host IP
            pid: Process PID
            iteration: Pre-dump iteration number (1-indexed)
            username: SSH username
            workload_type: Type of workload (for workload-specific flags)

        Returns:
            Dictionary with pre-dump metrics
        """
        client = self.get_ssh_client(host, username)

        # Create checkpoint directory for this iteration
        checkpoint_dir = f"{self.working_dir}/{iteration}"
        client.execute(f"mkdir -p {checkpoint_dir}")

        # Build CRIU pre-dump command
        # -t pid: checkpoint entire process tree starting from pid
        log_file = f"{checkpoint_dir}/criu-pre-dump.log"
        criu_cmd = f"sudo criu pre-dump -D {checkpoint_dir} -t {pid} --shell-job --track-mem"
        criu_cmd += f" --log-file {log_file} -v4"

        # Redis needs --tcp-established for Python-Redis TCP connection
        if workload_type == 'redis':
            criu_cmd += " --tcp-established"

        # Add --prev-images-dir for iterations after the first
        if iteration > 1:
            criu_cmd += f" --prev-images-dir ../{iteration - 1}"

        logger.info(f"Pre-dump iteration {iteration} on {host} (log: {log_file})")

        start_time = time.time()
        stdout, stderr, status = client.execute(criu_cmd, timeout=120)
        duration = time.time() - start_time

        # Fix permissions for rsync (CRIU creates files as root)
        # Do this even on failure so we can collect logs
        client.execute(f"sudo chmod -R a+r {checkpoint_dir}")

        if status != 0:
            # Try to get more details from CRIU log
            log_content = ""
            log_stdout, _, log_status = client.execute(f"tail -30 {log_file} 2>/dev/null")
            if log_status == 0:
                log_content = log_stdout

            error_msg = stderr if stderr else "Unknown error (check CRIU log)"
            logger.error(f"Pre-dump failed: {error_msg}")
            if log_content:
                logger.error(f"CRIU log tail:\n{log_content}")

            return {
                'success': False,
                'iteration': iteration,
                'duration': duration,
                'error': error_msg,
                'log_file': log_file,
                'log_content': log_content
            }

        logger.info(f"Pre-dump {iteration} completed in {duration:.2f}s")

        return {
            'success': True,
            'iteration': iteration,
            'duration': duration,
            'checkpoint_dir': checkpoint_dir,
            'log_file': log_file
        }

    def final_dump(self, host: str, pid: str, last_iteration: int, lazy_pages: bool = False,
                   page_server_port: int = 22222, username: str = 'ubuntu',
                   workload_type: str = None) -> Dict[str, Any]:
        """
        Perform final CRIU dump.

        Args:
            host: Remote host IP
            pid: Process PID
            last_iteration: Last pre-dump iteration number
            lazy_pages: Enable lazy-pages mode
            page_server_port: Port for lazy-pages server
            username: SSH username
            workload_type: Type of workload (for workload-specific flags)

        Returns:
            Dictionary with dump metrics
        """
        client = self.get_ssh_client(host, username)

        # Create checkpoint directory
        iteration = last_iteration + 1
        checkpoint_dir = f"{self.working_dir}/{iteration}"
        client.execute(f"mkdir -p {checkpoint_dir}")

        # Build CRIU dump command
        # -t pid: checkpoint entire process tree starting from pid
        log_file = f"{checkpoint_dir}/criu-dump.log"
        criu_cmd = f"sudo criu dump -D {checkpoint_dir} -t {pid} --shell-job --track-mem"
        criu_cmd += f" --log-file {log_file} -v4"

        # Redis needs --tcp-established for Python-Redis TCP connection
        if workload_type == 'redis':
            criu_cmd += " --tcp-established"

        # Add --prev-images-dir if there were pre-dumps
        if last_iteration > 0:
            criu_cmd += f" --prev-images-dir ../{last_iteration}"

        # Add lazy-pages options
        if lazy_pages:
            criu_cmd += f" --lazy-pages --address 0.0.0.0 --port {page_server_port}"
            # Run in background for lazy-pages
            criu_cmd += " &"

        logger.info(f"Final dump on {host} (iteration {iteration}, lazy_pages={lazy_pages}, log: {log_file})")

        start_time = time.time()

        if lazy_pages:
            # Execute in background
            client.execute_background(criu_cmd)

            # Wait for dump to complete by monitoring file changes
            wait_cmd = f"""
            latest_change_time=$(date +%s)
            while true
            do
                latest_mod_time=$(find {self.working_dir} -type f -exec stat --format='%Y' {{}} + 2>/dev/null | sort -nr | head -n 1)
                if [[ $latest_mod_time -gt $latest_change_time ]]; then
                    latest_change_time=$latest_mod_time
                fi

                current_time=$(date +%s)
                idle_time=$((current_time - latest_change_time))

                if [[ $idle_time -ge 2 ]]; then
                    break
                fi
                sleep 0.5
            done
            """
            client.execute(wait_cmd, timeout=300)

        else:
            # Execute synchronously
            stdout, stderr, status = client.execute(criu_cmd, timeout=300)

            if status != 0:
                duration = time.time() - start_time

                # Fix permissions so we can read the log
                client.execute(f"sudo chmod -R a+r {checkpoint_dir}")

                # Try to get more details from CRIU log
                log_content = ""
                log_stdout, _, log_status = client.execute(f"tail -30 {log_file} 2>/dev/null")
                if log_status == 0:
                    log_content = log_stdout

                error_msg = stderr if stderr else "Unknown error (check CRIU log)"
                logger.error(f"Final dump failed: {error_msg}")
                if log_content:
                    logger.error(f"CRIU log tail:\n{log_content}")

                return {
                    'success': False,
                    'iteration': iteration,
                    'duration': duration,
                    'error': error_msg,
                    'log_file': log_file,
                    'log_content': log_content
                }

        duration = time.time() - start_time

        # Fix permissions for rsync (CRIU creates files as root)
        client.execute(f"sudo chmod -R a+r {checkpoint_dir}")

        logger.info(f"Final dump completed in {duration:.2f}s")

        return {
            'success': True,
            'iteration': iteration,
            'duration': duration,
            'checkpoint_dir': checkpoint_dir,
            'lazy_pages': lazy_pages,
            'log_file': log_file
        }

    def restore(self, host: str, checkpoint_dir: str, lazy_pages: bool = False,
                page_server_host: Optional[str] = None, page_server_port: int = 22222,
                username: str = 'ubuntu', pid_file: Optional[str] = None,
                workload_type: str = None) -> Dict[str, Any]:
        """
        Restore process from checkpoint.

        Args:
            host: Destination host IP
            checkpoint_dir: Checkpoint directory path
            lazy_pages: Use lazy-pages mode
            page_server_host: Page server host (for lazy-pages)
            page_server_port: Page server port
            username: SSH username
            pid_file: Optional path to write restored process PID
            workload_type: Type of workload (for workload-specific flags)

        Returns:
            Dictionary with restore metrics
        """
        client = self.get_ssh_client(host, username)

        # Log file paths
        restore_log_file = f"{checkpoint_dir}/criu-restore.log"
        lazy_pages_log_file = f"{checkpoint_dir}/criu-lazy-pages.log" if lazy_pages else None

        # If using lazy-pages, start page server first
        if lazy_pages and page_server_host:
            logger.info(f"Starting page server on {host} (log: {lazy_pages_log_file})")
            page_server_cmd = f"sudo criu lazy-pages --images-dir {checkpoint_dir} --page-server --address {page_server_host} --port {page_server_port}"
            page_server_cmd += f" --log-file {lazy_pages_log_file} -v4 &"
            client.execute_background(page_server_cmd)
            time.sleep(2)  # Give page server time to start

        # Build CRIU restore command
        # -d (--detach): detach from restored process immediately after restore
        # This allows us to measure actual restore time, not process runtime
        # --pidfile: write restored process PID to file for verification
        criu_cmd = f"sudo criu restore -D {checkpoint_dir} --shell-job -d"
        criu_cmd += f" --log-file {restore_log_file} -v4"

        # Redis needs --tcp-established for Python-Redis TCP connection
        if workload_type == 'redis':
            criu_cmd += " --tcp-established"

        if pid_file:
            criu_cmd += f" --pidfile {pid_file}"

        if lazy_pages:
            criu_cmd += " --lazy-pages"

        logger.info(f"Restoring on {host} from {checkpoint_dir} (log: {restore_log_file})")

        start_time = time.time()
        stdout, stderr, status = client.execute(criu_cmd, timeout=300)
        duration = time.time() - start_time

        # Fix permissions for log collection (CRIU creates files as root)
        # Do this even on failure so we can collect logs
        client.execute(f"sudo chmod -R a+r {checkpoint_dir}")

        if status != 0:
            # Try to get more details from CRIU log
            log_content = ""
            log_stdout, _, log_status = client.execute(f"tail -30 {restore_log_file} 2>/dev/null")
            if log_status == 0:
                log_content = log_stdout

            error_msg = stderr if stderr else "Unknown error (check CRIU log)"
            logger.error(f"Restore failed: {error_msg}")
            if log_content:
                logger.error(f"CRIU log tail:\n{log_content}")

            return {
                'success': False,
                'duration': duration,
                'error': error_msg,
                'stdout': stdout,
                'log_file': restore_log_file,
                'log_content': log_content
            }

        logger.info(f"Restore completed in {duration:.2f}s")

        return {
            'success': True,
            'duration': duration,
            'lazy_pages': lazy_pages,
            'log_file': restore_log_file,
            'lazy_pages_log_file': lazy_pages_log_file
        }

    def verify_restore(self, host: str, pid: Optional[str] = None,
                       pid_file: Optional[str] = None,
                       timeout: int = 30, username: str = 'ubuntu') -> Dict[str, Any]:
        """
        Verify that restored process is running correctly.

        Args:
            host: Host where process was restored
            pid: Known PID to check (if available)
            pid_file: File containing restored PID (from --pidfile option)
            timeout: Timeout for verification in seconds
            username: SSH username

        Returns:
            Dictionary with verification results
        """
        client = self.get_ssh_client(host, username)

        # Get PID from file if provided
        if pid_file and not pid:
            stdout, stderr, status = client.execute(f"cat {pid_file} 2>/dev/null")
            if status == 0 and stdout.strip():
                pid = stdout.strip()

        if not pid:
            logger.warning("No PID available for verification")
            return {
                'verified': False,
                'error': 'No PID available'
            }

        # Check if process is running
        start_time = time.time()
        while time.time() - start_time < timeout:
            # Check process exists
            stdout, stderr, status = client.execute(f"ps -p {pid} -o state= 2>/dev/null")

            if status == 0 and stdout.strip():
                state = stdout.strip()
                # Process states: R=running, S=sleeping, D=disk sleep, etc.
                # All are valid "alive" states
                if state in ['R', 'S', 'D', 'T', 'Z']:
                    logger.info(f"Process {pid} verified running (state: {state})")
                    return {
                        'verified': True,
                        'pid': pid,
                        'state': state,
                        'verification_time': time.time() - start_time
                    }

            time.sleep(0.5)

        logger.error(f"Process {pid} verification failed after {timeout}s")
        return {
            'verified': False,
            'pid': pid,
            'error': f'Process not found after {timeout}s'
        }

    def verify_workload_health(self, host: str, workload_type: str,
                               config: Dict[str, Any], username: str = 'ubuntu') -> Dict[str, Any]:
        """
        Verify workload-specific health after restore.

        Args:
            host: Host where workload was restored
            workload_type: Type of workload (redis, video, etc.)
            config: Workload configuration
            username: SSH username

        Returns:
            Dictionary with health check results
        """
        client = self.get_ssh_client(host, username)

        if workload_type == 'redis':
            # Check Redis is responding
            port = config.get('redis_port', 6379)
            stdout, stderr, status = client.execute(
                f"redis-cli -p {port} ping 2>/dev/null",
                timeout=10
            )
            if status == 0 and 'PONG' in stdout:
                # Also check key count
                stdout2, _, status2 = client.execute(
                    f"redis-cli -p {port} dbsize 2>/dev/null",
                    timeout=10
                )
                return {
                    'healthy': True,
                    'service': 'redis',
                    'response': 'PONG',
                    'dbsize': stdout2.strip() if status2 == 0 else 'unknown'
                }
            return {
                'healthy': False,
                'service': 'redis',
                'error': stderr or 'No PONG response'
            }

        elif workload_type == 'video':
            # Check ffmpeg process is running
            stdout, stderr, status = client.execute(
                "pgrep -x ffmpeg",
                timeout=10
            )
            if status == 0 and stdout.strip():
                return {
                    'healthy': True,
                    'service': 'ffmpeg',
                    'pid': stdout.strip()
                }
            return {
                'healthy': False,
                'service': 'ffmpeg',
                'error': 'ffmpeg process not found'
            }

        else:
            # Generic Python process check
            stdout, stderr, status = client.execute(
                f"pgrep -f '{workload_type}_standalone.py'",
                timeout=10
            )
            if status == 0 and stdout.strip():
                return {
                    'healthy': True,
                    'service': workload_type,
                    'pid': stdout.strip()
                }
            return {
                'healthy': False,
                'service': workload_type,
                'error': f'{workload_type} process not found'
            }

    def cleanup_processes(self, host: str, workload_type: str, username: str = 'ubuntu') -> Dict[str, Any]:
        """
        Terminate workload processes on remote host.

        Args:
            host: Remote host IP
            workload_type: Type of workload to terminate
            username: SSH username

        Returns:
            Dictionary with cleanup results
        """
        client = self.get_ssh_client(host, username)

        # Build process pattern based on workload type
        if workload_type == 'redis':
            pattern = 'redis-server'
        elif workload_type == 'video':
            pattern = 'ffmpeg'
        else:
            pattern = f'{workload_type}_standalone.py'

        # Kill matching processes
        kill_cmd = f"pkill -f '{pattern}' 2>/dev/null || true"
        stdout, stderr, status = client.execute(kill_cmd)

        # Also kill any lingering CRIU processes
        client.execute("sudo pkill -f 'criu lazy-pages' 2>/dev/null || true")

        logger.info(f"Cleaned up {workload_type} processes on {host}")

        return {
            'cleaned': True,
            'host': host,
            'workload_type': workload_type
        }

    def wait_for_lazy_pages_complete(self, host: str, timeout: int = 600,
                                      username: str = 'ubuntu') -> Dict[str, Any]:
        """
        Wait for lazy-pages server to complete all page transfers.

        The lazy-pages server automatically exits when all pages have been transferred.

        Args:
            host: Host where lazy-pages server is running
            timeout: Maximum time to wait in seconds
            username: SSH username

        Returns:
            Dictionary with completion status and timing
        """
        client = self.get_ssh_client(host, username)

        logger.info(f"Waiting for lazy-pages completion on {host}...")

        start_time = time.time()
        check_interval = 0.5

        while time.time() - start_time < timeout:
            # Check if lazy-pages process is still running
            stdout, stderr, status = client.execute(
                "pgrep -f 'criu lazy-pages' 2>/dev/null"
            )

            if status != 0 or not stdout.strip():
                # Process not found = completed
                duration = time.time() - start_time
                logger.info(f"Lazy-pages completed in {duration:.2f}s")
                return {
                    'completed': True,
                    'duration': duration
                }

            time.sleep(check_interval)

        duration = time.time() - start_time
        logger.warning(f"Lazy-pages timeout after {duration:.2f}s")
        return {
            'completed': False,
            'duration': duration,
            'error': f'Timeout after {timeout}s'
        }

    def collect_logs(self, source_host: str, dest_host: str, local_output_dir: str,
                     username: str = 'ubuntu', experiment_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Collect CRIU log files from source and destination nodes.

        Args:
            source_host: Source node IP
            dest_host: Destination node IP
            local_output_dir: Local directory to save logs
            username: SSH username
            experiment_name: Optional experiment name for directory naming

        Returns:
            Dictionary with collected log file paths
        """
        from datetime import datetime

        # Create timestamped output directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if experiment_name:
            # Use experiment name with timestamp: my_exp_20240101_120000
            dir_name = f"{experiment_name}_{timestamp}"
        else:
            # Default: criu_logs_20240101_120000
            dir_name = f"criu_logs_{timestamp}"
        output_dir = Path(local_output_dir) / dir_name
        output_dir.mkdir(parents=True, exist_ok=True)

        collected = {
            'output_dir': str(output_dir),
            'source': [],
            'dest': [],
            'timestamp': timestamp
        }

        # Source log patterns: pre-dump, dump, page-server, workload status and stdout
        source_patterns = ['criu-pre-dump.log', 'criu-dump.log', 'criu-page-server.log',
                          'workload_status_pre_dump.txt', 'workload_stdout_pre_dump.log',
                          'workload_stdout_pre_dump.log.info']
        # Dest log patterns: restore, lazy-pages, workload status and stdout
        dest_patterns = ['criu-restore.log', 'criu-lazy-pages.log',
                        'workload_status_post_restore.txt', 'workload_stdout_post_restore.log',
                        'workload_stdout_post_restore.log.info']

        # Collect from source node
        source_client = self.get_ssh_client(source_host, username)
        source_dir = output_dir / "source"
        source_dir.mkdir(exist_ok=True)

        # Find checkpoint directories and collect source-specific logs
        stdout, stderr, status = source_client.execute(
            f"find {self.working_dir} \\( -name '*.log' -o -name '*.log.info' -o -name 'workload_status_*.txt' \\) -type f 2>/dev/null"
        )

        if status == 0 and stdout.strip():
            for remote_log in stdout.strip().split('\n'):
                if remote_log:
                    filename = Path(remote_log).name
                    # Only collect source-specific logs
                    if filename in source_patterns:
                        rel_path = remote_log.replace(self.working_dir + '/', '')
                        local_path = source_dir / rel_path
                        local_path.parent.mkdir(parents=True, exist_ok=True)

                        if source_client.download_file(remote_log, str(local_path)):
                            collected['source'].append(str(local_path))

        # Collect from destination node
        dest_client = self.get_ssh_client(dest_host, username)
        dest_dir = output_dir / "dest"
        dest_dir.mkdir(exist_ok=True)

        stdout, stderr, status = dest_client.execute(
            f"find {self.working_dir} \\( -name '*.log' -o -name '*.log.info' -o -name 'workload_status_*.txt' \\) -type f 2>/dev/null"
        )

        if status == 0 and stdout.strip():
            for remote_log in stdout.strip().split('\n'):
                if remote_log:
                    filename = Path(remote_log).name
                    # Only collect dest-specific logs (restore, lazy-pages)
                    if filename in dest_patterns:
                        rel_path = remote_log.replace(self.working_dir + '/', '')
                        local_path = dest_dir / rel_path
                        local_path.parent.mkdir(parents=True, exist_ok=True)

                        if dest_client.download_file(remote_log, str(local_path)):
                            collected['dest'].append(str(local_path))

        total = len(collected['source']) + len(collected['dest'])
        logger.info(f"Collected {total} log files to {output_dir}")

        return collected

    def capture_workload_log(self, host: str, label: str, username: str = 'ubuntu',
                             strace_duration: float = 6.0) -> str:
        """
        Capture workload status and stdout via strace.

        Uses strace to capture process stdout/stderr writes for a short duration,
        then detaches before checkpoint to avoid CRIU issues.

        Args:
            host: Remote host IP
            label: Label for this snapshot (e.g., 'pre_dump', 'post_restore')
            username: SSH username
            strace_duration: How long to capture stdout via strace (seconds)

        Returns:
            Status info as string
        """
        client = self.get_ssh_client(host, username)
        status_file = f"{self.working_dir}/workload_status_{label}.txt"
        strace_file = f"{self.working_dir}/workload_stdout_{label}.log"

        # First, capture stdout via strace for a short duration
        # Find workload PID and run strace
        # Note: strace captures write syscalls even if stdout is /dev/null
        # Use longer duration (6s) since some workloads print every 5 seconds
        strace_cmd = f"""
        # Find workload process - try multiple patterns
        PID=""
        for pattern in "standalone.py" "python3.*workload" "ffmpeg" "redis-server"; do
            PID=$(pgrep -f "$pattern" 2>/dev/null | head -1)
            if [ -n "$PID" ]; then
                break
            fi
        done

        echo "=== Strace Debug Info ===" > {strace_file}.info
        echo "Timestamp: $(date -Iseconds)" >> {strace_file}.info
        echo "Looking for workload process..." >> {strace_file}.info

        if [ -n "$PID" ]; then
            echo "Found PID: $PID" >> {strace_file}.info
            echo "Process info:" >> {strace_file}.info
            ps -p $PID -o pid,ppid,cmd 2>&1 >> {strace_file}.info

            echo "Attaching strace for {strace_duration}s..." >> {strace_file}.info

            # Run strace with timeout, redirect strace's own stderr to .info
            sudo timeout {strace_duration} strace -p $PID -e trace=write -e write=1,2 -s 1000 -o {strace_file} 2>> {strace_file}.info
            STRACE_EXIT=$?

            echo "Strace exit code: $STRACE_EXIT" >> {strace_file}.info
            echo "Strace output file:" >> {strace_file}.info
            ls -la {strace_file} 2>&1 >> {strace_file}.info
            echo "First 5 lines of strace output:" >> {strace_file}.info
            head -5 {strace_file} 2>&1 >> {strace_file}.info
        else
            echo "No workload PID found" >> {strace_file}.info
            echo "Running processes:" >> {strace_file}.info
            ps aux | grep -E 'python|ffmpeg|redis' | grep -v grep >> {strace_file}.info 2>&1
        fi
        """
        client.execute(strace_cmd)

        # Collect process status information
        status_cmd = f"""
        echo "=== Workload Status ({label}) ===" > {status_file}
        echo "Timestamp: $(date -Iseconds)" >> {status_file}
        echo "" >> {status_file}

        # Find workload processes
        echo "=== Process List ===" >> {status_file}
        ps aux | grep -E 'python3|ffmpeg|redis-server' | grep -v grep >> {status_file} 2>/dev/null || echo "No processes found" >> {status_file}
        echo "" >> {status_file}

        # Memory info for each process
        echo "=== Process Memory (from /proc) ===" >> {status_file}
        for pid in $(pgrep -f 'standalone.py|ffmpeg|redis-server' 2>/dev/null); do
            if [ -f /proc/$pid/status ]; then
                echo "PID $pid:" >> {status_file}
                grep -E '^(Name|VmRSS|VmSize|VmPeak|Threads)' /proc/$pid/status >> {status_file} 2>/dev/null
                echo "" >> {status_file}
            fi
        done

        # Check checkpoint_ready file
        echo "=== Checkpoint Ready File ===" >> {status_file}
        cat {self.working_dir}/checkpoint_ready 2>/dev/null || echo "Not found" >> {status_file}
        echo "" >> {status_file}

        # Include strace info and output if captured
        if [ -f {strace_file}.info ]; then
            echo "=== Strace Info ===" >> {status_file}
            cat {strace_file}.info >> {status_file} 2>/dev/null
            echo "" >> {status_file}
        fi
        if [ -f {strace_file} ] && [ -s {strace_file} ]; then
            echo "=== Workload Stdout (via strace) ===" >> {status_file}
            head -50 {strace_file} >> {status_file} 2>/dev/null
            echo "" >> {status_file}
        else
            echo "=== Workload Stdout ===" >> {status_file}
            echo "(strace log empty or not found)" >> {status_file}
            echo "" >> {status_file}
        fi

        cat {status_file}
        """

        stdout, stderr, status = client.execute(status_cmd)

        if status == 0:
            logger.info(f"Captured workload status ({label})")
            return stdout
        else:
            logger.warning(f"Could not capture workload status on {host}")
            return ""

    def verify_restored_process(self, host: str, workload_type: str,
                                 wait_time: float = 6.0, username: str = 'ubuntu') -> Dict[str, Any]:
        """
        Verify restored process is running and capture post-restore log.

        Args:
            host: Remote host IP
            workload_type: Type of workload
            wait_time: Time to wait/strace duration (6s default to catch output from 5s interval workloads)
            username: SSH username

        Returns:
            Dictionary with verification results
        """
        client = self.get_ssh_client(host, username)

        # Check if process is still running
        process_patterns = {
            'memory': 'memory_standalone.py',
            'matmul': 'matmul_standalone.py',
            'redis': 'redis-server',
            'video': 'ffmpeg',
            'dataproc': 'dataproc_standalone.py',
            'ml_training': 'ml_training_standalone.py',
        }

        pattern = process_patterns.get(workload_type, workload_type)
        ps_cmd = f"pgrep -f '{pattern}'"
        stdout, stderr, status = client.execute(ps_cmd)

        is_running = status == 0 and stdout.strip()
        pids = stdout.strip().split('\n') if is_running else []

        # Capture post-restore workload status (includes strace capture)
        # Use wait_time as strace duration (default 6s to catch 5s interval outputs)
        post_log = self.capture_workload_log(host, 'post_restore', username, strace_duration=wait_time)

        result = {
            'is_running': is_running,
            'post_restore_log': post_log,
            'pids': pids
        }

        if is_running:
            logger.info(f"Restored process is running (PIDs: {result['pids']})")
        else:
            logger.warning(f"Restored process is NOT running")

        return result

    def close_all_connections(self):
        """Close all SSH connections."""
        for host, client in self.ssh_clients.items():
            client.close()
        self.ssh_clients.clear()
