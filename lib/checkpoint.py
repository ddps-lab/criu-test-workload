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

            if exit_status != 0:
                logger.warning(f"Command failed with exit code {exit_status}: {stderr_str}")

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
        full_command = f"cd {self.working_dir} && {command} &"
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

    def pre_dump(self, host: str, pid: str, iteration: int, username: str = 'ubuntu') -> Dict[str, Any]:
        """
        Perform CRIU pre-dump.

        Args:
            host: Remote host IP
            pid: Process PID
            iteration: Pre-dump iteration number (1-indexed)
            username: SSH username

        Returns:
            Dictionary with pre-dump metrics
        """
        client = self.get_ssh_client(host, username)

        # Create checkpoint directory for this iteration
        checkpoint_dir = f"{self.working_dir}/{iteration}"
        client.execute(f"mkdir -p {checkpoint_dir}")

        # Build CRIU pre-dump command
        # -t pid: checkpoint entire process tree starting from pid
        criu_cmd = f"sudo criu pre-dump -D {checkpoint_dir} -t {pid} --shell-job --track-mem"

        # Add --prev-images-dir for iterations after the first
        if iteration > 1:
            criu_cmd += f" --prev-images-dir ../{iteration - 1}"

        logger.info(f"Pre-dump iteration {iteration} on {host}")

        start_time = time.time()
        stdout, stderr, status = client.execute(criu_cmd, timeout=120)
        duration = time.time() - start_time

        if status != 0:
            logger.error(f"Pre-dump failed: {stderr}")
            return {
                'success': False,
                'iteration': iteration,
                'duration': duration,
                'error': stderr
            }

        logger.info(f"Pre-dump {iteration} completed in {duration:.2f}s")

        return {
            'success': True,
            'iteration': iteration,
            'duration': duration,
            'checkpoint_dir': checkpoint_dir
        }

    def final_dump(self, host: str, pid: str, last_iteration: int, lazy_pages: bool = False,
                   page_server_port: int = 22222, username: str = 'ubuntu') -> Dict[str, Any]:
        """
        Perform final CRIU dump.

        Args:
            host: Remote host IP
            pid: Process PID
            last_iteration: Last pre-dump iteration number
            lazy_pages: Enable lazy-pages mode
            page_server_port: Port for lazy-pages server
            username: SSH username

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
        criu_cmd = f"sudo criu dump -D {checkpoint_dir} -t {pid} --shell-job --track-mem"

        # Add --prev-images-dir if there were pre-dumps
        if last_iteration > 0:
            criu_cmd += f" --prev-images-dir ../{last_iteration}"

        # Add lazy-pages options
        if lazy_pages:
            criu_cmd += f" --lazy-pages --address 0.0.0.0 --port {page_server_port}"
            # Run in background for lazy-pages
            criu_cmd += " &"

        logger.info(f"Final dump on {host} (iteration {iteration}, lazy_pages={lazy_pages})")

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
                logger.error(f"Final dump failed: {stderr}")
                duration = time.time() - start_time
                return {
                    'success': False,
                    'iteration': iteration,
                    'duration': duration,
                    'error': stderr
                }

        duration = time.time() - start_time
        logger.info(f"Final dump completed in {duration:.2f}s")

        return {
            'success': True,
            'iteration': iteration,
            'duration': duration,
            'checkpoint_dir': checkpoint_dir,
            'lazy_pages': lazy_pages
        }

    def restore(self, host: str, checkpoint_dir: str, lazy_pages: bool = False,
                page_server_host: Optional[str] = None, page_server_port: int = 22222,
                username: str = 'ubuntu', pid_file: Optional[str] = None) -> Dict[str, Any]:
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

        Returns:
            Dictionary with restore metrics
        """
        client = self.get_ssh_client(host, username)

        # If using lazy-pages, start page server first
        if lazy_pages and page_server_host:
            logger.info(f"Starting page server on {host}")
            page_server_cmd = f"sudo criu lazy-pages --images-dir {checkpoint_dir} --page-server --address {page_server_host} --port {page_server_port} &"
            client.execute_background(page_server_cmd)
            time.sleep(2)  # Give page server time to start

        # Build CRIU restore command
        # -d (--detach): detach from restored process immediately after restore
        # This allows us to measure actual restore time, not process runtime
        # --pidfile: write restored process PID to file for verification
        criu_cmd = f"sudo criu restore -D {checkpoint_dir} --shell-job -d"

        if pid_file:
            criu_cmd += f" --pidfile {pid_file}"

        if lazy_pages:
            criu_cmd += " --lazy-pages"

        logger.info(f"Restoring on {host} from {checkpoint_dir}")

        start_time = time.time()
        stdout, stderr, status = client.execute(criu_cmd, timeout=300)
        duration = time.time() - start_time

        if status != 0:
            logger.error(f"Restore failed: {stderr}")
            return {
                'success': False,
                'duration': duration,
                'error': stderr,
                'stdout': stdout
            }

        logger.info(f"Restore completed in {duration:.2f}s")

        return {
            'success': True,
            'duration': duration,
            'lazy_pages': lazy_pages
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

    def close_all_connections(self):
        """Close all SSH connections."""
        for host, client in self.ssh_clients.items():
            client.close()
        self.ssh_clients.clear()
