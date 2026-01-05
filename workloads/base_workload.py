"""
Base workload class for CRIU experiments.

All workloads should inherit from BaseWorkload and implement:
- get_standalone_script(): Returns the standalone script content
- get_command(): Returns the command to start the workload
"""

import os
import paramiko
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class BaseWorkload(ABC):
    """
    Abstract base class for all workloads.

    Manages the lifecycle of a workload:
    1. Deploy standalone script to remote workload node via SCP
    2. Provide command to start the workload
    3. Handle workload-specific configuration
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize workload.

        Args:
            config: Workload configuration dictionary
        """
        self.config = config
        self.name = self.__class__.__name__
        self.working_dir = config.get('working_dir', '/tmp/criu_checkpoint')
        self.ssh_user = config.get('ssh_user', 'ubuntu')
        self.ssh_key = config.get('ssh_key', '~/.ssh/id_ed25519')

    @abstractmethod
    def get_standalone_script_name(self) -> str:
        """
        Get the name of the standalone script file.

        Returns:
            Script filename (e.g., 'memory_standalone.py')
        """
        pass

    @abstractmethod
    def get_standalone_script_content(self) -> str:
        """
        Get the content of the standalone script.

        This script will be deployed to workload nodes and should:
        - Have no external dependencies beyond standard library + specified packages
        - Create a 'checkpoint_ready' file when ready for checkpointing
        - Check for 'checkpoint_flag' file to know when restore is complete
        - Exit cleanly when checkpoint_flag is removed

        Returns:
            Script content as string
        """
        pass

    @abstractmethod
    def get_command(self) -> str:
        """
        Get the command to start the workload.

        Returns:
            Command string (e.g., 'python3 memory_standalone.py --size 256')
        """
        pass

    def get_dependencies(self) -> list[str]:
        """
        Get list of Python packages required by this workload.

        Override in subclasses if additional packages are needed.

        Returns:
            List of package names (e.g., ['numpy', 'torch'])
        """
        return []

    def deploy(self, host: str) -> bool:
        """
        Deploy standalone script to remote host.

        Args:
            host: Remote host IP/hostname

        Returns:
            True if deployment successful
        """
        try:
            # Connect to host
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            key_path = str(Path(self.ssh_key).expanduser())
            ssh.connect(hostname=host, username=self.ssh_user, key_filename=key_path)

            # Create working directory
            stdin, stdout, stderr = ssh.exec_command(f"mkdir -p {self.working_dir}")
            stdout.channel.recv_exit_status()

            # Write standalone script
            sftp = ssh.open_sftp()
            script_path = f"{self.working_dir}/{self.get_standalone_script_name()}"
            script_content = self.get_standalone_script_content()

            with sftp.open(script_path, 'w') as f:
                f.write(script_content)

            sftp.close()

            # Make script executable
            ssh.exec_command(f"chmod +x {script_path}")

            logger.info(f"Deployed {self.get_standalone_script_name()} to {host}:{script_path}")

            ssh.close()
            return True

        except Exception as e:
            logger.error(f"Failed to deploy workload to {host}: {e}")
            return False

    def install_dependencies(self, host: str) -> bool:
        """
        Install required Python packages on remote host.

        Args:
            host: Remote host IP/hostname

        Returns:
            True if installation successful
        """
        deps = self.get_dependencies()
        if not deps:
            return True

        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            key_path = str(Path(self.ssh_key).expanduser())
            ssh.connect(hostname=host, username=self.ssh_user, key_filename=key_path)

            # Install packages
            packages = ' '.join(deps)
            cmd = f"pip3 install {packages} --break-system-packages 2>/dev/null || pip3 install {packages}"

            logger.info(f"Installing dependencies on {host}: {packages}")

            stdin, stdout, stderr = ssh.exec_command(cmd, timeout=300)
            exit_status = stdout.channel.recv_exit_status()

            if exit_status != 0:
                logger.warning(f"Some dependencies may have failed to install: {stderr.read().decode()}")

            ssh.close()
            return True

        except Exception as e:
            logger.error(f"Failed to install dependencies on {host}: {e}")
            return False

    def prepare(self, host: str) -> bool:
        """
        Prepare workload on remote host (deploy + install dependencies).

        Args:
            host: Remote host IP/hostname

        Returns:
            True if preparation successful
        """
        if not self.deploy(host):
            return False

        if not self.install_dependencies(host):
            return False

        return True

    def validate_config(self) -> bool:
        """
        Validate workload configuration.

        Override in subclasses for workload-specific validation.

        Returns:
            True if configuration is valid

        Raises:
            ValueError: If configuration is invalid
        """
        return True


class WorkloadFactory:
    """Factory for creating workload instances."""

    _workloads: Dict[str, type] = {}

    @classmethod
    def register(cls, name: str, workload_class: type):
        """
        Register a workload class.

        Args:
            name: Workload name
            workload_class: Workload class (must inherit from BaseWorkload)
        """
        if not issubclass(workload_class, BaseWorkload):
            raise TypeError(f"{workload_class} must inherit from BaseWorkload")
        cls._workloads[name] = workload_class

    @classmethod
    def create(cls, name: str, config: Dict[str, Any]) -> BaseWorkload:
        """
        Create a workload instance.

        Args:
            name: Workload name
            config: Workload configuration

        Returns:
            Workload instance

        Raises:
            ValueError: If workload name not registered
        """
        if name not in cls._workloads:
            raise ValueError(f"Unknown workload: {name}. Available: {list(cls._workloads.keys())}")

        return cls._workloads[name](config)

    @classmethod
    def list_workloads(cls) -> list[str]:
        """
        List registered workloads.

        Returns:
            List of workload names
        """
        return list(cls._workloads.keys())
