"""
Checkpoint transfer management for CRIU experiments.

Supports multiple transfer methods: rsync, S3, EFS, EBS.
"""

import subprocess
import time
import os
from typing import Dict, Any, Optional
from pathlib import Path
import logging

from .s3_config import S3Config, S3Type
from .lazy_mode import LazyMode, LazyConfig

logger = logging.getLogger(__name__)


class TransferManager:
    """Manage checkpoint data transfer between nodes."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize transfer manager.

        Args:
            config: Transfer configuration dictionary
        """
        self.config = config
        self.method = config.get('method', 'rsync')
        self.source_dir = config.get('source_dir')
        self.dest_dir = config.get('dest_dir')
        self.ssh_user = config.get('ssh_user', 'ubuntu')
        self.ssh_key = config.get('ssh_key', '~/.ssh/id_ed25519')

    def transfer(self, source_host: str, dest_host: str, checkpoint_dir: str) -> Dict[str, Any]:
        """
        Transfer checkpoint data from source to destination.

        Args:
            source_host: Source host IP/hostname
            dest_host: Destination host IP/hostname
            checkpoint_dir: Directory containing checkpoint files

        Returns:
            Dictionary with transfer metrics (duration, size, etc.)
        """
        start_time = time.time()

        if self.method == 'rsync':
            result = self._transfer_rsync(source_host, dest_host, checkpoint_dir)
        elif self.method == 's3':
            result = self._transfer_s3(source_host, checkpoint_dir)
        elif self.method == 'efs':
            result = self._transfer_efs(checkpoint_dir)
        elif self.method == 'ebs':
            result = self._transfer_ebs(source_host, dest_host, checkpoint_dir)
        else:
            raise ValueError(f"Unsupported transfer method: {self.method}")

        duration = time.time() - start_time
        result['duration'] = duration
        result['method'] = self.method

        return result

    def _transfer_rsync(self, source_host: str, dest_host: str, checkpoint_dir: str) -> Dict[str, Any]:
        """
        Transfer using rsync over SSH.

        Args:
            source_host: Source host IP
            dest_host: Destination host IP
            checkpoint_dir: Checkpoint directory path

        Returns:
            Transfer metrics
        """
        # Build rsync command
        # From source node: rsync to destination
        rsync_cmd = [
            'ssh', f'{self.ssh_user}@{source_host}',
            f'rsync -av --update --inplace --links {checkpoint_dir} {self.ssh_user}@{dest_host}:{self.dest_dir}'
        ]

        logger.info(f"Running rsync: {' '.join(rsync_cmd)}")

        try:
            result = subprocess.run(
                rsync_cmd,
                capture_output=True,
                text=True,
                check=True
            )

            # Parse rsync output for size
            size_mb = self._parse_rsync_output(result.stdout)

            return {
                'success': True,
                'size_mb': size_mb,
                'stdout': result.stdout,
                'stderr': result.stderr
            }

        except subprocess.CalledProcessError as e:
            logger.error(f"rsync failed: {e.stderr}")
            return {
                'success': False,
                'error': str(e),
                'stderr': e.stderr
            }

    def _transfer_s3(self, source_host: str, checkpoint_dir: str) -> Dict[str, Any]:
        """
        Transfer checkpoint to S3.

        Args:
            source_host: Source host IP
            checkpoint_dir: Checkpoint directory path

        Returns:
            Transfer metrics
        """
        bucket = self.config.get('s3_bucket')
        prefix = self.config.get('s3_prefix', 'checkpoints')

        if not bucket:
            raise ValueError("S3 bucket not configured")

        # Build S3 sync command to run on source host
        s3_sync_cmd = [
            'ssh', f'{self.ssh_user}@{source_host}',
            f'aws s3 sync {checkpoint_dir} s3://{bucket}/{prefix}/ --quiet'
        ]

        logger.info(f"Uploading to S3: s3://{bucket}/{prefix}/")

        try:
            start_time = time.time()
            result = subprocess.run(
                s3_sync_cmd,
                capture_output=True,
                text=True,
                check=True
            )
            upload_duration = time.time() - start_time

            # Get checkpoint size
            size_cmd = ['ssh', f'{self.ssh_user}@{source_host}', f'du -sm {checkpoint_dir}']
            size_result = subprocess.run(size_cmd, capture_output=True, text=True)
            size_mb = float(size_result.stdout.split()[0]) if size_result.returncode == 0 else 0

            return {
                'success': True,
                'bucket': bucket,
                'prefix': prefix,
                'size_mb': size_mb,
                'upload_duration': upload_duration
            }

        except subprocess.CalledProcessError as e:
            logger.error(f"S3 upload failed: {e.stderr}")
            return {
                'success': False,
                'error': str(e),
                'stderr': e.stderr
            }

    def _transfer_efs(self, checkpoint_dir: str) -> Dict[str, Any]:
        """
        Transfer using EFS (no actual transfer needed, just validation).

        Args:
            checkpoint_dir: Checkpoint directory path (should be on EFS mount)

        Returns:
            Transfer metrics
        """
        # For EFS, checkpoints are already on shared filesystem
        # Just verify the mount exists and get size
        efs_mount = self.config.get('efs_mount', '/mnt/efs')

        if not checkpoint_dir.startswith(efs_mount):
            logger.warning(f"Checkpoint dir {checkpoint_dir} not on EFS mount {efs_mount}")

        # Get size
        try:
            size_result = subprocess.run(
                ['du', '-sm', checkpoint_dir],
                capture_output=True,
                text=True
            )
            size_mb = float(size_result.stdout.split()[0]) if size_result.returncode == 0 else 0

            return {
                'success': True,
                'efs_mount': efs_mount,
                'size_mb': size_mb,
                'note': 'No transfer needed - using shared EFS'
            }

        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

    def _transfer_ebs(self, source_host: str, dest_host: str, checkpoint_dir: str) -> Dict[str, Any]:
        """
        Transfer using EBS volume (detach/attach).

        This requires AWS EC2 API access and is handled separately in the checkpoint manager.

        Args:
            source_host: Source host IP
            dest_host: Destination host IP
            checkpoint_dir: Checkpoint directory path

        Returns:
            Transfer metrics
        """
        # EBS transfer is more complex and requires:
        # 1. Sync checkpoint to EBS volume on source
        # 2. Unmount and detach EBS volume
        # 3. Attach EBS volume to destination
        # 4. Mount on destination

        # For now, just rsync to EBS mount point
        ebs_mount = self.config.get('ebs_mount', '/mnt/ebs_test')
        ebs_checkpoint_dir = f"{ebs_mount}/{Path(checkpoint_dir).name}"

        # First, rsync to EBS on source host
        rsync_cmd = [
            'ssh', f'{self.ssh_user}@{source_host}',
            f'rsync -av --update --inplace --links {checkpoint_dir} {ebs_mount}/'
        ]

        logger.info(f"Syncing to EBS: {' '.join(rsync_cmd)}")

        try:
            result = subprocess.run(
                rsync_cmd,
                capture_output=True,
                text=True,
                check=True
            )

            size_mb = self._parse_rsync_output(result.stdout)

            return {
                'success': True,
                'ebs_mount': ebs_mount,
                'size_mb': size_mb,
                'note': 'Synced to EBS volume - volume detach/attach handled separately'
            }

        except subprocess.CalledProcessError as e:
            logger.error(f"EBS sync failed: {e.stderr}")
            return {
                'success': False,
                'error': str(e),
                'stderr': e.stderr
            }

    def _parse_rsync_output(self, stdout: str) -> float:
        """
        Parse rsync output to extract total size.

        Args:
            stdout: rsync stdout

        Returns:
            Size in MB
        """
        # Look for "total size" line in rsync output
        for line in stdout.split('\n'):
            if 'total size is' in line:
                # Extract number (format: "total size is 1,234,567")
                parts = line.split()
                for i, part in enumerate(parts):
                    if part == 'is' and i + 1 < len(parts):
                        size_str = parts[i + 1].replace(',', '')
                        try:
                            size_bytes = float(size_str)
                            return size_bytes / (1024 * 1024)  # Convert to MB
                        except ValueError:
                            pass
        return 0.0

    def download_from_s3(self, dest_host: str, s3_uri: str, dest_dir: str) -> Dict[str, Any]:
        """
        Download checkpoint from S3 to destination host.

        Args:
            dest_host: Destination host IP
            s3_uri: S3 URI (s3://bucket/prefix/)
            dest_dir: Destination directory on host

        Returns:
            Download metrics
        """
        s3_sync_cmd = [
            'ssh', f'{self.ssh_user}@{dest_host}',
            f'aws s3 sync {s3_uri} {dest_dir}/ --quiet'
        ]

        logger.info(f"Downloading from S3: {s3_uri}")

        try:
            start_time = time.time()
            result = subprocess.run(
                s3_sync_cmd,
                capture_output=True,
                text=True,
                check=True
            )
            download_duration = time.time() - start_time

            return {
                'success': True,
                'download_duration': download_duration
            }

        except subprocess.CalledProcessError as e:
            logger.error(f"S3 download failed: {e.stderr}")
            return {
                'success': False,
                'error': str(e),
                'stderr': e.stderr
            }

    def upload_to_s3_with_config(self, source_host: str, checkpoint_dir: str,
                                  s3_config: S3Config, clean_first: bool = True) -> Dict[str, Any]:
        """
        Upload checkpoint to S3 using S3Config.

        Args:
            source_host: Source host IP
            checkpoint_dir: Checkpoint directory path on source host
            s3_config: S3 configuration object
            clean_first: If True, delete existing S3 data before upload (default: True)

        Returns:
            Upload metrics
        """
        upload_cmd = s3_config.get_upload_cmd(checkpoint_dir, clean_first=clean_first)

        ssh_cmd = [
            'ssh', f'{self.ssh_user}@{source_host}',
            upload_cmd
        ]

        clean_msg = " (cleaning existing data first)" if clean_first else ""
        logger.info(f"Uploading to S3{clean_msg}: {s3_config.get_s3_uri()}")

        try:
            start_time = time.time()
            result = subprocess.run(
                ssh_cmd,
                capture_output=True,
                text=True,
                check=True
            )
            upload_duration = time.time() - start_time

            # Get checkpoint size
            size_cmd = ['ssh', f'{self.ssh_user}@{source_host}', f'du -sm {checkpoint_dir}']
            size_result = subprocess.run(size_cmd, capture_output=True, text=True)
            size_mb = float(size_result.stdout.split()[0]) if size_result.returncode == 0 else 0

            return {
                'success': True,
                's3_uri': s3_config.get_s3_uri(),
                's3_type': s3_config.s3_type.value,
                'size_mb': size_mb,
                'upload_duration': upload_duration
            }

        except subprocess.CalledProcessError as e:
            logger.error(f"S3 upload failed: {e.stderr}")
            return {
                'success': False,
                'error': str(e),
                'stderr': e.stderr
            }

    def download_from_s3_with_config(self, dest_host: str, dest_dir: str,
                                      s3_config: S3Config,
                                      lazy_config: LazyConfig = None) -> Dict[str, Any]:
        """
        Download checkpoint from S3 using S3Config.

        For lazy restore modes, excludes pages-*.img files (CRIU fetches on-demand).

        Args:
            dest_host: Destination host IP
            dest_dir: Destination directory on host
            s3_config: S3 configuration object
            lazy_config: LazyConfig for determining if pages should be excluded

        Returns:
            Download metrics
        """
        # Exclude pages for lazy modes (any mode except NONE)
        exclude_pages = lazy_config is not None and lazy_config.mode != LazyMode.NONE
        download_cmd = s3_config.get_download_cmd(dest_dir, exclude_pages=exclude_pages)

        ssh_cmd = [
            'ssh', f'{self.ssh_user}@{dest_host}',
            download_cmd
        ]

        mode_str = "excluding pages-*.img" if exclude_pages else "all files"
        logger.info(f"Downloading from S3 ({mode_str}): {s3_config.get_s3_uri()}")

        try:
            start_time = time.time()
            result = subprocess.run(
                ssh_cmd,
                capture_output=True,
                text=True,
                check=True
            )
            download_duration = time.time() - start_time

            return {
                'success': True,
                's3_uri': s3_config.get_s3_uri(),
                'lazy_mode': lazy_config.mode.value if lazy_config else 'none',
                'excluded_pages': exclude_pages,
                'download_duration': download_duration
            }

        except subprocess.CalledProcessError as e:
            logger.error(f"S3 download failed: {e.stderr}")
            return {
                'success': False,
                'error': str(e),
                'stderr': e.stderr
            }

    def transfer_with_s3_config(self, source_host: str, dest_host: str,
                                 checkpoint_dir: str, s3_config: S3Config,
                                 lazy_config: LazyConfig = None,
                                 clean_first: bool = True) -> Dict[str, Any]:
        """
        Complete S3 transfer workflow: upload from source, download to dest.

        Args:
            source_host: Source host IP
            dest_host: Destination host IP
            checkpoint_dir: Checkpoint directory path on source
            s3_config: S3 configuration object
            lazy_config: LazyConfig for determining restore mode
            clean_first: If True, delete existing S3 data before upload (default: True)

        Returns:
            Combined transfer metrics
        """
        start_time = time.time()

        # Step 1: Upload to S3 (with optional cleanup of existing data)
        upload_result = self.upload_to_s3_with_config(source_host, checkpoint_dir, s3_config, clean_first=clean_first)
        if not upload_result.get('success'):
            return upload_result

        # Step 2: Download to destination (excluding pages for lazy modes)
        # Use the same directory structure on destination
        dest_checkpoint_dir = checkpoint_dir
        download_result = self.download_from_s3_with_config(
            dest_host, dest_checkpoint_dir, s3_config, lazy_config
        )
        if not download_result.get('success'):
            download_result['upload_result'] = upload_result
            return download_result

        total_duration = time.time() - start_time

        return {
            'success': True,
            'method': 's3',
            's3_type': s3_config.s3_type.value,
            'lazy_mode': lazy_config.mode.value if lazy_config else 'none',
            's3_uri': s3_config.get_s3_uri(),
            'upload_duration': upload_result['upload_duration'],
            'download_duration': download_result['download_duration'],
            'total_duration': total_duration,
            'size_mb': upload_result.get('size_mb', 0),
            'excluded_pages': download_result.get('excluded_pages', False)
        }
