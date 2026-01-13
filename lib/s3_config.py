"""
S3 Object Storage configuration for CRIU checkpoint/restore.

Supports multiple S3 types:
- Standard S3: Upload/download via AWS CLI, CRIU fetches pages from S3
- CloudFront: Upload to S3, download metadata from S3, CRIU fetches pages via CloudFront
- Express One Zone: Upload/download via AWS CLI with express-one-zone specific options

Note: Restore mode (lazy, live-migration, etc.) is now configured via LazyMode in lazy_mode.py.
S3Config focuses on S3-specific settings only.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

from .lazy_mode import LazyMode, LazyConfig


class S3Type(Enum):
    """S3 storage type for checkpoint data."""
    STANDARD = "standard"
    CLOUDFRONT = "cloudfront"
    EXPRESS_ONE_ZONE = "express-one-zone"


@dataclass
class S3Config:
    """
    S3 Object Storage configuration.

    This class handles configuration for uploading checkpoints to S3
    and downloading/restoring from S3 with various modes.

    Workflow:
    1. Source node: dump + upload all checkpoint files to S3
    2. Dest node: download checkpoint files (excluding pages-*.img for lazy mode)
    3. Dest node: CRIU restore with object-storage options (fetches pages on-demand)

    Note: Lazy mode settings (LAZY, LAZY_PREFETCH, LIVE_MIGRATION, etc.) are
    configured via LazyConfig. S3Config works together with LazyConfig.
    """
    # S3 type (standard, cloudfront, express-one-zone)
    s3_type: S3Type = S3Type.STANDARD

    # Upload settings (source node -> S3)
    upload_bucket: str = ""
    upload_prefix: str = ""  # No leading slash, e.g., "checkpoints/exp1"
    upload_region: str = ""

    # Download settings (S3 -> dest node)
    # For CRIU object storage endpoint:
    # - Standard S3: s3.{region}.amazonaws.com
    # - CloudFront: {distribution}.cloudfront.net
    # - Express One Zone: s3express-{az}.{region}.amazonaws.com
    download_endpoint: str = ""
    download_bucket: str = ""  # Usually same as upload_bucket (empty for CloudFront)

    # Express One Zone specific
    access_key: str = ""
    secret_key: str = ""

    def __post_init__(self):
        """Validate and normalize configuration."""
        # Convert string types to enum if needed
        if isinstance(self.s3_type, str):
            self.s3_type = S3Type(self.s3_type)

        # Normalize prefix (remove leading/trailing slashes)
        self.upload_prefix = self.upload_prefix.strip('/')

        # Default download_bucket to upload_bucket if not specified
        if not self.download_bucket and self.s3_type != S3Type.CLOUDFRONT:
            self.download_bucket = self.upload_bucket

    def get_s3_uri(self) -> str:
        """Get full S3 URI for the checkpoint location."""
        if self.upload_prefix:
            return f"s3://{self.upload_bucket}/{self.upload_prefix}/"
        return f"s3://{self.upload_bucket}/"

    def get_upload_cmd(self, local_dir: str) -> str:
        """
        Generate S3 upload command.

        All S3 types use the same AWS CLI command for upload.

        Args:
            local_dir: Local directory containing checkpoint files

        Returns:
            AWS CLI command string
        """
        s3_uri = self.get_s3_uri()
        # Use sync for efficiency, recursive for all files
        return f"aws s3 sync {local_dir}/ {s3_uri} --quiet"

    def get_download_cmd(self, local_dir: str, exclude_pages: bool = True) -> str:
        """
        Generate S3 download command.

        For lazy restore modes, excludes pages-*.img files (CRIU fetches them on-demand).
        For standard restore mode, downloads all files.

        CloudFront case: still download from S3 (CloudFront is only for CRIU page fetches).

        Args:
            local_dir: Local directory to download to
            exclude_pages: Whether to exclude pages-*.img files

        Returns:
            AWS CLI command string
        """
        s3_uri = self.get_s3_uri()

        cmd = f"aws s3 sync {s3_uri} {local_dir}/ --quiet"

        if exclude_pages:
            cmd += " --exclude 'pages-*.img'"

        return cmd

    def get_criu_object_storage_args(self) -> List[str]:
        """
        Generate CRIU object storage arguments for restore.

        Returns:
            List of CRIU command line arguments
        """
        args = ["--enable-object-storage"]

        # Endpoint URL
        args.append(f"--object-storage-endpoint-url")
        args.append(self.download_endpoint)

        # Bucket (not needed for CloudFront)
        if self.s3_type != S3Type.CLOUDFRONT and self.download_bucket:
            args.append("--object-storage-bucket")
            args.append(self.download_bucket)

        # Object prefix
        if self.upload_prefix:
            args.append("--object-storage-object-prefix")
            args.append(f"{self.upload_prefix}/")

        # Express One Zone specific options
        if self.s3_type == S3Type.EXPRESS_ONE_ZONE:
            args.append("--express-one-zone")
            if self.access_key:
                args.append("--aws-access-key")
                args.append(self.access_key)
            if self.secret_key:
                args.append("--aws-secret-key")
                args.append(self.secret_key)
            if self.upload_region:
                args.append("--aws-region")
                args.append(self.upload_region)

        return args

    def get_criu_lazy_pages_args(self, lazy_config: LazyConfig = None) -> List[str]:
        """
        Generate CRIU lazy-pages daemon arguments for S3 object storage.

        This provides S3-specific arguments. Combine with LazyConfig.get_lazy_pages_daemon_args()
        for complete lazy-pages configuration.

        Args:
            lazy_config: Optional LazyConfig for prefetch settings

        Returns:
            List of CRIU lazy-pages command line arguments
        """
        args = self.get_criu_object_storage_args()

        # Async prefetch options (from LazyConfig if provided)
        if lazy_config and lazy_config.has_async_prefetch():
            args.append("--async-prefetch")
            args.append("--prefetch-workers")
            args.append(str(lazy_config.prefetch_workers))

        return args

    @classmethod
    def from_dict(cls, config: dict) -> 'S3Config':
        """
        Create S3Config from dictionary.

        Args:
            config: Configuration dictionary

        Returns:
            S3Config instance
        """
        return cls(
            s3_type=S3Type(config.get('type', 'standard')),
            upload_bucket=config.get('upload_bucket', ''),
            upload_prefix=config.get('prefix', ''),
            upload_region=config.get('region', ''),
            download_endpoint=config.get('download_endpoint', ''),
            download_bucket=config.get('download_bucket', ''),
            access_key=config.get('access_key', ''),
            secret_key=config.get('secret_key', ''),
        )

    def to_dict(self) -> dict:
        """
        Convert to dictionary.

        Returns:
            Configuration dictionary
        """
        return {
            'type': self.s3_type.value,
            'upload_bucket': self.upload_bucket,
            'prefix': self.upload_prefix,
            'region': self.upload_region,
            'download_endpoint': self.download_endpoint,
            'download_bucket': self.download_bucket,
            'access_key': self.access_key,
            'secret_key': self.secret_key,
        }

    def validate(self) -> List[str]:
        """
        Validate configuration.

        Returns:
            List of validation errors (empty if valid)
        """
        errors = []

        if not self.upload_bucket:
            errors.append("upload_bucket is required")

        if not self.download_endpoint:
            errors.append("download_endpoint is required")

        if self.s3_type == S3Type.EXPRESS_ONE_ZONE:
            if not self.access_key:
                errors.append("access_key is required for Express One Zone")
            if not self.secret_key:
                errors.append("secret_key is required for Express One Zone")
            if not self.upload_region:
                errors.append("region is required for Express One Zone")

        return errors
