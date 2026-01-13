"""
Lazy restore mode configuration for CRIU checkpoint/restore.

Defines restore modes independent of transfer method (rsync, S3, EFS, EBS).
Each mode determines how page data is handled during restore:

- NONE: Standard restore - all pages must be present locally
- LAZY: Local lazy-pages - pages fetched on-demand from local directory
- LAZY_PREFETCH: Lazy with S3 async prefetch (requires S3)
- LIVE_MIGRATION: Page-server based live migration (post-copy)
- LIVE_MIGRATION_PREFETCH: Live migration with S3 pre-copy + page-server post-copy

Note: *_PREFETCH modes require S3 object storage for async prefetch functionality.
"""

from enum import Enum
from dataclasses import dataclass
from typing import Optional


class LazyMode(Enum):
    """
    Restore mode for CRIU checkpoint recovery.

    Some modes require S3 object storage:
    - LAZY_PREFETCH: requires S3 for async prefetch
    - LIVE_MIGRATION_PREFETCH: requires S3 for pre-copy async prefetch
    """
    # Standard restore: all pages-*.img must be present locally before restore
    NONE = "none"

    # Lazy-pages: pages fetched on-demand from local directory
    # - Dump: normal dump (no page-server)
    # - Transfer: all files including pages-*.img
    # - Restore: criu restore --lazy-pages, lazy-pages daemon reads from local dir
    LAZY = "lazy"

    # Lazy-pages with S3 async prefetch (REQUIRES S3)
    # - Dump: normal dump
    # - Transfer: metadata only (pages-*.img excluded, fetched from S3)
    # - Restore: lazy-pages daemon with --async-prefetch, prefetches from S3
    LAZY_PREFETCH = "lazy-prefetch"

    # Live migration with page-server (post-copy only)
    # - Dump: criu dump --lazy-pages --address 0.0.0.0 --port <port>
    #   This starts a page-server that serves pages over network
    # - Transfer: rsync/efs/ebs excluding pages-*.img (pages served via page-server)
    # - Restore: criu restore --lazy-pages, lazy-pages connects to source page-server
    LIVE_MIGRATION = "live-migration"

    # Live migration with S3 pre-copy + page-server post-copy (REQUIRES S3)
    # Combines two mechanisms that work together:
    # - S3 async prefetch (pre-copy): proactively fetches pages from S3 in background
    # - Page-server (post-copy): serves pages on-demand for page faults not yet prefetched
    #
    # Workflow:
    # - Dump: criu dump --lazy-pages --address --port (starts page-server)
    # - Upload: checkpoint files including pages-*.img to S3
    # - Transfer: rsync/efs/ebs excluding pages-*.img
    # - Restore: lazy-pages with --async-prefetch (S3) + --page-server (source node)
    LIVE_MIGRATION_PREFETCH = "live-migration-prefetch"


@dataclass
class LazyConfig:
    """
    Configuration for lazy restore modes.

    This class is independent of S3 configuration and works with any transfer method.
    However, LAZY_PREFETCH and LIVE_MIGRATION_PREFETCH require S3 to be configured.
    """
    # Restore mode
    mode: LazyMode = LazyMode.NONE

    # Page server settings (for LIVE_MIGRATION modes)
    page_server_port: int = 27
    page_server_address: str = "0.0.0.0"  # Address to bind on source node

    # Async prefetch settings (for *_PREFETCH modes, requires S3)
    prefetch_workers: int = 4

    def __post_init__(self):
        """Validate and normalize configuration."""
        if isinstance(self.mode, str):
            self.mode = LazyMode(self.mode)

    def requires_lazy_pages(self) -> bool:
        """Check if this configuration requires lazy-pages mode."""
        return self.mode != LazyMode.NONE

    def requires_page_server(self) -> bool:
        """Check if this configuration requires page-server (live migration)."""
        return self.mode in [LazyMode.LIVE_MIGRATION, LazyMode.LIVE_MIGRATION_PREFETCH]

    def requires_s3(self) -> bool:
        """Check if this configuration requires S3 object storage."""
        return self.mode in [LazyMode.LAZY_PREFETCH, LazyMode.LIVE_MIGRATION_PREFETCH]

    def has_async_prefetch(self) -> bool:
        """Check if async prefetch is enabled (requires S3)."""
        return self.mode in [LazyMode.LAZY_PREFETCH, LazyMode.LIVE_MIGRATION_PREFETCH]

    def get_dump_args(self) -> list:
        """
        Get CRIU dump arguments for this lazy mode.

        Returns:
            List of additional CRIU dump command arguments
        """
        if not self.requires_page_server():
            return []

        # Live migration: dump with page-server options
        return [
            "--lazy-pages",
            "--address", self.page_server_address,
            "--port", str(self.page_server_port)
        ]

    def get_restore_args(self) -> list:
        """
        Get CRIU restore arguments for this lazy mode.

        Returns:
            List of additional CRIU restore command arguments
        """
        if not self.requires_lazy_pages():
            return []

        return ["--lazy-pages"]

    def get_lazy_pages_daemon_args(self, page_server_host: Optional[str] = None) -> list:
        """
        Get CRIU lazy-pages daemon arguments.

        For LIVE_MIGRATION_PREFETCH mode, this returns both:
        - Page-server connection args (for post-copy)
        - Async prefetch args (for pre-copy from S3, S3 args added separately)

        Args:
            page_server_host: Source node address for live migration

        Returns:
            List of CRIU lazy-pages daemon command arguments
        """
        if not self.requires_lazy_pages():
            return []

        args = []

        # For live migration, connect to page-server on source node (post-copy)
        if self.requires_page_server() and page_server_host:
            args.extend(["--page-server", "--address", page_server_host, "--port", str(self.page_server_port)])

        # Async prefetch from S3 (pre-copy, S3 args must be added separately)
        if self.has_async_prefetch():
            args.extend(["--async-prefetch", "--prefetch-workers", str(self.prefetch_workers)])

        return args

    @classmethod
    def from_dict(cls, config: dict) -> 'LazyConfig':
        """
        Create LazyConfig from dictionary.

        Args:
            config: Configuration dictionary

        Returns:
            LazyConfig instance
        """
        return cls(
            mode=LazyMode(config.get('mode', 'none')),
            page_server_port=config.get('page_server_port', 27),
            page_server_address=config.get('page_server_address', '0.0.0.0'),
            prefetch_workers=config.get('prefetch_workers', 4),
        )

    def to_dict(self) -> dict:
        """
        Convert to dictionary.

        Returns:
            Configuration dictionary
        """
        return {
            'mode': self.mode.value,
            'page_server_port': self.page_server_port,
            'page_server_address': self.page_server_address,
            'prefetch_workers': self.prefetch_workers,
        }
