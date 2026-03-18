"""
CRIU Workload implementations.

Each workload consists of:
- *_standalone.py: Standalone script that runs on workload nodes (no lib dependencies)
- *_workload.py: Wrapper class that deploys and manages the standalone script from control node

Available workloads:
- memory: Memory allocation workload (pure Python)
- matmul: Matrix multiplication (NumPy)
- redis: Redis server + optional YCSB benchmark
- ml_training: ML training (PyTorch CPU)
- video: FFmpeg video processing (real ffmpeg process)
- dataproc: Data processing (NumPy)
- xgboost: XGBoost CPU training (tree-based ML)
- memcached: Memcached server + YCSB benchmark
"""

from .base_workload import BaseWorkload, WorkloadFactory

# Import all workloads to register them with the factory
from .memory_workload import MemoryWorkload
from .matmul_workload import MatMulWorkload
from .redis_workload import RedisWorkload
from .ml_training_workload import MLTrainingWorkload
from .video_workload import VideoWorkload
from .dataproc_workload import DataProcWorkload
from .xgboost_workload import XGBoostWorkload
from .memcached_workload import MemcachedWorkload
from .sevenzip_workload import SevenZipWorkload

__all__ = [
    'BaseWorkload',
    'WorkloadFactory',
    'MemoryWorkload',
    'MatMulWorkload',
    'RedisWorkload',
    'MLTrainingWorkload',
    'VideoWorkload',
    'DataProcWorkload',
    'XGBoostWorkload',
    'MemcachedWorkload',
    'SevenZipWorkload',
]
