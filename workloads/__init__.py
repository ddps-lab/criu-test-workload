"""
CRIU Workload implementations.

Each workload consists of:
- *_standalone.py: Standalone script that runs on workload nodes (no lib dependencies)
- *_workload.py: Wrapper class that deploys and manages the standalone script from control node

Available workloads:
- memory: Memory allocation workload (pure Python)
- matmul: Matrix multiplication (NumPy)
- redis: Redis server (real redis-server process)
- ml_training: ML training (PyTorch CPU)
- jupyter: Jupyter notebook simulation
- video: FFmpeg video processing (real ffmpeg process)
- dataproc: Data processing (NumPy)
"""

from .base_workload import BaseWorkload, WorkloadFactory

# Import all workloads to register them with the factory
from .memory_workload import MemoryWorkload
from .matmul_workload import MatMulWorkload
from .redis_workload import RedisWorkload
from .ml_training_workload import MLTrainingWorkload
from .jupyter_workload import JupyterWorkload
from .video_workload import VideoWorkload
from .dataproc_workload import DataProcWorkload

__all__ = [
    'BaseWorkload',
    'WorkloadFactory',
    'MemoryWorkload',
    'MatMulWorkload',
    'RedisWorkload',
    'MLTrainingWorkload',
    'JupyterWorkload',
    'VideoWorkload',
    'DataProcWorkload',
]
