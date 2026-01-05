#!/usr/bin/env python3
"""
Redis Server Workload (CRIU Checkpoint with Process Tree)

This script manages a Redis server process for CRIU checkpoint testing.
CRIU checkpoints this wrapper script, and with --tree option, redis-server
(child process) is also checkpointed together.

Usage:
    python3 redis_standalone.py --redis-port 6379 --num-keys 100000

Checkpoint Protocol:
    1. This script starts redis-server as child process
    2. Populates data
    3. Creates 'checkpoint_ready' file with THIS script's PID (wrapper)
    4. CRIU with --tree option checkpoints: wrapper + redis-server
    5. After restore, both processes resume together

Important:
    - CRIU checkpoints THIS script's PID with --tree option
    - redis-server is automatically included as child process
    - No need to track redis-server PID separately

Scenario:
    - Redis caching layers
    - Session stores
    - Real-time data caches
"""

import time
import os
import sys
import argparse
import random
import hashlib
import subprocess

try:
    import redis
    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False


def generate_value(size: int, seed: int) -> bytes:
    """Generate a deterministic value of given size."""
    random.seed(seed)
    return bytes(random.getrandbits(8) for _ in range(size))


def compute_checksum(client, num_keys: int) -> str:
    """Compute checksum of stored data for integrity verification."""
    h = hashlib.md5()
    for i in range(num_keys):
        key = f"key:{i:08d}"
        value = client.get(key)
        if value:
            h.update(key.encode())
            h.update(value)
    return h.hexdigest()


def wait_for_redis(host: str, port: int, timeout: int = 30) -> bool:
    """Wait for Redis server to be ready."""
    if not HAS_REDIS:
        return False

    start = time.time()
    while time.time() - start < timeout:
        try:
            client = redis.Redis(host=host, port=port)
            client.ping()
            return True
        except:
            time.sleep(0.5)
    return False


def start_redis_server(port: int, working_dir: str) -> subprocess.Popen:
    """Start Redis server process."""
    # Check if redis-server is available
    try:
        subprocess.run(['which', 'redis-server'], check=True, capture_output=True)
    except subprocess.CalledProcessError:
        print("[Redis] ERROR: redis-server not found. Install Redis first.")
        print("[Redis] Run: sudo apt-get install redis-server")
        sys.exit(1)

    # Redis config for CRIU compatibility
    cmd = [
        'redis-server',
        '--port', str(port),
        '--dir', working_dir,
        '--dbfilename', 'redis_dump.rdb',
        '--save', '',  # Disable background saves (interferes with CRIU)
        '--daemonize', 'no',
        '--loglevel', 'warning',
        '--bind', '0.0.0.0',
        '--protected-mode', 'no',
    ]

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        preexec_fn=os.setsid  # Create new session for proper signal handling
    )

    return process


def create_ready_signal(working_dir: str, wrapper_pid: int, redis_pid: int):
    """Create checkpoint ready signal file with wrapper PID."""
    ready_path = os.path.join(working_dir, 'checkpoint_ready')
    with open(ready_path, 'w') as f:
        # Write the WRAPPER PID - CRIU --tree will include redis-server as child
        f.write(f'ready:{wrapper_pid}\n')
    print(f"[Redis] Checkpoint ready signal created")
    print(f"[Redis] Wrapper PID: {wrapper_pid} (checkpoint target)")
    print(f"[Redis] Redis PID: {redis_pid} (child, included via --tree)")


def check_restore_complete(working_dir: str) -> bool:
    """Check if restore is complete (checkpoint_flag removed)."""
    flag_path = os.path.join(working_dir, 'checkpoint_flag')
    return not os.path.exists(flag_path)


def run_redis_workload(
    redis_port: int = 6379,
    num_keys: int = 100000,
    value_size: int = 1024,
    working_dir: str = '.'
):
    """
    Main Redis workload.

    Args:
        redis_port: Redis server port
        num_keys: Number of keys to populate
        value_size: Size of each value in bytes
        working_dir: Working directory for signal files
    """
    if not HAS_REDIS:
        print("[Redis] ERROR: redis-py not installed. Run: pip install redis")
        sys.exit(1)

    print(f"[Redis] Starting Redis workload")
    print(f"[Redis] Config: port={redis_port}, keys={num_keys}, value_size={value_size}B")
    print(f"[Redis] Working directory: {working_dir}")

    # Start Redis server
    print(f"[Redis] Starting redis-server...")
    redis_process = start_redis_server(redis_port, working_dir)
    redis_pid = redis_process.pid

    print(f"[Redis] Redis server started with PID: {redis_pid}")

    # Wait for Redis to be ready
    if not wait_for_redis('localhost', redis_port):
        print("[Redis] ERROR: Redis server failed to start")
        redis_process.terminate()
        sys.exit(1)

    # Connect to Redis
    client = redis.Redis(host='localhost', port=redis_port)
    print(f"[Redis] Connected to Redis")

    # Flush and populate data
    print(f"[Redis] Flushing database...")
    client.flushdb()

    print(f"[Redis] Populating {num_keys} keys...")
    pipeline = client.pipeline()
    batch_size = 1000

    for i in range(num_keys):
        key = f"key:{i:08d}"
        value = generate_value(value_size, i)
        pipeline.set(key, value)

        if (i + 1) % batch_size == 0:
            pipeline.execute()
            pipeline = client.pipeline()

        if (i + 1) % 10000 == 0:
            print(f"[Redis] Populated {i + 1}/{num_keys} keys...")

    pipeline.execute()

    # Get initial checksum
    initial_checksum = compute_checksum(client, num_keys)
    info = client.info('memory')
    memory_mb = info.get('used_memory', 0) / (1024 * 1024)

    print(f"[Redis] Population complete")
    print(f"[Redis] Memory usage: {memory_mb:.2f} MB")
    print(f"[Redis] Keys in DB: {client.dbsize()}")
    print(f"[Redis] Initial checksum: {initial_checksum[:16]}...")

    # Signal ready - with WRAPPER PID (this script)
    wrapper_pid = os.getpid()
    create_ready_signal(working_dir, wrapper_pid, redis_pid)

    print(f"[Redis]")
    print(f"[Redis] ====== READY FOR CHECKPOINT ======")
    print(f"[Redis] Wrapper PID: {wrapper_pid} (checkpoint this)")
    print(f"[Redis] Redis PID: {redis_pid} (child process)")
    print(f"[Redis] To checkpoint: sudo criu dump -t {wrapper_pid} --tree -D <dir> --shell-job --tcp-established")
    print(f"[Redis] ===================================")
    print(f"[Redis]")

    # Wait for checkpoint/restore cycle
    try:
        while True:
            if check_restore_complete(working_dir):
                print(f"[Redis] Restore detected - checkpoint_flag removed")

                # Verify data integrity
                print(f"[Redis] Verifying data integrity...")

                # Reconnect to Redis (connection may have been broken during migration)
                try:
                    client = redis.Redis(host='localhost', port=redis_port)
                    client.ping()
                except:
                    print(f"[Redis] Waiting for Redis to be available...")
                    if not wait_for_redis('localhost', redis_port, timeout=30):
                        print(f"[Redis] ERROR: Cannot connect to Redis after restore")
                        break
                    client = redis.Redis(host='localhost', port=redis_port)

                current_checksum = compute_checksum(client, num_keys)

                if current_checksum == initial_checksum:
                    print(f"[Redis] Data integrity verified - checksums match!")
                else:
                    print(f"[Redis] WARNING: Data integrity check failed!")
                    print(f"[Redis]   Expected: {initial_checksum[:16]}...")
                    print(f"[Redis]   Got: {current_checksum[:16]}...")

                print(f"[Redis] Keys after restore: {client.dbsize()}")
                print("[Redis] Workload complete")
                break

            time.sleep(1)

    except KeyboardInterrupt:
        print(f"[Redis] Interrupted")

    finally:
        # Clean shutdown
        print(f"[Redis] Shutting down Redis server...")
        try:
            client.shutdown(nosave=True)
        except:
            pass
        redis_process.terminate()
        redis_process.wait(timeout=5)

    sys.exit(0)


def main():
    parser = argparse.ArgumentParser(
        description="Redis server workload for CRIU checkpoint testing"
    )
    parser.add_argument(
        '--redis-port',
        type=int,
        default=6379,
        help='Redis server port (default: 6379)'
    )
    parser.add_argument(
        '--num-keys',
        type=int,
        default=100000,
        help='Number of keys to populate (default: 100000)'
    )
    parser.add_argument(
        '--value-size',
        type=int,
        default=1024,
        help='Size of each value in bytes (default: 1024)'
    )
    parser.add_argument(
        '--working_dir',
        type=str,
        default='.',
        help='Working directory for signal files'
    )

    args = parser.parse_args()

    run_redis_workload(
        redis_port=args.redis_port,
        num_keys=args.num_keys,
        value_size=args.value_size,
        working_dir=args.working_dir
    )


if __name__ == '__main__':
    main()
