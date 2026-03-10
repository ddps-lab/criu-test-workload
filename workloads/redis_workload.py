"""
Redis Server Workload Wrapper

Control node wrapper for Redis server workload.
The redis-server process itself is checkpointed and migrated.
"""

from typing import Dict, Any
from .base_workload import BaseWorkload, WorkloadFactory


REDIS_STANDALONE_SCRIPT = '''#!/usr/bin/env python3
"""
Redis Server Workload - Auto-generated standalone script
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
    random.seed(seed)
    return bytes(random.getrandbits(8) for _ in range(size))


def compute_checksum(client, num_keys: int) -> str:
    h = hashlib.md5()
    for i in range(num_keys):
        key = f"key:{i:08d}"
        value = client.get(key)
        if value:
            h.update(key.encode())
            h.update(value)
    return h.hexdigest()


def wait_for_redis(host: str, port: int, timeout: int = 30) -> bool:
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


def start_redis_server(port: int, working_dir: str):
    try:
        subprocess.run(['which', 'redis-server'], check=True, capture_output=True)
    except subprocess.CalledProcessError:
        print("[Redis] ERROR: redis-server not found")
        sys.exit(1)

    cmd = [
        'redis-server', '--port', str(port), '--dir', working_dir,
        '--dbfilename', 'redis_dump.rdb', '--save', '',
        '--daemonize', 'no', '--loglevel', 'warning',
        '--bind', '0.0.0.0', '--protected-mode', 'no',
    ]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setsid)
    return process


def create_ready_signal(working_dir: str, redis_pid: int):
    ready_path = os.path.join(working_dir, 'checkpoint_ready')
    with open(ready_path, 'w') as f:
        f.write(f'ready:{redis_pid}\\n')
    print(f"[Redis] Checkpoint ready (Redis PID: {redis_pid})")


def check_restore_complete(working_dir: str) -> bool:
    flag_path = os.path.join(working_dir, 'checkpoint_flag')
    return not os.path.exists(flag_path)


def run_continuous_ops(client, num_keys, value_size, duration, working_dir):
    start_time = time.time()
    ops_count = 0
    extra_cursor = 0
    extra_written = 0
    max_extra = num_keys
    tx_log_entries = 0
    sorted_set_members = 0
    max_sorted = 10000
    max_txlog = 50000
    updates = 0
    last_report_time = start_time

    def make_result():
        return {'restored': True, 'ops_count': ops_count, 'extra_written': extra_written,
                'extra_live': min(extra_written, max_extra), 'tx_log_entries': tx_log_entries,
                'sorted_set_members': sorted_set_members, 'updates': updates,
                'elapsed': time.time() - start_time}

    while True:
        if check_restore_complete(working_dir):
            return make_result()

        elapsed = time.time() - start_time
        if duration > 0 and elapsed >= duration:
            if check_restore_complete(working_dir):
                return make_result()
            time.sleep(1)
            continue

        op_type = random.choice(['write_new', 'update_existing', 'sorted_set', 'hash_update', 'tx_log', 'read'])
        try:
            if op_type == 'write_new':
                client.set(f"extra:{extra_cursor:08d}", generate_value(value_size, ops_count))
                extra_cursor = (extra_cursor + 1) % max_extra
                extra_written += 1
            elif op_type == 'update_existing':
                idx = random.randint(0, num_keys - 1)
                client.set(f"key:{idx:08d}", generate_value(value_size, idx + ops_count))
                updates += 1
            elif op_type == 'sorted_set':
                client.zadd('leaderboard', {f"member:{ops_count % max_sorted}": random.random() * 1000})
                sorted_set_members += 1
                if sorted_set_members % 1000 == 0:
                    client.zremrangebyrank('leaderboard', 0, -(max_sorted + 1))
            elif op_type == 'hash_update':
                client.hset('stats_hash', f"field:{ops_count % 1000}", str(random.random()))
            elif op_type == 'tx_log':
                client.rpush('transaction_log', f"{time.time():.6f}:op_{ops_count}")
                tx_log_entries += 1
                if tx_log_entries % 5000 == 0:
                    client.ltrim('transaction_log', -max_txlog, -1)
            elif op_type == 'read':
                client.get(f"key:{random.randint(0, num_keys-1):08d}")
        except Exception:
            pass
        ops_count += 1

        current_time = time.time()
        if current_time - last_report_time >= 5.0:
            print(f"[Redis] Ops: {ops_count}, ExtraWrites: {extra_written}, Updates: {updates}, Elapsed: {current_time - start_time:.0f}s")
            last_report_time = current_time
        time.sleep(0.01)


def run_redis_workload(redis_port, num_keys, value_size, duration, working_dir):
    if not HAS_REDIS:
        print("[Redis] ERROR: redis-py not installed")
        sys.exit(1)

    print(f"[Redis] Starting Redis workload (duration={duration}s)")
    redis_process = start_redis_server(redis_port, working_dir)
    redis_pid = redis_process.pid

    if not wait_for_redis('localhost', redis_port):
        print("[Redis] ERROR: Redis failed to start")
        redis_process.terminate()
        sys.exit(1)

    client = redis.Redis(host='localhost', port=redis_port)
    client.flushdb()

    print(f"[Redis] Populating {num_keys} keys...")
    pipeline = client.pipeline()
    for i in range(num_keys):
        pipeline.set(f"key:{i:08d}", generate_value(value_size, i))
        if (i + 1) % 1000 == 0:
            pipeline.execute()
            pipeline = client.pipeline()
    pipeline.execute()

    info = client.info('memory')
    print(f"[Redis] Memory: {info.get('used_memory', 0) / (1024*1024):.2f} MB")

    wrapper_pid = os.getpid()
    create_ready_signal(working_dir, wrapper_pid)

    try:
        result = run_continuous_ops(client, num_keys, value_size, duration, working_dir)
        if result['restored']:
            print(f"[Redis] Restore detected")
            try:
                client = redis.Redis(host='localhost', port=redis_port)
                client.ping()
            except Exception:
                if not wait_for_redis('localhost', redis_port, timeout=30):
                    print(f"[Redis] ERROR: Cannot connect after restore")
                    return
                client = redis.Redis(host='localhost', port=redis_port)
            print(f"[Redis] === STATE SUMMARY ===")
            print(f"[Redis]   Ops: {result['ops_count']}, ExtraWrites: {result['extra_written']}, Updates: {result['updates']}")
            print(f"[Redis]   ALL live data state LOST on restart")
            print(f"[Redis] =========================")
    finally:
        try:
            client.shutdown(nosave=True)
        except Exception:
            pass
        redis_process.terminate()
        try:
            redis_process.wait(timeout=5)
        except Exception:
            redis_process.kill()

    sys.exit(0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--redis-port', type=int, default=6379)
    parser.add_argument('--num-keys', type=int, default=100000)
    parser.add_argument('--value-size', type=int, default=1024)
    parser.add_argument('--duration', type=int, default=0)
    parser.add_argument('--working_dir', type=str, default='.')
    args = parser.parse_args()
    run_redis_workload(args.redis_port, args.num_keys, args.value_size, args.duration, args.working_dir)


if __name__ == '__main__':
    main()
'''


class RedisWorkload(BaseWorkload):
    """
    Redis server workload.

    The redis-server process is started, populated with data, then checkpointed.
    CRIU captures the redis-server process directly.

    Requirements (must be pre-installed in AMI):
    - redis-server: apt install redis-server
    - redis-py: pip install redis
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.redis_port = config.get('redis_port', 6379)
        self.num_keys = config.get('num_keys', 100000)
        self.value_size = config.get('value_size', 1024)
        self.duration = config.get('duration', 0)

    def get_standalone_script_name(self) -> str:
        return 'redis_standalone.py'

    def get_standalone_script_content(self) -> str:
        return REDIS_STANDALONE_SCRIPT

    def get_command(self) -> str:
        cmd = f"python3 {self.get_standalone_script_name()}"
        cmd += f" --redis-port {self.redis_port}"
        cmd += f" --num-keys {self.num_keys}"
        cmd += f" --value-size {self.value_size}"
        cmd += f" --duration {self.duration}"
        cmd += f" --working_dir {self.working_dir}"
        return cmd

    def get_dependencies(self) -> list[str]:
        return ['redis']  # redis-py package (redis-server must be in AMI)

    def validate_config(self) -> bool:
        if self.num_keys <= 0:
            raise ValueError("num_keys must be positive")
        if self.value_size <= 0:
            raise ValueError("value_size must be positive")
        if self.redis_port <= 0 or self.redis_port > 65535:
            raise ValueError("redis_port must be valid port number")
        return True

    def estimate_memory_mb(self) -> float:
        # Redis overhead is roughly 1.5x the data size
        key_size = 12  # "key:XXXXXXXX"
        data_size = self.num_keys * (key_size + self.value_size)
        return (data_size * 1.5) / (1024 * 1024)


WorkloadFactory.register('redis', RedisWorkload)
