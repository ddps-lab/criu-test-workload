"""
Redis Server Workload Wrapper

Control node wrapper for Redis server workload.
The redis-server process itself is checkpointed and migrated.
Supports built-in mixed operations or YCSB Java benchmark.
"""

from typing import Dict, Any
from .base_workload import BaseWorkload, WorkloadFactory


REDIS_STANDALONE_SCRIPT = '''#!/usr/bin/env python3
"""
Redis Server Workload - Auto-generated standalone script
Supports built-in mode and YCSB benchmark mode.
"""

import time
import os
import sys
import argparse
import random
import hashlib
import subprocess
import signal

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


def create_ready_signal(working_dir: str, wrapper_pid: int):
    ready_path = os.path.join(working_dir, 'checkpoint_ready')
    with open(ready_path, 'w') as f:
        f.write(f'ready:{wrapper_pid}\\n')
    print(f"[Redis] Checkpoint ready (Wrapper PID: {wrapper_pid})")


def check_restore_complete(working_dir: str) -> bool:
    flag_path = os.path.join(working_dir, 'checkpoint_flag')
    return not os.path.exists(flag_path)


# === YCSB Functions ===

def check_ycsb_installed(ycsb_home: str) -> bool:
    ycsb_bin = os.path.join(ycsb_home, 'bin', 'ycsb')
    if not os.path.exists(ycsb_bin):
        return os.path.exists(os.path.join(ycsb_home, 'bin', 'ycsb.sh'))
    return True


def get_ycsb_bin(ycsb_home: str) -> str:
    ycsb_bin = os.path.join(ycsb_home, 'bin', 'ycsb')
    if os.path.exists(ycsb_bin):
        return ycsb_bin
    ycsb_sh = os.path.join(ycsb_home, 'bin', 'ycsb.sh')
    if os.path.exists(ycsb_sh):
        return ycsb_sh
    return ycsb_bin


def create_ycsb_properties(working_dir, ycsb_workload, redis_port, record_count,
                           duration, ycsb_threads, target_throughput):
    proportions = {
        'a': 'readproportion=0.5\\nupdateproportion=0.5\\nscanproportion=0\\ninsertproportion=0',
        'b': 'readproportion=0.95\\nupdateproportion=0.05\\nscanproportion=0\\ninsertproportion=0',
        'c': 'readproportion=1.0\\nupdateproportion=0\\nscanproportion=0\\ninsertproportion=0',
        'd': 'readproportion=0.95\\nupdateproportion=0\\nscanproportion=0\\ninsertproportion=0.05',
        'e': 'readproportion=0\\nupdateproportion=0\\nscanproportion=0.95\\ninsertproportion=0.05\\nmaxscanlength=100',
        'f': 'readproportion=0.5\\nupdateproportion=0\\nscanproportion=0\\ninsertproportion=0\\nreadmodifywriteproportion=0.5',
    }
    props = f"""workload=site.ycsb.workloads.CoreWorkload
recordcount={record_count}
operationcount=2147483647
maxexecutiontime={duration}
requestdistribution=zipfian
fieldcount=10
fieldlength=100
redis.host=localhost
redis.port={redis_port}
{proportions[ycsb_workload]}
"""
    props_path = os.path.join(working_dir, f'ycsb_workload_{ycsb_workload}.properties')
    with open(props_path, 'w') as f:
        f.write(props)
    return props_path


def run_ycsb_phase(ycsb_home, phase, props_path, ycsb_threads, target_throughput):
    ycsb_bin = get_ycsb_bin(ycsb_home)
    cmd = [ycsb_bin, phase, 'redis', '-s', '-P', props_path, '-threads', str(ycsb_threads)]
    if target_throughput > 0:
        cmd.extend(['-target', str(target_throughput)])
    print(f"[Redis] YCSB {phase}: {' '.join(cmd)}")
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


# === Built-in Operations ===

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


# === Main Workload ===

def run_redis_workload(redis_port, num_keys, value_size, duration, working_dir,
                       ycsb_workload=None, ycsb_home='/opt/ycsb', record_count=100000,
                       ycsb_threads=1, target_throughput=0):
    use_ycsb = ycsb_workload is not None

    if use_ycsb:
        if not check_ycsb_installed(ycsb_home):
            print(f"[Redis] ERROR: YCSB not found at {ycsb_home}")
            sys.exit(1)
    else:
        if not HAS_REDIS:
            print("[Redis] ERROR: redis-py not installed")
            sys.exit(1)

    mode_str = f"YCSB workload {ycsb_workload.upper()}" if use_ycsb else "built-in"
    print(f"[Redis] Starting Redis workload (mode={mode_str}, duration={duration}s)")

    redis_process = start_redis_server(redis_port, working_dir)
    redis_pid = redis_process.pid

    if not wait_for_redis('localhost', redis_port):
        print("[Redis] ERROR: Redis failed to start")
        redis_process.terminate()
        sys.exit(1)

    client = redis.Redis(host='localhost', port=redis_port) if HAS_REDIS else None
    wrapper_pid = os.getpid()

    if use_ycsb:
        # YCSB load phase
        props_path = create_ycsb_properties(working_dir, ycsb_workload, redis_port,
                                            record_count, duration, ycsb_threads, target_throughput)
        print(f"[Redis] Starting YCSB load phase (records={record_count})...")
        load_proc = run_ycsb_phase(ycsb_home, 'load', props_path, ycsb_threads, 0)
        load_stdout, load_stderr = load_proc.communicate(timeout=600)
        if load_proc.returncode != 0:
            print(f"[Redis] ERROR: YCSB load failed")
            stderr_text = load_stderr.decode('utf-8', errors='replace')
            for line in stderr_text.strip().split('\\n')[-5:]:
                print(f"[Redis]   {line}")
            redis_process.terminate()
            sys.exit(1)

        if client:
            try:
                info = client.info('memory')
                print(f"[Redis] Memory: {info.get('used_memory', 0) / (1024*1024):.2f} MB")
            except Exception:
                pass

        create_ready_signal(working_dir, wrapper_pid)

        # YCSB run phase
        print(f"[Redis] Starting YCSB run phase...")
        run_proc = run_ycsb_phase(ycsb_home, 'run', props_path, ycsb_threads, target_throughput)
        start_time = time.time()
        last_report = start_time

        try:
            while True:
                if check_restore_complete(working_dir):
                    print(f"[Redis] Restore detected")
                    if run_proc.poll() is None:
                        try:
                            run_proc.terminate()
                            run_proc.wait(timeout=5)
                        except Exception:
                            run_proc.kill()
                    elapsed = time.time() - start_time
                    print(f"[Redis] === STATE SUMMARY ===")
                    print(f"[Redis]   Mode: YCSB workload {ycsb_workload.upper()}")
                    print(f"[Redis]   Elapsed: {elapsed:.1f}s")
                    print(f"[Redis]   ALL live data state LOST on restart")
                    print(f"[Redis] =========================")
                    break

                if run_proc.poll() is not None:
                    print(f"[Redis] YCSB run finished (exit={run_proc.returncode})")

                current_time = time.time()
                if current_time - last_report >= 5.0:
                    status = "running" if run_proc.poll() is None else "finished"
                    print(f"[Redis] YCSB {status}, elapsed={current_time - start_time:.0f}s")
                    last_report = current_time
                time.sleep(1)
        finally:
            try:
                if client:
                    client.shutdown(nosave=True)
            except Exception:
                pass
            redis_process.terminate()
            try:
                redis_process.wait(timeout=5)
            except Exception:
                redis_process.kill()

    else:
        # Built-in mode
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
    parser.add_argument('--ycsb-workload', type=str, choices=['a','b','c','d','e','f'], default=None)
    parser.add_argument('--ycsb-home', type=str, default='/opt/ycsb')
    parser.add_argument('--record-count', type=int, default=100000)
    parser.add_argument('--ycsb-threads', type=int, default=1)
    parser.add_argument('--target-throughput', type=int, default=0)
    args = parser.parse_args()
    run_redis_workload(args.redis_port, args.num_keys, args.value_size, args.duration,
                       args.working_dir, args.ycsb_workload, args.ycsb_home,
                       args.record_count, args.ycsb_threads, args.target_throughput)


if __name__ == '__main__':
    main()
'''


class RedisWorkload(BaseWorkload):
    """
    Redis server workload.

    The redis-server process is started, populated with data, then checkpointed.
    CRIU captures the redis-server process directly via --tree.

    Supports two modes:
    - Built-in: Simple mixed read/write operations (default)
    - YCSB: Standard Yahoo Cloud Serving Benchmark (requires YCSB Java binary)

    Requirements (must be pre-installed in AMI):
    - redis-server: apt install redis-server
    - redis-py: pip install redis
    - YCSB (optional): /opt/ycsb with Java runtime
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.redis_port = config.get('redis_port', 6379)
        self.num_keys = config.get('num_keys', 100000)
        self.value_size = config.get('value_size', 1024)
        self.duration = config.get('duration', 0)
        # YCSB options
        self.ycsb_workload = config.get('ycsb_workload', None)
        self.ycsb_home = config.get('ycsb_home', '/opt/ycsb')
        self.record_count = config.get('record_count', 100000)
        self.ycsb_threads = config.get('ycsb_threads', 1)
        self.target_throughput = config.get('target_throughput', 0)

    def get_standalone_script_name(self) -> str:
        return 'redis_standalone.py'


    def get_command(self) -> str:
        cmd = f"python3 {self.get_standalone_script_name()}"
        cmd += f" --redis-port {self.redis_port}"
        cmd += f" --duration {self.duration}"
        cmd += f" --working_dir {self.working_dir}"

        if self.ycsb_workload:
            cmd += f" --ycsb-workload {self.ycsb_workload}"
            cmd += f" --ycsb-home {self.ycsb_home}"
            cmd += f" --record-count {self.record_count}"
            cmd += f" --ycsb-threads {self.ycsb_threads}"
            cmd += f" --target-throughput {self.target_throughput}"
        else:
            cmd += f" --num-keys {self.num_keys}"
            cmd += f" --value-size {self.value_size}"

        return cmd

    def get_dependencies(self) -> list[str]:
        return ['redis']  # redis-py package (redis-server must be in AMI)

    def validate_config(self) -> bool:
        if self.redis_port <= 0 or self.redis_port > 65535:
            raise ValueError("redis_port must be valid port number")
        if self.ycsb_workload:
            if self.ycsb_workload not in ('a', 'b', 'c', 'd', 'e', 'f'):
                raise ValueError("ycsb_workload must be a-f")
            if self.record_count <= 0:
                raise ValueError("record_count must be positive")
        else:
            if self.num_keys <= 0:
                raise ValueError("num_keys must be positive")
            if self.value_size <= 0:
                raise ValueError("value_size must be positive")
        return True

    def estimate_memory_mb(self) -> float:
        if self.ycsb_workload:
            # YCSB: 10 fields * 100 bytes + key overhead per record
            record_size = 10 * 100 + 50  # ~1050 bytes per record
            data_size = self.record_count * record_size
        else:
            key_size = 12  # "key:XXXXXXXX"
            data_size = self.num_keys * (key_size + self.value_size)
        # Redis overhead is roughly 1.5x the data size
        return (data_size * 1.5) / (1024 * 1024)


WorkloadFactory.register('redis', RedisWorkload)
