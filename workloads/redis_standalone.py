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


def run_continuous_operations(client, num_keys, value_size, duration, working_dir):
    """
    Run continuous mixed read/write operations on Redis.

    Memory is bounded:
    - extra keys: circular buffer up to num_keys (overwrites oldest)
    - sorted set: capped at 10,000 members (evicts lowest scores)
    - transaction log: trimmed to last 50,000 entries
    - hash fields: capped at 1,000 fields (natural modulo)

    Despite bounded memory, the STATE is continuously evolving:
    values keep changing, sorted set rankings shift, tx log rolls forward.
    Restarting loses the current snapshot of all this live state.
    """
    start_time = time.time()
    ops_count = 0
    extra_key_cursor = 0       # circular cursor for extra keys
    extra_keys_written = 0     # total extra keys ever written
    max_extra_keys = num_keys  # cap: same as initial key count
    tx_log_entries = 0
    sorted_set_members = 0
    max_sorted_set = 10000
    max_tx_log = 50000
    updates_to_existing = 0
    last_report_time = start_time

    while True:
        if check_restore_complete(working_dir):
            return {
                'restored': True,
                'ops_count': ops_count,
                'extra_keys_written': extra_keys_written,
                'extra_keys_live': min(extra_keys_written, max_extra_keys),
                'tx_log_entries': tx_log_entries,
                'sorted_set_members': sorted_set_members,
                'updates_to_existing': updates_to_existing,
                'elapsed': time.time() - start_time,
            }

        elapsed = time.time() - start_time
        if duration > 0 and elapsed >= duration:
            if check_restore_complete(working_dir):
                return {
                    'restored': True,
                    'ops_count': ops_count,
                    'extra_keys_written': extra_keys_written,
                    'extra_keys_live': min(extra_keys_written, max_extra_keys),
                    'tx_log_entries': tx_log_entries,
                    'sorted_set_members': sorted_set_members,
                    'updates_to_existing': updates_to_existing,
                    'elapsed': elapsed,
                }
            time.sleep(1)
            continue

        op_type = random.choice(['write_new', 'update_existing', 'sorted_set', 'hash_update', 'tx_log', 'read'])

        try:
            if op_type == 'write_new':
                # Circular buffer: overwrite oldest extra key when cap reached
                key = f"extra:{extra_key_cursor:08d}"
                value = generate_value(value_size, ops_count)
                client.set(key, value)
                extra_key_cursor = (extra_key_cursor + 1) % max_extra_keys
                extra_keys_written += 1
            elif op_type == 'update_existing':
                idx = random.randint(0, num_keys - 1)
                key = f"key:{idx:08d}"
                value = generate_value(value_size, idx + ops_count)
                client.set(key, value)
                updates_to_existing += 1
            elif op_type == 'sorted_set':
                member = f"member:{ops_count % max_sorted_set}"
                score = random.random() * 1000
                client.zadd('leaderboard', {member: score})
                sorted_set_members += 1
                # Trim to cap (keep top scores)
                if sorted_set_members % 1000 == 0:
                    client.zremrangebyrank('leaderboard', 0, -(max_sorted_set + 1))
            elif op_type == 'hash_update':
                field = f"field:{ops_count % 1000}"
                client.hset('stats_hash', field, str(random.random()))
            elif op_type == 'tx_log':
                entry = f"{time.time():.6f}:op_{ops_count}:data_{random.randint(0,9999)}"
                client.rpush('transaction_log', entry)
                tx_log_entries += 1
                # Trim to keep last N entries
                if tx_log_entries % 5000 == 0:
                    client.ltrim('transaction_log', -max_tx_log, -1)
            elif op_type == 'read':
                idx = random.randint(0, num_keys - 1)
                client.get(f"key:{idx:08d}")
        except Exception as e:
            print(f"[Redis] Operation error: {e}")

        ops_count += 1

        current_time = time.time()
        if current_time - last_report_time >= 5.0:
            try:
                info = client.info('memory')
                mem_mb = info.get('used_memory', 0) / (1024 * 1024)
                total_keys = client.dbsize()
            except Exception:
                mem_mb = 0
                total_keys = 0
            print(f"[Redis] Ops: {ops_count}, Keys: {total_keys}, "
                  f"ExtraWrites: {extra_keys_written}, Updates: {updates_to_existing}, "
                  f"TxLog: {tx_log_entries}, Memory: {mem_mb:.1f}MB, "
                  f"Elapsed: {current_time - start_time:.0f}s")
            last_report_time = current_time

        time.sleep(0.01)


def run_redis_workload(
    redis_port: int = 6379,
    num_keys: int = 100000,
    value_size: int = 1024,
    duration: int = 0,
    working_dir: str = '.'
):
    """
    Main Redis workload.

    Args:
        redis_port: Redis server port
        num_keys: Number of keys to populate
        value_size: Size of each value in bytes
        duration: Duration for continuous operations (0 = populate only)
        working_dir: Working directory for signal files
    """
    if not HAS_REDIS:
        print("[Redis] ERROR: redis-py not installed. Run: pip install redis")
        sys.exit(1)

    print(f"[Redis] Starting Redis workload")
    duration_str = f"{duration}s" if duration > 0 else "populate only"
    print(f"[Redis] Config: port={redis_port}, keys={num_keys}, value_size={value_size}B, duration={duration_str}")
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

    # Continuous operations phase
    print(f"[Redis] Starting continuous operations (duration={duration_str})...")
    try:
        result = run_continuous_operations(client, num_keys, value_size, duration, working_dir)

        if result['restored']:
            print(f"[Redis] Restore detected - checkpoint_flag removed")

            # Reconnect
            try:
                client = redis.Redis(host='localhost', port=redis_port)
                client.ping()
            except Exception:
                print(f"[Redis] Waiting for Redis to be available...")
                if not wait_for_redis('localhost', redis_port, timeout=30):
                    print(f"[Redis] ERROR: Cannot connect to Redis after restore")
                    return
                client = redis.Redis(host='localhost', port=redis_port)

            try:
                total_keys = client.dbsize()
                info = client.info('memory')
                mem_mb = info.get('used_memory', 0) / (1024 * 1024)
            except Exception:
                total_keys = 0
                mem_mb = 0

            print(f"[Redis] === STATE SUMMARY (lost on restart) ===")
            print(f"[Redis]   Total operations: {result['ops_count']}")
            print(f"[Redis]   Extra key writes: {result['extra_keys_written']} (live: {result['extra_keys_live']})")
            print(f"[Redis]   Updates to existing keys: {result['updates_to_existing']}")
            print(f"[Redis]   Transaction log entries: {result['tx_log_entries']}")
            print(f"[Redis]   Sorted set operations: {result['sorted_set_members']}")
            print(f"[Redis]   Current keys in DB: {total_keys}")
            print(f"[Redis]   Redis memory: {mem_mb:.1f} MB")
            print(f"[Redis]   Elapsed time: {result['elapsed']:.1f}s")
            print(f"[Redis]   ALL live data state LOST on restart (values, rankings, tx log)")
            print(f"[Redis] ==========================================")

    except KeyboardInterrupt:
        print(f"[Redis] Interrupted")

    finally:
        # Clean shutdown
        print(f"[Redis] Shutting down Redis server...")
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
        '--duration',
        type=int,
        default=0,
        help='Duration in seconds for continuous operations (0 = populate only)'
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
        duration=args.duration,
        working_dir=args.working_dir
    )


if __name__ == '__main__':
    main()
