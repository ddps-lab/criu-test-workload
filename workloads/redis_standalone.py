#!/usr/bin/env python3
"""
Redis Server Workload (CRIU Checkpoint with Process Tree)

This script manages a Redis server process for CRIU checkpoint testing.
CRIU checkpoints this wrapper script, and with --tree option, redis-server
(child process) is also checkpointed together.

Supports two load generation modes:
  1. Built-in: Simple mixed read/write operations (default, no external deps)
  2. YCSB: Standard Yahoo Cloud Serving Benchmark with Zipfian distribution
     (requires YCSB Java binary at --ycsb-home)

Usage:
    # Built-in mode (backward compatible)
    python3 redis_standalone.py --redis-port 6379 --num-keys 100000

    # YCSB mode (standard benchmark)
    python3 redis_standalone.py --ycsb-workload a --ycsb-home /opt/ycsb --record-count 100000

Checkpoint Protocol:
    1. This script starts redis-server as child process
    2. Populates data (built-in pipeline or YCSB load phase)
    3. Creates 'checkpoint_ready' file with THIS script's PID (wrapper)
    4. CRIU with --tree option checkpoints: wrapper + redis-server
    5. After restore, both processes resume together

Important:
    - CRIU checkpoints THIS script's PID with --tree option
    - redis-server is automatically included as child process
    - YCSB Java client is NOT checkpoint target (load generator only)

Scenario:
    - Redis caching layers
    - Session stores
    - Real-time data caches
    - YCSB benchmark comparison (HeatSnap, OoH)
"""

import time
import os
import sys
import argparse
import random
import hashlib
import subprocess
import signal
import tempfile

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
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        preexec_fn=None if os.environ.get("CRIU_NO_SETSID") else os.setsid
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


def check_ycsb_installed(ycsb_home: str) -> bool:
    """Check if YCSB binary is available."""
    ycsb_bin = os.path.join(ycsb_home, 'bin', 'ycsb')
    if not os.path.exists(ycsb_bin):
        ycsb_sh = os.path.join(ycsb_home, 'bin', 'ycsb.sh')
        return os.path.exists(ycsb_sh)
    return True


def get_ycsb_bin(ycsb_home: str) -> str:
    """Get the YCSB binary path. Prefer ycsb.sh (bash) over ycsb (python2)."""
    ycsb_sh = os.path.join(ycsb_home, 'bin', 'ycsb.sh')
    if os.path.exists(ycsb_sh):
        return ycsb_sh
    ycsb_bin = os.path.join(ycsb_home, 'bin', 'ycsb')
    if os.path.exists(ycsb_bin):
        return ycsb_bin
    return ycsb_sh  # will fail with clear error











def create_ycsb_properties(
    working_dir: str,
    ycsb_workload: str,
    redis_port: int,
    record_count: int,
    duration: int,
    ycsb_threads: int,
    target_throughput: int,
) -> str:
    """Create YCSB workload properties file.

    Returns path to the properties file.
    """
    # Map single-letter workload names to YCSB workload classes
    workload_map = {
        'a': 'site.ycsb.workloads.CoreWorkload',
        'b': 'site.ycsb.workloads.CoreWorkload',
        'c': 'site.ycsb.workloads.CoreWorkload',
        'd': 'site.ycsb.workloads.CoreWorkload',
        'e': 'site.ycsb.workloads.CoreWorkload',
        'f': 'site.ycsb.workloads.CoreWorkload',
    }

    # Workload-specific proportions
    # A: 50/50 read/update (Update heavy)
    # B: 95/5 read/update (Read mostly)
    # C: 100% read (Read only)
    # D: 95/5 read/insert (Read latest)
    # E: 95/5 scan/insert (Short ranges)
    # F: 50/50 read/read-modify-write
    proportions = {
        'a': 'readproportion=0.5\nupdateproportion=0.5\nscanproportion=0\ninsertproportion=0',
        'b': 'readproportion=0.95\nupdateproportion=0.05\nscanproportion=0\ninsertproportion=0',
        'c': 'readproportion=1.0\nupdateproportion=0\nscanproportion=0\ninsertproportion=0',
        'd': 'readproportion=0.95\nupdateproportion=0\nscanproportion=0\ninsertproportion=0.05',
        'e': 'readproportion=0\nupdateproportion=0\nscanproportion=0.95\ninsertproportion=0.05\nmaxscanlength=100',
        'f': 'readproportion=0.5\nupdateproportion=0\nscanproportion=0\ninsertproportion=0\nreadmodifywriteproportion=0.5',
    }

    props = f"""workload={workload_map[ycsb_workload]}
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

    print(f"[Redis] YCSB properties written to {props_path}")
    return props_path


def run_ycsb_phase(ycsb_home: str, phase: str, props_path: str,
                   ycsb_threads: int, target_throughput: int) -> subprocess.Popen:
    """Run a YCSB phase (load or run).

    Returns the subprocess.Popen object. YCSB client is NOT started with
    setsid — it is a load generator, NOT a checkpoint target.
    """
    ycsb_bin = get_ycsb_bin(ycsb_home)
    # -jvm-args=-Xint disables JIT compilation. JIT's class dependency tracking
    # races with CRIU's process freeze, causing JVM crash at dump time
    # (DependencyContext::mark_dependent_nmethods SIGSEGV → glibc malloc
    # double-fault → SIGABRT). Interpreter mode avoids this entirely at the
    # cost of YCSB throughput, which is irrelevant for our restore measurements.
    # Note: YCSB's bin/ycsb script doesn't honor JAVA_OPTS env; must use
    # -jvm-args on the YCSB command line.
    cmd = [
        ycsb_bin, phase, 'redis', '-s',
        '-jvm-args=-Xint',
        '-P', props_path,
        '-threads', str(ycsb_threads),
    ]
    if target_throughput > 0:
        cmd.extend(['-target', str(target_throughput)])

    print(f"[Redis] YCSB {phase}: {' '.join(cmd)}")

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return process


def run_ycsb_operations(
    ycsb_home: str,
    ycsb_workload: str,
    redis_port: int,
    record_count: int,
    duration: int,
    ycsb_threads: int,
    target_throughput: int,
    working_dir: str,
):
    """Run YCSB load and run phases.

    1. YCSB load phase: populate data
    2. Signal checkpoint ready
    3. YCSB run phase: generate load until duration or restore
    4. Monitor for checkpoint_flag removal (restore complete)

    Returns dict with results.
    """
    props_path = create_ycsb_properties(
        working_dir, ycsb_workload, redis_port,
        record_count, duration, ycsb_threads, target_throughput,
    )

    # YCSB load phase
    print(f"[Redis] Starting YCSB load phase (recordcount={record_count})...")
    load_proc = run_ycsb_phase(ycsb_home, 'load', props_path, ycsb_threads, 0)
    load_stdout, load_stderr = load_proc.communicate(timeout=600)

    if load_proc.returncode != 0:
        print(f"[Redis] ERROR: YCSB load failed (exit={load_proc.returncode})")
        if load_stderr:
            stderr_text = load_stderr.decode('utf-8', errors='replace')
            for line in stderr_text.strip().split('\n')[-10:]:
                print(f"[Redis]   {line}")
        sys.exit(1)

    # Parse load phase throughput
    if load_stdout:
        load_output = load_stdout.decode('utf-8', errors='replace')
        for line in load_output.split('\n'):
            if '[OVERALL], Throughput' in line:
                print(f"[Redis] YCSB load: {line.strip()}")
            break

    return props_path


def monitor_ycsb_run(
    ycsb_home: str,
    props_path: str,
    ycsb_threads: int,
    target_throughput: int,
    duration: int,
    working_dir: str,
    keep_running: bool = True,
) -> dict:
    """Start YCSB run phase and monitor for restore.

    Returns result dict.
    """
    print(f"[Redis] Starting YCSB run phase...")
    run_proc = run_ycsb_phase(ycsb_home, 'run', props_path, ycsb_threads, target_throughput)

    start_time = time.time()
    last_report_time = start_time
    ycsb_finished = False

    while True:
        # Check restore
        if not keep_running and check_restore_complete(working_dir):
            print(f"[Redis] Restore detected during YCSB run phase")
            # Kill YCSB client if still running (it's not checkpointed)
            if run_proc.poll() is None:
                try:
                    run_proc.terminate()
                    run_proc.wait(timeout=5)
                except Exception:
                    run_proc.kill()
            elapsed = time.time() - start_time
            return {
                'restored': True,
                'mode': 'ycsb',
                'elapsed': elapsed,
                'ycsb_finished': ycsb_finished,
            }

        # Check if YCSB run finished naturally
        if run_proc.poll() is not None and not ycsb_finished:
            ycsb_finished = True
            if run_proc.stdout:
                stdout_data = run_proc.stdout.read().decode('utf-8', errors='replace')
            else:
                stdout_data = ''
            for line in stdout_data.split('\n'):
                if '[OVERALL]' in line or '[READ]' in line or '[UPDATE]' in line:
                    print(f"[Redis] YCSB: {line.strip()}")
            print(f"[Redis] YCSB run phase finished (exit={run_proc.returncode})")
            if not keep_running:
                print(f"[Redis] YCSB done, exiting")
                elapsed = time.time() - start_time
                return {
                    'restored': False,
                    'mode': 'ycsb',
                    'elapsed': elapsed,
                    'ycsb_finished': True,
                }
            # keep_running: stay alive so redis-server keeps serving. Restart
            # YCSB so post-restore measurements have an active load generator
            # (without this, page-fault counts after lazy restore would be
            # dominated by redis idle behavior, not workload accesses).
            print(f"[Redis] YCSB done, restarting YCSB and keeping redis alive")
            try:
                run_proc = run_ycsb_phase(ycsb_home, 'run', props_path,
                                          ycsb_threads, target_throughput)
                print(f"[Redis] YCSB restarted (pid={run_proc.pid})")
            except Exception as e:
                print(f"[Redis] YCSB restart failed: {e}")
            while True:
                time.sleep(5)

        current_time = time.time()
        if current_time - last_report_time >= 5.0:
            elapsed = current_time - start_time
            status = "running" if not ycsb_finished else "finished (waiting for checkpoint)"
            print(f"[Redis] YCSB {status}, elapsed={elapsed:.0f}s")
            last_report_time = current_time

        time.sleep(1)


def run_continuous_operations(client, num_keys, value_size, duration, working_dir, keep_running=False):
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
        if not keep_running and check_restore_complete(working_dir):
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
            if keep_running:
                print(f"[Redis] Duration {duration}s reached, exiting")
                return {
                    'restored': False,
                    'ops_count': ops_count,
                    'extra_keys_written': extra_keys_written,
                    'extra_keys_live': min(extra_keys_written, max_extra_keys),
                    'tx_log_entries': tx_log_entries,
                    'sorted_set_members': sorted_set_members,
                    'updates_to_existing': updates_to_existing,
                    'elapsed': elapsed,
                }
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
    working_dir: str = '.',
    ycsb_workload: str = None,
    ycsb_home: str = '/opt/ycsb',
    record_count: int = 100000,
    ycsb_threads: int = 1,
    target_throughput: int = 0,
    keep_running: bool = True,
):
    """
    Main Redis workload.

    Args:
        redis_port: Redis server port
        num_keys: Number of keys to populate (built-in mode)
        value_size: Size of each value in bytes (built-in mode)
        duration: Duration for continuous operations (0 = populate only)
        working_dir: Working directory for signal files
        ycsb_workload: YCSB workload type (a-f), None for built-in mode
        ycsb_home: Path to YCSB installation
        record_count: Number of records for YCSB
        ycsb_threads: Number of YCSB client threads
        target_throughput: YCSB target ops/sec (0 = unlimited)
    """
    use_ycsb = ycsb_workload is not None

    if use_ycsb:
        # YCSB mode: only need redis-server, YCSB handles data
        if not check_ycsb_installed(ycsb_home):
            print(f"[Redis] ERROR: YCSB not found at {ycsb_home}")
            print(f"[Redis] Install YCSB: curl -O --location https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz")
            print(f"[Redis] Then: tar xfvz ycsb-0.17.0.tar.gz && mv ycsb-0.17.0 /opt/ycsb")
            sys.exit(1)
    else:
        if not HAS_REDIS:
            print("[Redis] ERROR: redis-py not installed. Run: pip install redis")
            sys.exit(1)

    mode_str = f"YCSB workload {ycsb_workload.upper()}" if use_ycsb else "built-in"
    duration_str = f"{duration}s" if duration > 0 else "populate only"
    print(f"[Redis] Starting Redis workload (mode={mode_str})")
    if use_ycsb:
        print(f"[Redis] Config: port={redis_port}, ycsb={ycsb_workload}, records={record_count}, "
              f"threads={ycsb_threads}, target={target_throughput} ops/s, duration={duration_str}")
    else:
        print(f"[Redis] Config: port={redis_port}, keys={num_keys}, value_size={value_size}B, duration={duration_str}")
    print(f"[Redis] Working directory: {working_dir}")
    os.makedirs(working_dir, exist_ok=True)

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

    # Connect to Redis (needed for both modes for status checks)
    client = redis.Redis(host='localhost', port=redis_port) if HAS_REDIS else None

    if use_ycsb:
        # === YCSB Mode ===
        # YCSB load phase populates data
        props_path = run_ycsb_operations(
            ycsb_home, ycsb_workload, redis_port, record_count,
            duration, ycsb_threads, target_throughput, working_dir,
        )

        # Get memory stats after load
        if client:
            try:
                info = client.info('memory')
                memory_mb = info.get('used_memory', 0) / (1024 * 1024)
                total_keys = client.dbsize()
                print(f"[Redis] After YCSB load: {total_keys} keys, {memory_mb:.2f} MB")
            except Exception:
                pass

        # Signal ready
        wrapper_pid = os.getpid()
        create_ready_signal(working_dir, wrapper_pid, redis_pid)

        print(f"[Redis]")
        print(f"[Redis] ====== READY FOR CHECKPOINT ======")
        print(f"[Redis] Wrapper PID: {wrapper_pid} (checkpoint this)")
        print(f"[Redis] Redis PID: {redis_pid} (child, included via --tree)")
        print(f"[Redis] YCSB client: NOT checkpointed (load generator)")
        print(f"[Redis] To checkpoint: sudo criu dump -t {wrapper_pid} --tree -D <dir> --shell-job --tcp-established")
        print(f"[Redis] ===================================")
        print(f"[Redis]")

        # IMPORTANT: do not start YCSB run phase before dump.
        # If java is alive at dump time, the JVM races with CRIU's freeze
        # (DependencyContext / GC / glibc malloc) and causes thread crashes.
        # Instead we wait here with NO java/ycsb running. CRIU dumps the
        # wrapper + redis-server tree only. After restore (checkpoint_flag
        # removed by the framework), we start YCSB run phase fresh.
        print(f"[Redis] YCSB load done. Sleeping until restore (no YCSB running).")
        while not check_restore_complete(working_dir):
            time.sleep(1)
        print(f"[Redis] Restore detected at {time.time():.0f}, starting YCSB run phase")
        run_proc = run_ycsb_phase(ycsb_home, 'run', props_path,
                                  ycsb_threads, target_throughput)
        print(f"[Redis] YCSB run started (pid={run_proc.pid})")
        # Stay alive forever; the run process drives load while we sleep.
        try:
            while True:
                time.sleep(5)
                if run_proc.poll() is not None:
                    # YCSB exited (e.g., wall-clock duration); spawn another
                    # so the load generator stays active for measurement.
                    run_proc = run_ycsb_phase(ycsb_home, 'run', props_path,
                                              ycsb_threads, target_throughput)
                    print(f"[Redis] YCSB respawned (pid={run_proc.pid})")
        except KeyboardInterrupt:
            print(f"[Redis] Interrupted")
        finally:
            try: run_proc.terminate()
            except Exception: pass
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
        return  # don't fall through to legacy code

        # ---- legacy path (kept below but unreachable) ----
        try:
            result = monitor_ycsb_run(
                ycsb_home, props_path, ycsb_threads,
                target_throughput, duration, working_dir,
                keep_running=keep_running,
            )

            if result.get('restored'):
                print(f"[Redis] Restore detected - checkpoint_flag removed")
                if client:
                    try:
                        client = redis.Redis(host='localhost', port=redis_port)
                        client.ping()
                    except Exception:
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
                    print(f"[Redis]   Mode: YCSB workload {ycsb_workload.upper()}")
                    print(f"[Redis]   Current keys in DB: {total_keys}")
                    print(f"[Redis]   Redis memory: {mem_mb:.1f} MB")
                    print(f"[Redis]   Elapsed time: {result['elapsed']:.1f}s")
                    print(f"[Redis]   ALL live data state LOST on restart")
                    print(f"[Redis] ==========================================")

        except KeyboardInterrupt:
            print(f"[Redis] Interrupted")

        finally:
            print(f"[Redis] Shutting down Redis server...")
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
        # === Built-in Mode (backward compatible) ===
        print(f"[Redis] Connected to Redis")

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

        initial_checksum = compute_checksum(client, num_keys)
        info = client.info('memory')
        memory_mb = info.get('used_memory', 0) / (1024 * 1024)

        print(f"[Redis] Population complete")
        print(f"[Redis] Memory usage: {memory_mb:.2f} MB")
        print(f"[Redis] Keys in DB: {client.dbsize()}")
        print(f"[Redis] Initial checksum: {initial_checksum[:16]}...")

        wrapper_pid = os.getpid()
        create_ready_signal(working_dir, wrapper_pid, redis_pid)

        print(f"[Redis]")
        print(f"[Redis] ====== READY FOR CHECKPOINT ======")
        print(f"[Redis] Wrapper PID: {wrapper_pid} (checkpoint this)")
        print(f"[Redis] Redis PID: {redis_pid} (child process)")
        print(f"[Redis] To checkpoint: sudo criu dump -t {wrapper_pid} --tree -D <dir> --shell-job --tcp-established")
        print(f"[Redis] ===================================")
        print(f"[Redis]")

        print(f"[Redis] Starting continuous operations (duration={duration_str})...")
        try:
            result = run_continuous_operations(client, num_keys, value_size, duration, working_dir, keep_running=keep_running)

            if result['restored']:
                print(f"[Redis] Restore detected - checkpoint_flag removed")

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
        help='Number of keys to populate in built-in mode (default: 100000)'
    )
    parser.add_argument(
        '--value-size',
        type=int,
        default=1024,
        help='Size of each value in bytes in built-in mode (default: 1024)'
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
    # YCSB options
    parser.add_argument(
        '--ycsb-workload',
        type=str,
        choices=['a', 'b', 'c', 'd', 'e', 'f'],
        default=None,
        help='YCSB workload type (a-f). If set, uses YCSB Java binary instead of built-in ops'
    )
    parser.add_argument(
        '--ycsb-home',
        type=str,
        default='/opt/ycsb',
        help='Path to YCSB installation (default: /opt/ycsb)'
    )
    parser.add_argument(
        '--record-count',
        type=int,
        default=100000,
        help='Number of records for YCSB (default: 100000)'
    )
    parser.add_argument(
        '--ycsb-threads',
        type=int,
        default=1,
        help='Number of YCSB client threads (default: 1)'
    )
    parser.add_argument(
        '--target-throughput',
        type=int,
        default=0,
        help='YCSB target throughput in ops/sec (0 = unlimited, default: 0)'
    )
    parser.add_argument(
        '--stop-on-restore',
        action='store_true',
        help='Stop when restore is detected (checkpoint_flag removed). Default: keep running.'
    )

    args = parser.parse_args()
    args.keep_running = not args.stop_on_restore

    run_redis_workload(
        redis_port=args.redis_port,
        num_keys=args.num_keys,
        value_size=args.value_size,
        duration=args.duration,
        working_dir=args.working_dir,
        ycsb_workload=args.ycsb_workload,
        ycsb_home=args.ycsb_home,
        record_count=args.record_count,
        ycsb_threads=args.ycsb_threads,
        target_throughput=args.target_throughput,
        keep_running=args.keep_running,
    )


if __name__ == '__main__':
    main()
