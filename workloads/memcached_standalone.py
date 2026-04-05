#!/usr/bin/env python3
"""
Memcached + YCSB Standalone Workload (CRIU Checkpoint with Process Tree)

This script manages a Memcached server process for CRIU checkpoint testing.
CRIU checkpoints this wrapper script, and with --tree option, memcached
(child process) is also checkpointed together.

Load generation uses YCSB Java binary (standard benchmark).

Usage:
    python3 memcached_standalone.py --ycsb-workload a --ycsb-home /opt/ycsb --record-count 100000

Checkpoint Protocol:
    1. This script starts memcached as child process
    2. YCSB load phase populates data
    3. Creates 'checkpoint_ready' file with THIS script's PID (wrapper)
    4. CRIU with --tree option checkpoints: wrapper + memcached
    5. YCSB client is NOT checkpointed (load generator only)
    6. After restore, both wrapper and memcached resume

Important:
    - CRIU checkpoints THIS script's PID with --tree option
    - memcached is automatically included as child process
    - YCSB Java client is NOT checkpoint target

Dirty page pattern:
    - Memcached slab allocator: page-aligned slab units (different from Redis jemalloc)
    - Slab lazy allocation: memory grows during YCSB load phase
    - Zipfian distribution creates hot/cold slab separation

Scenario:
    - Memcached caching layers (HeatSnap comparison)
    - In-memory key-value stores
    - Web application caches
"""

import time
import os
import sys
import argparse
import subprocess
import signal
import socket


def start_memcached_server(port: int, memory_mb: int, threads: int = 1) -> subprocess.Popen:
    """Start memcached server process."""
    try:
        subprocess.run(['which', 'memcached'], check=True, capture_output=True)
    except subprocess.CalledProcessError:
        print("[Memcached] ERROR: memcached not found. Install: sudo apt install memcached")
        sys.exit(1)

    cmd = [
        'memcached',
        '-p', str(port),
        '-m', str(memory_mb),
        '-l', '0.0.0.0',
        '-t', str(threads),
        '-u', 'nobody',
    ]

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        preexec_fn=None if os.environ.get("CRIU_NO_SETSID") else os.setsid,
    )
    return process


def wait_for_memcached(host: str, port: int, timeout: int = 30) -> bool:
    """Wait for memcached to be ready via TCP connect."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            sock.connect((host, port))
            # Send stats command to verify it's memcached
            sock.send(b'stats\r\n')
            data = sock.recv(1024)
            sock.close()
            if b'STAT' in data:
                return True
        except (socket.error, socket.timeout, ConnectionRefusedError):
            time.sleep(0.5)
    return False


def get_memcached_stats(host: str, port: int) -> dict:
    """Get memcached stats via TCP."""
    stats = {}
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        sock.connect((host, port))
        sock.send(b'stats\r\n')
        data = b''
        while True:
            chunk = sock.recv(4096)
            data += chunk
            if b'END\r\n' in data:
                break
        sock.close()
        for line in data.decode('utf-8', errors='replace').split('\r\n'):
            if line.startswith('STAT '):
                parts = line.split(' ', 2)
                if len(parts) == 3:
                    stats[parts[1]] = parts[2]
    except Exception:
        pass
    return stats


def create_ready_signal(working_dir: str, wrapper_pid: int, memcached_pid: int):
    """Create checkpoint ready signal file with wrapper PID."""
    ready_path = os.path.join(working_dir, 'checkpoint_ready')
    with open(ready_path, 'w') as f:
        f.write(f'ready:{wrapper_pid}\n')
    print(f"[Memcached] Checkpoint ready signal created")
    print(f"[Memcached] Wrapper PID: {wrapper_pid} (checkpoint target)")
    print(f"[Memcached] Memcached PID: {memcached_pid} (child, included via --tree)")


def check_restore_complete(working_dir: str) -> bool:
    """Check if restore is complete (checkpoint_flag removed)."""
    flag_path = os.path.join(working_dir, 'checkpoint_flag')
    return not os.path.exists(flag_path)


def check_ycsb_installed(ycsb_home: str) -> bool:
    """Check if YCSB binary is available."""
    ycsb_bin = os.path.join(ycsb_home, 'bin', 'ycsb')
    if not os.path.exists(ycsb_bin):
        return os.path.exists(os.path.join(ycsb_home, 'bin', 'ycsb.sh'))
    return True


def get_ycsb_bin(ycsb_home: str) -> str:
    """Get the YCSB binary path."""
    ycsb_bin = os.path.join(ycsb_home, 'bin', 'ycsb')
    if os.path.exists(ycsb_bin):
        return ycsb_bin
    ycsb_sh = os.path.join(ycsb_home, 'bin', 'ycsb.sh')
    if os.path.exists(ycsb_sh):
        return ycsb_sh
    return ycsb_bin


def create_ycsb_properties(
    working_dir: str,
    ycsb_workload: str,
    port: int,
    record_count: int,
    duration: int,
    ycsb_threads: int,
    target_throughput: int,
) -> str:
    """Create YCSB workload properties file for memcached."""
    proportions = {
        'a': 'readproportion=0.5\nupdateproportion=0.5\nscanproportion=0\ninsertproportion=0',
        'b': 'readproportion=0.95\nupdateproportion=0.05\nscanproportion=0\ninsertproportion=0',
        'c': 'readproportion=1.0\nupdateproportion=0\nscanproportion=0\ninsertproportion=0',
        'd': 'readproportion=0.95\nupdateproportion=0\nscanproportion=0\ninsertproportion=0.05',
        'e': 'readproportion=0\nupdateproportion=0\nscanproportion=0.95\ninsertproportion=0.05\nmaxscanlength=100',
        'f': 'readproportion=0.5\nupdateproportion=0\nscanproportion=0\ninsertproportion=0\nreadmodifywriteproportion=0.5',
    }

    props = f"""workload=site.ycsb.workloads.CoreWorkload
recordcount={record_count}
operationcount=2147483647
maxexecutiontime={duration}
requestdistribution=zipfian
fieldcount=10
fieldlength=100
memcached.hosts=localhost:{port}
{proportions[ycsb_workload]}
"""

    props_path = os.path.join(working_dir, f'ycsb_memcached_{ycsb_workload}.properties')
    with open(props_path, 'w') as f:
        f.write(props)

    print(f"[Memcached] YCSB properties written to {props_path}")
    return props_path


def run_ycsb_phase(ycsb_home: str, phase: str, props_path: str,
                   ycsb_threads: int, target_throughput: int) -> subprocess.Popen:
    """Run a YCSB phase (load or run) against memcached."""
    ycsb_bin = get_ycsb_bin(ycsb_home)
    cmd = [
        ycsb_bin, phase, 'memcached', '-s',
        '-P', props_path,
        '-threads', str(ycsb_threads),
    ]
    if target_throughput > 0:
        cmd.extend(['-target', str(target_throughput)])

    print(f"[Memcached] YCSB {phase}: {' '.join(cmd)}")

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return process


def run_memcached_workload(
    port: int = 11211,
    memory_mb: int = 256,
    memcached_threads: int = 1,
    ycsb_workload: str = 'a',
    ycsb_home: str = '/opt/ycsb',
    record_count: int = 100000,
    ycsb_threads: int = 1,
    target_throughput: int = 0,
    duration: int = 0,
    working_dir: str = '.',
    keep_running: bool = False,
):
    """
    Main Memcached + YCSB workload.

    Args:
        port: Memcached port
        memory_mb: Memcached memory limit in MB
        memcached_threads: Memcached server threads
        ycsb_workload: YCSB workload type (a-f)
        ycsb_home: Path to YCSB installation
        record_count: Number of records for YCSB
        ycsb_threads: Number of YCSB client threads
        target_throughput: YCSB target ops/sec (0 = unlimited)
        duration: Duration in seconds
        working_dir: Working directory for signal files
    """
    if not check_ycsb_installed(ycsb_home):
        print(f"[Memcached] ERROR: YCSB not found at {ycsb_home}")
        print(f"[Memcached] Install YCSB: curl -O --location https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz")
        sys.exit(1)

    duration_str = f"{duration}s" if duration > 0 else "unlimited"
    print(f"[Memcached] Starting Memcached + YCSB workload")
    print(f"[Memcached] Config: port={port}, memory={memory_mb}MB, ycsb={ycsb_workload}, "
          f"records={record_count}, threads={ycsb_threads}, target={target_throughput} ops/s, "
          f"duration={duration_str}")
    print(f"[Memcached] Working directory: {working_dir}")
    os.makedirs(working_dir, exist_ok=True)

    # Start memcached server
    print(f"[Memcached] Starting memcached server...")
    mc_process = start_memcached_server(port, memory_mb, memcached_threads)
    mc_pid = mc_process.pid
    print(f"[Memcached] Memcached started with PID: {mc_pid}")

    # Wait for memcached to be ready
    if not wait_for_memcached('localhost', port):
        print("[Memcached] ERROR: Memcached failed to start")
        mc_process.terminate()
        sys.exit(1)
    print(f"[Memcached] Memcached is ready")

    # Create YCSB properties
    props_path = create_ycsb_properties(
        working_dir, ycsb_workload, port,
        record_count, duration, ycsb_threads, target_throughput,
    )

    # YCSB load phase
    print(f"[Memcached] Starting YCSB load phase (records={record_count})...")
    load_proc = run_ycsb_phase(ycsb_home, 'load', props_path, ycsb_threads, 0)
    load_stdout, load_stderr = load_proc.communicate(timeout=600)

    if load_proc.returncode != 0:
        print(f"[Memcached] ERROR: YCSB load failed (exit={load_proc.returncode})")
        stderr_text = load_stderr.decode('utf-8', errors='replace')
        for line in stderr_text.strip().split('\n')[-10:]:
            print(f"[Memcached]   {line}")
        mc_process.terminate()
        sys.exit(1)

    # Parse load throughput
    load_output = load_stdout.decode('utf-8', errors='replace')
    for line in load_output.split('\n'):
        if '[OVERALL], Throughput' in line:
            print(f"[Memcached] YCSB load: {line.strip()}")
            break

    # Get memory stats after load
    stats = get_memcached_stats('localhost', port)
    if stats:
        items = stats.get('curr_items', '?')
        mem_bytes = int(stats.get('bytes', 0))
        mem_mb_used = mem_bytes / (1024 * 1024)
        print(f"[Memcached] After load: {items} items, {mem_mb_used:.2f} MB used")

    # Signal ready
    wrapper_pid = os.getpid()
    create_ready_signal(working_dir, wrapper_pid, mc_pid)

    print(f"[Memcached]")
    print(f"[Memcached] ====== READY FOR CHECKPOINT ======")
    print(f"[Memcached] Wrapper PID: {wrapper_pid} (checkpoint this)")
    print(f"[Memcached] Memcached PID: {mc_pid} (child, included via --tree)")
    print(f"[Memcached] YCSB client: NOT checkpointed (load generator)")
    print(f"[Memcached] To checkpoint: sudo criu dump -t {wrapper_pid} --tree -D <dir> --shell-job")
    print(f"[Memcached] ===================================")
    print(f"[Memcached]")

    # YCSB run phase
    print(f"[Memcached] Starting YCSB run phase...")
    run_proc = run_ycsb_phase(ycsb_home, 'run', props_path, ycsb_threads, target_throughput)

    start_time = time.time()
    last_report_time = start_time
    ycsb_finished = False

    try:
        while True:
            # Check restore
            if not keep_running and check_restore_complete(working_dir):
                print(f"[Memcached] Restore detected - checkpoint_flag removed")
                # Kill YCSB client if still running
                if run_proc.poll() is None:
                    try:
                        run_proc.terminate()
                        run_proc.wait(timeout=5)
                    except Exception:
                        run_proc.kill()

                elapsed = time.time() - start_time

                # Get post-restore stats
                stats = get_memcached_stats('localhost', port)
                items = stats.get('curr_items', '?') if stats else '?'
                mem_bytes = int(stats.get('bytes', 0)) if stats else 0
                mem_mb_used = mem_bytes / (1024 * 1024)

                print(f"[Memcached] === STATE SUMMARY (lost on restart) ===")
                print(f"[Memcached]   Mode: YCSB workload {ycsb_workload.upper()}")
                print(f"[Memcached]   Items in cache: {items}")
                print(f"[Memcached]   Memory used: {mem_mb_used:.2f} MB")
                print(f"[Memcached]   Elapsed time: {elapsed:.1f}s")
                print(f"[Memcached]   ALL cache state LOST on restart")
                print(f"[Memcached] ==========================================")
                break

            # Check if YCSB finished
            if run_proc.poll() is not None and not ycsb_finished:
                ycsb_finished = True
                stdout_data = run_proc.stdout.read().decode('utf-8', errors='replace')
                for line in stdout_data.split('\n'):
                    if '[OVERALL]' in line or '[READ]' in line or '[UPDATE]' in line:
                        print(f"[Memcached] YCSB: {line.strip()}")
                print(f"[Memcached] YCSB run finished (exit={run_proc.returncode})")
                if keep_running:
                    print(f"[Memcached] YCSB done, exiting")
                    break

            # Progress report
            current_time = time.time()
            if current_time - last_report_time >= 5.0:
                elapsed = current_time - start_time
                status = "running" if not ycsb_finished else "finished (waiting for checkpoint)"
                stats = get_memcached_stats('localhost', port)
                mem_str = ""
                if stats:
                    mem_bytes = int(stats.get('bytes', 0))
                    mem_str = f", mem={mem_bytes/(1024*1024):.1f}MB"
                print(f"[Memcached] YCSB {status}{mem_str}, elapsed={elapsed:.0f}s")
                last_report_time = current_time

            time.sleep(1)

    except KeyboardInterrupt:
        print(f"[Memcached] Interrupted")

    finally:
        # Clean shutdown
        print(f"[Memcached] Shutting down memcached...")
        mc_process.terminate()
        try:
            mc_process.wait(timeout=5)
        except Exception:
            mc_process.kill()

    sys.exit(0)


def main():
    parser = argparse.ArgumentParser(
        description="Memcached + YCSB workload for CRIU checkpoint testing"
    )
    parser.add_argument(
        '--port',
        type=int,
        default=11211,
        help='Memcached port (default: 11211)'
    )
    parser.add_argument(
        '--memory-mb',
        type=int,
        default=256,
        help='Memcached memory limit in MB (default: 256)'
    )
    parser.add_argument(
        '--memcached-threads',
        type=int,
        default=1,
        help='Memcached server threads (default: 1)'
    )
    parser.add_argument(
        '--ycsb-workload',
        type=str,
        choices=['a', 'b', 'c', 'd', 'e', 'f'],
        default='a',
        help='YCSB workload type (default: a)'
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
        '--duration',
        type=int,
        default=0,
        help='Duration in seconds (default: 0 = unlimited)'
    )
    parser.add_argument(
        '--working_dir',
        type=str,
        default='.',
        help='Working directory for signal files'
    )
    parser.add_argument(
        '--keep-running',
        action='store_true',
        help='Keep running after restore (ignore checkpoint_flag removal)'
    )

    args = parser.parse_args()

    run_memcached_workload(
        port=args.port,
        memory_mb=args.memory_mb,
        memcached_threads=args.memcached_threads,
        ycsb_workload=args.ycsb_workload,
        ycsb_home=args.ycsb_home,
        record_count=args.record_count,
        ycsb_threads=args.ycsb_threads,
        target_throughput=args.target_throughput,
        duration=args.duration,
        working_dir=args.working_dir,
        keep_running=args.keep_running,
    )


if __name__ == '__main__':
    main()
