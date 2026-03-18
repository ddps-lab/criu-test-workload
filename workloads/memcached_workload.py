"""
Memcached + YCSB Workload Wrapper

Control node wrapper for Memcached server workload with YCSB benchmark.
Matches HeatSnap (WWW 2025) evaluation setup.
"""

from typing import Dict, Any
from .base_workload import BaseWorkload, WorkloadFactory


MEMCACHED_STANDALONE_SCRIPT = '''#!/usr/bin/env python3
"""Memcached + YCSB - Auto-generated standalone script"""

import time
import os
import sys
import argparse
import subprocess
import signal
import socket


def start_memcached_server(port, memory_mb, threads=1):
    try:
        subprocess.run(['which', 'memcached'], check=True, capture_output=True)
    except subprocess.CalledProcessError:
        print("[Memcached] ERROR: memcached not found")
        sys.exit(1)
    cmd = ['memcached', '-p', str(port), '-m', str(memory_mb), '-l', '0.0.0.0',
           '-t', str(threads), '-u', 'nobody']
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setsid)


def wait_for_memcached(host, port, timeout=30):
    start = time.time()
    while time.time() - start < timeout:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            sock.connect((host, port))
            sock.send(b'stats\\r\\n')
            data = sock.recv(1024)
            sock.close()
            if b'STAT' in data:
                return True
        except (socket.error, socket.timeout, ConnectionRefusedError):
            time.sleep(0.5)
    return False


def get_memcached_stats(host, port):
    stats = {}
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        sock.connect((host, port))
        sock.send(b'stats\\r\\n')
        data = b''
        while True:
            chunk = sock.recv(4096)
            data += chunk
            if b'END\\r\\n' in data:
                break
        sock.close()
        for line in data.decode('utf-8', errors='replace').split('\\r\\n'):
            if line.startswith('STAT '):
                parts = line.split(' ', 2)
                if len(parts) == 3:
                    stats[parts[1]] = parts[2]
    except Exception:
        pass
    return stats


def create_ready_signal(working_dir, wrapper_pid):
    ready_path = os.path.join(working_dir, 'checkpoint_ready')
    with open(ready_path, 'w') as f:
        f.write(f'ready:{wrapper_pid}\\n')
    print(f"[Memcached] Checkpoint ready (Wrapper PID: {wrapper_pid})")


def check_restore_complete(working_dir):
    flag_path = os.path.join(working_dir, 'checkpoint_flag')
    return not os.path.exists(flag_path)


def check_ycsb_installed(ycsb_home):
    ycsb_bin = os.path.join(ycsb_home, 'bin', 'ycsb')
    if not os.path.exists(ycsb_bin):
        return os.path.exists(os.path.join(ycsb_home, 'bin', 'ycsb.sh'))
    return True


def get_ycsb_bin(ycsb_home):
    ycsb_bin = os.path.join(ycsb_home, 'bin', 'ycsb')
    if os.path.exists(ycsb_bin):
        return ycsb_bin
    ycsb_sh = os.path.join(ycsb_home, 'bin', 'ycsb.sh')
    if os.path.exists(ycsb_sh):
        return ycsb_sh
    return ycsb_bin


def create_ycsb_properties(working_dir, ycsb_workload, port, record_count,
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
memcached.hosts=localhost:{port}
{proportions[ycsb_workload]}
"""
    props_path = os.path.join(working_dir, f'ycsb_memcached_{ycsb_workload}.properties')
    with open(props_path, 'w') as f:
        f.write(props)
    return props_path


def run_ycsb_phase(ycsb_home, phase, props_path, ycsb_threads, target_throughput):
    ycsb_bin = get_ycsb_bin(ycsb_home)
    cmd = [ycsb_bin, phase, 'memcached', '-s', '-P', props_path, '-threads', str(ycsb_threads)]
    if target_throughput > 0:
        cmd.extend(['-target', str(target_throughput)])
    print(f"[Memcached] YCSB {phase}: {' '.join(cmd)}")
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def run_memcached_workload(port=11211, memory_mb=256, memcached_threads=1,
                           ycsb_workload='a', ycsb_home='/opt/ycsb', record_count=100000,
                           ycsb_threads=1, target_throughput=0, duration=0, working_dir='.'):
    if not check_ycsb_installed(ycsb_home):
        print(f"[Memcached] ERROR: YCSB not found at {ycsb_home}")
        sys.exit(1)

    print(f"[Memcached] Starting workload (ycsb={ycsb_workload}, records={record_count}, duration={duration}s)")

    mc_process = start_memcached_server(port, memory_mb, memcached_threads)
    mc_pid = mc_process.pid
    print(f"[Memcached] Started with PID: {mc_pid}")

    if not wait_for_memcached('localhost', port):
        print("[Memcached] ERROR: Memcached failed to start")
        mc_process.terminate()
        sys.exit(1)

    props_path = create_ycsb_properties(working_dir, ycsb_workload, port,
                                        record_count, duration, ycsb_threads, target_throughput)

    # YCSB load
    print(f"[Memcached] YCSB load phase...")
    load_proc = run_ycsb_phase(ycsb_home, 'load', props_path, ycsb_threads, 0)
    load_stdout, load_stderr = load_proc.communicate(timeout=600)
    if load_proc.returncode != 0:
        print(f"[Memcached] ERROR: YCSB load failed")
        mc_process.terminate()
        sys.exit(1)

    stats = get_memcached_stats('localhost', port)
    if stats:
        print(f"[Memcached] Items: {stats.get('curr_items', '?')}, "
              f"Mem: {int(stats.get('bytes', 0))/(1024*1024):.2f} MB")

    wrapper_pid = os.getpid()
    create_ready_signal(working_dir, wrapper_pid)

    # YCSB run
    print(f"[Memcached] YCSB run phase...")
    run_proc = run_ycsb_phase(ycsb_home, 'run', props_path, ycsb_threads, target_throughput)
    start_time = time.time()
    last_report = start_time

    try:
        while True:
            if check_restore_complete(working_dir):
                print(f"[Memcached] Restore detected")
                if run_proc.poll() is None:
                    try:
                        run_proc.terminate()
                        run_proc.wait(timeout=5)
                    except Exception:
                        run_proc.kill()
                elapsed = time.time() - start_time
                print(f"[Memcached] === STATE SUMMARY ===")
                print(f"[Memcached]   YCSB workload {ycsb_workload.upper()}, elapsed={elapsed:.1f}s")
                print(f"[Memcached]   ALL cache state LOST on restart")
                print(f"[Memcached] =========================")
                break

            if run_proc.poll() is not None:
                print(f"[Memcached] YCSB run finished (exit={run_proc.returncode})")

            current_time = time.time()
            if current_time - last_report >= 5.0:
                status = "running" if run_proc.poll() is None else "finished"
                print(f"[Memcached] YCSB {status}, elapsed={current_time - start_time:.0f}s")
                last_report = current_time
            time.sleep(1)
    finally:
        mc_process.terminate()
        try:
            mc_process.wait(timeout=5)
        except Exception:
            mc_process.kill()

    sys.exit(0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=11211)
    parser.add_argument('--memory-mb', type=int, default=256)
    parser.add_argument('--memcached-threads', type=int, default=1)
    parser.add_argument('--ycsb-workload', type=str, choices=['a','b','c','d','e','f'], default='a')
    parser.add_argument('--ycsb-home', type=str, default='/opt/ycsb')
    parser.add_argument('--record-count', type=int, default=100000)
    parser.add_argument('--ycsb-threads', type=int, default=1)
    parser.add_argument('--target-throughput', type=int, default=0)
    parser.add_argument('--duration', type=int, default=0)
    parser.add_argument('--working_dir', type=str, default='.')
    args = parser.parse_args()
    run_memcached_workload(args.port, args.memory_mb, args.memcached_threads,
                           args.ycsb_workload, args.ycsb_home, args.record_count,
                           args.ycsb_threads, args.target_throughput, args.duration, args.working_dir)


if __name__ == '__main__':
    main()
'''


class MemcachedWorkload(BaseWorkload):
    """
    Memcached + YCSB workload.

    Starts memcached server and runs YCSB benchmark against it.
    Matches HeatSnap (WWW 2025) evaluation setup.

    Dirty page pattern:
    - Memcached slab allocator: page-aligned slab units
    - Different pattern from Redis jemalloc
    - Slab lazy allocation: memory grows during YCSB load

    Requirements (must be pre-installed in AMI):
    - memcached: apt install memcached
    - YCSB: /opt/ycsb with Java runtime
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.port = config.get('port', 11211)
        self.memory_mb = config.get('memory_mb', 256)
        self.memcached_threads = config.get('memcached_threads', 1)
        self.ycsb_workload = config.get('ycsb_workload', 'a')
        self.ycsb_home = config.get('ycsb_home', '/opt/ycsb')
        self.record_count = config.get('record_count', 100000)
        self.ycsb_threads = config.get('ycsb_threads', 1)
        self.target_throughput = config.get('target_throughput', 0)
        self.duration = config.get('duration', 0)

    def get_standalone_script_name(self) -> str:
        return 'memcached_standalone.py'

    def get_standalone_script_content(self) -> str:
        return MEMCACHED_STANDALONE_SCRIPT

    def get_command(self) -> str:
        cmd = f"python3 {self.get_standalone_script_name()}"
        cmd += f" --port {self.port}"
        cmd += f" --memory-mb {self.memory_mb}"
        cmd += f" --memcached-threads {self.memcached_threads}"
        cmd += f" --ycsb-workload {self.ycsb_workload}"
        cmd += f" --ycsb-home {self.ycsb_home}"
        cmd += f" --record-count {self.record_count}"
        cmd += f" --ycsb-threads {self.ycsb_threads}"
        cmd += f" --target-throughput {self.target_throughput}"
        cmd += f" --duration {self.duration}"
        cmd += f" --working_dir {self.working_dir}"
        return cmd

    def get_dependencies(self) -> list[str]:
        return []  # memcached is system package, YCSB is pre-installed

    def validate_config(self) -> bool:
        if self.port <= 0 or self.port > 65535:
            raise ValueError("port must be valid port number")
        if self.memory_mb <= 0:
            raise ValueError("memory_mb must be positive")
        if self.ycsb_workload not in ('a', 'b', 'c', 'd', 'e', 'f'):
            raise ValueError("ycsb_workload must be a-f")
        if self.record_count <= 0:
            raise ValueError("record_count must be positive")
        return True

    def estimate_memory_mb(self) -> float:
        return self.memory_mb


WorkloadFactory.register('memcached', MemcachedWorkload)
