"""
7zip Compression Workload Wrapper

Control node wrapper for 7zip compression workload.
Matches HeatSnap (WWW 2025) evaluation setup.
"""

from typing import Dict, Any
from .base_workload import BaseWorkload, WorkloadFactory


SEVENZIP_STANDALONE_SCRIPT = '''#!/usr/bin/env python3
"""7zip Compression - Auto-generated standalone script"""

import time
import os
import sys
import argparse
import subprocess
import signal
import random


def create_ready_signal(working_dir, wrapper_pid, child_pid):
    ready_path = os.path.join(working_dir, 'checkpoint_ready')
    with open(ready_path, 'w') as f:
        f.write(f'ready:{wrapper_pid}\\n')
    print(f"[7zip] Checkpoint ready (Wrapper PID: {wrapper_pid}, 7z PID: {child_pid})")


def check_restore_complete(working_dir):
    flag_path = os.path.join(working_dir, 'checkpoint_flag')
    return not os.path.exists(flag_path)


def check_7z_installed():
    try:
        result = subprocess.run(['which', '7z'], capture_output=True)
        return result.returncode == 0
    except:
        return False


def generate_input_file(filepath, size_mb, seed=42):
    chunk_size = 1024 * 1024
    try:
        import numpy as np
        rng = np.random.RandomState(seed)
        with open(filepath, 'wb') as f:
            for i in range(size_mb):
                if i % 2 == 0:
                    data = rng.bytes(chunk_size)
                else:
                    pattern = rng.bytes(256)
                    data = pattern * (chunk_size // 256)
                f.write(data)
    except ImportError:
        random.seed(seed)
        with open(filepath, 'wb') as f:
            for i in range(size_mb):
                if i % 2 == 0:
                    data = random.randbytes(chunk_size)
                else:
                    pattern = random.randbytes(256)
                    data = pattern * (chunk_size // 256)
                f.write(data)
    print(f"[7zip] Input file: {filepath} ({size_mb} MB)")
    return filepath


def start_compression(input_file, output_file, level, threads):
    if os.path.exists(output_file):
        os.remove(output_file)
    cmd = ['7z', 'a', f'-mx{level}', f'-mmt{threads}', '-y', output_file, input_file]
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setsid)


def run_sevenzip_workload(compression_level=9, threads=1, input_size_mb=256,
                          seed=42, duration=0, working_dir='.'):
    if not check_7z_installed():
        print("[7zip] ERROR: 7z not found. Install: sudo apt install p7zip-full")
        sys.exit(1)

    print(f"[7zip] Starting (level={compression_level}, threads={threads}, input={input_size_mb}MB)")

    output_dir = os.path.join(working_dir, '7zip_output')
    os.makedirs(output_dir, exist_ok=True)

    input_file = os.path.join(output_dir, 'input.dat')
    generate_input_file(input_file, input_size_mb, seed)
    output_file = os.path.join(output_dir, 'output.7z')

    z_process = start_compression(input_file, output_file, compression_level, threads)
    time.sleep(0.2)
    ret = z_process.poll()
    if ret is not None and ret != 0:
        print(f"[7zip] ERROR: 7z failed (exit={ret})")
        sys.exit(1)

    wrapper_pid = os.getpid()
    create_ready_signal(working_dir, wrapper_pid, z_process.pid)

    start_time = time.time()
    last_report = start_time
    cycles = 0

    try:
        while True:
            if check_restore_complete(working_dir):
                elapsed = time.time() - start_time
                print(f"[7zip] Restore detected")
                print(f"[7zip] === STATE SUMMARY ===")
                print(f"[7zip]   Cycles: {cycles}, elapsed={elapsed:.1f}s")
                print(f"[7zip]   ALL compression state LOST on restart")
                print(f"[7zip] =========================")
                break

            if z_process.poll() is not None:
                cycles += 1
                elapsed = time.time() - start_time
                if duration > 0 and elapsed < duration:
                    if os.path.exists(output_file):
                        os.remove(output_file)
                    z_process = start_compression(input_file, output_file, compression_level, threads)
                else:
                    while not check_restore_complete(working_dir):
                        time.sleep(1)
                    print(f"[7zip] Restore detected")
                    print(f"[7zip] Cycles: {cycles}, elapsed={time.time() - start_time:.1f}s")
                    break

            current_time = time.time()
            if current_time - last_report >= 5.0:
                elapsed = current_time - start_time
                out_mb = os.path.getsize(output_file) / (1024*1024) if os.path.exists(output_file) else 0
                print(f"[7zip] Cycle {cycles+1}: output={out_mb:.1f}MB, elapsed={elapsed:.0f}s")
                last_report = current_time
            time.sleep(1)
    finally:
        if z_process.poll() is None:
            try:
                os.killpg(os.getpgid(z_process.pid), signal.SIGTERM)
                z_process.wait(timeout=5)
            except:
                z_process.kill()

    sys.exit(0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--compression-level', type=int, default=9, choices=range(1,10))
    parser.add_argument('--threads', type=int, default=1)
    parser.add_argument('--input-size-mb', type=int, default=256)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--duration', type=int, default=0)
    parser.add_argument('--working_dir', type=str, default='.')
    args = parser.parse_args()
    run_sevenzip_workload(args.compression_level, args.threads, args.input_size_mb,
                          args.seed, args.duration, args.working_dir)


if __name__ == '__main__':
    main()
'''


class SevenZipWorkload(BaseWorkload):
    """
    7zip compression workload.

    Runs 7z compression for CRIU checkpoint testing.
    Matches HeatSnap (WWW 2025) evaluation setup.

    Dirty page pattern:
    - LZMA dictionary + hash table: working set proportional to dict size
    - Sliding window access pattern (different from matmul matrix)
    - Periodic I/O buffer flushes

    Requirements (must be pre-installed in AMI):
    - p7zip-full: apt install p7zip-full
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.compression_level = config.get('compression_level', 9)
        self.threads = config.get('threads', 1)
        self.input_size_mb = config.get('input_size_mb', 256)
        self.seed = config.get('seed', 42)
        self.duration = config.get('duration', 0)

    def get_standalone_script_name(self) -> str:
        return 'sevenzip_standalone.py'


    def get_command(self) -> str:
        cmd = f"python3 {self.get_standalone_script_name()}"
        cmd += f" --compression-level {self.compression_level}"
        cmd += f" --threads {self.threads}"
        cmd += f" --input-size-mb {self.input_size_mb}"
        cmd += f" --seed {self.seed}"
        cmd += f" --duration {self.duration}"
        cmd += f" --working_dir {self.working_dir}"
        return cmd

    def get_dependencies(self) -> list[str]:
        return []  # p7zip-full is system package

    def validate_config(self) -> bool:
        if self.compression_level < 1 or self.compression_level > 9:
            raise ValueError("compression_level must be 1-9")
        if self.input_size_mb <= 0:
            raise ValueError("input_size_mb must be positive")
        if self.threads <= 0:
            raise ValueError("threads must be positive")
        return True

    def estimate_memory_mb(self) -> float:
        # 7z memory usage depends on compression level and dictionary size
        # Level 9 uses ~800MB dictionary, level 5 uses ~48MB
        dict_sizes = {1: 16, 2: 16, 3: 32, 4: 32, 5: 48, 6: 48, 7: 128, 8: 256, 9: 800}
        return dict_sizes.get(self.compression_level, 128) + self.input_size_mb


WorkloadFactory.register('7zip', SevenZipWorkload)
