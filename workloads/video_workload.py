"""
FFmpeg Video Processing Workload Wrapper

Control node wrapper for real ffmpeg video processing workload.
The ffmpeg process itself is checkpointed and migrated.
"""

from typing import Dict, Any
from .base_workload import BaseWorkload, WorkloadFactory


VIDEO_STANDALONE_SCRIPT = '''#!/usr/bin/env python3
"""FFmpeg Video Processing - Auto-generated standalone script (Process Tree)"""

import time
import os
import sys
import argparse
import subprocess
import signal


def create_ready_signal(working_dir: str, wrapper_pid: int, ffmpeg_pid: int):
    ready_path = os.path.join(working_dir, 'checkpoint_ready')
    with open(ready_path, 'w') as f:
        f.write(f'ready:{wrapper_pid}\\n')
    print(f"[Video] Wrapper PID: {wrapper_pid}, FFmpeg PID: {ffmpeg_pid}")


def check_restore_complete(working_dir: str) -> bool:
    return not os.path.exists(os.path.join(working_dir, 'checkpoint_flag'))


def check_ffmpeg():
    try:
        return subprocess.run(['which', 'ffmpeg'], capture_output=True).returncode == 0
    except:
        return False


def start_ffmpeg(resolution, fps, duration, output_dir, mode):
    output_file = os.path.join(output_dir, 'output.mp4')

    if mode == 'live':
        output_pattern = os.path.join(output_dir, 'segment_%04d.ts')
        cmd = [
            'ffmpeg', '-y', '-re', '-f', 'lavfi',
            '-i', f'testsrc2=size={resolution}:rate={fps}',
            '-c:v', 'libx264', '-preset', 'veryfast', '-b:v', '2M',
            '-g', str(fps * 2), '-f', 'segment', '-segment_time', '10',
            '-segment_format', 'mpegts', '-reset_timestamps', '1',
            output_pattern
        ]
        output_file = output_pattern
    else:
        cmd = [
            'ffmpeg', '-y', '-f', 'lavfi',
            '-i', f'testsrc2=size={resolution}:rate={fps}:duration={duration}',
            '-c:v', 'libx264', '-preset', 'medium', '-b:v', '5M',
            '-pix_fmt', 'yuv420p', '-movflags', '+faststart',
            output_file
        ]

    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setsid)
    return process, output_file


def count_segments(output_dir):
    return sum(1 for f in os.listdir(output_dir) if f.startswith('segment_') and f.endswith('.ts'))


def run_video_workload(resolution, fps, duration, mode, working_dir):
    if not check_ffmpeg():
        print("[Video] ERROR: ffmpeg not found")
        sys.exit(1)

    print(f"[Video] Starting video processing")
    print(f"[Video] Config: {resolution} @ {fps}fps, mode={mode}")

    output_dir = os.path.join(working_dir, 'video_output')
    os.makedirs(output_dir, exist_ok=True)

    ffmpeg_process, output_path = start_ffmpeg(resolution, fps, duration, output_dir, mode)
    ffmpeg_pid = ffmpeg_process.pid
    print(f"[Video] FFmpeg PID: {ffmpeg_pid}")

    time.sleep(2)
    if ffmpeg_process.poll() is not None:
        print(f"[Video] ERROR: ffmpeg exited")
        sys.exit(1)

    wrapper_pid = os.getpid()
    create_ready_signal(working_dir, wrapper_pid, ffmpeg_pid)
    start_time = time.time()
    last_report = start_time

    try:
        while True:
            if check_restore_complete(working_dir):
                print(f"[Video] Restore detected")
                time.sleep(2)
                if mode == 'live':
                    print(f"[Video] Segments: {count_segments(output_dir)}")
                elif os.path.exists(output_path.replace('%04d', '0000') if '%' in output_path else output_path):
                    size = os.path.getsize(output_path) / (1024*1024) if os.path.exists(output_path) else 0
                    print(f"[Video] Output: {size:.1f}MB")
                print(f"[Video] Elapsed: {time.time() - start_time:.1f}s")
                break

            if ffmpeg_process.poll() is not None:
                time.sleep(1)
                continue

            now = time.time()
            if now - last_report >= 5.0:
                if mode == 'live':
                    print(f"[Video] Processing... segments={count_segments(output_dir)}")
                last_report = now

            time.sleep(1)
    finally:
        if ffmpeg_process.poll() is None:
            try:
                os.killpg(os.getpgid(ffmpeg_pid), signal.SIGTERM)
                ffmpeg_process.wait(timeout=5)
            except:
                ffmpeg_process.kill()

    sys.exit(0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--resolution', type=str, default='1920x1080')
    parser.add_argument('--fps', type=int, default=30)
    parser.add_argument('--duration', type=int, default=300)
    parser.add_argument('--mode', type=str, choices=['file', 'live'], default='file')
    parser.add_argument('--working_dir', type=str, default='.')
    args = parser.parse_args()
    run_video_workload(args.resolution, args.fps, args.duration, args.mode, args.working_dir)


if __name__ == '__main__':
    main()
'''


class VideoWorkload(BaseWorkload):
    """
    FFmpeg video processing workload.

    Runs real ffmpeg transcoding. The ffmpeg process is checkpointed directly.

    Modes:
    - file: Transcode to single output file
    - live: Continuous segment output (simulates live streaming)

    Requirements (must be pre-installed in AMI):
    - ffmpeg: apt install ffmpeg
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.resolution = config.get('resolution', '1920x1080')
        self.fps = config.get('fps', 30)
        self.duration = config.get('duration', 300)
        self.mode = config.get('mode', 'file')

    def get_standalone_script_name(self) -> str:
        return 'video_standalone.py'

    def get_standalone_script_content(self) -> str:
        return VIDEO_STANDALONE_SCRIPT

    def get_command(self) -> str:
        cmd = f"python3 {self.get_standalone_script_name()}"
        cmd += f" --resolution {self.resolution}"
        cmd += f" --fps {self.fps}"
        cmd += f" --duration {self.duration}"
        cmd += f" --mode {self.mode}"
        cmd += f" --working_dir {self.working_dir}"
        return cmd

    def get_dependencies(self) -> list[str]:
        return []  # ffmpeg must be installed via apt in AMI

    def validate_config(self) -> bool:
        if self.fps <= 0:
            raise ValueError("fps must be positive")
        if self.duration <= 0:
            raise ValueError("duration must be positive")
        if self.mode not in ['file', 'live']:
            raise ValueError("mode must be 'file' or 'live'")

        # Validate resolution format
        parts = self.resolution.split('x')
        if len(parts) != 2:
            raise ValueError("resolution must be WxH format")
        try:
            int(parts[0])
            int(parts[1])
        except ValueError:
            raise ValueError("resolution must be WxH with integer values")

        return True

    def estimate_memory_mb(self) -> float:
        # FFmpeg memory depends on resolution and codec
        # Rough estimate: 100MB base + resolution factor
        width, height = map(int, self.resolution.split('x'))
        pixels = width * height
        return 100 + (pixels / 1000000) * 50  # ~50MB per megapixel


WorkloadFactory.register('video', VideoWorkload)
