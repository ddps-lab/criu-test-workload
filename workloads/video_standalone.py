#!/usr/bin/env python3
"""
FFmpeg Video Processing Standalone Workload (CRIU with Process Tree)

This script runs real ffmpeg video processing for CRIU checkpoint testing.
CRIU checkpoints this wrapper script, and with --tree option, ffmpeg
(child process) is also checkpointed together.

Usage:
    python3 video_standalone.py --resolution 1920x1080 --duration 300 --output-dir /tmp/video

Checkpoint Protocol:
    1. Generates synthetic video source or uses test pattern
    2. Starts ffmpeg transcoding process as child
    3. Creates 'checkpoint_ready' file with THIS script's PID (wrapper)
    4. CRIU with --tree option checkpoints: wrapper + ffmpeg
    5. After restore, both processes resume together

Important:
    - CRIU checkpoints THIS script's PID with --tree option
    - ffmpeg is automatically included as child process
    - Uses testsrc or lavfi for synthetic input

Scenario:
    - Video transcoding jobs
    - Live streaming processing
    - Media encoding pipelines
"""

import time
import os
import sys
import argparse
import subprocess
import signal
import hashlib


def create_ready_signal(working_dir: str, wrapper_pid: int, ffmpeg_pid: int):
    """Create checkpoint ready signal file with wrapper PID."""
    ready_path = os.path.join(working_dir, 'checkpoint_ready')
    with open(ready_path, 'w') as f:
        # Write the WRAPPER PID - CRIU --tree will include ffmpeg as child
        f.write(f'ready:{wrapper_pid}\n')
    print(f"[Video] Checkpoint ready signal created")
    print(f"[Video] Wrapper PID: {wrapper_pid} (checkpoint target)")
    print(f"[Video] FFmpeg PID: {ffmpeg_pid} (child, included via --tree)")


def check_restore_complete(working_dir: str) -> bool:
    """Check if restore is complete (checkpoint_flag removed)."""
    flag_path = os.path.join(working_dir, 'checkpoint_flag')
    return not os.path.exists(flag_path)


def check_ffmpeg_installed() -> bool:
    """Check if ffmpeg is installed."""
    try:
        result = subprocess.run(['which', 'ffmpeg'], capture_output=True)
        return result.returncode == 0
    except:
        return False


def get_ffmpeg_version() -> str:
    """Get ffmpeg version string."""
    try:
        result = subprocess.run(['ffmpeg', '-version'], capture_output=True, text=True)
        return result.stdout.split('\n')[0]
    except:
        return "unknown"


def start_ffmpeg_transcode(
    resolution: str,
    fps: int,
    duration: int,
    output_dir: str,
    codec: str = 'libx264',
    bitrate: str = '5M'
) -> subprocess.Popen:
    """
    Start ffmpeg transcoding process.

    Uses testsrc filter to generate synthetic video, then transcodes it.
    This simulates real video processing workload.
    """
    width, height = resolution.split('x')
    output_file = os.path.join(output_dir, 'output.mp4')

    # FFmpeg command:
    # - Generate test pattern video (testsrc2 for more complex pattern)
    # - Transcode to H.264
    # - Output to file
    cmd = [
        'ffmpeg',
        '-y',  # Overwrite output
        '-f', 'lavfi',
        '-i', f'testsrc2=size={resolution}:rate={fps}:duration={duration}',
        '-c:v', codec,
        '-preset', 'medium',  # Balance between speed and compression
        '-b:v', bitrate,
        '-pix_fmt', 'yuv420p',
        '-movflags', '+faststart',
        output_file
    ]

    print(f"[Video] Starting ffmpeg: {' '.join(cmd)}")

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        preexec_fn=os.setsid
    )

    return process, output_file


def start_ffmpeg_live_transcode(
    resolution: str,
    fps: int,
    output_dir: str,
    segment_time: int = 10
) -> subprocess.Popen:
    """
    Start ffmpeg in "live" mode - continuous transcoding with segments.

    This better simulates live streaming/transcoding scenarios.
    """
    width, height = resolution.split('x')
    output_pattern = os.path.join(output_dir, 'segment_%04d.ts')

    cmd = [
        'ffmpeg',
        '-y',
        '-re',  # Read input at native frame rate (simulate real-time)
        '-f', 'lavfi',
        '-i', f'testsrc2=size={resolution}:rate={fps}',  # Infinite duration
        '-c:v', 'libx264',
        '-preset', 'veryfast',
        '-b:v', '2M',
        '-g', str(fps * 2),  # Keyframe every 2 seconds
        '-f', 'segment',
        '-segment_time', str(segment_time),
        '-segment_format', 'mpegts',
        '-reset_timestamps', '1',
        output_pattern
    ]

    print(f"[Video] Starting live transcode: {' '.join(cmd[:10])}...")

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        preexec_fn=os.setsid
    )

    return process, output_pattern


def get_output_stats(output_file: str) -> dict:
    """Get statistics about output file."""
    if not os.path.exists(output_file):
        return {'exists': False}

    stats = {
        'exists': True,
        'size_mb': os.path.getsize(output_file) / (1024 * 1024),
    }

    # Get video info using ffprobe
    try:
        cmd = [
            'ffprobe', '-v', 'error',
            '-select_streams', 'v:0',
            '-show_entries', 'stream=width,height,duration,nb_frames',
            '-of', 'csv=p=0',
            output_file
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            parts = result.stdout.strip().split(',')
            if len(parts) >= 3:
                stats['width'] = parts[0]
                stats['height'] = parts[1]
                stats['duration'] = parts[2] if parts[2] != 'N/A' else 'unknown'
    except:
        pass

    return stats


def count_segments(output_dir: str) -> int:
    """Count number of segment files."""
    count = 0
    for f in os.listdir(output_dir):
        if f.startswith('segment_') and f.endswith('.ts'):
            count += 1
    return count


def run_video_workload(
    resolution: str = '1920x1080',
    fps: int = 30,
    duration: int = 300,
    mode: str = 'file',  # 'file' or 'live'
    working_dir: str = '.'
):
    """
    Main video processing workload.

    Args:
        resolution: Video resolution (e.g., '1920x1080')
        fps: Frames per second
        duration: Duration in seconds (for file mode)
        mode: 'file' for single file output, 'live' for segment output
        working_dir: Working directory
    """
    if not check_ffmpeg_installed():
        print("[Video] ERROR: ffmpeg not found. Install with: sudo apt install ffmpeg")
        sys.exit(1)

    print(f"[Video] Starting video processing workload")
    print(f"[Video] FFmpeg: {get_ffmpeg_version()}")
    print(f"[Video] Config: resolution={resolution}, fps={fps}, duration={duration}s, mode={mode}")
    print(f"[Video] Working directory: {working_dir}")

    # Create output directory
    output_dir = os.path.join(working_dir, 'video_output')
    os.makedirs(output_dir, exist_ok=True)

    # Start ffmpeg
    if mode == 'live':
        ffmpeg_process, output_path = start_ffmpeg_live_transcode(resolution, fps, output_dir)
    else:
        ffmpeg_process, output_path = start_ffmpeg_transcode(resolution, fps, duration, output_dir)

    ffmpeg_pid = ffmpeg_process.pid
    print(f"[Video] FFmpeg started with PID: {ffmpeg_pid}")

    # Wait a moment for ffmpeg to initialize
    time.sleep(2)

    # Check if ffmpeg is still running
    if ffmpeg_process.poll() is not None:
        stderr = ffmpeg_process.stderr.read().decode()
        print(f"[Video] ERROR: ffmpeg exited unexpectedly: {stderr}")
        sys.exit(1)

    # Signal ready - with WRAPPER PID (this script)
    wrapper_pid = os.getpid()
    create_ready_signal(working_dir, wrapper_pid, ffmpeg_pid)

    print(f"[Video]")
    print(f"[Video] ====== READY FOR CHECKPOINT ======")
    print(f"[Video] Wrapper PID: {wrapper_pid} (checkpoint this)")
    print(f"[Video] FFmpeg PID: {ffmpeg_pid} (child process)")
    print(f"[Video] To checkpoint: sudo criu dump -t {wrapper_pid} --tree -D <dir> --shell-job")
    print(f"[Video] ===================================")
    print(f"[Video]")

    start_time = time.time()
    last_report_time = start_time

    try:
        while True:
            # Check if restore completed
            if check_restore_complete(working_dir):
                print(f"[Video] Restore detected - checkpoint_flag removed")

                # Give ffmpeg a moment after restore
                time.sleep(2)

                # Check output status
                if mode == 'live':
                    segments = count_segments(output_dir)
                    print(f"[Video] Segments created: {segments}")
                else:
                    stats = get_output_stats(output_path)
                    if stats.get('exists'):
                        print(f"[Video] Output file: {stats['size_mb']:.2f} MB")
                        if 'duration' in stats:
                            print(f"[Video] Duration: {stats['duration']}s")
                    else:
                        print(f"[Video] Output file not yet created")

                elapsed = time.time() - start_time
                print(f"[Video] Total processing time: {elapsed:.1f}s")
                print("[Video] Workload complete")
                break

            # Check if ffmpeg is still running
            if ffmpeg_process.poll() is not None:
                print(f"[Video] FFmpeg finished (exit code: {ffmpeg_process.returncode})")
                # In file mode, this is expected when done
                if mode == 'file':
                    stats = get_output_stats(output_path)
                    print(f"[Video] Output: {stats}")
                # Keep running to wait for checkpoint_flag removal
                time.sleep(1)
                continue

            # Progress report every 5 seconds
            current_time = time.time()
            if current_time - last_report_time >= 5.0:
                elapsed = current_time - start_time

                if mode == 'live':
                    segments = count_segments(output_dir)
                    print(f"[Video] Processing... segments={segments}, elapsed={elapsed:.0f}s")
                else:
                    if os.path.exists(output_path):
                        size_mb = os.path.getsize(output_path) / (1024 * 1024)
                        print(f"[Video] Processing... output={size_mb:.1f}MB, elapsed={elapsed:.0f}s")
                    else:
                        print(f"[Video] Processing... elapsed={elapsed:.0f}s")

                last_report_time = current_time

            time.sleep(1)

    except KeyboardInterrupt:
        print(f"[Video] Interrupted")

    finally:
        # Clean shutdown
        if ffmpeg_process.poll() is None:
            print(f"[Video] Stopping ffmpeg...")
            try:
                os.killpg(os.getpgid(ffmpeg_pid), signal.SIGTERM)
                ffmpeg_process.wait(timeout=5)
            except:
                ffmpeg_process.kill()

    sys.exit(0)


def main():
    parser = argparse.ArgumentParser(
        description="FFmpeg video processing workload for CRIU checkpoint testing"
    )
    parser.add_argument(
        '--resolution',
        type=str,
        default='1920x1080',
        help='Video resolution WxH (default: 1920x1080)'
    )
    parser.add_argument(
        '--fps',
        type=int,
        default=30,
        help='Frames per second (default: 30)'
    )
    parser.add_argument(
        '--duration',
        type=int,
        default=300,
        help='Duration in seconds for file mode (default: 300)'
    )
    parser.add_argument(
        '--mode',
        type=str,
        choices=['file', 'live'],
        default='file',
        help='Output mode: file or live segments (default: file)'
    )
    parser.add_argument(
        '--working_dir',
        type=str,
        default='.',
        help='Working directory'
    )

    args = parser.parse_args()

    run_video_workload(
        resolution=args.resolution,
        fps=args.fps,
        duration=args.duration,
        mode=args.mode,
        working_dir=args.working_dir
    )


if __name__ == '__main__':
    main()
