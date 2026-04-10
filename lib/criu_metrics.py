"""
CRIU Metrics Parser

Extracts detailed CRIU metrics from lazy-pages and restore log files
for experiment analysis and paper data collection.
"""

import re
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


def parse_lazy_pages_log(log_content: str) -> Dict[str, Any]:
    """
    Parse CRIU lazy-pages daemon log to extract metrics.

    Args:
        log_content: Full text of criu-lazy-pages.log

    Returns:
        Dictionary with extracted metrics
    """
    metrics = {
        'cache': {},
        'prefetch': {},
        'controller': {},
        'uffd_transfers': [],
        'pre_queue': [],
        'hot_vma': {},
        'daemon_duration_s': None,
    }

    for line in log_content.split('\n'):
        # Extract timestamp from CRIU log format: (seconds.microseconds)
        ts_match = re.match(r'\((\d+\.\d+)\)', line)

        # Cache stats: lookups=208 hits=64 misses=144 hit_rate=30.8%
        m = re.search(r'Cache stats: lookups=(\d+) hits=(\d+) misses=(\d+) hit_rate=([\d.]+)%', line)
        if m:
            metrics['cache'] = {
                'lookups': int(m.group(1)),
                'hits': int(m.group(2)),
                'misses': int(m.group(3)),
                'hit_rate': float(m.group(4)),
            }

        # Prefetch stats: total=0 completed=135 failed=0 bytes=468750336
        m = re.search(r'STATS requests=(\d+) completed=(\d+) failed=(\d+) hits=(\d+) misses=(\d+)', line)
        if m:
            metrics['prefetch'] = {
                'total_requests': int(m.group(1)),
                'completed': int(m.group(2)),
                'failed': int(m.group(3)),
                'cache_stored': int(m.group(4)),
                'bytes_prefetched': int(m.group(5)),
            }

        # Controller stats
        m = re.search(
            r'CONTROLLER faults=(\d+) removes=(\d+) promotes=(\d+) '
            r'obsolete=(\d+) proximity=(\d+)'
            r'(?: hot_faults=(\d+) cold_faults=(\d+) hot_prefetched=(\d+))?',
            line
        )
        if m:
            metrics['controller'] = {
                'faults_processed': int(m.group(1)),
                'queue_removes': int(m.group(2)),
                'priority_promotions': int(m.group(3)),
                'obsolete_prevented': int(m.group(4)),
                'proximity_removed': int(m.group(5)),
                'hot_vma_faults': int(m.group(6)) if m.group(6) else 0,
                'cold_vma_faults': int(m.group(7)) if m.group(7) else 0,
                'hot_vma_prefetched': int(m.group(8)) if m.group(8) else 0,
            }

        # UFFD transferred pages: (58378/58378)
        m = re.search(r'uffd: (\d+)-+(\d+): UFFD transferred pages: \((\d+)/(\d+)\)', line)
        if m and ts_match:
            metrics['uffd_transfers'].append({
                'pid': int(m.group(1)),
                'uffd_id': int(m.group(2)),
                'pages_transferred': int(m.group(3)),
                'pages_total': int(m.group(4)),
                'timestamp': float(ts_match.group(1)),
            })

        # Pre-queued IOVs
        m = re.search(
            r'Pre-queued (\d+) IOVs \((\d+) hot, (\d+) sequential, filtered (\d+) small\)',
            line
        )
        if m:
            metrics['pre_queue'].append({
                'total_queued': int(m.group(1)),
                'hot': int(m.group(2)),
                'sequential': int(m.group(3)),
                'filtered_small': int(m.group(4)),
            })

        # Hot VMA count
        m = re.search(r'Marked (\d+) IOVs as hot', line)
        if m:
            metrics['hot_vma']['marked_hot'] = int(m.group(1))

        # No hot-vmas.json
        if 'No hot-vmas.json found' in line:
            metrics['hot_vma']['available'] = False

        # Page cache final stats
        m = re.search(r'Page cache cleanup.*lookups=(\d+), hits=(\d+) \(([\d.]+)%\), stores=(\d+)', line)
        if m:
            metrics['cache']['final_lookups'] = int(m.group(1))
            metrics['cache']['final_hits'] = int(m.group(2))
            metrics['cache']['final_hit_rate'] = float(m.group(3))
            metrics['cache']['final_stores'] = int(m.group(4))

        # Daemon duration: last timestamp
        if ts_match:
            ts = float(ts_match.group(1))
            if metrics['daemon_duration_s'] is None or ts > metrics['daemon_duration_s']:
                metrics['daemon_duration_s'] = ts

    # Per-fault analysis from PAGE FAULT / PAGE FAULT SERVED pairs
    fault_events = []
    pending_fault = {}  # pid -> timestamp
    pending_bytes = {}  # pid -> bytes copied in current fault
    for line in log_content.split('\n'):
        ts_match = re.match(r'\((\d+\.\d+)\)', line)
        if not ts_match:
            continue
        ts = float(ts_match.group(1))

        # Fault start
        m = re.search(r'uffd: (\d+)-+\d+: === PAGE FAULT at (0x[0-9a-f]+)', line)
        if m:
            pid = m.group(1)
            pending_fault[pid] = ts
            pending_bytes[pid] = 0

        # uffd_copy with size
        m = re.search(r'uffd: (\d+)-+\d+: uffd_copy: 0x[0-9a-f]+/(\d+)', line)
        if m:
            pid = m.group(1)
            if pid in pending_bytes:
                pending_bytes[pid] += int(m.group(2))

        # Fault served (with source: S3 or CACHE)
        m = re.search(r'uffd: (\d+)-+\d+: === PAGE FAULT SERVED from (\S+)', line)
        if m:
            pid = m.group(1)
            source = m.group(2)  # "S3" or "CACHE"
            if pid in pending_fault:
                stall_ms = (ts - pending_fault[pid]) * 1000
                served_bytes = pending_bytes.get(pid, 0)
                fault_events.append({
                    'stall_ms': round(stall_ms, 3),
                    'source': source,
                    'bytes': served_bytes,
                    'pages': served_bytes // 4096 if served_bytes else 0,
                })
                del pending_fault[pid]
                pending_bytes.pop(pid, None)

    # Fault summary
    fault_count = len(fault_events)
    if fault_count > 0:
        metrics['uffd_faults'] = fault_count

        s3_faults = [f for f in fault_events if f['source'] == 'S3']
        cache_faults = [f for f in fault_events if f['source'] == 'CACHE']

        all_stalls = [f['stall_ms'] for f in fault_events]
        s3_stalls = [f['stall_ms'] for f in s3_faults]
        cache_stalls = [f['stall_ms'] for f in cache_faults]

        all_pages = [f['pages'] for f in fault_events if f['pages'] > 0]
        s3_pages = [f['pages'] for f in s3_faults if f['pages'] > 0]

        metrics['fault_stats'] = {
            'total': fault_count,
            's3_served': len(s3_faults),
            'cache_served': len(cache_faults),
            'stall_ms_avg': round(sum(all_stalls) / len(all_stalls), 3) if all_stalls else 0,
            'stall_ms_min': round(min(all_stalls), 3) if all_stalls else 0,
            'stall_ms_max': round(max(all_stalls), 3) if all_stalls else 0,
            'stall_ms_p50': round(sorted(all_stalls)[len(all_stalls)//2], 3) if all_stalls else 0,
            's3_stall_ms_avg': round(sum(s3_stalls) / len(s3_stalls), 3) if s3_stalls else 0,
            's3_stall_ms_min': round(min(s3_stalls), 3) if s3_stalls else 0,
            's3_stall_ms_max': round(max(s3_stalls), 3) if s3_stalls else 0,
            's3_stall_ms_p50': round(sorted(s3_stalls)[len(s3_stalls)//2], 3) if s3_stalls else 0,
            'cache_stall_ms_avg': round(sum(cache_stalls) / len(cache_stalls), 3) if cache_stalls else 0,
            'cache_stall_ms_min': round(min(cache_stalls), 3) if cache_stalls else 0,
            'cache_stall_ms_max': round(max(cache_stalls), 3) if cache_stalls else 0,
            'pages_per_fault_avg': round(sum(all_pages) / len(all_pages), 1) if all_pages else 0,
            'pages_per_fault_min': min(all_pages) if all_pages else 0,
            'pages_per_fault_max': max(all_pages) if all_pages else 0,
            's3_pages_per_fault_avg': round(sum(s3_pages) / len(s3_pages), 1) if s3_pages else 0,
        }
        # Raw fault event list for detailed analysis
        metrics['fault_events'] = fault_events

    else:
        # Fallback: count PAGE FAULT keywords
        fault_count = log_content.count('PAGE FAULT at')
        if fault_count > 0:
            metrics['uffd_faults'] = fault_count

    # Compute aggregates
    if metrics['uffd_transfers']:
        total_pages = sum(t['pages_transferred'] for t in metrics['uffd_transfers'])
        total_expected = sum(t['pages_total'] for t in metrics['uffd_transfers'])
        metrics['uffd_summary'] = {
            'total_pages_transferred': total_pages,
            'total_pages_expected': total_expected,
            'total_bytes_transferred': total_pages * 4096,
            'num_processes': len(metrics['uffd_transfers']),
            'total_faults': fault_count,
        }

    if metrics['pre_queue']:
        total_hot = sum(p['hot'] for p in metrics['pre_queue'])
        total_seq = sum(p['sequential'] for p in metrics['pre_queue'])
        total_queued = sum(p['total_queued'] for p in metrics['pre_queue'])
        metrics['pre_queue_summary'] = {
            'total_queued': total_queued,
            'total_hot': total_hot,
            'total_sequential': total_seq,
        }

    return metrics


def parse_restore_log(log_content: str) -> Dict[str, Any]:
    """
    Parse CRIU restore log to extract timing and metadata fetch metrics.

    Args:
        log_content: Full text of criu-restore.log

    Returns:
        Dictionary with extracted metrics
    """
    metrics = {
        'metadata_fetches': [],
        'page_fetches': [],
        'errors': [],
        'restore_duration_s': None,
    }

    for line in log_content.split('\n'):
        ts_match = re.match(r'\((\d+\.\d+)\)', line)

        # Metadata fetched from S3
        m = re.search(r'Fetched (\S+\.img) from object storage \((\d+) bytes\)', line)
        if m and ts_match:
            metrics['metadata_fetches'].append({
                'file': m.group(1),
                'bytes': int(m.group(2)),
                'timestamp': float(ts_match.group(1)),
            })

        # Object storage fetch range (page-by-page)
        m = re.search(r'objstor: FETCH_DONE key=(\S+) offset=(\d+) len=(\d+) dur_ms=([\d.]+)', line)
        if m and ts_match:
            metrics['page_fetches'].append({
                'key': m.group(1),
                'offset': int(m.group(2)),
                'length': int(m.group(3)),
                'duration_ms': float(m.group(4)),
                'timestamp': float(ts_match.group(1)),
            })

        # Errors
        if 'Error' in line and ts_match:
            metrics['errors'].append({
                'message': line.strip(),
                'timestamp': float(ts_match.group(1)),
            })

        # Last timestamp = restore duration
        if ts_match:
            ts = float(ts_match.group(1))
            if metrics['restore_duration_s'] is None or ts > metrics['restore_duration_s']:
                metrics['restore_duration_s'] = ts

    # Aggregates
    if metrics['page_fetches']:
        total_bytes = sum(f['length'] for f in metrics['page_fetches'])
        total_count = len(metrics['page_fetches'])
        total_ms = sum(f['duration_ms'] for f in metrics['page_fetches'])
        avg_ms = total_ms / total_count if total_count else 0
        metrics['page_fetch_summary'] = {
            'total_fetches': total_count,
            'total_bytes': total_bytes,
            'total_duration_ms': total_ms,
            'avg_fetch_ms': round(avg_ms, 2),
        }

    if metrics['metadata_fetches']:
        total_meta_bytes = sum(f['bytes'] for f in metrics['metadata_fetches'])
        metrics['metadata_summary'] = {
            'total_files': len(metrics['metadata_fetches']),
            'total_bytes': total_meta_bytes,
        }

    return metrics


def parse_dump_log(log_content: str) -> Dict[str, Any]:
    """
    Parse CRIU dump log for S3 upload metrics.

    Args:
        log_content: Full text of criu-dump.log

    Returns:
        Dictionary with extracted metrics
    """
    metrics = {
        'uploads': [],
        'multipart_uploads': [],
        'dump_duration_s': None,
    }

    for line in log_content.split('\n'):
        ts_match = re.match(r'\((\d+\.\d+)\)', line)

        # PUT metadata
        m = re.search(r'PUT (\S+\.img) succeeded \(HTTP \d+\)', line)
        if m and ts_match:
            metrics['uploads'].append({
                'file': m.group(1),
                'timestamp': float(ts_match.group(1)),
            })

        # Multipart upload completed
        m = re.search(r'Multipart upload completed: (\S+) \((\d+) parts\)', line)
        if m and ts_match:
            metrics['multipart_uploads'].append({
                'file': m.group(1),
                'parts': int(m.group(2)),
                'timestamp': float(ts_match.group(1)),
            })

        if ts_match:
            ts = float(ts_match.group(1))
            if metrics['dump_duration_s'] is None or ts > metrics['dump_duration_s']:
                metrics['dump_duration_s'] = ts

    metrics['upload_summary'] = {
        'metadata_files': len(metrics['uploads']),
        'multipart_files': len(metrics['multipart_uploads']),
        'total_parts': sum(u['parts'] for u in metrics['multipart_uploads']),
    }

    return metrics


def collect_criu_metrics(host: str, checkpoint_dir: str,
                         ssh_user: str = 'ubuntu') -> Dict[str, Any]:
    """
    Collect all CRIU metrics from a remote host.

    Args:
        host: Remote host IP
        checkpoint_dir: Path to checkpoint directory on remote host
        ssh_user: SSH username

    Returns:
        Combined metrics dictionary
    """
    import subprocess

    result = {}

    log_files = {
        'lazy_pages': f'{checkpoint_dir}/criu-lazy-pages.log',
        'restore': f'{checkpoint_dir}/criu-restore.log',
        'dump': f'{checkpoint_dir}/criu-dump.log',
    }

    for name, path in log_files.items():
        cmd = f"ssh -o StrictHostKeyChecking=no {ssh_user}@{host} 'strings {path} 2>/dev/null'"
        try:
            proc = subprocess.run(cmd, shell=True, capture_output=True, timeout=30)
            if proc.returncode == 0 and proc.stdout:
                content = proc.stdout.decode('utf-8', errors='replace')
                if name == 'lazy_pages':
                    result['lazy_pages'] = parse_lazy_pages_log(content)
                elif name == 'restore':
                    result['restore'] = parse_restore_log(content)
                elif name == 'dump':
                    result['dump'] = parse_dump_log(content)
        except Exception as e:
            logger.warning(f"Failed to collect {name} log from {host}: {e}")

    return result
