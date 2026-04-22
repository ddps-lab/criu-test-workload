#!/usr/bin/env python3
"""Parse a directory of CRIU ablation result logs into a flat row-per-run table.

Works on result directories laid out like `run_restore_experiment.sh` writes:
    <dir>/
        1_baseline_run1.json
        3_semi_sync_run1_lazy.log
        3_semi_sync_run1.json
        4_async_run1_lazy.log
        ...

Usage:
    parse_ablation.py DIR [DIR ...]
    parse_ablation.py --mean DIR [DIR ...]         # collapse repeats to mean
    parse_ablation.py --format csv DIR             # CSV output
    parse_ablation.py --match 16gb-compressed DIR  # filter by substring

Only reads files with grep-style patterns (no massive regex over the full log),
so it stays fast on >10 MB lazy logs.
"""
import argparse
import glob
import json
import os
import re
import subprocess
import sys
from statistics import mean, stdev

# --- grep helpers: run grep on the file, return matched lines -------------
def _grep(path, pattern, flags='-aE'):
    try:
        out = subprocess.run(['grep', flags, pattern, path],
                             capture_output=True, text=True, timeout=30)
        return out.stdout.splitlines()
    except Exception:
        return []

def _grep_count(path, pattern):
    try:
        out = subprocess.run(['grep', '-acE', pattern, path],
                             capture_output=True, text=True, timeout=30)
        return int(out.stdout.strip() or 0)
    except Exception:
        return 0

_TS = re.compile(r'^\((\d+\.\d+)\)')

# --- field extractors (each returns None or a value) ----------------------
def _ts_from(line):
    m = _TS.match(line)
    return float(m.group(1)) if m else None

def extract_lazy_log(path):
    """Extract per-run metrics from a *_lazy.log.
    All fields are optional — caller checks for key presence."""
    out = {}

    # daemon end: prefer "Prefetch system cleaned up" (async/full),
    # fallback to last "UFFD transferred pages" line (semi-sync path).
    end_lines = _grep(path, 'Prefetch system cleaned up')
    if not end_lines:
        end_lines = _grep(path, 'UFFD transferred pages:')
    if end_lines:
        ts = _ts_from(end_lines[-1])
        if ts is not None:
            out['daemon_end_s'] = ts

    # workers (async/full only)
    wl = _grep(path, 'Initializing prefetch system with')
    if wl:
        m = re.search(r'with (\d+) workers', wl[-1])
        if m:
            out['workers'] = int(m.group(1))
    autow = _grep(path, 'auto-detected \\d+ worker threads')
    if autow:
        m = re.search(r'auto-detected (\d+)', autow[-1])
        if m:
            out['workers_auto'] = int(m.group(1))

    # controller stats (async/full only)
    cl = _grep(path, 'CONTROLLER faults=')
    if cl:
        m = re.search(r'CONTROLLER faults=(\d+) removes=(\d+) promotes=(\d+) '
                      r'obsolete=(\d+)(?: proximity=\d+)? hot_faults=(\d+) '
                      r'cold_faults=(\d+) hot_prefetched=(\d+)', cl[-1])
        if m:
            out['ctrl_faults'] = int(m.group(1))
            out['ctrl_removes'] = int(m.group(2))
            out['ctrl_promotes'] = int(m.group(3))
            out['ctrl_obsolete'] = int(m.group(4))
            out['hot_faults'] = int(m.group(5))
            out['cold_faults'] = int(m.group(6))
            out['hot_prefetched'] = int(m.group(7))

    pl = _grep(path, 'PREFETCH requests=')
    if pl:
        m = re.search(r'completed=(\d+) failed=(\d+) bytes=(\d+)', pl[-1])
        if m:
            out['prefetch_completed'] = int(m.group(1))
            out['prefetch_failed'] = int(m.group(2))
            out['prefetch_bytes'] = int(m.group(3))

    fw = _grep(path, 'FAULT_WAIT attempted=')
    if fw:
        m = re.search(r'attempted=(\d+) absorbed=(\d+) timed_out=(\d+) '
                      r'not_fetching=(\d+)', fw[-1])
        if m:
            out['fault_wait_attempted'] = int(m.group(1))
            out['fault_wait_absorbed'] = int(m.group(2))
            out['fault_wait_timed_out'] = int(m.group(3))
            out['fault_wait_not_fetching'] = int(m.group(4))

    # Fetch counts by src (works for ALL modes including semi-sync)
    out['fetch_fault_n'] = _grep_count(path, 'FETCH_DONE.*src=fault')
    out['fetch_prefetch_n'] = _grep_count(path, 'FETCH_DONE.*src=prefetch')

    # UFFDIO_COPY first/last/count
    uc = _grep(path, 'uffd_copy:')
    if uc:
        first_ts = _ts_from(uc[0])
        last_ts = _ts_from(uc[-1])
        if first_ts is not None:
            out['first_uffd_copy_s'] = first_ts
        if last_ts is not None:
            out['last_uffd_copy_s'] = last_ts
        out['uffd_copy_n'] = len(uc)

    # Total pages transferred across all PIDs
    ul = _grep(path, 'UFFD transferred pages:')
    total = 0
    for line in ul:
        m = re.search(r'\((\d+)/\d+\)', line)
        if m:
            total += int(m.group(1))
    if total:
        out['pages_transferred'] = total

    # hot-vmas.json load status (RAW/COMP parity check)
    if _grep_count(path, 'No hot-vmas.json found') > 0:
        out['hot_vmas'] = 'missing'
    elif _grep_count(path, 'hot-vmas.json:.*HTTP 200') > 0:
        out['hot_vmas'] = 'loaded'

    # Stall (per-fault wait) summary — printed by restore wrapper near daemon end
    # Format: "S3 stall: avg=152.3ms | Cache stall: avg=0.0ms"
    sl = _grep(path, 'S3 stall: avg=')
    if sl:
        m = re.search(r'S3 stall: avg=([\d.]+)ms.*Cache stall: avg=([\d.]+)ms', sl[-1])
        if m:
            out['s3_stall_avg_ms'] = float(m.group(1))
            out['cache_stall_avg_ms'] = float(m.group(2))

    # Per-fault duration distribution (FETCH_DONE src=fault dur_ms=...)
    # Runs a single grep that streams line-by-line; avoids holding full log in Python.
    # Per-fetch dur_ms and byte size distributions. Parse both src=fault and
    # src=prefetch in a single grep pass over FETCH_DONE lines to avoid three
    # separate greps on a multi-MB log.
    fd_out = subprocess.run(['grep', '-aE', 'FETCH_DONE', path],
                             capture_output=True, text=True, timeout=60)
    fault_durs, pref_durs, fault_bts, pref_bts = [], [], [], []
    for line in fd_out.stdout.splitlines():
        msrc = re.search(r'src=(\w+)', line)
        if not msrc: continue
        md = re.search(r'dur_ms=([\d.]+)', line)
        ml = re.search(r'len=(\d+)', line)
        if msrc.group(1) == 'fault':
            if md: fault_durs.append(float(md.group(1)))
            if ml: fault_bts.append(int(ml.group(1)))
        elif msrc.group(1) == 'prefetch':
            if md: pref_durs.append(float(md.group(1)))
            if ml: pref_bts.append(int(ml.group(1)))

    def _pct(s, p):
        return s[min(int(len(s)*p), len(s)-1)] if s else None

    if fault_durs:
        fd = sorted(fault_durs)
        out['fetch_fault_dur_sum_s'] = sum(fd) / 1000
        out['fetch_fault_dur_mean_ms'] = mean(fd)
        out['fetch_fault_dur_p50_ms'] = _pct(fd, 0.5)
        out['fetch_fault_dur_p90_ms'] = _pct(fd, 0.9)
    if pref_durs:
        pd = sorted(pref_durs)
        out['fetch_prefetch_dur_sum_s'] = sum(pd) / 1000
        out['fetch_prefetch_dur_mean_ms'] = mean(pd)
    if fault_bts:
        out['fetch_fault_bytes_sum'] = sum(fault_bts)
    if pref_bts:
        out['fetch_prefetch_bytes_sum'] = sum(pref_bts)

    # compression.c / compress_pipeline.c per-ctx stats. Multiple lines per
    # log (one per decompress_ctx, one per pipeline) — accumulate.
    # decompress_stats: mode=... calls=N frames=M bytes_comp=X bytes_decomp=Y
    #   ratio=R fetch_ms=F decomp_ms=D fetch_mbps=... decomp_mbps=...
    dec_stats = {'n_ctx': 0, 'calls': 0, 'frames': 0, 'bytes_comp': 0,
                 'bytes_decomp': 0, 'fetch_ms': 0.0, 'decomp_ms': 0.0}
    for line in _grep(path, 'decompress_stats:'):
        for k, typ in (('calls', int), ('frames', int), ('bytes_comp', int),
                       ('bytes_decomp', int), ('fetch_ms', float),
                       ('decomp_ms', float)):
            m = re.search(rf'{k}=([\d.]+)', line)
            if m:
                dec_stats[k] += typ(m.group(1))
        dec_stats['n_ctx'] += 1
    if dec_stats['n_ctx']:
        out['decomp_n_ctx'] = dec_stats['n_ctx']
        out['decomp_calls'] = dec_stats['calls']
        out['decomp_frames'] = dec_stats['frames']
        out['decomp_bytes_comp'] = dec_stats['bytes_comp']
        out['decomp_bytes_decomp'] = dec_stats['bytes_decomp']
        out['decomp_fetch_ms'] = dec_stats['fetch_ms']
        out['decomp_decomp_ms'] = dec_stats['decomp_ms']
        if dec_stats['bytes_decomp']:
            out['decomp_ratio'] = dec_stats['bytes_comp'] / dec_stats['bytes_decomp']
        if dec_stats['fetch_ms']:
            out['decomp_fetch_mbps'] = ((dec_stats['bytes_comp'] / 1048576.0)
                                        / (dec_stats['fetch_ms'] / 1000.0))
        if dec_stats['decomp_ms']:
            out['decomp_decomp_mbps'] = ((dec_stats['bytes_decomp'] / 1048576.0)
                                         / (dec_stats['decomp_ms'] / 1000.0))

    # compress_stats: key=... n_workers=N frames=M bytes_in=X bytes_out=Y
    #   ratio=R compress_ms_sum=... compress_mbps_per_worker=...
    for line in _grep(path, 'compress_stats:'):
        for k, typ in (('n_workers', int), ('frames', int), ('bytes_in', int),
                       ('bytes_out', int), ('ratio', float),
                       ('compress_ms_sum', float)):
            m = re.search(rf'{k}=([\d.]+)', line)
            if m:
                out[f'comp_{k}'] = typ(m.group(1))
        # compute aggregate throughput
        if out.get('comp_compress_ms_sum') and out.get('comp_bytes_in'):
            out['comp_mbps_aggregate'] = (
                (out['comp_bytes_in'] / 1048576.0)
                / (out['comp_compress_ms_sum'] / 1000.0)
                * out.get('comp_n_workers', 1)  # workers ran in parallel
            )

    return out

def extract_json(path):
    """Pull per-run metrics from the .json baseline_experiment.py writes.

    baseline_experiment.py already captures richer CRIU-side stats than the
    lazy log does (stall_ms_*, pages_per_fault_*, pre_queue_summary,
    daemon_duration_s), so we prefer this source when available."""
    try:
        with open(path) as f:
            d = json.load(f)
    except Exception:
        return {}
    out = {}
    r = d.get('restore') or {}
    if r.get('duration') is not None:
        out['restore_s'] = r['duration']
    t = d.get('transfer') or {}
    if t.get('duration') is not None:
        out['transfer_s'] = t['duration']

    cm = d.get('criu_metrics') or {}
    lp = cm.get('lazy_pages') or {}
    if 'daemon_duration_s' in lp:
        out['daemon_s'] = lp['daemon_duration_s']
    if 'uffd_faults' in lp:
        out['uffd_faults'] = lp['uffd_faults']

    fs = lp.get('fault_stats') or {}
    for k in ('stall_ms_avg', 'stall_ms_p50', 'stall_ms_max',
              's3_stall_ms_avg', 'cache_stall_ms_avg',
              'pages_per_fault_avg'):
        if k in fs:
            out[k] = fs[k]

    us = lp.get('uffd_summary') or {}
    if 'total_pages_transferred' in us:
        out['uffd_pages_transferred'] = us['total_pages_transferred']
    if 'total_bytes_transferred' in us:
        out['uffd_bytes_transferred'] = us['total_bytes_transferred']
    if 'total_faults' in us:
        out['uffd_total_faults'] = us['total_faults']

    pq = lp.get('pre_queue_summary') or {}
    if 'total_queued' in pq:
        out['iov_queued'] = pq['total_queued']
    if 'total_hot' in pq:
        out['iov_queued_hot'] = pq['total_hot']

    hv = lp.get('hot_vma') or {}
    if 'marked_hot' in hv:
        out['hot_vma_marked'] = hv['marked_hot']

    return out

# --- discovery ------------------------------------------------------------
_RUN_RE = re.compile(r'^(?P<mode>[^/]+)_run(?P<run>\d+)\.json$')
_LOG_RE = re.compile(r'^(?P<mode>[^/]+)_run(?P<run>\d+)_lazy\.log$')

def scan_dir(d, include_log=True):
    """Return list of {dir, mode, run, lazy_log?, json?}."""
    entries = {}
    for fn in os.listdir(d):
        full = os.path.join(d, fn)
        m = _RUN_RE.match(fn)
        if m:
            key = (m.group('mode'), int(m.group('run')))
            entries.setdefault(key, {})['json'] = full
            continue
        m = _LOG_RE.match(fn)
        if m:
            key = (m.group('mode'), int(m.group('run')))
            entries.setdefault(key, {})['lazy_log'] = full
    rows = []
    for (mode, run), paths in sorted(entries.items()):
        row = {'dir': d, 'mode': mode, 'run': run}
        if include_log and 'lazy_log' in paths:
            row.update(extract_lazy_log(paths['lazy_log']))
        if 'json' in paths:
            row.update(extract_json(paths['json']))
        rows.append(row)
    return rows

# --- aggregation ----------------------------------------------------------
_NUMERIC = (
    # from JSON (fast, always present)
    'daemon_s', 'restore_s', 'transfer_s',
    'uffd_faults', 'uffd_total_faults',
    'stall_ms_avg', 'stall_ms_p50', 'stall_ms_max',
    's3_stall_ms_avg', 'cache_stall_ms_avg',
    'pages_per_fault_avg',
    'uffd_pages_transferred', 'uffd_bytes_transferred',
    'iov_queued', 'iov_queued_hot', 'hot_vma_marked',
    # from lazy log (slower, needs grep)
    'daemon_end_s', 'first_uffd_copy_s', 'last_uffd_copy_s',
    'workers', 'workers_auto', 'ctrl_faults', 'ctrl_removes',
    'ctrl_promotes', 'ctrl_obsolete', 'hot_faults', 'cold_faults',
    'hot_prefetched', 'prefetch_completed', 'prefetch_failed',
    'prefetch_bytes', 'fault_wait_attempted', 'fault_wait_absorbed',
    'fault_wait_timed_out', 'fault_wait_not_fetching',
    'fetch_fault_n', 'fetch_prefetch_n',
    'fetch_fault_dur_sum_s', 'fetch_fault_dur_mean_ms',
    'fetch_fault_dur_p50_ms', 'fetch_fault_dur_p90_ms',
    'fetch_prefetch_dur_sum_s', 'fetch_prefetch_dur_mean_ms',
    'fetch_fault_bytes_sum', 'fetch_prefetch_bytes_sum',
    'uffd_copy_n', 'pages_transferred',
    # compress/decompress ZSTD instrumentation (compression.c / compress_pipeline.c)
    'decomp_n_ctx', 'decomp_calls', 'decomp_frames',
    'decomp_bytes_comp', 'decomp_bytes_decomp', 'decomp_ratio',
    'decomp_fetch_ms', 'decomp_decomp_ms',
    'decomp_fetch_mbps', 'decomp_decomp_mbps',
    'comp_n_workers', 'comp_frames', 'comp_bytes_in', 'comp_bytes_out',
    'comp_ratio', 'comp_compress_ms_sum', 'comp_mbps_aggregate',
)

def aggregate(rows):
    """Collapse repeats to mean per (dir, mode). Keeps run count + stdev on daemon."""
    groups = {}
    for r in rows:
        key = (r['dir'], r['mode'])
        groups.setdefault(key, []).append(r)
    out = []
    for (d, mode), rs in sorted(groups.items()):
        agg = {'dir': d, 'mode': mode, 'n_runs': len(rs)}
        for k in _NUMERIC:
            vals = [r[k] for r in rs if k in r]
            if vals:
                agg[k] = mean(vals)
                if k == 'daemon_end_s' and len(vals) >= 2:
                    agg['daemon_stdev_s'] = stdev(vals)
        # propagate hot_vmas if all runs agree
        hv = {r.get('hot_vmas') for r in rs if 'hot_vmas' in r}
        if len(hv) == 1:
            agg['hot_vmas'] = next(iter(hv))
        out.append(agg)
    return out

# --- formatting -----------------------------------------------------------
def format_table(rows, columns=None):
    if not rows:
        return ''
    if columns is None:
        columns = ['dir', 'mode', 'n_runs', 'daemon_end_s', 'daemon_stdev_s',
                   'workers', 'ctrl_faults', 'fetch_fault_n', 'fetch_prefetch_n',
                   'prefetch_completed', 'hot_prefetched', 'pages_transferred',
                   'restore_s', 'hot_vmas']
    # only keep columns that appear in at least one row
    cols = [c for c in columns if any(c in r for r in rows)]
    widths = {}
    headers = []
    for c in cols:
        h = c
        widths[c] = len(h)
        headers.append(h)
    def fmt(v, c):
        if v is None or v == '':
            return ''
        if isinstance(v, float):
            return f'{v:.2f}'
        return str(v)
    for r in rows:
        for c in cols:
            widths[c] = max(widths[c], len(fmt(r.get(c), c)))
    # shorten 'dir' column
    max_dir = 32
    if 'dir' in widths:
        widths['dir'] = min(widths['dir'], max_dir)
    out_lines = ['  '.join(h.rjust(widths[c]) for h,c in zip(headers, cols))]
    out_lines.append('  '.join('-' * widths[c] for c in cols))
    for r in rows:
        cells = []
        for c in cols:
            v = r.get(c, '')
            s = fmt(v, c)
            if c == 'dir':
                s = s if len(s) <= max_dir else '…' + s[-(max_dir-1):]
            cells.append(s.rjust(widths[c]))
        out_lines.append('  '.join(cells))
    return '\n'.join(out_lines)

def format_csv(rows):
    if not rows:
        return ''
    all_keys = sorted({k for r in rows for k in r})
    header = ','.join(all_keys)
    lines = [header]
    for r in rows:
        cells = []
        for k in all_keys:
            v = r.get(k, '')
            if isinstance(v, float):
                v = f'{v:.4f}'
            cells.append(str(v))
        lines.append(','.join(cells))
    return '\n'.join(lines)

# --- main -----------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('dirs', nargs='+', help='One or more ablation result dirs')
    ap.add_argument('--mean', action='store_true', help='Collapse repeats to mean')
    ap.add_argument('--format', choices=('table','csv','json'), default='table')
    ap.add_argument('--match', default=None, help='Only keep dirs whose path contains this substring')
    ap.add_argument('--columns', help='Comma-separated column list (table format only)')
    ap.add_argument('--no-log', action='store_true',
                    help='Skip lazy log parsing (JSON-only, much faster)')
    args = ap.parse_args()

    all_rows = []
    for d in args.dirs:
        if args.match and args.match not in d:
            continue
        if not os.path.isdir(d):
            sys.stderr.write(f'skip non-dir: {d}\n')
            continue
        all_rows.extend(scan_dir(d, include_log=not args.no_log))

    if args.mean:
        all_rows = aggregate(all_rows)

    if args.format == 'csv':
        print(format_csv(all_rows))
    elif args.format == 'json':
        print(json.dumps(all_rows, indent=2))
    else:
        cols = args.columns.split(',') if args.columns else None
        print(format_table(all_rows, columns=cols))

if __name__ == '__main__':
    main()
