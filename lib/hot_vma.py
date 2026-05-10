"""
Hot chunk extraction from dirty tracker output.

Implements chunk-level (4 MB IOV-aligned) hot/cold classification matching
the Kubernetes operator's profiler (pkg/profiler/heat.go) and the paper's
Eq.1: a chunk is hot iff the last N consecutive scans all have
dirty-ratio > theta.

- theta: dirty ratio threshold (default 0.3)
- N: consecutive-scan count (default 3)

Produces hot-iovs.json compatible with CRIU's prefetch seeding. Adjacent
hot chunks within the same VMA are coalesced into a single (start, end)
range so the output file size stays small.
"""

import json
import os
import logging

logger = logging.getLogger(__name__)

DEFAULT_THETA = 0.3
DEFAULT_CONSECUTIVE_N = 3
PAGE_SIZE = 4096
CHUNK_BYTES = 4 * 1024 * 1024
CHUNK_PAGES = CHUNK_BYTES // PAGE_SIZE


def _chunk_geometry(vma_start: int, vma_end: int):
    """Return (chunk_count, chunk_pages_per_index[]) for a VMA.

    The last chunk may be partial when (vma_end - vma_start) is not a
    multiple of CHUNK_BYTES, so its denominator differs.
    """
    bytes_total = vma_end - vma_start
    n = (bytes_total + CHUNK_BYTES - 1) // CHUNK_BYTES
    pages = []
    for i in range(n):
        chunk_start = vma_start + i * CHUNK_BYTES
        chunk_end = min(chunk_start + CHUNK_BYTES, vma_end)
        pages.append(max(1, (chunk_end - chunk_start) // PAGE_SIZE))
    return n, pages


def extract_hot_chunks(dirty_output_path: str, theta: float = DEFAULT_THETA,
                       consecutive_n: int = DEFAULT_CONSECUTIVE_N) -> list:
    """Extract hot chunks from dirty tracker JSON.

    Returns a list of dicts with 'start'/'end' hex strings, 'size_mb',
    'avg_ratio'. Adjacent hot chunks within the same VMA are coalesced
    into a single range.
    """
    with open(dirty_output_path) as f:
        data = json.load(f)

    timeline = data.get('dirty_rate_timeline', [])
    if not timeline:
        logger.warning("No dirty_rate_timeline in tracker output")
        return []

    # Build per-(vma, chunk) ratio history.
    # Key: (vma_start_hex, vma_end_hex, chunk_idx)
    # Value: list of dirty_pages_at_chunk / chunk_total_pages
    chunk_history = {}
    n_samples_with_chunks = 0
    n_samples_total = len(timeline)
    for sample in timeline:
        sample_has_chunks = False
        for v in sample.get('vma_dirty', []):
            chunk_dirty = v.get('chunk_dirty')
            if not chunk_dirty:
                # No chunk data — fall back to VMA-aggregate so this VMA
                # still gets classified (compatibility with old traces).
                start_i = int(v['start'], 16)
                end_i = int(v['end'], 16)
                total = v.get('total', 0)
                ratio = v['dirty'] / total if total > 0 else 0
                key = (v['start'], v['end'], 0)
                if key not in chunk_history:
                    chunk_history[key] = {
                        'ratios': [],
                        'chunk_start': start_i,
                        'chunk_end': end_i,
                        'page_count': max(1, total),
                    }
                chunk_history[key]['ratios'].append(ratio)
                continue
            sample_has_chunks = True
            vma_start = int(v['start'], 16)
            vma_end = int(v['end'], 16)
            chunk_count, chunk_pages = _chunk_geometry(vma_start, vma_end)
            for ci, dirty in enumerate(chunk_dirty[:chunk_count]):
                page_count = chunk_pages[ci] if ci < len(chunk_pages) else CHUNK_PAGES
                ratio = dirty / page_count if page_count > 0 else 0
                key = (v['start'], v['end'], ci)
                if key not in chunk_history:
                    chunk_history[key] = {
                        'ratios': [],
                        'chunk_start': vma_start + ci * CHUNK_BYTES,
                        'chunk_end': min(vma_start + (ci + 1) * CHUNK_BYTES, vma_end),
                        'page_count': page_count,
                    }
                chunk_history[key]['ratios'].append(ratio)
        if sample_has_chunks:
            n_samples_with_chunks += 1

    # Classify: hot if last N consecutive scans all have ratio > theta.
    hot_chunks = []
    for key, info in chunk_history.items():
        ratios = info['ratios']
        if len(ratios) < consecutive_n:
            continue
        last_n = ratios[-consecutive_n:]
        if all(r > theta for r in last_n):
            hot_chunks.append({
                'vma_start': key[0],
                'vma_end': key[1],
                'chunk_idx': key[2],
                'start_int': info['chunk_start'],
                'end_int': info['chunk_end'],
                'avg_ratio': sum(last_n) / len(last_n),
            })

    # Coalesce adjacent hot chunks within the same VMA. Iterate in sort
    # order so consecutive chunk_idx within a VMA can be merged.
    hot_chunks.sort(key=lambda h: (h['vma_start'], h['chunk_idx']))
    coalesced = []
    for h in hot_chunks:
        if coalesced \
                and coalesced[-1]['vma_start'] == h['vma_start'] \
                and coalesced[-1]['end_int'] == h['start_int']:
            coalesced[-1]['end_int'] = h['end_int']
            coalesced[-1]['ratios'].append(h['avg_ratio'])
            coalesced[-1]['chunks'] += 1
        else:
            coalesced.append({
                'vma_start': h['vma_start'],
                'start_int': h['start_int'],
                'end_int': h['end_int'],
                'ratios': [h['avg_ratio']],
                'chunks': 1,
            })

    out = []
    for c in coalesced:
        size_mb = (c['end_int'] - c['start_int']) / 1024 / 1024
        out.append({
            'start': f"0x{c['start_int']:x}",
            'end': f"0x{c['end_int']:x}",
            'size_mb': size_mb,
            'avg_ratio': sum(c['ratios']) / len(c['ratios']),
            'chunks_merged': c['chunks'],
        })

    total_mb = sum(o['size_mb'] for o in out)
    chunk_total = sum(o['chunks_merged'] for o in out)
    logger.info(
        f"Hot chunk classification (theta={theta}, N={consecutive_n}): "
        f"{chunk_total} hot chunks coalesced into {len(out)} ranges, "
        f"{total_mb:.1f} MB total. "
        f"Samples with chunk data: {n_samples_with_chunks}/{n_samples_total}"
    )
    return out


# Back-compat alias — older callers import this name.
extract_hot_vmas = extract_hot_chunks


def save_hot_iovs_json(hot_iovs: list, output_path: str):
    """Save hot ranges in CRIU-compatible hot-iovs.json format."""
    output = {
        "excluded": [{"start": v["start"], "end": v["end"]} for v in hot_iovs],
        "no_parent": []
    }
    with open(output_path, 'w') as f:
        json.dump(output, f, indent=2)
    logger.info(f"Saved hot-iovs.json: {output_path} ({len(hot_iovs)} ranges)")


# Back-compat alias for callers still using the old name.
save_hot_vmas_json = save_hot_iovs_json


def extract_and_save(dirty_output_path: str, dump_dir: str,
                     theta: float = DEFAULT_THETA,
                     consecutive_n: int = DEFAULT_CONSECUTIVE_N) -> str:
    """Extract hot chunks and save to dump directory.

    Returns the path to the written hot-iovs.json.
    """
    hot = extract_hot_chunks(dirty_output_path, theta, consecutive_n)
    out = os.path.join(dump_dir, 'hot-iovs.json')
    save_hot_iovs_json(hot, out)
    return out
