"""
Hot VMA extraction from dirty tracker output.

Implements the same classification as the Kubernetes operator's write profiler:
- θ (theta): dirty ratio threshold (default: 0.3)
- N (consecutive): number of consecutive scans above threshold (default: 3)

Produces hot-vmas.json compatible with CRIU's prefetch seeding.
"""

import json
import os
import logging

logger = logging.getLogger(__name__)

DEFAULT_THETA = 0.3
DEFAULT_CONSECUTIVE_N = 3


def extract_hot_vmas(dirty_output_path: str, theta: float = DEFAULT_THETA,
                     consecutive_n: int = DEFAULT_CONSECUTIVE_N) -> list:
    """
    Extract hot VMAs from dirty tracker output JSON.

    Args:
        dirty_output_path: Path to dirty tracker output JSON
        theta: Dirty ratio threshold for hot classification
        consecutive_n: Number of consecutive scans above theta

    Returns:
        List of dicts with 'start' and 'end' hex strings
    """
    with open(dirty_output_path) as f:
        data = json.load(f)

    timeline = data.get('dirty_rate_timeline', [])
    if not timeline:
        logger.warning("No dirty_rate_timeline in tracker output")
        return []

    # Build per-VMA history: {(start,end): [ratio_per_scan]}
    vma_history = {}
    for sample in timeline:
        for v in sample.get('vma_dirty', []):
            key = (v['start'], v['end'])
            total = v.get('total', 0)
            ratio = v['dirty'] / total if total > 0 else 0
            if key not in vma_history:
                vma_history[key] = []
            vma_history[key].append(ratio)

    # Classify: hot if last N consecutive scans all have ratio > theta
    hot_vmas = []
    for (start, end), ratios in vma_history.items():
        if len(ratios) >= consecutive_n:
            last_n = ratios[-consecutive_n:]
            if all(r > theta for r in last_n):
                size_mb = (int(end, 16) - int(start, 16)) / 1024 / 1024
                avg_ratio = sum(last_n) / len(last_n)
                hot_vmas.append({
                    'start': start,
                    'end': end,
                    'size_mb': size_mb,
                    'avg_ratio': avg_ratio
                })

    logger.info(f"Hot VMA classification (theta={theta}, N={consecutive_n}): "
                f"{len(hot_vmas)} hot VMAs, "
                f"{sum(v['size_mb'] for v in hot_vmas):.1f} MB total")

    return hot_vmas


def save_hot_vmas_json(hot_vmas: list, output_path: str):
    """
    Save hot VMAs in CRIU-compatible hot-vmas.json format.

    Args:
        hot_vmas: List from extract_hot_vmas()
        output_path: Path to write hot-vmas.json
    """
    output = {
        "excluded": [{"start": v["start"], "end": v["end"]} for v in hot_vmas],
        "no_parent": []
    }

    with open(output_path, 'w') as f:
        json.dump(output, f, indent=2)

    logger.info(f"Saved hot-vmas.json: {output_path} ({len(hot_vmas)} ranges)")


def extract_and_save(dirty_output_path: str, dump_dir: str,
                     theta: float = DEFAULT_THETA,
                     consecutive_n: int = DEFAULT_CONSECUTIVE_N) -> str:
    """
    Extract hot VMAs and save to dump directory.

    Args:
        dirty_output_path: Path to dirty tracker output JSON
        dump_dir: CRIU dump directory (hot-vmas.json will be saved here)
        theta: Dirty ratio threshold
        consecutive_n: Consecutive scan count

    Returns:
        Path to saved hot-vmas.json
    """
    hot_vmas = extract_hot_vmas(dirty_output_path, theta, consecutive_n)
    output_path = os.path.join(dump_dir, 'hot-vmas.json')
    save_hot_vmas_json(hot_vmas, output_path)
    return output_path
