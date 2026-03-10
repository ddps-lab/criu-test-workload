#!/usr/bin/env python3
"""
Convert dirty_pattern.json files to simulation scenario format.

This script transforms raw dirty page tracking data into a format suitable
for checkpoint scheduling simulation and algorithm evolution.
"""

import json
import argparse
import glob
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import List, Optional
import numpy as np


@dataclass
class ScenarioMetadata:
    """Metadata about the scenario's characteristics"""
    total_dirty_pages: int
    avg_dirty_rate_pages_per_sec: float
    peak_dirty_rate_pages_per_sec: float
    dirty_sample_ratio: float  # % of samples with dirty pages
    sample_count: int
    sample_interval_ms: float


@dataclass
class Scenario:
    """Simulation scenario format"""
    name: str
    source: str
    duration_ms: float
    page_size: int
    dirty_samples: List[dict]
    metadata: dict


def calculate_metadata(samples: List[dict], duration_ms: float) -> ScenarioMetadata:
    """Calculate scenario metadata from dirty samples"""
    dirty_counts = [s['delta_dirty_count'] for s in samples]
    total_dirty = sum(dirty_counts)

    # Calculate dirty rate per sample
    rates = []
    for i in range(1, len(samples)):
        dt_ms = samples[i]['timestamp_ms'] - samples[i-1]['timestamp_ms']
        if dt_ms > 0:
            rate = samples[i]['delta_dirty_count'] / (dt_ms / 1000)  # pages/sec
            rates.append(rate)

    # Calculate sample interval
    intervals = []
    for i in range(1, len(samples)):
        intervals.append(samples[i]['timestamp_ms'] - samples[i-1]['timestamp_ms'])
    avg_interval = np.mean(intervals) if intervals else 10.0

    # Count samples with dirty pages
    dirty_sample_count = sum(1 for c in dirty_counts if c > 0)

    return ScenarioMetadata(
        total_dirty_pages=total_dirty,
        avg_dirty_rate_pages_per_sec=total_dirty / (duration_ms / 1000) if duration_ms > 0 else 0,
        peak_dirty_rate_pages_per_sec=max(rates) if rates else 0,
        dirty_sample_ratio=dirty_sample_count / len(samples) if samples else 0,
        sample_count=len(samples),
        sample_interval_ms=avg_interval,
    )


def convert_dirty_pattern(input_path: str, output_path: str) -> Scenario:
    """
    Convert a dirty_pattern.json file to scenario format.

    Args:
        input_path: Path to dirty_pattern.json
        output_path: Path to output scenario.json

    Returns:
        Scenario object
    """
    with open(input_path, 'r') as f:
        data = json.load(f)

    # Extract relevant fields
    workload = data.get('workload', 'unknown')
    duration_ms = data.get('tracking_duration_ms', 0)
    page_size = data.get('page_size', 4096)
    samples = data.get('samples', [])

    # Clean up samples - keep only essential fields
    clean_samples = []
    for s in samples:
        clean_samples.append({
            'timestamp_ms': s['timestamp_ms'],
            'delta_dirty_count': s['delta_dirty_count'],
        })

    # Calculate metadata
    metadata = calculate_metadata(clean_samples, duration_ms)

    # Create scenario
    scenario = Scenario(
        name=workload,
        source=input_path,
        duration_ms=duration_ms,
        page_size=page_size,
        dirty_samples=clean_samples,
        metadata=asdict(metadata),
    )

    # Save scenario
    with open(output_path, 'w') as f:
        json.dump(asdict(scenario), f, indent=2)

    return scenario


def convert_all(input_dir: str, output_dir: str, verbose: bool = False) -> List[Scenario]:
    """
    Convert all dirty_pattern.json files in input_dir to scenarios.

    Args:
        input_dir: Directory containing dirty_pattern.json files
        output_dir: Directory to save scenario files
        verbose: Print progress

    Returns:
        List of converted scenarios
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Find all dirty_pattern.json files
    pattern = f"{input_dir}/**/dirty_pattern.json"
    files = glob.glob(pattern, recursive=True)

    if verbose:
        print(f"Found {len(files)} dirty_pattern.json files")

    scenarios = []
    for input_file in files:
        # Extract workload name from path
        parts = Path(input_file).parts
        # Find the directory name containing the workload
        for part in reversed(parts[:-1]):
            if '_' in part:
                workload_name = part.split('_')[0]
                break
        else:
            workload_name = 'unknown'

        output_file = output_path / f"{workload_name}.json"

        if verbose:
            print(f"Converting: {input_file} -> {output_file}")

        scenario = convert_dirty_pattern(input_file, str(output_file))
        scenarios.append(scenario)

        if verbose:
            print(f"  Duration: {scenario.duration_ms/1000:.1f}s")
            print(f"  Samples: {scenario.metadata['sample_count']}")
            print(f"  Avg dirty rate: {scenario.metadata['avg_dirty_rate_pages_per_sec']:.0f} pages/sec")
            print(f"  Peak dirty rate: {scenario.metadata['peak_dirty_rate_pages_per_sec']:.0f} pages/sec")
            print()

    return scenarios


def print_summary(scenarios: List[Scenario]):
    """Print summary table of all scenarios"""
    print("\n" + "=" * 80)
    print("SCENARIO SUMMARY")
    print("=" * 80)
    print(f"{'Workload':<15} {'Duration':<10} {'Samples':<10} {'Avg Rate':<15} {'Peak Rate':<15} {'Dirty %':<10}")
    print("-" * 80)

    for s in sorted(scenarios, key=lambda x: x.metadata['avg_dirty_rate_pages_per_sec'], reverse=True):
        m = s.metadata
        duration_str = f"{s.duration_ms/1000:.1f}s"
        avg_rate = f"{m['avg_dirty_rate_pages_per_sec']:.0f}/s"
        peak_rate = f"{m['peak_dirty_rate_pages_per_sec']:.0f}/s"
        dirty_pct = f"{m['dirty_sample_ratio']*100:.1f}%"

        print(f"{s.name:<15} {duration_str:<10} {m['sample_count']:<10} {avg_rate:<15} {peak_rate:<15} {dirty_pct:<10}")

    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description='Convert dirty_pattern.json files to simulation scenarios'
    )
    parser.add_argument(
        '--input', '-i',
        required=True,
        help='Input directory containing dirty_pattern.json files'
    )
    parser.add_argument(
        '--output', '-o',
        default='.',
        help='Output directory for scenario files (default: current directory)'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Print verbose output'
    )

    args = parser.parse_args()

    scenarios = convert_all(args.input, args.output, args.verbose)
    print_summary(scenarios)

    print(f"\nConverted {len(scenarios)} scenarios to {args.output}")


if __name__ == '__main__':
    main()
