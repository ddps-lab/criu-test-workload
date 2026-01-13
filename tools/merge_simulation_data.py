#!/usr/bin/env python3
"""
Simulation Data Merger

Merges CRIU logs and dirty page tracking data into a unified format
for simulation analysis.

This tool combines:
1. CRIU dump/restore/lazy-pages logs (parsed by parse_criu_logs.py)
2. Dirty page tracking data (from dirty_tracker.py)
3. Experiment metrics (from baseline_experiment.py)

Output is a single JSON file containing all data needed for simulation.

Usage:
    # Merge all data sources
    python3 merge_simulation_data.py \
        --criu-logs results/simulation_input.json \
        --dirty-pattern results/dirty_pattern.json \
        --metrics results/metrics.json \
        --output results/simulation_data.json

    # Merge with dirty rate analysis
    python3 merge_simulation_data.py \
        --criu-logs results/simulation_input.json \
        --dirty-pattern results/dirty_pattern.json \
        --dirty-analysis results/dirty_analysis.json \
        --output results/simulation_data.json
"""

import argparse
import json
import sys
from typing import Dict, Any, List, Optional
from pathlib import Path


def load_json_file(filepath: str) -> Optional[Dict[str, Any]]:
    """Load JSON file, return None if not found."""
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing {filepath}: {e}", file=sys.stderr)
        return None


def merge_timelines(criu_timeline: List[Dict], dirty_timeline: List[Dict]) -> List[Dict]:
    """
    Merge CRIU event timeline with dirty page timeline.

    Both timelines use timestamp_ms as the time reference.
    Returns a unified timeline sorted by timestamp.
    """
    merged = []

    # Add CRIU events with source marker
    for event in criu_timeline:
        merged_event = event.copy()
        merged_event['source'] = 'criu'
        merged.append(merged_event)

    # Add dirty rate timeline events with source marker
    for entry in dirty_timeline:
        merged_event = {
            'timestamp': entry.get('timestamp_ms', 0) / 1000.0,  # Convert to seconds
            'timestamp_ms': entry.get('timestamp_ms', 0),
            'event_type': 'dirty_rate_sample',
            'message': f"Dirty rate: {entry.get('rate_pages_per_sec', 0):.1f} pages/sec",
            'details': {
                'rate_pages_per_sec': entry.get('rate_pages_per_sec', 0),
                'cumulative_pages': entry.get('cumulative_pages', 0)
            },
            'source': 'dirty_tracker'
        }
        merged.append(merged_event)

    # Sort by timestamp
    merged.sort(key=lambda e: e.get('timestamp', 0))

    return merged


def calculate_correlation_metrics(criu_data: Dict, dirty_data: Dict) -> Dict[str, Any]:
    """
    Calculate correlation metrics between CRIU events and dirty page patterns.

    This helps identify relationships between page faults and dirty rate.
    """
    correlation = {}

    # Get dirty rate timeline
    dirty_timeline = dirty_data.get('dirty_rate_timeline', [])
    if not dirty_timeline:
        return correlation

    # Get lazy page fault events
    lazy_events = criu_data.get('lazy_pages_events', [])
    fault_events = [e for e in lazy_events if e.get('event_type') == 'lazy_fault']

    if fault_events and dirty_timeline:
        # Calculate average dirty rate during page faults
        fault_timestamps = [e.get('timestamp', 0) for e in fault_events]
        if fault_timestamps:
            first_fault = min(fault_timestamps)
            last_fault = max(fault_timestamps)

            # Find dirty samples during fault window
            dirty_during_faults = [
                d for d in dirty_timeline
                if first_fault * 1000 <= d.get('timestamp_ms', 0) <= last_fault * 1000
            ]

            if dirty_during_faults:
                rates = [d.get('rate_pages_per_sec', 0) for d in dirty_during_faults]
                correlation['avg_dirty_rate_during_faults'] = sum(rates) / len(rates)
                correlation['fault_window_duration_sec'] = last_fault - first_fault
                correlation['faults_per_sec'] = len(fault_events) / (last_fault - first_fault) if last_fault > first_fault else 0

    # Calculate fetch vs dirty rate correlation
    objstor_events = criu_data.get('objstor_events', [])
    fetch_events = [e for e in objstor_events
                   if e.get('event_type') in ('objstor_fetch_start', 'objstor_fetch_done', 'objstor_fetch')]

    if fetch_events:
        total_bytes = sum(e.get('details', {}).get('length', 0) for e in fetch_events)
        correlation['total_bytes_fetched'] = total_bytes

        # Calculate fetch rate
        fetch_timestamps = [e.get('timestamp', 0) for e in fetch_events]
        if fetch_timestamps:
            duration = max(fetch_timestamps) - min(fetch_timestamps)
            if duration > 0:
                correlation['avg_fetch_rate_bytes_per_sec'] = total_bytes / duration

    return correlation


def build_simulation_input(criu_data: Dict, dirty_data: Dict,
                           dirty_analysis: Optional[Dict],
                           metrics: Optional[Dict]) -> Dict[str, Any]:
    """
    Build unified simulation input from all data sources.
    """
    output = {
        'version': '1.0',
        'format': 'criu_simulation_data',
        'components': []
    }

    # Add CRIU log data
    if criu_data:
        output['criu_events'] = {
            'dump_events': criu_data.get('dump_events', []),
            'restore_events': criu_data.get('restore_events', []),
            'lazy_pages_events': criu_data.get('lazy_pages_events', []),
            'objstor_events': criu_data.get('objstor_events', []),
            'prefetch_events': criu_data.get('prefetch_events', []),
            'summary': criu_data.get('summary', {})
        }
        output['components'].append('criu_logs')

    # Add dirty page tracking data
    if dirty_data:
        output['dirty_tracking'] = {
            'workload': dirty_data.get('workload', 'unknown'),
            'pid': dirty_data.get('pid', 0),
            'tracking_duration_ms': dirty_data.get('tracking_duration_ms', 0),
            'page_size': dirty_data.get('page_size', 4096),
            'summary': dirty_data.get('summary', {}),
            'dirty_rate_timeline': dirty_data.get('dirty_rate_timeline', []),
            # Include per-sample data (can be large, consider excluding for size)
            'sample_count': len(dirty_data.get('samples', []))
        }
        output['components'].append('dirty_tracking')

    # Add dirty rate analysis
    if dirty_analysis:
        output['dirty_analysis'] = {
            'pattern_analysis': dirty_analysis.get('dirty_rate_analysis', {}),
            'vma_analysis': dirty_analysis.get('vma_analysis', {}),
            'predump_recommendation': dirty_analysis.get('predump_recommendation', {}),
            'simulation_parameters': dirty_analysis.get('simulation_parameters', {})
        }
        output['components'].append('dirty_analysis')

    # Add experiment metrics
    if metrics:
        output['experiment_metrics'] = {
            'total_duration': metrics.get('total_duration', 0),
            'workload_type': metrics.get('workload_type', 'unknown'),
            'checkpoint_strategy': metrics.get('checkpoint_strategy', {}),
            'pre_dump_iterations': metrics.get('pre_dump_iterations', []),
            'final_dump': metrics.get('final_dump', {}),
            'transfer': metrics.get('transfer', {}),
            'restore': metrics.get('restore', {}),
            'lazy_pages_completion': metrics.get('lazy_pages_completion', {})
        }
        output['components'].append('experiment_metrics')

    # Build merged timeline
    criu_timeline = criu_data.get('timeline', []) if criu_data else []
    dirty_timeline = dirty_data.get('dirty_rate_timeline', []) if dirty_data else []
    output['merged_timeline'] = merge_timelines(criu_timeline, dirty_timeline)

    # Calculate correlation metrics
    if criu_data and dirty_data:
        output['correlation_metrics'] = calculate_correlation_metrics(criu_data, dirty_data)

    # Build simulation parameters (aggregated from all sources)
    output['simulation_parameters'] = build_simulation_parameters(
        criu_data, dirty_data, dirty_analysis, metrics
    )

    return output


def build_simulation_parameters(criu_data: Optional[Dict],
                                 dirty_data: Optional[Dict],
                                 dirty_analysis: Optional[Dict],
                                 metrics: Optional[Dict]) -> Dict[str, Any]:
    """
    Build aggregated simulation parameters from all data sources.
    """
    params = {}

    # From dirty analysis (highest priority for dirty-related params)
    if dirty_analysis:
        sim_params = dirty_analysis.get('simulation_parameters', {})
        params['recommended_predump_interval_ms'] = sim_params.get('recommended_predump_interval_ms')
        params['dirty_rate_pages_per_sec'] = sim_params.get('dirty_rate_pages_per_sec')
        params['dirty_pattern_type'] = sim_params.get('dirty_pattern_type')
        params['vma_distribution'] = sim_params.get('vma_distribution')

    # From dirty tracking (fallback for dirty params)
    if dirty_data and not params.get('dirty_rate_pages_per_sec'):
        summary = dirty_data.get('summary', {})
        params['dirty_rate_pages_per_sec'] = summary.get('avg_dirty_rate_per_sec')
        params['peak_dirty_rate'] = summary.get('peak_dirty_rate')
        params['vma_distribution'] = summary.get('vma_distribution')

    # From CRIU logs
    if criu_data:
        summary = criu_data.get('summary', {})
        params['objstor_avg_fetch_ms'] = summary.get('objstor_avg_fetch_ms')
        params['prefetch_hit_rate'] = summary.get('prefetch_hit_rate')
        params['lazy_fault_count'] = summary.get('lazy_fault_count')

    # From experiment metrics
    if metrics:
        params['checkpoint_strategy'] = metrics.get('checkpoint_strategy', {}).get('mode')
        params['transfer_method'] = metrics.get('transfer', {}).get('method')
        params['lazy_mode'] = metrics.get('checkpoint_strategy', {}).get('lazy_mode')

    # Remove None values
    params = {k: v for k, v in params.items() if v is not None}

    return params


def print_summary(output: Dict[str, Any]):
    """Print summary of merged data."""
    print("\n" + "=" * 60)
    print("SIMULATION DATA MERGE SUMMARY")
    print("=" * 60)

    print(f"\nComponents included: {', '.join(output['components'])}")

    if 'criu_events' in output:
        criu = output['criu_events']
        summary = criu.get('summary', {})
        print(f"\nCRIU Events:")
        print(f"  Total events: {summary.get('total_events', 0)}")
        print(f"  Lazy faults: {summary.get('lazy_fault_count', 0)}")
        print(f"  Object storage fetches: {summary.get('objstor_fetch_count', 0)}")
        print(f"  Prefetch hit rate: {summary.get('prefetch_hit_rate', 0):.2%}" if summary.get('prefetch_hit_rate') else "")

    if 'dirty_tracking' in output:
        dirty = output['dirty_tracking']
        summary = dirty.get('summary', {})
        print(f"\nDirty Page Tracking:")
        print(f"  Workload: {dirty.get('workload', 'unknown')}")
        print(f"  Duration: {dirty.get('tracking_duration_ms', 0):.1f} ms")
        print(f"  Unique dirty pages: {summary.get('total_unique_pages', 0)}")
        print(f"  Avg dirty rate: {summary.get('avg_dirty_rate_per_sec', 0):.1f} pages/sec")

    if 'dirty_analysis' in output:
        analysis = output['dirty_analysis']
        rec = analysis.get('predump_recommendation', {})
        print(f"\nPre-dump Recommendation:")
        print(f"  Interval: {rec.get('interval_ms', 0)} ms")
        print(f"  Confidence: {rec.get('confidence', 'unknown')}")

    if 'correlation_metrics' in output:
        corr = output['correlation_metrics']
        if corr:
            print(f"\nCorrelation Metrics:")
            for key, value in corr.items():
                if isinstance(value, float):
                    print(f"  {key}: {value:.2f}")
                else:
                    print(f"  {key}: {value}")

    print(f"\nMerged timeline events: {len(output.get('merged_timeline', []))}")

    print("\nSimulation Parameters:")
    for key, value in output.get('simulation_parameters', {}).items():
        if isinstance(value, float):
            print(f"  {key}: {value:.2f}")
        elif isinstance(value, dict):
            print(f"  {key}: {json.dumps(value)}")
        else:
            print(f"  {key}: {value}")

    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description='Merge CRIU logs and dirty page data for simulation',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument('--criu-logs', '-c', type=str,
                       help='CRIU logs JSON file (from parse_criu_logs.py)')
    parser.add_argument('--dirty-pattern', '-d', type=str,
                       help='Dirty pattern JSON file (from dirty_tracker.py)')
    parser.add_argument('--dirty-analysis', '-a', type=str,
                       help='Dirty analysis JSON file (from analyze_dirty_rate.py)')
    parser.add_argument('--metrics', '-m', type=str,
                       help='Experiment metrics JSON file (from baseline_experiment.py)')
    parser.add_argument('--output', '-o', type=str, required=True,
                       help='Output JSON file')
    parser.add_argument('--quiet', '-q', action='store_true',
                       help='Suppress summary output')
    parser.add_argument('--include-samples', action='store_true',
                       help='Include full dirty page samples in output (can be large)')

    args = parser.parse_args()

    if not any([args.criu_logs, args.dirty_pattern, args.metrics]):
        parser.error('At least one input file must be specified')

    # Load all input files
    criu_data = load_json_file(args.criu_logs) if args.criu_logs else None
    dirty_data = load_json_file(args.dirty_pattern) if args.dirty_pattern else None
    dirty_analysis = load_json_file(args.dirty_analysis) if args.dirty_analysis else None
    metrics = load_json_file(args.metrics) if args.metrics else None

    # Check at least one file loaded
    if not any([criu_data, dirty_data, metrics]):
        print("Error: No input files could be loaded", file=sys.stderr)
        sys.exit(1)

    # Build merged output
    output = build_simulation_input(criu_data, dirty_data, dirty_analysis, metrics)

    # Optionally include full dirty samples
    if args.include_samples and dirty_data:
        output['dirty_tracking']['samples'] = dirty_data.get('samples', [])

    # Write output
    with open(args.output, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"Simulation data written to {args.output}", file=sys.stderr)

    # Print summary
    if not args.quiet:
        print_summary(output)


if __name__ == '__main__':
    main()
