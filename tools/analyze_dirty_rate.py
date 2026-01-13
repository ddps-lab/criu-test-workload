#!/usr/bin/env python3
"""
Dirty Rate Analysis Tool

Analyzes dirty page tracking data to recommend optimal pre-dump intervals
for CRIU checkpoint/restore operations.

Usage:
    # Analyze dirty pattern file
    python3 analyze_dirty_rate.py --input dirty_pattern.json

    # Generate recommendations
    python3 analyze_dirty_rate.py --input dirty_pattern.json --recommend

    # Export analysis
    python3 analyze_dirty_rate.py --input dirty_pattern.json --output analysis.json
"""

import argparse
import json
import sys
import math
from typing import Dict, Any, List, Tuple, Optional
from dataclasses import dataclass


@dataclass
class PredumpRecommendation:
    """Pre-dump interval recommendation."""
    interval_ms: int
    expected_dirty_pages: int
    expected_dirty_size_mb: float
    efficiency_pct: float
    confidence: str  # 'high', 'medium', 'low'
    rationale: str


def load_dirty_pattern(filepath: str) -> Dict[str, Any]:
    """Load dirty pattern from JSON file."""
    with open(filepath, 'r') as f:
        return json.load(f)


def calculate_dirty_rates(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Calculate dirty rates from timeline data.

    Returns list of rate entries with timestamp and rate.
    """
    timeline = data.get('dirty_rate_timeline', [])
    if not timeline:
        return []

    return timeline


def detect_rate_pattern(rates: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Detect dirty rate pattern characteristics.

    Returns pattern info:
    - type: 'constant', 'increasing', 'decreasing', 'periodic', 'bursty'
    - stability: coefficient of variation (lower = more stable)
    - trend: rate of change per second
    """
    if len(rates) < 3:
        return {'type': 'unknown', 'stability': 0, 'trend': 0}

    rate_values = [r['rate_pages_per_sec'] for r in rates if r['rate_pages_per_sec'] > 0]

    if not rate_values:
        return {'type': 'zero', 'stability': 0, 'trend': 0}

    # Calculate statistics
    mean_rate = sum(rate_values) / len(rate_values)
    variance = sum((r - mean_rate) ** 2 for r in rate_values) / len(rate_values)
    std_dev = math.sqrt(variance) if variance > 0 else 0
    cv = std_dev / mean_rate if mean_rate > 0 else 0  # Coefficient of variation

    # Calculate trend (linear regression slope)
    n = len(rate_values)
    if n > 1:
        x_mean = (n - 1) / 2
        y_mean = mean_rate
        numerator = sum((i - x_mean) * (rate_values[i] - y_mean) for i in range(n))
        denominator = sum((i - x_mean) ** 2 for i in range(n))
        trend = numerator / denominator if denominator > 0 else 0
    else:
        trend = 0

    # Detect pattern type
    if cv < 0.2:
        pattern_type = 'constant'
    elif abs(trend) > mean_rate * 0.01:  # Significant trend
        pattern_type = 'increasing' if trend > 0 else 'decreasing'
    elif cv > 0.8:
        pattern_type = 'bursty'
    else:
        pattern_type = 'variable'

    return {
        'type': pattern_type,
        'stability': 1 - min(cv, 1.0),  # 0-1 scale, higher = more stable
        'trend': trend,
        'mean_rate': mean_rate,
        'std_dev': std_dev,
        'cv': cv
    }


def find_rate_stabilization_point(rates: List[Dict[str, Any]]) -> Optional[float]:
    """
    Find the point where dirty rate stabilizes.

    This helps identify when a workload reaches steady state.

    Returns timestamp_ms where rate stabilizes, or None if not found.
    """
    if len(rates) < 5:
        return None

    rate_values = [r['rate_pages_per_sec'] for r in rates]

    # Use rolling window to detect stabilization
    window_size = min(5, len(rate_values) // 3)
    if window_size < 2:
        return None

    for i in range(window_size, len(rate_values) - window_size):
        # Compare variance of current window vs next window
        current_window = rate_values[i - window_size:i]
        next_window = rate_values[i:i + window_size]

        current_var = sum((r - sum(current_window) / len(current_window)) ** 2 for r in current_window)
        next_var = sum((r - sum(next_window) / len(next_window)) ** 2 for r in next_window)

        # If variance decreases significantly, rate is stabilizing
        if current_var > 0 and next_var / current_var < 0.5:
            return rates[i]['timestamp_ms']

    return None


def calculate_optimal_predump_interval(data: Dict[str, Any]) -> PredumpRecommendation:
    """
    Calculate optimal pre-dump interval based on dirty rate analysis.

    Methodology:
    1. Analyze dirty rate pattern (constant, increasing, etc.)
    2. Find rate stabilization point
    3. Calculate interval that balances:
       - Pre-dump overhead (shorter = more overhead)
       - Dirty page accumulation (longer = more pages to dump)

    Returns PredumpRecommendation with suggested interval.
    """
    summary = data.get('summary', {})
    rates = data.get('dirty_rate_timeline', [])

    avg_rate = summary.get('avg_dirty_rate_per_sec', 0)
    peak_rate = summary.get('peak_dirty_rate', 0)
    page_size = data.get('page_size', 4096)

    # Analyze rate pattern
    pattern = detect_rate_pattern(rates)
    stabilization_point = find_rate_stabilization_point(rates)

    # Default parameters
    target_dirty_pages = 1000  # Target dirty pages per pre-dump
    min_interval_ms = 500      # Minimum interval (avoid too frequent pre-dumps)
    max_interval_ms = 10000    # Maximum interval (avoid too large deltas)

    # Calculate base interval
    if avg_rate > 0:
        base_interval_ms = (target_dirty_pages / avg_rate) * 1000
    else:
        base_interval_ms = 5000  # Default 5 seconds if no rate data

    # Adjust based on pattern
    if pattern['type'] == 'constant':
        # Stable rate: use calculated interval
        recommended_interval = base_interval_ms
        confidence = 'high'
        rationale = f"Constant dirty rate ({avg_rate:.1f} pages/sec) allows predictable pre-dump scheduling."

    elif pattern['type'] == 'increasing':
        # Increasing rate: use shorter interval to avoid runaway dirty pages
        recommended_interval = base_interval_ms * 0.7
        confidence = 'medium'
        rationale = f"Increasing dirty rate (trend: {pattern['trend']:.2f}/sample). Shorter intervals recommended."

    elif pattern['type'] == 'decreasing':
        # Decreasing rate: use longer interval
        recommended_interval = base_interval_ms * 1.3
        confidence = 'medium'
        rationale = f"Decreasing dirty rate. Longer intervals are safe."

    elif pattern['type'] == 'bursty':
        # Bursty: use conservative (shorter) interval based on peak rate
        if peak_rate > 0:
            recommended_interval = (target_dirty_pages / peak_rate) * 1000
        else:
            recommended_interval = min_interval_ms
        confidence = 'low'
        rationale = f"Bursty dirty pattern (CV: {pattern['cv']:.2f}). Using peak-based interval for safety."

    else:
        # Unknown/variable: use moderate interval
        recommended_interval = base_interval_ms
        confidence = 'low'
        rationale = f"Variable dirty rate pattern. Using average-based interval."

    # Apply bounds
    recommended_interval = max(min_interval_ms, min(max_interval_ms, recommended_interval))

    # Calculate expected metrics at recommended interval
    expected_dirty_pages = int(avg_rate * (recommended_interval / 1000))
    expected_dirty_size_mb = (expected_dirty_pages * page_size) / (1024 * 1024)

    # Calculate efficiency (percentage of total pages that would be pre-dumped vs re-dumped)
    total_unique = summary.get('total_unique_pages', 0)
    if total_unique > 0:
        efficiency_pct = min(100, (1 - expected_dirty_pages / total_unique) * 100)
    else:
        efficiency_pct = 0

    return PredumpRecommendation(
        interval_ms=int(recommended_interval),
        expected_dirty_pages=expected_dirty_pages,
        expected_dirty_size_mb=expected_dirty_size_mb,
        efficiency_pct=efficiency_pct,
        confidence=confidence,
        rationale=rationale
    )


def analyze_vma_distribution(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Analyze VMA (Virtual Memory Area) distribution of dirty pages.

    Returns insights about which memory regions are most active.
    """
    summary = data.get('summary', {})
    vma_dist = summary.get('vma_distribution', {})
    vma_sizes = summary.get('vma_size_distribution', {})

    if not vma_dist:
        return {'dominant_vma': 'unknown', 'insights': []}

    # Find dominant VMA
    dominant_vma = max(vma_dist.items(), key=lambda x: x[1])[0] if vma_dist else 'unknown'

    # Generate insights
    insights = []

    heap_pct = vma_dist.get('heap', 0) * 100
    if heap_pct > 50:
        insights.append(f"Heap-dominant ({heap_pct:.1f}%): Workload allocates significant dynamic memory.")

    anon_pct = vma_dist.get('anonymous', 0) * 100
    if anon_pct > 30:
        insights.append(f"Anonymous memory ({anon_pct:.1f}%): Significant mmap'd memory usage.")

    stack_pct = vma_dist.get('stack', 0) * 100
    if stack_pct > 10:
        insights.append(f"Stack activity ({stack_pct:.1f}%): High function call depth or large stack allocations.")

    data_pct = vma_dist.get('data', 0) * 100
    if data_pct > 20:
        insights.append(f"Data segment ({data_pct:.1f}%): Frequent global/static variable modifications.")

    return {
        'dominant_vma': dominant_vma,
        'distribution': vma_dist,
        'size_distribution_bytes': vma_sizes,
        'insights': insights
    }


def generate_analysis_report(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate comprehensive dirty rate analysis report.

    Returns report with:
    - Pattern analysis
    - VMA distribution
    - Pre-dump recommendations
    - Simulation parameters
    """
    summary = data.get('summary', {})
    rates = data.get('dirty_rate_timeline', [])

    # Rate pattern analysis
    pattern = detect_rate_pattern(rates)
    stabilization = find_rate_stabilization_point(rates)

    # Pre-dump recommendation
    recommendation = calculate_optimal_predump_interval(data)

    # VMA analysis
    vma_analysis = analyze_vma_distribution(data)

    # Build report
    report = {
        'workload': data.get('workload', 'unknown'),
        'pid': data.get('pid', 0),
        'tracking_duration_ms': data.get('tracking_duration_ms', 0),

        'dirty_rate_analysis': {
            'pattern_type': pattern['type'],
            'stability': pattern['stability'],
            'trend_per_sample': pattern.get('trend', 0),
            'mean_rate_pages_per_sec': pattern.get('mean_rate', 0),
            'std_dev': pattern.get('std_dev', 0),
            'coefficient_of_variation': pattern.get('cv', 0),
            'stabilization_point_ms': stabilization
        },

        'vma_analysis': vma_analysis,

        'predump_recommendation': {
            'interval_ms': recommendation.interval_ms,
            'expected_dirty_pages': recommendation.expected_dirty_pages,
            'expected_dirty_size_mb': recommendation.expected_dirty_size_mb,
            'efficiency_pct': recommendation.efficiency_pct,
            'confidence': recommendation.confidence,
            'rationale': recommendation.rationale
        },

        'summary_stats': {
            'total_unique_pages': summary.get('total_unique_pages', 0),
            'total_dirty_events': summary.get('total_dirty_events', 0),
            'total_dirty_size_mb': summary.get('total_dirty_size_bytes', 0) / (1024 * 1024),
            'avg_dirty_rate': summary.get('avg_dirty_rate_per_sec', 0),
            'peak_dirty_rate': summary.get('peak_dirty_rate', 0),
            'sample_count': summary.get('sample_count', 0),
            'interval_ms': summary.get('interval_ms', 0)
        },

        'simulation_parameters': {
            'recommended_predump_interval_ms': recommendation.interval_ms,
            'dirty_rate_pages_per_sec': pattern.get('mean_rate', 0),
            'dirty_pattern_type': pattern['type'],
            'vma_distribution': vma_analysis.get('distribution', {})
        }
    }

    return report


def print_analysis_summary(report: Dict[str, Any]):
    """Print human-readable analysis summary."""
    print("\n" + "=" * 60)
    print("DIRTY PAGE ANALYSIS REPORT")
    print("=" * 60)

    print(f"\nWorkload: {report['workload']}")
    print(f"PID: {report['pid']}")
    print(f"Duration: {report['tracking_duration_ms']:.1f} ms")

    print("\n--- Rate Analysis ---")
    rate_analysis = report['dirty_rate_analysis']
    print(f"  Pattern type: {rate_analysis['pattern_type']}")
    print(f"  Stability: {rate_analysis['stability']:.2f} (0=unstable, 1=stable)")
    print(f"  Mean rate: {rate_analysis['mean_rate_pages_per_sec']:.1f} pages/sec")
    print(f"  Std deviation: {rate_analysis['std_dev']:.1f}")
    if rate_analysis['stabilization_point_ms']:
        print(f"  Stabilization point: {rate_analysis['stabilization_point_ms']:.1f} ms")

    print("\n--- VMA Distribution ---")
    vma = report['vma_analysis']
    print(f"  Dominant VMA: {vma['dominant_vma']}")
    for vma_type, pct in vma.get('distribution', {}).items():
        print(f"    {vma_type}: {pct * 100:.1f}%")

    if vma.get('insights'):
        print("\n  Insights:")
        for insight in vma['insights']:
            print(f"    - {insight}")

    print("\n--- Pre-dump Recommendation ---")
    rec = report['predump_recommendation']
    print(f"  Recommended interval: {rec['interval_ms']} ms")
    print(f"  Expected dirty pages: {rec['expected_dirty_pages']}")
    print(f"  Expected dirty size: {rec['expected_dirty_size_mb']:.2f} MB")
    print(f"  Pre-dump efficiency: {rec['efficiency_pct']:.1f}%")
    print(f"  Confidence: {rec['confidence']}")
    print(f"\n  Rationale: {rec['rationale']}")

    print("\n--- Summary Statistics ---")
    stats = report['summary_stats']
    print(f"  Unique dirty pages: {stats['total_unique_pages']}")
    print(f"  Total dirty events: {stats['total_dirty_events']}")
    print(f"  Total dirty size: {stats['total_dirty_size_mb']:.2f} MB")
    print(f"  Avg dirty rate: {stats['avg_dirty_rate']:.1f} pages/sec")
    print(f"  Peak dirty rate: {stats['peak_dirty_rate']:.1f} pages/sec")

    print("\n" + "=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description='Analyze dirty page tracking data for CRIU pre-dump optimization',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze dirty pattern and print summary
  python3 analyze_dirty_rate.py --input dirty_pattern.json

  # Export analysis to JSON
  python3 analyze_dirty_rate.py --input dirty_pattern.json --output analysis.json

  # Get just the recommendation
  python3 analyze_dirty_rate.py --input dirty_pattern.json --recommend
"""
    )

    parser.add_argument('--input', '-i', type=str, required=True,
                        help='Input dirty pattern JSON file')
    parser.add_argument('--output', '-o', type=str, default=None,
                        help='Output analysis JSON file')
    parser.add_argument('--recommend', '-r', action='store_true',
                        help='Print only the pre-dump interval recommendation')
    parser.add_argument('--quiet', '-q', action='store_true',
                        help='Suppress summary output (only with --output)')

    args = parser.parse_args()

    # Load data
    try:
        data = load_dirty_pattern(args.input)
    except (OSError, json.JSONDecodeError) as e:
        print(f"Error loading {args.input}: {e}", file=sys.stderr)
        sys.exit(1)

    # Generate analysis
    report = generate_analysis_report(data)

    # Output
    if args.recommend:
        rec = report['predump_recommendation']
        print(f"Recommended pre-dump interval: {rec['interval_ms']} ms")
        print(f"Expected dirty pages: {rec['expected_dirty_pages']}")
        print(f"Confidence: {rec['confidence']}")
    else:
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(report, f, indent=2)
            print(f"Analysis written to {args.output}", file=sys.stderr)

        if not args.quiet:
            print_analysis_summary(report)


if __name__ == '__main__':
    main()
