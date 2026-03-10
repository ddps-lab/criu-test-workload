#!/usr/bin/env python3
"""
Run checkpoint scheduling simulations.

This script runs simulations with different algorithms and scenarios,
useful for testing and debugging before running full evolution.
"""

import argparse
import json
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from evolve.simulator import Scenario, CheckpointSimulator, SimulationConfig, ScenarioExtender
from evolve.algorithm import SchedulingAlgorithm, YoungDalyAlgorithm, FixedIntervalAlgorithm, AdaptiveAlgorithm, get_algorithm
from evolve.evaluator import CheckpointEvaluator, GridSearchEvaluator


def run_single_simulation(args):
    """Run simulation on a single scenario"""
    # Load scenario
    scenario = Scenario.from_file(args.scenario)
    print(f"Loaded scenario: {scenario.name}")
    print(f"  Duration: {scenario.duration_ms/1000:.1f}s")
    print(f"  Samples: {len(scenario.dirty_samples)}")
    print(f"  Avg dirty rate: {scenario.metadata.get('avg_dirty_rate_pages_per_sec', 0):.0f} pages/sec")

    # Extend scenario if requested
    if args.extend:
        extender = ScenarioExtender()
        if args.extend_mode == 'loop':
            scenario = extender.extend(scenario, mode='loop', loop_count=args.extend_factor)
        elif args.extend_mode == 'scale':
            scenario = extender.extend(scenario, mode='scale', scale_factor=args.extend_factor)
        elif args.extend_mode == 'synthetic':
            scenario = extender.extend(scenario, mode='synthetic',
                                       synthetic_duration_sec=args.extend_factor * 60)
        print(f"\nExtended scenario ({args.extend_mode} x{args.extend_factor}):")
        print(f"  Duration: {scenario.duration_ms/1000:.1f}s")
        print(f"  Samples: {len(scenario.dirty_samples)}")

    # Create simulation config
    config = SimulationConfig(
        preemption_mode=args.preemption_mode,
        preemption_count=args.preemption_count,
        cloud_type=args.cloud,
        seed=args.seed,
    )

    # Create algorithm
    algorithm = get_algorithm(args.algorithm, **_parse_algorithm_params(args.params))
    print(f"\nAlgorithm: {algorithm}")

    # Run simulation
    simulator = CheckpointSimulator(scenario, config)
    result = simulator.run(algorithm)

    # Print results
    print(f"\n{'='*60}")
    print("SIMULATION RESULTS")
    print('='*60)
    print(result.summary())
    print('='*60)

    # Save results if requested
    if args.output:
        output_data = {
            'scenario': scenario.name,
            'algorithm': str(algorithm),
            'config': {
                'preemption_mode': config.preemption_mode,
                'preemption_count': config.preemption_count,
                'cloud_type': config.cloud_type,
            },
            'results': {
                'success_rate': result.success_rate,
                'overhead_ratio': result.overhead_ratio,
                'checkpoint_count': result.checkpoint_count,
                'predump_count': result.predump_count,
                'dump_count': result.dump_count,
                'avg_checkpoint_duration_ms': result.avg_checkpoint_duration_ms,
                'total_data_loss_pages': result.total_data_loss_pages,
                'score': result.score(),
            },
        }
        with open(args.output, 'w') as f:
            json.dump(output_data, f, indent=2)
        print(f"\nResults saved to: {args.output}")

    return result


def run_comparison(args):
    """Compare multiple algorithms"""
    print(f"Comparing algorithms on scenarios in: {args.scenarios_dir}")

    config = SimulationConfig(
        preemption_mode=args.preemption_mode,
        preemption_count=args.preemption_count,
        cloud_type=args.cloud,
        seed=args.seed,
    )

    evaluator = CheckpointEvaluator(
        args.scenarios_dir,
        simulation_config=config,
        extend_scenarios=args.extend,
        extension_mode=args.extend_mode,
        extension_factor=args.extend_factor,
    )

    # Create algorithms to compare
    algorithms = {
        'SchedulingAlgorithm': SchedulingAlgorithm(),
        'YoungDaly (MTBF=300s)': YoungDalyAlgorithm(mtbf_sec=300.0),
        'YoungDaly (MTBF=60s)': YoungDalyAlgorithm(mtbf_sec=60.0),
        'Fixed (30s)': FixedIntervalAlgorithm(interval_sec=30.0),
        'Fixed (10s)': FixedIntervalAlgorithm(interval_sec=10.0),
        'Adaptive': AdaptiveAlgorithm(),
    }

    results = evaluator.compare_algorithms(algorithms, verbose=True)
    return results


def run_grid_search(args):
    """Run grid search over algorithm parameters"""
    print(f"Running grid search on scenarios in: {args.scenarios_dir}")

    config = SimulationConfig(
        preemption_mode=args.preemption_mode,
        preemption_count=args.preemption_count,
        cloud_type=args.cloud,
        seed=args.seed,
    )

    evaluator = CheckpointEvaluator(
        args.scenarios_dir,
        simulation_config=config,
        extend_scenarios=args.extend,
        extension_mode=args.extend_mode,
        extension_factor=args.extend_factor,
    )

    grid_search = GridSearchEvaluator(evaluator)

    # Define parameter grid
    param_grid = {
        'BASE_INTERVAL_SEC': [10.0, 20.0, 30.0, 60.0],
        'DIRTY_RATE_THRESHOLD': [5000.0, 10000.0, 50000.0, 100000.0],
        'MAX_CUMULATIVE_DIRTY': [100000, 500000, 1000000],
    }

    results = grid_search.search(SchedulingAlgorithm, param_grid, verbose=True)

    # Save results
    if args.output:
        output_data = [
            {
                'params': r['params'],
                'score': r['score'],
                'success_rate': r['result'].avg_success_rate,
                'overhead_ratio': r['result'].avg_overhead_ratio,
            }
            for r in results
        ]
        with open(args.output, 'w') as f:
            json.dump(output_data, f, indent=2)
        print(f"\nResults saved to: {args.output}")

    return results


def _parse_algorithm_params(params_str: str) -> dict:
    """Parse algorithm parameters from string"""
    if not params_str:
        return {}

    params = {}
    for item in params_str.split(','):
        key, value = item.split('=')
        # Try to convert to appropriate type
        try:
            value = float(value)
            if value == int(value):
                value = int(value)
        except ValueError:
            pass
        params[key] = value

    return params


def main():
    parser = argparse.ArgumentParser(
        description='Run checkpoint scheduling simulations',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run single simulation
  python run_simulation.py --scenario scenarios/ml_training.json

  # Compare algorithms
  python run_simulation.py --compare --scenarios-dir scenarios/

  # Grid search
  python run_simulation.py --grid-search --scenarios-dir scenarios/

  # With scenario extension
  python run_simulation.py --compare --scenarios-dir scenarios/ --extend --extend-mode loop --extend-factor 60
        """
    )

    # Mode selection
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--scenario', type=str, help='Path to scenario file (single simulation mode)')
    mode_group.add_argument('--compare', action='store_true', help='Compare multiple algorithms')
    mode_group.add_argument('--grid-search', action='store_true', help='Run grid search')

    # Common options
    parser.add_argument('--scenarios-dir', type=str, default='scenarios/',
                        help='Directory containing scenario files (for compare/grid-search)')
    parser.add_argument('--algorithm', type=str, default='scheduling',
                        choices=['scheduling', 'young_daly', 'fixed', 'adaptive'],
                        help='Algorithm to use (single simulation mode)')
    parser.add_argument('--params', type=str, default='',
                        help='Algorithm parameters as key=value,... (e.g., interval_sec=30)')
    parser.add_argument('--output', '-o', type=str, help='Output file for results')

    # Simulation config
    parser.add_argument('--preemption-mode', type=str, default='realistic',
                        choices=['random', 'periodic', 'realistic'],
                        help='Preemption generation mode')
    parser.add_argument('--preemption-count', type=int, default=5,
                        help='Number of preemptions to simulate')
    parser.add_argument('--cloud', type=str, default='aws',
                        choices=['aws', 'azure', 'gcp'],
                        help='Cloud provider for warning times')
    parser.add_argument('--seed', type=int, help='Random seed for reproducibility')

    # Scenario extension
    parser.add_argument('--extend', action='store_true',
                        help='Extend scenarios for longer simulation')
    parser.add_argument('--extend-mode', type=str, default='loop',
                        choices=['loop', 'scale', 'synthetic'],
                        help='Scenario extension mode')
    parser.add_argument('--extend-factor', type=int, default=60,
                        help='Extension factor (loop count or scale factor)')

    args = parser.parse_args()

    # Determine mode and run
    if args.scenario:
        run_single_simulation(args)
    elif args.compare:
        run_comparison(args)
    elif args.grid_search:
        run_grid_search(args)
    else:
        # Default: run comparison
        args.compare = True
        run_comparison(args)


if __name__ == '__main__':
    main()
