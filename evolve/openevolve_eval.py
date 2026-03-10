"""
OpenEvolve Evaluator for Checkpoint Scheduling Algorithm.

This module provides the `evaluate` function required by OpenEvolve.
It evaluates evolved algorithm code by running simulations against
historical dirty page patterns.
"""

import os
import sys
import tempfile
import importlib.util
import traceback
from pathlib import Path
from typing import Dict, Any, Optional

# Add parent directory to path for imports
_current_dir = Path(__file__).parent
_parent_dir = _current_dir.parent
if str(_parent_dir) not in sys.path:
    sys.path.insert(0, str(_parent_dir))

from evolve.simulator import (
    Scenario,
    CheckpointSimulator,
    SimulationConfig,
    ScenarioExtender,
)
from evolve.evaluator import CheckpointEvaluator


# Configuration for evaluation
SCENARIOS_DIR = _current_dir / "scenarios"
SEED = 42

# Simulation settings
SIMULATION_CONFIG = SimulationConfig(
    preemption_mode='realistic',
    preemption_count=5,
    cloud_type='aws',
    seed=SEED,
)

# Scenario extension for longer simulation
EXTEND_SCENARIOS = True
EXTENSION_MODE = 'loop'
EXTENSION_FACTOR = 30  # 30x extension (~15 minutes per scenario)


def load_algorithm_from_code(code: str) -> Optional[Any]:
    """
    Dynamically load the SchedulingAlgorithm class from evolved code.

    Args:
        code: Python code containing SchedulingAlgorithm class

    Returns:
        SchedulingAlgorithm instance or None if loading fails
    """
    try:
        # Create a temporary module
        spec = importlib.util.spec_from_loader(
            "evolved_algorithm",
            loader=None,
            origin="evolved_code"
        )
        module = importlib.util.module_from_spec(spec)

        # Import dependencies that the evolved code needs
        module.__dict__['math'] = __import__('math')
        module.__dict__['dataclass'] = __import__('dataclasses').dataclass
        module.__dict__['Optional'] = __import__('typing').Optional

        # Import simulator types
        from evolve.simulator import SystemState, Decision, UrgencyLevel
        module.__dict__['SystemState'] = SystemState
        module.__dict__['Decision'] = Decision
        module.__dict__['UrgencyLevel'] = UrgencyLevel

        # Execute the evolved code
        exec(code, module.__dict__)

        # Get the SchedulingAlgorithm class
        if 'SchedulingAlgorithm' not in module.__dict__:
            print("Error: SchedulingAlgorithm class not found in evolved code")
            return None

        # Instantiate the algorithm
        algorithm = module.__dict__['SchedulingAlgorithm']()
        return algorithm

    except Exception as e:
        print(f"Error loading algorithm from code: {e}")
        traceback.print_exc()
        return None


def evaluate(code: str) -> Dict[str, float]:
    """
    Evaluate the evolved checkpoint scheduling algorithm.

    This is the main entry point called by OpenEvolve.

    Args:
        code: Python code containing the SchedulingAlgorithm class

    Returns:
        Dictionary of metrics:
        - success_rate: Checkpoint success rate (0-1)
        - overhead_ratio: Checkpoint overhead ratio (0-1)
        - combined_score: Weighted combination for optimization
    """
    # Default failure metrics
    failure_metrics = {
        'success_rate': 0.0,
        'overhead_ratio': 1.0,
        'combined_score': -100.0,
        'error': 1.0,
    }

    try:
        # Load the evolved algorithm
        algorithm = load_algorithm_from_code(code)
        if algorithm is None:
            print("Failed to load algorithm")
            return failure_metrics

        # Load scenarios
        if not SCENARIOS_DIR.exists():
            print(f"Scenarios directory not found: {SCENARIOS_DIR}")
            return failure_metrics

        scenario_files = list(SCENARIOS_DIR.glob("*.json"))
        if not scenario_files:
            print("No scenario files found")
            return failure_metrics

        # Run simulations
        results = []
        for scenario_file in scenario_files:
            try:
                scenario = Scenario.from_file(str(scenario_file))

                # Extend scenario if configured
                if EXTEND_SCENARIOS:
                    extender = ScenarioExtender()
                    scenario = extender.extend(
                        scenario,
                        mode=EXTENSION_MODE,
                        loop_count=EXTENSION_FACTOR if EXTENSION_MODE == 'loop' else 1,
                        scale_factor=EXTENSION_FACTOR if EXTENSION_MODE == 'scale' else 1.0,
                    )

                # Run simulation
                simulator = CheckpointSimulator(scenario, SIMULATION_CONFIG)
                result = simulator.run(algorithm)
                results.append(result)

            except Exception as e:
                print(f"Error simulating {scenario_file.name}: {e}")
                continue

        if not results:
            print("No successful simulations")
            return failure_metrics

        # Aggregate results
        avg_success_rate = sum(r.success_rate for r in results) / len(results)
        avg_overhead_ratio = sum(r.overhead_ratio for r in results) / len(results)

        # Calculate combined score
        # Success rate is more important (weight 0.7)
        # Lower overhead is better (penalty)
        success_weight = 0.7
        overhead_weight = 0.3

        # Success contribution: 0-100 scale
        success_score = avg_success_rate * 100

        # Overhead penalty: lower is better
        # 10% overhead = 0 penalty, 50% overhead = 40 penalty
        overhead_penalty = max(0, (avg_overhead_ratio - 0.1)) * 100

        combined_score = success_score * success_weight - overhead_penalty * overhead_weight

        metrics = {
            'success_rate': avg_success_rate,
            'overhead_ratio': avg_overhead_ratio,
            'combined_score': combined_score,
            'checkpoint_count': sum(r.checkpoint_count for r in results),
            'scenarios_evaluated': len(results),
            'error': 0.0,
        }

        print(f"Evaluation complete: success={avg_success_rate:.2%}, "
              f"overhead={avg_overhead_ratio:.2%}, score={combined_score:.2f}")

        return metrics

    except Exception as e:
        print(f"Evaluation error: {e}")
        traceback.print_exc()
        failure_metrics['error_message'] = str(e)
        return failure_metrics


def evaluate_stage1(code: str) -> Dict[str, float]:
    """
    Stage 1 evaluation: Quick syntax and basic functionality check.

    This is an optional cascade evaluation stage for faster filtering.
    """
    try:
        # Quick syntax check
        compile(code, '<string>', 'exec')

        # Try to load the algorithm
        algorithm = load_algorithm_from_code(code)
        if algorithm is None:
            return {'stage1_passed': 0.0, 'combined_score': -100.0}

        # Check that decide method exists and is callable
        if not hasattr(algorithm, 'decide') or not callable(algorithm.decide):
            return {'stage1_passed': 0.0, 'combined_score': -100.0}

        return {'stage1_passed': 1.0, 'combined_score': 0.0}

    except SyntaxError as e:
        return {'stage1_passed': 0.0, 'combined_score': -100.0, 'syntax_error': str(e)}
    except Exception as e:
        return {'stage1_passed': 0.0, 'combined_score': -100.0, 'error': str(e)}


def evaluate_stage2(code: str) -> Dict[str, float]:
    """
    Stage 2 evaluation: Single scenario quick test.

    Run on a single small scenario for quick filtering.
    """
    try:
        algorithm = load_algorithm_from_code(code)
        if algorithm is None:
            return {'stage2_passed': 0.0, 'combined_score': -100.0}

        # Find the smallest scenario
        scenario_files = list(SCENARIOS_DIR.glob("*.json"))
        if not scenario_files:
            return {'stage2_passed': 0.0, 'combined_score': -100.0}

        # Use redis scenario (smallest)
        test_scenario = None
        for sf in scenario_files:
            if 'redis' in sf.name.lower():
                test_scenario = Scenario.from_file(str(sf))
                break

        if test_scenario is None:
            test_scenario = Scenario.from_file(str(scenario_files[0]))

        # Run quick simulation (no extension)
        config = SimulationConfig(
            preemption_mode='periodic',
            preemption_count=2,
            seed=SEED,
        )
        simulator = CheckpointSimulator(test_scenario, config)
        result = simulator.run(algorithm)

        # Basic sanity check
        if result.success_rate >= 0.5:
            return {
                'stage2_passed': 1.0,
                'combined_score': result.score(),
                'quick_success_rate': result.success_rate,
            }
        else:
            return {
                'stage2_passed': 0.5,
                'combined_score': result.score(),
                'quick_success_rate': result.success_rate,
            }

    except Exception as e:
        return {'stage2_passed': 0.0, 'combined_score': -100.0, 'error': str(e)}


# For testing
if __name__ == "__main__":
    # Test with the current algorithm
    from evolve.algorithm import SchedulingAlgorithm
    import inspect

    # Get the source code of the algorithm module
    from evolve import algorithm as algo_module
    code = inspect.getsource(algo_module)

    print("Testing evaluate function with current algorithm...")
    print("=" * 60)

    metrics = evaluate(code)

    print("\nMetrics:")
    for key, value in metrics.items():
        if isinstance(value, float):
            print(f"  {key}: {value:.4f}")
        else:
            print(f"  {key}: {value}")
