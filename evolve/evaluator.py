"""
Checkpoint Evaluator for Algorithm Evolution.

This module provides the evaluation framework for OpenEvolve integration.
It evaluates scheduling algorithms against multiple scenarios and computes
multi-objective fitness scores.
"""

import json
import glob
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional
import numpy as np

from .simulator import Scenario, CheckpointSimulator, SimulationConfig, SimulationResult, ScenarioExtender
from .algorithm import SchedulingAlgorithm, ALGORITHMS


@dataclass
class EvaluationResult:
    """Results from evaluating an algorithm across all scenarios"""
    algorithm_name: str
    avg_success_rate: float
    avg_overhead_ratio: float
    combined_score: float
    scenario_results: Dict[str, Dict[str, float]]
    total_checkpoints: int
    total_preemptions: int


class CheckpointEvaluator:
    """
    Evaluates scheduling algorithms against historical dirty page scenarios.

    Used by OpenEvolve to compute fitness scores for evolved algorithms.
    """

    def __init__(self,
                 scenarios_dir: str,
                 simulation_config: Optional[SimulationConfig] = None,
                 success_weight: float = 0.7,
                 overhead_weight: float = 0.3,
                 extend_scenarios: bool = False,
                 extension_mode: str = 'loop',
                 extension_factor: int = 60):
        """
        Initialize evaluator.

        Args:
            scenarios_dir: Directory containing scenario JSON files
            simulation_config: Configuration for simulations
            success_weight: Weight for success rate in score (0-1)
            overhead_weight: Weight for overhead ratio in score (0-1)
            extend_scenarios: Whether to extend scenarios for longer simulations
            extension_mode: 'loop', 'scale', or 'synthetic'
            extension_factor: Loop count or scale factor for extension
        """
        self.scenarios_dir = scenarios_dir
        self.simulation_config = simulation_config or SimulationConfig()
        self.success_weight = success_weight
        self.overhead_weight = overhead_weight
        self.extend_scenarios = extend_scenarios
        self.extension_mode = extension_mode
        self.extension_factor = extension_factor

        # Load scenarios
        self.scenarios = self._load_scenarios()
        self.extender = ScenarioExtender()

    def _load_scenarios(self) -> List[Scenario]:
        """Load all scenario files from directory"""
        scenarios = []
        pattern = f"{self.scenarios_dir}/*.json"

        for path in glob.glob(pattern):
            # Skip the converter script
            if 'convert' in path:
                continue
            try:
                scenario = Scenario.from_file(path)
                scenarios.append(scenario)
            except Exception as e:
                print(f"Warning: Failed to load scenario {path}: {e}")

        return scenarios

    def evaluate(self, algorithm: Any, verbose: bool = False) -> EvaluationResult:
        """
        Evaluate an algorithm against all scenarios.

        Args:
            algorithm: Algorithm with decide(state) -> Decision method
            verbose: Print progress

        Returns:
            EvaluationResult with metrics
        """
        scenario_results = {}
        all_success_rates = []
        all_overhead_ratios = []
        total_checkpoints = 0
        total_preemptions = 0

        for scenario in self.scenarios:
            # Optionally extend scenario
            if self.extend_scenarios:
                if self.extension_mode == 'loop':
                    scenario = self.extender.extend(scenario, mode='loop', loop_count=self.extension_factor)
                elif self.extension_mode == 'scale':
                    scenario = self.extender.extend(scenario, mode='scale', scale_factor=self.extension_factor)
                elif self.extension_mode == 'synthetic':
                    scenario = self.extender.extend(scenario, mode='synthetic',
                                                    synthetic_duration_sec=self.extension_factor * 60)

            # Run simulation
            simulator = CheckpointSimulator(scenario, self.simulation_config)
            result = simulator.run(algorithm)

            if verbose:
                print(f"  {scenario.name}: success={result.success_rate*100:.1f}%, "
                      f"overhead={result.overhead_ratio*100:.1f}%, "
                      f"checkpoints={result.checkpoint_count}")

            # Record results
            scenario_results[scenario.name] = {
                'success_rate': result.success_rate,
                'overhead_ratio': result.overhead_ratio,
                'checkpoint_count': result.checkpoint_count,
                'preemption_count': result.preemption_count,
                'score': result.score(self.success_weight, self.overhead_weight),
            }

            all_success_rates.append(result.success_rate)
            all_overhead_ratios.append(result.overhead_ratio)
            total_checkpoints += result.checkpoint_count
            total_preemptions += result.preemption_count

        # Calculate averages
        avg_success = np.mean(all_success_rates) if all_success_rates else 0
        avg_overhead = np.mean(all_overhead_ratios) if all_overhead_ratios else 0

        # Calculate combined score
        success_score = avg_success * 100
        overhead_penalty = max(0, (avg_overhead - 0.1)) * 100
        combined_score = success_score * self.success_weight - overhead_penalty * self.overhead_weight

        return EvaluationResult(
            algorithm_name=str(algorithm),
            avg_success_rate=avg_success,
            avg_overhead_ratio=avg_overhead,
            combined_score=combined_score,
            scenario_results=scenario_results,
            total_checkpoints=total_checkpoints,
            total_preemptions=total_preemptions,
        )

    def compare_algorithms(self,
                          algorithms: Dict[str, Any],
                          verbose: bool = True) -> Dict[str, EvaluationResult]:
        """
        Compare multiple algorithms.

        Args:
            algorithms: Dict of {name: algorithm_instance}
            verbose: Print results

        Returns:
            Dict of {name: EvaluationResult}
        """
        results = {}

        for name, algorithm in algorithms.items():
            if verbose:
                print(f"\nEvaluating: {name}")
            result = self.evaluate(algorithm, verbose=verbose)
            results[name] = result

        if verbose:
            self._print_comparison(results)

        return results

    def _print_comparison(self, results: Dict[str, EvaluationResult]):
        """Print comparison table"""
        print("\n" + "=" * 80)
        print("ALGORITHM COMPARISON")
        print("=" * 80)
        print(f"{'Algorithm':<30} {'Success %':<12} {'Overhead %':<12} {'Score':<10} {'Checkpoints':<12}")
        print("-" * 80)

        sorted_results = sorted(results.items(), key=lambda x: x[1].combined_score, reverse=True)

        for name, result in sorted_results:
            print(f"{name:<30} {result.avg_success_rate*100:>10.1f}% "
                  f"{result.avg_overhead_ratio*100:>10.1f}% "
                  f"{result.combined_score:>8.2f} "
                  f"{result.total_checkpoints:>10}")

        print("=" * 80)


class GridSearchEvaluator:
    """
    Grid search over algorithm parameters.

    Useful for finding good parameter combinations before evolution.
    """

    def __init__(self, evaluator: CheckpointEvaluator):
        """
        Initialize grid search.

        Args:
            evaluator: Base evaluator to use
        """
        self.evaluator = evaluator

    def search(self,
               algorithm_class: type,
               param_grid: Dict[str, List[Any]],
               verbose: bool = True) -> List[Dict[str, Any]]:
        """
        Perform grid search over parameters.

        Args:
            algorithm_class: Algorithm class to instantiate
            param_grid: Dict of {param_name: [values]}
            verbose: Print progress

        Returns:
            List of {params, result} sorted by score
        """
        from itertools import product

        # Generate all parameter combinations
        param_names = list(param_grid.keys())
        param_values = list(param_grid.values())
        combinations = list(product(*param_values))

        results = []

        for i, combo in enumerate(combinations):
            params = dict(zip(param_names, combo))

            if verbose:
                print(f"\nTesting {i+1}/{len(combinations)}: {params}")

            # Create algorithm with these parameters
            algorithm = algorithm_class()
            for name, value in params.items():
                setattr(algorithm, name, value)

            # Evaluate
            result = self.evaluator.evaluate(algorithm, verbose=False)

            results.append({
                'params': params,
                'result': result,
                'score': result.combined_score,
            })

            if verbose:
                print(f"  Score: {result.combined_score:.2f} "
                      f"(success={result.avg_success_rate*100:.1f}%, "
                      f"overhead={result.avg_overhead_ratio*100:.1f}%)")

        # Sort by score
        results.sort(key=lambda x: x['score'], reverse=True)

        if verbose:
            print("\n" + "=" * 80)
            print("TOP 5 PARAMETER COMBINATIONS")
            print("=" * 80)
            for i, r in enumerate(results[:5]):
                print(f"{i+1}. Score={r['score']:.2f}: {r['params']}")

        return results


def evaluate_algorithm_code(algorithm_code: str,
                           scenarios_dir: str,
                           config: Optional[SimulationConfig] = None) -> EvaluationResult:
    """
    Evaluate algorithm from Python code string.

    Used by OpenEvolve to evaluate evolved algorithm code.

    Args:
        algorithm_code: Python code defining the algorithm class
        scenarios_dir: Directory containing scenarios
        config: Simulation configuration

    Returns:
        EvaluationResult
    """
    # Execute the code to define the algorithm class
    local_vars = {}
    exec(algorithm_code, {'SystemState': None, 'Decision': None, 'UrgencyLevel': None}, local_vars)

    # Find the algorithm class (should be SchedulingAlgorithm or similar)
    algorithm_class = None
    for name, obj in local_vars.items():
        if isinstance(obj, type) and hasattr(obj, 'decide'):
            algorithm_class = obj
            break

    if algorithm_class is None:
        raise ValueError("No algorithm class found in code")

    # Create instance and evaluate
    algorithm = algorithm_class()
    evaluator = CheckpointEvaluator(scenarios_dir, config)
    return evaluator.evaluate(algorithm)
