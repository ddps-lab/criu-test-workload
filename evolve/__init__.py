"""
Checkpoint Scheduling Evolution Framework.

This package provides tools for simulating and evolving checkpoint scheduling
algorithms for spot instance environments.

Main components:
- simulator: CheckpointSimulator for replaying dirty page patterns
- algorithm: Scheduling algorithms including the evolvable SchedulingAlgorithm
- evaluator: Multi-scenario evaluation and comparison

Usage:
    from evolve.simulator import Scenario, CheckpointSimulator, SimulationConfig
    from evolve.algorithm import SchedulingAlgorithm
    from evolve.evaluator import CheckpointEvaluator
"""

from .simulator import (
    Scenario,
    CheckpointSimulator,
    SimulationConfig,
    SimulationResult,
    ScenarioExtender,
    UrgencyLevel,
    SystemState,
    Decision,
)

from .algorithm import (
    SchedulingAlgorithm,
    YoungDalyAlgorithm,
    FixedIntervalAlgorithm,
    AdaptiveAlgorithm,
    get_algorithm,
    ALGORITHMS,
)

from .evaluator import (
    CheckpointEvaluator,
    EvaluationResult,
    GridSearchEvaluator,
)

__all__ = [
    # Simulator
    'Scenario',
    'CheckpointSimulator',
    'SimulationConfig',
    'SimulationResult',
    'ScenarioExtender',
    'UrgencyLevel',
    'SystemState',
    'Decision',
    # Algorithms
    'SchedulingAlgorithm',
    'YoungDalyAlgorithm',
    'FixedIntervalAlgorithm',
    'AdaptiveAlgorithm',
    'get_algorithm',
    'ALGORITHMS',
    # Evaluator
    'CheckpointEvaluator',
    'EvaluationResult',
    'GridSearchEvaluator',
]
