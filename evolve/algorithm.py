"""
Scheduling Algorithms for Checkpoint Decision Making.

This module contains scheduling algorithms that can be evolved by OpenEvolve.
The algorithms decide when to trigger pre-dump or full dump based on
system state including dirty rate, urgency level, and time since last checkpoint.

The SchedulingAlgorithm class is the primary evolution target.
"""

from dataclasses import dataclass
from typing import Optional
import math

# Import from simulator to avoid circular imports
from .simulator import SystemState, Decision, UrgencyLevel


class SchedulingAlgorithm:
    """
    Evolvable pre-dump scheduling algorithm.

    This class is the target for OpenEvolve optimization.
    OpenEvolve will modify the parameters and logic to optimize:
    - Checkpoint success rate under preemption
    - Total checkpoint overhead (minimize unnecessary pre-dumps)

    The decide() method takes the current system state and returns
    a Decision (PREDUMP, DUMP_NOW, or WAIT).
    """

    # ============================================================
    # TUNABLE PARAMETERS - These are evolved by OpenEvolve
    # ============================================================

    # Base checkpoint interval in seconds (when urgency is LOW)
    BASE_INTERVAL_SEC: float = 30.0

    # Dirty rate threshold (pages/sec) for rate-based adjustment
    DIRTY_RATE_THRESHOLD: float = 10000.0

    # Maximum cumulative dirty pages before forcing checkpoint
    MAX_CUMULATIVE_DIRTY: int = 500000

    # Urgency multipliers - multiply base interval by these factors
    URGENCY_MULTIPLIERS = {
        'CRITICAL': 0.0,   # immediate dump
        'HIGH': 0.3,       # 30% of base interval
        'MEDIUM': 0.7,     # 70% of base interval
        'LOW': 1.0,        # full base interval
    }

    # Minimum interval between checkpoints (seconds)
    MIN_INTERVAL_SEC: float = 5.0

    # ============================================================
    # DECISION LOGIC
    # ============================================================

    def decide(self, state: SystemState) -> Decision:
        """
        Make checkpoint decision based on current system state.

        Args:
            state: Current system state with dirty rate, urgency, etc.

        Returns:
            Decision with action (PREDUMP, DUMP_NOW, or WAIT)
        """
        # Emergency: termination imminent
        if state.spot_urgency == UrgencyLevel.CRITICAL:
            # Only dump if we have significant dirty data since last checkpoint
            # Skip if we just checkpointed (cumulative_dirty is small)
            min_dirty_for_dump = 1000  # Minimum dirty pages to justify emergency dump
            if state.cumulative_dirty >= min_dirty_for_dump:
                return Decision.dump_now(
                    confidence=1.0,
                    reason=f'Termination imminent with {state.cumulative_dirty} dirty pages'
                )
            # Already checkpointed recently, wait
            return Decision.wait(
                confidence=0.9,
                reason=f'CRITICAL but recently checkpointed (cumul={state.cumulative_dirty})'
            )

        # Calculate adjusted interval based on urgency
        urgency_key = state.spot_urgency.value
        urgency_mult = self.URGENCY_MULTIPLIERS.get(urgency_key, 1.0)
        adjusted_interval = self.BASE_INTERVAL_SEC * urgency_mult

        # Adjust by dirty rate (higher rate = more frequent checkpoints)
        if state.dirty_rate > self.DIRTY_RATE_THRESHOLD:
            rate_factor = min(state.dirty_rate / self.DIRTY_RATE_THRESHOLD, 3.0)
            adjusted_interval /= rate_factor

        # Ensure minimum interval
        adjusted_interval = max(adjusted_interval, self.MIN_INTERVAL_SEC)

        # Force checkpoint if cumulative dirty is too high
        if state.cumulative_dirty >= self.MAX_CUMULATIVE_DIRTY:
            return Decision.predump(
                confidence=0.9,
                reason=f'Cumulative dirty ({state.cumulative_dirty}) exceeds threshold'
            )

        # Time-based checkpoint
        if state.time_since_predump >= adjusted_interval:
            return Decision.predump(
                confidence=0.8,
                reason=f'Interval elapsed ({state.time_since_predump:.1f}s >= {adjusted_interval:.1f}s)'
            )

        # High urgency but not time yet - consider preemptive checkpoint
        if state.spot_urgency == UrgencyLevel.HIGH and state.time_since_predump >= adjusted_interval * 0.5:
            return Decision.predump(
                confidence=0.7,
                reason='HIGH urgency with significant time elapsed'
            )

        return Decision.wait(
            confidence=0.9,
            reason=f'Waiting ({state.time_since_predump:.1f}s < {adjusted_interval:.1f}s)'
        )

    def __str__(self) -> str:
        return (f"SchedulingAlgorithm(BASE_INTERVAL={self.BASE_INTERVAL_SEC}s, "
                f"DIRTY_THRESHOLD={self.DIRTY_RATE_THRESHOLD}, "
                f"MAX_CUMULATIVE={self.MAX_CUMULATIVE_DIRTY})")


class YoungDalyAlgorithm:
    """
    Classical Young/Daly checkpoint interval algorithm.

    Based on the formula: τ_opt = √(2 * C * M)
    where:
      - C = checkpoint duration
      - M = mean time between failures (MTBF)

    This serves as a baseline for comparison.
    """

    def __init__(self, mtbf_sec: float = 300.0, checkpoint_overhead_sec: float = 2.0):
        """
        Initialize Young/Daly algorithm.

        Args:
            mtbf_sec: Mean time between failures in seconds
            checkpoint_overhead_sec: Checkpoint duration in seconds
        """
        self.mtbf_sec = mtbf_sec
        self.checkpoint_overhead_sec = checkpoint_overhead_sec
        self._optimal_interval = math.sqrt(2 * checkpoint_overhead_sec * mtbf_sec)

    @property
    def optimal_interval(self) -> float:
        """Calculate optimal checkpoint interval using Young's formula"""
        return self._optimal_interval

    def decide(self, state: SystemState) -> Decision:
        """Make checkpoint decision based on Young/Daly formula"""
        # Emergency override
        if state.spot_urgency == UrgencyLevel.CRITICAL:
            if state.cumulative_dirty >= 1000:
                return Decision.dump_now(
                    confidence=1.0,
                    reason=f'Termination imminent with {state.cumulative_dirty} dirty pages'
                )
            return Decision.wait(confidence=0.9, reason='CRITICAL but recently checkpointed')

        # Update interval based on observed checkpoint duration
        observed_overhead = state.checkpoint_duration_estimate
        if observed_overhead > 0:
            self._optimal_interval = math.sqrt(2 * observed_overhead * self.mtbf_sec)

        if state.time_since_predump >= self._optimal_interval:
            return Decision.predump(
                confidence=0.8,
                reason=f'Young/Daly interval ({self._optimal_interval:.1f}s) elapsed'
            )

        return Decision.wait(
            confidence=0.9,
            reason=f'Waiting ({state.time_since_predump:.1f}s < {self._optimal_interval:.1f}s)'
        )

    def __str__(self) -> str:
        return f"YoungDalyAlgorithm(MTBF={self.mtbf_sec}s, optimal_interval={self._optimal_interval:.1f}s)"


class FixedIntervalAlgorithm:
    """
    Simple fixed interval checkpoint algorithm.

    Checkpoints at regular intervals regardless of system state.
    Used as a baseline for comparison.
    """

    def __init__(self, interval_sec: float = 30.0):
        """
        Initialize fixed interval algorithm.

        Args:
            interval_sec: Checkpoint interval in seconds
        """
        self.interval_sec = interval_sec

    def decide(self, state: SystemState) -> Decision:
        """Make checkpoint decision based on fixed interval"""
        # Emergency override
        if state.spot_urgency == UrgencyLevel.CRITICAL:
            if state.cumulative_dirty >= 1000:
                return Decision.dump_now(
                    confidence=1.0,
                    reason=f'Termination imminent with {state.cumulative_dirty} dirty pages'
                )
            return Decision.wait(confidence=0.9, reason='CRITICAL but recently checkpointed')

        if state.time_since_predump >= self.interval_sec:
            return Decision.predump(
                confidence=0.8,
                reason=f'Fixed interval ({self.interval_sec}s) elapsed'
            )

        return Decision.wait(
            confidence=0.9,
            reason=f'Waiting ({state.time_since_predump:.1f}s < {self.interval_sec}s)'
        )

    def __str__(self) -> str:
        return f"FixedIntervalAlgorithm(interval={self.interval_sec}s)"


class AdaptiveAlgorithm:
    """
    Adaptive algorithm that adjusts based on observed dirty rate patterns.

    This algorithm learns from the dirty rate history and adapts its
    checkpoint interval accordingly.
    """

    def __init__(self,
                 base_interval_sec: float = 30.0,
                 min_interval_sec: float = 5.0,
                 max_interval_sec: float = 120.0,
                 dirty_rate_ema_alpha: float = 0.3):
        """
        Initialize adaptive algorithm.

        Args:
            base_interval_sec: Starting checkpoint interval
            min_interval_sec: Minimum allowed interval
            max_interval_sec: Maximum allowed interval
            dirty_rate_ema_alpha: EMA smoothing factor for dirty rate
        """
        self.base_interval_sec = base_interval_sec
        self.min_interval_sec = min_interval_sec
        self.max_interval_sec = max_interval_sec
        self.dirty_rate_ema_alpha = dirty_rate_ema_alpha

        # Internal state
        self._avg_dirty_rate = 0.0
        self._checkpoint_count = 0
        self._current_interval = base_interval_sec

    def decide(self, state: SystemState) -> Decision:
        """Make adaptive checkpoint decision"""
        # Emergency override
        if state.spot_urgency == UrgencyLevel.CRITICAL:
            if state.cumulative_dirty >= 1000:
                return Decision.dump_now(
                    confidence=1.0,
                    reason=f'Termination imminent with {state.cumulative_dirty} dirty pages'
                )
            return Decision.wait(confidence=0.9, reason='CRITICAL but recently checkpointed')

        # Update dirty rate estimate
        self._avg_dirty_rate = (self.dirty_rate_ema_alpha * state.dirty_rate +
                                (1 - self.dirty_rate_ema_alpha) * self._avg_dirty_rate)

        # Adapt interval based on dirty rate
        # Higher dirty rate = shorter interval
        if self._avg_dirty_rate > 0:
            # Target: checkpoint when ~100K dirty pages accumulated
            target_dirty = 100000
            estimated_interval = target_dirty / self._avg_dirty_rate
            self._current_interval = max(self.min_interval_sec,
                                         min(self.max_interval_sec, estimated_interval))
        else:
            self._current_interval = self.base_interval_sec

        # Apply urgency modifier
        urgency_modifiers = {
            UrgencyLevel.LOW: 1.0,
            UrgencyLevel.MEDIUM: 0.7,
            UrgencyLevel.HIGH: 0.3,
            UrgencyLevel.CRITICAL: 0.0,
        }
        adjusted_interval = self._current_interval * urgency_modifiers.get(state.spot_urgency, 1.0)
        adjusted_interval = max(self.min_interval_sec, adjusted_interval)

        if state.time_since_predump >= adjusted_interval:
            self._checkpoint_count += 1
            return Decision.predump(
                confidence=0.8,
                reason=f'Adaptive interval ({adjusted_interval:.1f}s) elapsed'
            )

        return Decision.wait(
            confidence=0.9,
            reason=f'Waiting ({state.time_since_predump:.1f}s < {adjusted_interval:.1f}s)'
        )

    def __str__(self) -> str:
        return (f"AdaptiveAlgorithm(base={self.base_interval_sec}s, "
                f"current={self._current_interval:.1f}s, "
                f"avg_dirty_rate={self._avg_dirty_rate:.0f}/s)")


# Dictionary of available algorithms for easy selection
ALGORITHMS = {
    'scheduling': SchedulingAlgorithm,
    'young_daly': YoungDalyAlgorithm,
    'fixed': FixedIntervalAlgorithm,
    'adaptive': AdaptiveAlgorithm,
}


def get_algorithm(name: str, **kwargs) -> object:
    """
    Get an algorithm instance by name.

    Args:
        name: Algorithm name ('scheduling', 'young_daly', 'fixed', 'adaptive')
        **kwargs: Algorithm-specific parameters

    Returns:
        Algorithm instance
    """
    if name not in ALGORITHMS:
        raise ValueError(f"Unknown algorithm: {name}. Available: {list(ALGORITHMS.keys())}")

    return ALGORITHMS[name](**kwargs)
