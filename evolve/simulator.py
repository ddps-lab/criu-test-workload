"""
Checkpoint Simulator for evaluating scheduling algorithms.

This module provides a simulation framework that replays historical dirty page
patterns and evaluates checkpoint scheduling algorithms against simulated
preemption events.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from enum import Enum
import json
import numpy as np


class UrgencyLevel(Enum):
    """Spot instance urgency levels"""
    CRITICAL = 'CRITICAL'  # Termination imminent (< 2 min for AWS, 30s for Azure/GCP)
    HIGH = 'HIGH'          # Rebalance warning received (AWS only)
    MEDIUM = 'MEDIUM'      # Low placement score
    LOW = 'LOW'            # Normal operation


@dataclass
class DirtySample:
    """Single dirty page measurement from historical data"""
    timestamp_ms: float
    delta_dirty_count: int

    @classmethod
    def from_dict(cls, d: dict) -> 'DirtySample':
        return cls(
            timestamp_ms=d['timestamp_ms'],
            delta_dirty_count=d['delta_dirty_count'],
        )


@dataclass
class SystemState:
    """Current system state for algorithm decision making"""
    dirty_rate: float           # pages/sec (EWMA smoothed)
    dirty_rate_trend: str       # 'increasing', 'decreasing', 'stable'
    cumulative_dirty: int       # pages since last checkpoint
    time_since_predump: float   # seconds since last predump
    spot_urgency: UrgencyLevel  # current urgency level
    checkpoint_duration_estimate: float  # estimated seconds for checkpoint

    def to_dict(self) -> dict:
        return {
            'dirty_rate': self.dirty_rate,
            'dirty_rate_trend': self.dirty_rate_trend,
            'cumulative_dirty': self.cumulative_dirty,
            'time_since_predump': self.time_since_predump,
            'spot_urgency': self.spot_urgency.value,
            'checkpoint_duration_estimate': self.checkpoint_duration_estimate,
        }


@dataclass
class Decision:
    """Algorithm decision output"""
    action: str       # 'PREDUMP', 'DUMP_NOW', 'WAIT'
    confidence: float = 0.5  # 0.0 - 1.0
    reason: str = ''   # human-readable explanation

    @classmethod
    def predump(cls, confidence: float = 0.8, reason: str = '') -> 'Decision':
        return cls(action='PREDUMP', confidence=confidence, reason=reason)

    @classmethod
    def dump_now(cls, confidence: float = 1.0, reason: str = '') -> 'Decision':
        return cls(action='DUMP_NOW', confidence=confidence, reason=reason)

    @classmethod
    def wait(cls, confidence: float = 0.9, reason: str = '') -> 'Decision':
        return cls(action='WAIT', confidence=confidence, reason=reason)


@dataclass
class Checkpoint:
    """Recorded checkpoint event during simulation"""
    start_time_ms: float
    duration_ms: float
    pages_saved: int
    is_predump: bool  # True=pre-dump, False=full dump
    cumulative_dirty_at_checkpoint: int = 0


@dataclass
class PreemptionEvent:
    """Simulated preemption event"""
    time_ms: float
    warning_time_ms: float  # When warning was issued
    cloud_type: str = 'aws'  # 'aws', 'azure', 'gcp'


@dataclass
class Scenario:
    """Simulation scenario loaded from file"""
    name: str
    source: str
    duration_ms: float
    page_size: int
    dirty_samples: List[DirtySample]
    metadata: dict

    @classmethod
    def from_file(cls, path: str) -> 'Scenario':
        with open(path, 'r') as f:
            data = json.load(f)

        return cls(
            name=data['name'],
            source=data.get('source', path),
            duration_ms=data['duration_ms'],
            page_size=data.get('page_size', 4096),
            dirty_samples=[DirtySample.from_dict(s) for s in data['dirty_samples']],
            metadata=data.get('metadata', {}),
        )


@dataclass
class SimulationConfig:
    """Configuration for checkpoint simulation"""

    # === Checkpoint Duration Model ===
    # duration = base_ms + (pages * per_page_us / 1000)
    checkpoint_base_ms: float = 500.0       # Base overhead (freeze, setup, finalize)
    checkpoint_per_page_us: float = 0.5     # Time per 4KB page (~8 GB/s throughput)
    predump_overhead_ratio: float = 0.8     # Pre-dump is 80% of full dump time

    # === Preemption Model ===
    preemption_mode: str = 'realistic'  # 'random', 'periodic', 'realistic'
    preemption_count: int = 5           # Number of preemptions to simulate
    preemption_min_interval_sec: float = 30.0   # Minimum time between preemptions

    # === Cloud-specific settings ===
    cloud_type: str = 'aws'  # 'aws', 'azure', 'gcp'

    # === Spot Urgency Simulation ===
    rebalance_probability: float = 0.3  # 30% of preemptions have early warning

    # === Random seed for reproducibility ===
    seed: Optional[int] = None

    @property
    def warning_time_sec(self) -> float:
        """Get warning time for current cloud type"""
        times = {
            'aws': 120.0,    # 2 minutes
            'azure': 30.0,   # 30 seconds
            'gcp': 30.0,     # 30 seconds
        }
        return times.get(self.cloud_type, 120.0)


@dataclass
class SimulationResult:
    """Results from running a simulation"""
    success_rate: float              # 0.0 - 1.0
    overhead_ratio: float            # checkpoint_time / total_time
    checkpoint_count: int
    predump_count: int
    dump_count: int
    avg_checkpoint_duration_ms: float
    total_data_loss_pages: int       # pages lost due to failed checkpoints
    preemption_count: int
    preemption_successes: int
    preemption_failures: int
    timeline: List[dict]             # for visualization
    checkpoints: List[Checkpoint]
    preemptions: List[PreemptionEvent]

    def score(self, success_weight: float = 0.7, overhead_weight: float = 0.3) -> float:
        """
        Calculate combined score for optimization.
        Higher is better.
        """
        # Success rate: 0-100 scale
        success_score = self.success_rate * 100

        # Overhead penalty: lower overhead is better
        # 10% overhead → 0 penalty, 50% overhead → 40 penalty
        overhead_penalty = max(0, (self.overhead_ratio - 0.1)) * 100

        return success_score * success_weight - overhead_penalty * overhead_weight

    def summary(self) -> str:
        """Return human-readable summary"""
        lines = [
            f"Success Rate: {self.success_rate*100:.1f}%",
            f"Overhead Ratio: {self.overhead_ratio*100:.1f}%",
            f"Checkpoints: {self.checkpoint_count} (predump: {self.predump_count}, dump: {self.dump_count})",
            f"Avg Checkpoint Duration: {self.avg_checkpoint_duration_ms:.1f}ms",
            f"Preemptions: {self.preemption_count} (success: {self.preemption_successes}, fail: {self.preemption_failures})",
            f"Data Loss: {self.total_data_loss_pages} pages",
            f"Score: {self.score():.2f}",
        ]
        return '\n'.join(lines)


class CheckpointSimulator:
    """
    Simulates checkpoint scheduling against historical dirty patterns.

    The simulator:
    1. Replays dirty page samples from historical data
    2. At each sample, calls the scheduling algorithm to decide action
    3. If checkpoint triggered, simulates checkpoint duration
    4. Generates preemption events and evaluates success/failure
    """

    def __init__(self, scenario: Scenario, config: SimulationConfig):
        self.scenario = scenario
        self.config = config
        self.dirty_samples = scenario.dirty_samples
        self.duration_ms = scenario.duration_ms

        # Set random seed for reproducibility
        if config.seed is not None:
            np.random.seed(config.seed)

        # Pre-generate preemption events
        self.preemption_events = self._generate_preemptions()

        # State tracking (reset on each run)
        self.checkpoints: List[Checkpoint] = []
        self.timeline: List[dict] = []

    def run(self, algorithm: Any) -> SimulationResult:
        """
        Run simulation with the given scheduling algorithm.

        Args:
            algorithm: Object with decide(state: SystemState) -> Decision method

        Returns:
            SimulationResult with metrics and timeline
        """
        self.checkpoints = []
        self.timeline = []

        current_time_ms = 0.0
        last_checkpoint_time_ms = 0.0
        cumulative_dirty = 0

        # EWMA state for dirty rate
        ewma_rate = 0.0
        alpha = 0.3

        # Sliding window for trend detection
        rate_history: List[float] = []

        for i, sample in enumerate(self.dirty_samples):
            current_time_ms = sample.timestamp_ms
            cumulative_dirty += sample.delta_dirty_count

            # Calculate instantaneous dirty rate
            if i > 0:
                dt_sec = (sample.timestamp_ms - self.dirty_samples[i-1].timestamp_ms) / 1000
                if dt_sec > 0:
                    instant_rate = sample.delta_dirty_count / dt_sec
                    ewma_rate = alpha * instant_rate + (1 - alpha) * ewma_rate
                    rate_history.append(ewma_rate)
                    if len(rate_history) > 10:
                        rate_history.pop(0)

            # Determine trend
            trend = self._detect_trend(rate_history)

            # Get simulated spot urgency
            spot_urgency = self._get_urgency_at_time(current_time_ms)

            # Estimate checkpoint duration
            ckpt_duration_estimate = self._estimate_checkpoint_duration(cumulative_dirty)

            # Build state
            state = SystemState(
                dirty_rate=ewma_rate,
                dirty_rate_trend=trend,
                cumulative_dirty=cumulative_dirty,
                time_since_predump=(current_time_ms - last_checkpoint_time_ms) / 1000,
                spot_urgency=spot_urgency,
                checkpoint_duration_estimate=ckpt_duration_estimate / 1000,
            )

            # Get algorithm decision
            decision = algorithm.decide(state)

            # Record timeline event
            self.timeline.append({
                'time_ms': current_time_ms,
                'dirty_rate': ewma_rate,
                'cumulative_dirty': cumulative_dirty,
                'urgency': spot_urgency.value,
                'decision': decision.action,
            })

            # Execute decision
            if decision.action in ('PREDUMP', 'DUMP_NOW'):
                is_predump = (decision.action == 'PREDUMP')
                duration_ms = self._simulate_checkpoint(cumulative_dirty, is_predump)

                self.checkpoints.append(Checkpoint(
                    start_time_ms=current_time_ms,
                    duration_ms=duration_ms,
                    pages_saved=cumulative_dirty,
                    is_predump=is_predump,
                    cumulative_dirty_at_checkpoint=cumulative_dirty,
                ))

                last_checkpoint_time_ms = current_time_ms + duration_ms
                cumulative_dirty = 0

        # Evaluate results
        return self._evaluate()

    def _generate_preemptions(self) -> List[PreemptionEvent]:
        """Generate preemption events based on config"""
        events = []
        duration_ms = self.duration_ms

        if self.config.preemption_mode == 'random':
            # Random preemptions within simulation duration
            min_time = self.config.preemption_min_interval_sec * 1000
            times = np.random.uniform(min_time, duration_ms, self.config.preemption_count)
            times = sorted(times)

        elif self.config.preemption_mode == 'periodic':
            # Evenly spaced preemptions
            interval = duration_ms / (self.config.preemption_count + 1)
            times = [interval * (i + 1) for i in range(self.config.preemption_count)]

        elif self.config.preemption_mode == 'realistic':
            # Exponential distribution (memoryless - realistic for Spot)
            mean_interval = duration_ms / self.config.preemption_count
            times = []
            # Start after warning time + buffer to ensure first preemption has valid warning window
            warning_buffer_ms = self.config.warning_time_sec * 1000 * 1.5
            t = max(self.config.preemption_min_interval_sec * 1000, warning_buffer_ms)
            while len(times) < self.config.preemption_count:
                t += np.random.exponential(mean_interval)
                if t < duration_ms:
                    times.append(t)
                else:
                    break
        else:
            times = []

        warning_sec = self.config.warning_time_sec

        for t in times:
            # Some preemptions have early warning (rebalance for AWS)
            if np.random.random() < self.config.rebalance_probability:
                # Rebalance comes earlier than termination
                warning_time = t - warning_sec * 1000 * np.random.uniform(1, 3)
            else:
                warning_time = t - warning_sec * 1000

            events.append(PreemptionEvent(
                time_ms=t,
                warning_time_ms=max(0, warning_time),
                cloud_type=self.config.cloud_type,
            ))

        return events

    def _get_urgency_at_time(self, time_ms: float) -> UrgencyLevel:
        """Get simulated spot urgency level at given time"""
        warning_sec = self.config.warning_time_sec

        for event in self.preemption_events:
            # Within termination window (warning_sec before termination)
            termination_window_start = max(0, event.time_ms - warning_sec * 1000)
            if termination_window_start <= time_ms < event.time_ms:
                return UrgencyLevel.CRITICAL

            # Within rebalance warning window (before termination window)
            # Only if warning_time is before termination window
            if event.warning_time_ms > 0 and event.warning_time_ms < termination_window_start:
                if event.warning_time_ms <= time_ms < termination_window_start:
                    return UrgencyLevel.HIGH

        return UrgencyLevel.LOW

    def _estimate_checkpoint_duration(self, pages: int) -> float:
        """Estimate checkpoint duration in milliseconds"""
        return (self.config.checkpoint_base_ms +
                pages * self.config.checkpoint_per_page_us / 1000)

    def _simulate_checkpoint(self, pages: int, is_predump: bool) -> float:
        """Simulate checkpoint and return actual duration"""
        base_duration = self._estimate_checkpoint_duration(pages)
        if is_predump:
            base_duration *= self.config.predump_overhead_ratio

        # Add some variance (±10%)
        variance = np.random.uniform(0.9, 1.1)
        return base_duration * variance

    def _detect_trend(self, rate_history: List[float]) -> str:
        """Detect dirty rate trend from recent history"""
        if len(rate_history) < 3:
            return 'stable'

        recent = np.mean(rate_history[-3:])
        older = np.mean(rate_history[:-3]) if len(rate_history) > 3 else recent

        if recent > older * 1.2:
            return 'increasing'
        elif recent < older * 0.8:
            return 'decreasing'
        return 'stable'

    def _evaluate(self) -> SimulationResult:
        """Evaluate checkpoints against preemption events"""
        successes = 0
        failures = 0
        total_data_loss = 0
        total_checkpoint_time = 0.0

        for preemption in self.preemption_events:
            # Find most recent completed checkpoint before preemption
            ready_checkpoint = None
            for ckpt in self.checkpoints:
                ckpt_end = ckpt.start_time_ms + ckpt.duration_ms
                if ckpt_end < preemption.time_ms:
                    ready_checkpoint = ckpt

            if ready_checkpoint:
                successes += 1
            else:
                failures += 1
                # Estimate data loss (all dirty pages since start or last checkpoint)
                total_data_loss += self._estimate_data_loss_at_time(preemption.time_ms)

        for ckpt in self.checkpoints:
            total_checkpoint_time += ckpt.duration_ms

        total_preemptions = len(self.preemption_events)
        success_rate = successes / total_preemptions if total_preemptions > 0 else 1.0
        overhead_ratio = total_checkpoint_time / self.duration_ms if self.duration_ms > 0 else 0

        predump_count = sum(1 for c in self.checkpoints if c.is_predump)
        dump_count = len(self.checkpoints) - predump_count

        return SimulationResult(
            success_rate=success_rate,
            overhead_ratio=overhead_ratio,
            checkpoint_count=len(self.checkpoints),
            predump_count=predump_count,
            dump_count=dump_count,
            avg_checkpoint_duration_ms=total_checkpoint_time / len(self.checkpoints) if self.checkpoints else 0,
            total_data_loss_pages=total_data_loss,
            preemption_count=total_preemptions,
            preemption_successes=successes,
            preemption_failures=failures,
            timeline=self.timeline,
            checkpoints=self.checkpoints,
            preemptions=self.preemption_events,
        )

    def _estimate_data_loss_at_time(self, time_ms: float) -> int:
        """Estimate dirty pages at given time (data that would be lost)"""
        total_dirty = 0
        last_checkpoint_time = 0.0

        # Find last checkpoint before this time
        for ckpt in self.checkpoints:
            if ckpt.start_time_ms + ckpt.duration_ms < time_ms:
                last_checkpoint_time = ckpt.start_time_ms + ckpt.duration_ms

        # Sum dirty pages since last checkpoint
        for sample in self.dirty_samples:
            if last_checkpoint_time < sample.timestamp_ms <= time_ms:
                total_dirty += sample.delta_dirty_count

        return total_dirty


class ScenarioExtender:
    """Extend short scenarios for long-running simulation"""

    def extend(self, scenario: Scenario, mode: str = 'loop',
               loop_count: int = 1, scale_factor: float = 1.0,
               synthetic_duration_sec: float = 3600.0) -> Scenario:
        """
        Extend a scenario to a longer duration.

        Args:
            scenario: Original scenario
            mode: 'loop', 'scale', or 'synthetic'
            loop_count: Number of times to repeat (for loop mode)
            scale_factor: Time scaling factor (for scale mode)
            synthetic_duration_sec: Target duration (for synthetic mode)
        """
        if mode == 'loop':
            return self._loop_extend(scenario, loop_count)
        elif mode == 'scale':
            return self._scale_extend(scenario, scale_factor)
        elif mode == 'synthetic':
            return self._synthetic_extend(scenario, synthetic_duration_sec)
        else:
            return scenario

    def _loop_extend(self, scenario: Scenario, count: int) -> Scenario:
        """Loop: repeat the same pattern N times"""
        if count <= 1:
            return scenario

        extended_samples = []
        base_duration = scenario.duration_ms

        for i in range(count):
            offset = i * base_duration
            for sample in scenario.dirty_samples:
                extended_samples.append(DirtySample(
                    timestamp_ms=sample.timestamp_ms + offset,
                    delta_dirty_count=sample.delta_dirty_count,
                ))

        return Scenario(
            name=f"{scenario.name}_loop{count}",
            source=scenario.source,
            duration_ms=base_duration * count,
            page_size=scenario.page_size,
            dirty_samples=extended_samples,
            metadata={**scenario.metadata, 'extended': True, 'extension_mode': 'loop', 'loop_count': count},
        )

    def _scale_extend(self, scenario: Scenario, factor: float) -> Scenario:
        """Scale: stretch time axis by factor"""
        if factor <= 1.0:
            return scenario

        scaled_samples = []
        for sample in scenario.dirty_samples:
            scaled_samples.append(DirtySample(
                timestamp_ms=sample.timestamp_ms * factor,
                delta_dirty_count=sample.delta_dirty_count,
            ))

        return Scenario(
            name=f"{scenario.name}_scale{factor}",
            source=scenario.source,
            duration_ms=scenario.duration_ms * factor,
            page_size=scenario.page_size,
            dirty_samples=scaled_samples,
            metadata={**scenario.metadata, 'extended': True, 'extension_mode': 'scale', 'scale_factor': factor},
        )

    def _synthetic_extend(self, scenario: Scenario, target_duration_sec: float) -> Scenario:
        """Synthetic: generate new data based on statistics"""
        dirty_counts = [s.delta_dirty_count for s in scenario.dirty_samples]
        mean_rate = np.mean(dirty_counts)
        std_rate = np.std(dirty_counts)

        # Calculate sample interval from original data
        intervals = []
        for i in range(1, len(scenario.dirty_samples)):
            intervals.append(scenario.dirty_samples[i].timestamp_ms - scenario.dirty_samples[i-1].timestamp_ms)
        avg_interval = np.mean(intervals) if intervals else 10.0

        # Generate synthetic sequence
        target_samples = int(target_duration_sec * 1000 / avg_interval)
        synthetic_counts = np.random.normal(mean_rate, std_rate, target_samples)
        synthetic_counts = np.maximum(0, synthetic_counts).astype(int)

        synthetic_samples = [
            DirtySample(
                timestamp_ms=i * avg_interval,
                delta_dirty_count=int(count),
            )
            for i, count in enumerate(synthetic_counts)
        ]

        return Scenario(
            name=f"{scenario.name}_synthetic_{target_duration_sec}s",
            source=scenario.source,
            duration_ms=target_duration_sec * 1000,
            page_size=scenario.page_size,
            dirty_samples=synthetic_samples,
            metadata={**scenario.metadata, 'extended': True, 'extension_mode': 'synthetic', 'target_duration_sec': target_duration_sec},
        )
