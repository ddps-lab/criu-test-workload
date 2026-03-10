#!/usr/bin/env python3
"""
Jupyter Notebook Pipeline Simulation Standalone Workload

This script simulates a data science notebook session executing a sequential
analysis pipeline. Each phase depends on the previous phase's output,
representing a typical data science workflow where intermediate results
accumulate in memory.

Usage:
    python3 jupyter_standalone.py --duration 3600 --working_dir /tmp/jupyter

Checkpoint Protocol:
    1. Creates 'checkpoint_ready' file after initial setup
    2. Checks 'checkpoint_flag' to detect restore completion
    3. Exits gracefully when checkpoint_flag is removed

Pipeline Phases (duration proportionally distributed):
    1. data_generation  (10%): Generate and accumulate raw data chunks
    2. data_cleaning    (15%): Normalize, remove outliers
    3. feature_engineering (20%): Compute PCA via covariance eigenvectors
    4. model_training   (35%): Linear regression via gradient descent
    5. evaluation       (20%): Compute MSE, R², cross-validation

State accumulated in memory (lost on restart):
    - raw_data: concatenated data chunks
    - cleaned_data: normalized, outlier-removed dataset
    - features: PCA-transformed features + eigenvectors
    - model_weights: gradient descent learned parameters
    - evaluation_metrics: MSE, R², CV scores history

Scenario:
    - Interactive Jupyter data science sessions
    - Research computing environments
    - ML experiment notebooks
    - Educational computing labs
"""

import time
import os
import sys
import argparse
import math

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False


def create_ready_signal(working_dir: str = '.'):
    """Create checkpoint ready signal file."""
    ready_path = os.path.join(working_dir, 'checkpoint_ready')
    with open(ready_path, 'w') as f:
        f.write(f'ready:{os.getpid()}\n')
    print(f"[Jupyter] Checkpoint ready signal created (PID: {os.getpid()})")


def check_restore_complete(working_dir: str = '.') -> bool:
    """Check if restore is complete (checkpoint_flag removed)."""
    flag_path = os.path.join(working_dir, 'checkpoint_flag')
    return not os.path.exists(flag_path)


class PipelineState:
    """
    Sequential data science pipeline with 5 phases.

    Each phase depends on the previous phase's output. All intermediate
    results are stored in-memory and lost on restart.

    Phases:
        1. data_generation: Accumulate raw data chunks
        2. data_cleaning: Normalize and remove outliers
        3. feature_engineering: PCA via covariance matrix eigenvectors
        4. model_training: Linear regression via gradient descent
        5. evaluation: MSE, R², cross-validation metrics
    """

    PHASES = ['data_generation', 'data_cleaning', 'feature_engineering',
              'model_training', 'evaluation']
    PHASE_WEIGHTS = [0.10, 0.15, 0.20, 0.35, 0.20]

    def __init__(self, num_features: int = 20, chunk_size: int = 500):
        self.num_features = num_features
        self.chunk_size = chunk_size

        # Cycle tracking (pipeline repeats with new data each cycle)
        self.cycle = 0
        self.current_phase_idx = 0
        self.phase_step = 0  # steps within current phase
        self.cells_executed = 0

        # Phase 1: Data generation state
        self.raw_chunks = []
        self.raw_data = None
        self.total_rows = 0
        self.total_rows_all_cycles = 0

        # Phase 2: Data cleaning state
        self.cleaned_data = None
        self.col_means = None
        self.col_stds = None
        self.outliers_removed = 0

        # Phase 3: Feature engineering state
        self.cov_matrix = None
        self.eigenvalues = None
        self.eigenvectors = None
        self.n_components = 0
        self.features = None
        self.explained_variance_ratio = None

        # Phase 4: Model training state (PERSISTS across cycles - transfer learning)
        self.weights = None
        self.bias = 0.0
        self.target = None
        self.learning_rate = 0.01
        self.train_losses = []
        self.gradient_norms = []
        self.epochs_completed = 0

        # Phase 5: Evaluation state (PERSISTS across cycles - tracks improvement)
        self.mse_history = []
        self.r2_history = []
        self.cv_scores = []
        self.best_mse = float('inf')
        self.best_r2 = float('-inf')

    @property
    def current_phase(self) -> str:
        if self.current_phase_idx < len(self.PHASES):
            return self.PHASES[self.current_phase_idx]
        return 'complete'

    def get_phase_duration(self, total_duration: int, phase_idx: int) -> float:
        """Get duration allocated for a specific phase."""
        return total_duration * self.PHASE_WEIGHTS[phase_idx]

    def step(self) -> dict:
        """Execute one step of the current phase. Returns step info."""
        self.cells_executed += 1
        self.phase_step += 1

        if self.current_phase_idx == 0:
            return self._step_data_generation()
        elif self.current_phase_idx == 1:
            return self._step_data_cleaning()
        elif self.current_phase_idx == 2:
            return self._step_feature_engineering()
        elif self.current_phase_idx == 3:
            return self._step_model_training()
        elif self.current_phase_idx == 4:
            return self._step_evaluation()
        else:
            return {'phase': 'complete', 'output': 'Pipeline complete, idle'}

    def advance_phase(self):
        """Move to the next phase, cycling back after evaluation."""
        old_phase = self.current_phase
        if self.current_phase_idx < len(self.PHASES) - 1:
            self.current_phase_idx += 1
            self.phase_step = 0
            print(f"[Jupyter] Phase transition: {old_phase} -> {self.current_phase}")
            return True
        else:
            # Cycle back: start new cycle with fresh data, keep model weights
            self._start_new_cycle()
            print(f"[Jupyter] Phase transition: {old_phase} -> {self.current_phase} (cycle {self.cycle})")
            return True

    def _start_new_cycle(self):
        """Start a new pipeline cycle with fresh data but preserved model.

        Simulates a data scientist getting new batch of data and retraining
        the existing model (transfer learning / incremental learning).
        Model weights and evaluation history carry over.
        """
        self.cycle += 1
        self.current_phase_idx = 0
        self.phase_step = 0

        # Reset per-cycle data state (new data each cycle)
        self.raw_chunks = []
        self.raw_data = None
        self.total_rows = 0
        self.cleaned_data = None
        self.col_means = None
        self.col_stds = None

        # Reset PCA (needs recomputation for new data)
        self.cov_matrix = None
        self.eigenvalues = None
        self.eigenvectors = None
        self.features = None
        self.explained_variance_ratio = None

        # Keep model weights (transfer learning) but reset target
        # Weights will be reused as warm start for the new data
        self.target = None

    def _step_data_generation(self) -> dict:
        """Generate a chunk of raw data (different distribution each cycle)."""
        if HAS_NUMPY:
            # Generate data with cycle-dependent distribution shift
            chunk = np.random.randn(self.chunk_size, self.num_features).astype(np.float64)
            # Add trends and correlations (shift with cycle for new patterns)
            drift = self.cycle * 0.5
            chunk[:, 0] += self.total_rows / 1000.0 + drift
            if self.num_features >= 3:
                chunk[:, 1] += 0.5 * chunk[:, 0] + np.random.randn(self.chunk_size) * (0.3 + self.cycle * 0.1)
                chunk[:, 2] = 0.3 * chunk[:, 0] - 0.2 * chunk[:, 1] + np.random.randn(self.chunk_size) * 0.5
            self.raw_chunks.append(chunk)
            self.total_rows += self.chunk_size
            self.total_rows_all_cycles += self.chunk_size
        else:
            import random
            chunk = [[random.gauss(self.cycle * 0.5, 1) for _ in range(self.num_features)]
                     for _ in range(self.chunk_size)]
            self.raw_chunks.append(chunk)
            self.total_rows += self.chunk_size
            self.total_rows_all_cycles += self.chunk_size

        cycle_str = f" (cycle {self.cycle})" if self.cycle > 0 else ""
        return {
            'phase': 'data_generation',
            'output': f'Chunk {len(self.raw_chunks)}: {self.chunk_size} rows (this cycle: {self.total_rows:,}, all: {self.total_rows_all_cycles:,}){cycle_str}'
        }

    def _step_data_cleaning(self) -> dict:
        """Clean data: normalize and remove outliers."""
        if self.cleaned_data is None:
            # First step: concatenate raw data
            if HAS_NUMPY:
                self.raw_data = np.vstack(self.raw_chunks)
                self.col_means = np.mean(self.raw_data, axis=0)
                self.col_stds = np.std(self.raw_data, axis=0) + 1e-8
                self.cleaned_data = (self.raw_data - self.col_means) / self.col_stds
                return {
                    'phase': 'data_cleaning',
                    'output': f'Normalized {self.raw_data.shape[0]:,} rows x {self.raw_data.shape[1]} cols'
                }
            else:
                flat = []
                for chunk in self.raw_chunks:
                    flat.extend(chunk)
                self.raw_data = flat
                self.cleaned_data = flat  # simplified
                return {
                    'phase': 'data_cleaning',
                    'output': f'Normalized {len(flat):,} rows'
                }

        # Subsequent steps: iterative outlier removal (z-score based)
        if HAS_NUMPY:
            z_scores = np.abs(self.cleaned_data)
            threshold = 3.0 - self.phase_step * 0.1  # progressively stricter
            threshold = max(2.0, threshold)
            mask = np.all(z_scores < threshold, axis=1)
            removed = np.sum(~mask)
            if removed > 0:
                self.cleaned_data = self.cleaned_data[mask]
                self.outliers_removed += int(removed)
            return {
                'phase': 'data_cleaning',
                'output': f'Outlier pass (z>{threshold:.1f}): removed {removed}, total removed: {self.outliers_removed}, remaining: {self.cleaned_data.shape[0]:,}'
            }
        else:
            return {
                'phase': 'data_cleaning',
                'output': f'Outlier removal step {self.phase_step}'
            }

    def _step_feature_engineering(self) -> dict:
        """PCA via covariance matrix eigenvectors."""
        if self.cov_matrix is None:
            # Compute covariance matrix
            if HAS_NUMPY:
                self.cov_matrix = np.cov(self.cleaned_data, rowvar=False)
                return {
                    'phase': 'feature_engineering',
                    'output': f'Computed covariance matrix ({self.num_features}x{self.num_features})'
                }
            else:
                self.cov_matrix = [[0.0] * self.num_features for _ in range(self.num_features)]
                return {
                    'phase': 'feature_engineering',
                    'output': 'Computed covariance matrix'
                }

        if self.eigenvalues is None:
            # Eigendecomposition
            if HAS_NUMPY:
                self.eigenvalues, self.eigenvectors = np.linalg.eigh(self.cov_matrix)
                # Sort by descending eigenvalue
                idx = np.argsort(self.eigenvalues)[::-1]
                self.eigenvalues = self.eigenvalues[idx]
                self.eigenvectors = self.eigenvectors[:, idx]
                # Explained variance ratio
                total_var = np.sum(self.eigenvalues)
                self.explained_variance_ratio = self.eigenvalues / total_var
                # Select components explaining 95% variance
                cumvar = np.cumsum(self.explained_variance_ratio)
                self.n_components = int(np.searchsorted(cumvar, 0.95)) + 1
                self.n_components = min(self.n_components, self.num_features)
                return {
                    'phase': 'feature_engineering',
                    'output': f'Eigendecomposition: {self.n_components} components explain 95% variance'
                }
            else:
                self.eigenvalues = [1.0] * self.num_features
                self.n_components = min(5, self.num_features)
                return {
                    'phase': 'feature_engineering',
                    'output': f'PCA: {self.n_components} components selected'
                }

        if self.features is None:
            # Project data onto principal components
            if HAS_NUMPY:
                W = self.eigenvectors[:, :self.n_components]
                self.features = self.cleaned_data @ W
                return {
                    'phase': 'feature_engineering',
                    'output': f'Projected data: {self.features.shape[0]:,} x {self.features.shape[1]} features'
                }
            else:
                self.features = self.cleaned_data
                return {
                    'phase': 'feature_engineering',
                    'output': 'Projected data to features'
                }

        # Additional feature analysis steps
        if HAS_NUMPY:
            feat_means = np.mean(self.features, axis=0)
            feat_stds = np.std(self.features, axis=0)
            return {
                'phase': 'feature_engineering',
                'output': f'Feature stats: mean_norm={np.linalg.norm(feat_means):.4f}, std_range=[{feat_stds.min():.4f}, {feat_stds.max():.4f}]'
            }
        return {'phase': 'feature_engineering', 'output': 'Feature analysis step'}

    def _step_model_training(self) -> dict:
        """Linear regression via gradient descent (warm-start across cycles)."""
        if self.target is None:
            # Generate target for current data. Weights may already exist
            # from a previous cycle (warm start / transfer learning).
            if HAS_NUMPY:
                n_features = self.features.shape[1]
                # Generate target: linear combination + noise
                np.random.seed(42 + self.cycle)
                true_weights = np.random.randn(n_features) * 0.5
                self.target = self.features @ true_weights + np.random.randn(self.features.shape[0]) * 0.1

                if self.weights is None:
                    # First cycle: cold start
                    self.weights = np.zeros(n_features, dtype=np.float64)
                    self.bias = 0.0
                    return {
                        'phase': 'model_training',
                        'output': f'Cold start: {n_features} weights, {self.features.shape[0]:,} samples'
                    }
                else:
                    # Subsequent cycles: warm start with previous weights
                    # Resize weights if PCA components changed
                    if len(self.weights) != n_features:
                        old_n = len(self.weights)
                        new_weights = np.zeros(n_features, dtype=np.float64)
                        copy_n = min(old_n, n_features)
                        new_weights[:copy_n] = self.weights[:copy_n]
                        self.weights = new_weights
                    return {
                        'phase': 'model_training',
                        'output': f'Warm start (cycle {self.cycle}): reusing weights, {self.features.shape[0]:,} new samples'
                    }
            else:
                if self.weights is None:
                    self.weights = [0.0] * self.n_components
                self.target = [0.0] * len(self.features)
                return {
                    'phase': 'model_training',
                    'output': f'Model ready (cycle {self.cycle})'
                }

        # Gradient descent epoch
        if HAS_NUMPY:
            self.epochs_completed += 1
            predictions = self.features @ self.weights + self.bias
            errors = predictions - self.target
            n = len(self.target)

            # MSE loss
            loss = float(np.mean(errors ** 2))
            self.train_losses.append(loss)

            # Gradients
            grad_w = (2.0 / n) * (self.features.T @ errors)
            grad_b = (2.0 / n) * np.sum(errors)
            grad_norm = float(np.linalg.norm(grad_w))
            self.gradient_norms.append(grad_norm)

            # Update
            self.weights -= self.learning_rate * grad_w
            self.bias -= self.learning_rate * grad_b

            return {
                'phase': 'model_training',
                'output': f'Epoch {self.epochs_completed}: loss={loss:.6f}, grad_norm={grad_norm:.6f}'
            }
        else:
            self.epochs_completed += 1
            self.train_losses.append(1.0 / self.epochs_completed)
            return {
                'phase': 'model_training',
                'output': f'Epoch {self.epochs_completed}: loss={self.train_losses[-1]:.6f}'
            }

    def _step_evaluation(self) -> dict:
        """Evaluate model: MSE, R², cross-validation."""
        if HAS_NUMPY:
            predictions = self.features @ self.weights + self.bias
            errors = predictions - self.target
            mse = float(np.mean(errors ** 2))
            ss_res = float(np.sum(errors ** 2))
            ss_tot = float(np.sum((self.target - np.mean(self.target)) ** 2))
            r2 = 1.0 - (ss_res / max(ss_tot, 1e-8))

            self.mse_history.append(mse)
            self.r2_history.append(r2)
            self.best_mse = min(self.best_mse, mse)
            self.best_r2 = max(self.best_r2, r2)

            # Cross-validation fold
            n = self.features.shape[0]
            fold_size = n // 5
            if fold_size > 0:
                fold_idx = len(self.cv_scores) % 5
                start = fold_idx * fold_size
                end = start + fold_size
                val_pred = self.features[start:end] @ self.weights + self.bias
                val_mse = float(np.mean((val_pred - self.target[start:end]) ** 2))
                self.cv_scores.append(val_mse)

            return {
                'phase': 'evaluation',
                'output': f'Eval {len(self.mse_history)}: MSE={mse:.6f}, R²={r2:.4f}, CV_folds={len(self.cv_scores)}'
            }
        else:
            self.mse_history.append(1.0 / max(1, len(self.mse_history) + 1))
            self.r2_history.append(0.5)
            return {
                'phase': 'evaluation',
                'output': f'Eval {len(self.mse_history)}'
            }

    def estimate_memory_mb(self) -> float:
        """Estimate total memory usage of pipeline state."""
        total = 0
        if HAS_NUMPY:
            for chunk in self.raw_chunks:
                total += chunk.nbytes
            if self.raw_data is not None:
                total += self.raw_data.nbytes
            if self.cleaned_data is not None:
                total += self.cleaned_data.nbytes
            if self.cov_matrix is not None:
                total += self.cov_matrix.nbytes
            if self.eigenvectors is not None:
                total += self.eigenvectors.nbytes
            if self.features is not None:
                total += self.features.nbytes
            if self.weights is not None:
                total += self.weights.nbytes
            if self.target is not None:
                total += self.target.nbytes
        else:
            total = self.total_rows * self.num_features * 8 * 3  # rough
        # Add history lists
        total += (len(self.train_losses) + len(self.gradient_norms) +
                  len(self.mse_history) + len(self.r2_history) + len(self.cv_scores)) * 8
        return total / (1024 * 1024)

    def get_state_summary(self) -> dict:
        return {
            'cycle': self.cycle,
            'current_phase': self.current_phase,
            'phase_step': self.phase_step,
            'cells_executed': self.cells_executed,
            'total_rows': self.total_rows,
            'total_rows_all_cycles': self.total_rows_all_cycles,
            'data_chunks': len(self.raw_chunks),
            'outliers_removed': self.outliers_removed,
            'n_components': self.n_components,
            'epochs_completed': self.epochs_completed,
            'train_losses_count': len(self.train_losses),
            'eval_count': len(self.mse_history),
            'cv_folds': len(self.cv_scores),
            'memory_mb': self.estimate_memory_mb(),
        }


def run_jupyter_workload(
    num_cells: int = 0,  # 0 = infinite (use duration)
    num_features: int = 20,
    chunk_size: int = 500,
    duration: int = 0,  # 0 = infinite (use num_cells limit)
    working_dir: str = '.'
):
    """
    Sequential data science pipeline workload.

    Executes a 5-phase pipeline where each phase depends on previous results.
    Time is distributed across phases proportionally. All intermediate results
    are in-memory and lost on restart.

    Args:
        num_cells: Max pipeline steps (0 for infinite)
        num_features: Number of data features/columns
        chunk_size: Rows per data chunk in generation phase
        duration: Duration in seconds (0 for infinite)
        working_dir: Working directory for signal files
    """
    if not HAS_NUMPY:
        print("[Jupyter] WARNING: NumPy not available, using simplified pipeline")

    duration_str = f"{duration}s" if duration > 0 else "infinite"
    print(f"[Jupyter] Starting data science pipeline simulation")
    print(f"[Jupyter] Config: num_features={num_features}, chunk_size={chunk_size}, duration={duration_str}")
    print(f"[Jupyter] Pipeline phases: {' -> '.join(PipelineState.PHASES)}")
    print(f"[Jupyter] Working directory: {working_dir}")

    pipeline = PipelineState(num_features=num_features, chunk_size=chunk_size)

    # Signal ready for checkpoint
    create_ready_signal(working_dir)

    start_time = time.time()
    last_report_time = start_time
    phase_start_time = start_time

    while True:
        # Check if restore completed
        if check_restore_complete(working_dir):
            elapsed = time.time() - start_time
            state = pipeline.get_state_summary()
            print(f"[Jupyter] Restore detected - checkpoint_flag removed")
            print(f"[Jupyter] === STATE SUMMARY (lost on restart) ===")
            print(f"[Jupyter]   Pipeline cycles completed: {state['cycle']}")
            print(f"[Jupyter]   Current phase: {state['current_phase']} (step {state['phase_step']})")
            print(f"[Jupyter]   Cells executed: {state['cells_executed']}")
            print(f"[Jupyter]   Data this cycle: {state['total_rows']:,} rows in {state['data_chunks']} chunks")
            print(f"[Jupyter]   Data all cycles: {state['total_rows_all_cycles']:,} rows processed")
            print(f"[Jupyter]   Outliers removed: {state['outliers_removed']}")
            print(f"[Jupyter]   PCA components: {state['n_components']}")
            print(f"[Jupyter]   Training epochs (all cycles): {state['epochs_completed']}")
            if pipeline.train_losses:
                print(f"[Jupyter]   Final train loss: {pipeline.train_losses[-1]:.6f}")
            if pipeline.r2_history:
                print(f"[Jupyter]   Best R²: {pipeline.best_r2:.4f}")
            print(f"[Jupyter]   CV folds completed: {state['cv_folds']}")
            print(f"[Jupyter]   Pipeline memory: {state['memory_mb']:.2f} MB")
            print(f"[Jupyter]   Elapsed time: {elapsed:.1f}s")
            print(f"[Jupyter]   ALL intermediate results LOST on restart")
            print(f"[Jupyter] ==========================================")
            sys.exit(0)

        # Duration check
        elapsed = time.time() - start_time
        if duration > 0 and elapsed >= duration:
            time.sleep(1)
            continue

        # Cell limit check
        if num_cells > 0 and pipeline.cells_executed >= num_cells:
            time.sleep(1)
            continue

        # Check if current phase should advance (time-based)
        if duration > 0 and pipeline.current_phase_idx < len(PipelineState.PHASES):
            phase_elapsed = time.time() - phase_start_time
            # For cycling: use per-cycle phase budget (duration / expected_cycles is unknown,
            # so reuse same weights each cycle — total cycle time ≈ duration for first cycle)
            cycle_duration = duration if pipeline.cycle == 0 else duration * 0.5
            phase_budget = cycle_duration * PipelineState.PHASE_WEIGHTS[pipeline.current_phase_idx]
            if phase_elapsed >= phase_budget and pipeline.phase_step >= 2:
                pipeline.advance_phase()
                phase_start_time = time.time()

        # Execute one pipeline step
        result = pipeline.step()

        # Report every 5 seconds
        current_time = time.time()
        if current_time - last_report_time >= 5.0:
            elapsed = current_time - start_time
            mem = pipeline.estimate_memory_mb()
            print(f"[Jupyter] [{result['phase']}] Cell {pipeline.cells_executed}: "
                  f"{result['output']} | mem={mem:.1f}MB, elapsed={elapsed:.0f}s")
            last_report_time = current_time

        # Simulate cell execution time (faster than real notebooks)
        time.sleep(0.1)


def main():
    parser = argparse.ArgumentParser(
        description="Data science pipeline simulation for CRIU checkpoint testing"
    )
    parser.add_argument(
        '--num-cells',
        type=int,
        default=0,
        help='Max pipeline steps, 0 for infinite (default: 0)'
    )
    parser.add_argument(
        '--num-features',
        type=int,
        default=20,
        help='Number of data features/columns (default: 20)'
    )
    parser.add_argument(
        '--chunk-size',
        type=int,
        default=500,
        help='Rows per data chunk (default: 500)'
    )
    parser.add_argument(
        '--duration',
        type=int,
        default=0,
        help='Duration in seconds (0 for infinite)'
    )
    parser.add_argument(
        '--working_dir',
        type=str,
        default='.',
        help='Working directory for signal files (default: current directory)'
    )

    args = parser.parse_args()

    run_jupyter_workload(
        num_cells=args.num_cells,
        num_features=args.num_features,
        chunk_size=args.chunk_size,
        duration=args.duration,
        working_dir=args.working_dir
    )


if __name__ == '__main__':
    main()
