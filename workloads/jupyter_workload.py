"""
Jupyter Notebook Pipeline Simulation Workload Wrapper

Control node wrapper for Jupyter notebook pipeline simulation.
Simulates interactive data science sessions with sequential analysis phases.
"""

from typing import Dict, Any
from .base_workload import BaseWorkload, WorkloadFactory


JUPYTER_STANDALONE_SCRIPT = '''#!/usr/bin/env python3
"""Jupyter Notebook Pipeline Simulation - Auto-generated standalone script"""

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


def create_ready_signal(working_dir='.'):
    ready_path = os.path.join(working_dir, 'checkpoint_ready')
    with open(ready_path, 'w') as f:
        f.write(f'ready:{os.getpid()}\\n')
    print(f"[Jupyter] Checkpoint ready (PID: {os.getpid()})")


def check_restore_complete(working_dir='.'):
    return not os.path.exists(os.path.join(working_dir, 'checkpoint_flag'))


class PipelineState:
    PHASES = ['data_generation', 'data_cleaning', 'feature_engineering',
              'model_training', 'evaluation']
    PHASE_WEIGHTS = [0.10, 0.15, 0.20, 0.35, 0.20]

    def __init__(self, num_features=20, chunk_size=500):
        self.num_features = num_features
        self.chunk_size = chunk_size
        self.cycle = 0
        self.current_phase_idx = 0
        self.phase_step = 0
        self.cells_executed = 0

        # Phase 1: data generation
        self.raw_chunks = []
        self.raw_data = None
        self.total_rows = 0
        self.total_rows_all_cycles = 0

        # Phase 2: data cleaning
        self.cleaned_data = None
        self.col_means = None
        self.col_stds = None
        self.outliers_removed = 0

        # Phase 3: feature engineering
        self.cov_matrix = None
        self.eigenvalues = None
        self.eigenvectors = None
        self.n_components = 0
        self.features = None
        self.explained_variance_ratio = None

        # Phase 4: model training
        self.weights = None
        self.bias = 0.0
        self.target = None
        self.learning_rate = 0.01
        self.train_losses = []
        self.gradient_norms = []
        self.epochs_completed = 0

        # Phase 5: evaluation
        self.mse_history = []
        self.r2_history = []
        self.cv_scores = []
        self.best_mse = float('inf')
        self.best_r2 = float('-inf')

    @property
    def current_phase(self):
        if self.current_phase_idx < len(self.PHASES):
            return self.PHASES[self.current_phase_idx]
        return 'complete'

    def get_phase_duration(self, total_duration, phase_idx):
        return total_duration * self.PHASE_WEIGHTS[phase_idx]

    def step(self):
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
        return {'phase': 'complete', 'output': 'Pipeline complete'}

    def advance_phase(self):
        old = self.current_phase
        if self.current_phase_idx < len(self.PHASES) - 1:
            self.current_phase_idx += 1
            self.phase_step = 0
            print(f"[Jupyter] Phase: {old} -> {self.current_phase}")
            return True
        else:
            self._start_new_cycle()
            print(f"[Jupyter] Phase: {old} -> {self.current_phase} (cycle {self.cycle})")
            return True

    def _start_new_cycle(self):
        self.cycle += 1
        self.current_phase_idx = 0
        self.phase_step = 0
        self.raw_chunks = []
        self.raw_data = None
        self.total_rows = 0
        self.cleaned_data = None
        self.col_means = None
        self.col_stds = None
        self.cov_matrix = None
        self.eigenvalues = None
        self.eigenvectors = None
        self.features = None
        self.explained_variance_ratio = None
        self.target = None

    def _step_data_generation(self):
        if HAS_NUMPY:
            chunk = np.random.randn(self.chunk_size, self.num_features).astype(np.float64)
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
            chunk = [[random.gauss(self.cycle * 0.5, 1) for _ in range(self.num_features)] for _ in range(self.chunk_size)]
            self.raw_chunks.append(chunk)
            self.total_rows += self.chunk_size
            self.total_rows_all_cycles += self.chunk_size
        cs = f" (cycle {self.cycle})" if self.cycle > 0 else ""
        return {'phase': 'data_generation', 'output': f'Chunk {len(self.raw_chunks)}: {self.total_rows:,} rows{cs}'}

    def _step_data_cleaning(self):
        if self.cleaned_data is None:
            if HAS_NUMPY:
                self.raw_data = np.vstack(self.raw_chunks)
                self.col_means = np.mean(self.raw_data, axis=0)
                self.col_stds = np.std(self.raw_data, axis=0) + 1e-8
                self.cleaned_data = (self.raw_data - self.col_means) / self.col_stds
                return {'phase': 'data_cleaning', 'output': f'Normalized {self.raw_data.shape[0]:,} rows'}
            else:
                flat = []
                for c in self.raw_chunks:
                    flat.extend(c)
                self.raw_data = flat
                self.cleaned_data = flat
                return {'phase': 'data_cleaning', 'output': f'Normalized {len(flat):,} rows'}
        if HAS_NUMPY:
            z = np.abs(self.cleaned_data)
            thresh = max(2.0, 3.0 - self.phase_step * 0.1)
            mask = np.all(z < thresh, axis=1)
            removed = int(np.sum(~mask))
            if removed > 0:
                self.cleaned_data = self.cleaned_data[mask]
                self.outliers_removed += removed
            return {'phase': 'data_cleaning', 'output': f'Outliers z>{thresh:.1f}: removed {removed}, remaining {self.cleaned_data.shape[0]:,}'}
        return {'phase': 'data_cleaning', 'output': f'Cleaning step {self.phase_step}'}

    def _step_feature_engineering(self):
        if self.cov_matrix is None:
            if HAS_NUMPY:
                self.cov_matrix = np.cov(self.cleaned_data, rowvar=False)
                return {'phase': 'feature_engineering', 'output': f'Covariance matrix {self.num_features}x{self.num_features}'}
            self.cov_matrix = [[0.0] * self.num_features for _ in range(self.num_features)]
            return {'phase': 'feature_engineering', 'output': 'Covariance computed'}
        if self.eigenvalues is None:
            if HAS_NUMPY:
                self.eigenvalues, self.eigenvectors = np.linalg.eigh(self.cov_matrix)
                idx = np.argsort(self.eigenvalues)[::-1]
                self.eigenvalues = self.eigenvalues[idx]
                self.eigenvectors = self.eigenvectors[:, idx]
                self.explained_variance_ratio = self.eigenvalues / np.sum(self.eigenvalues)
                cumvar = np.cumsum(self.explained_variance_ratio)
                self.n_components = min(int(np.searchsorted(cumvar, 0.95)) + 1, self.num_features)
                return {'phase': 'feature_engineering', 'output': f'{self.n_components} components for 95% variance'}
            self.eigenvalues = [1.0] * self.num_features
            self.n_components = min(5, self.num_features)
            return {'phase': 'feature_engineering', 'output': f'{self.n_components} components'}
        if self.features is None:
            if HAS_NUMPY:
                W = self.eigenvectors[:, :self.n_components]
                self.features = self.cleaned_data @ W
                return {'phase': 'feature_engineering', 'output': f'Projected: {self.features.shape[0]:,} x {self.features.shape[1]}'}
            self.features = self.cleaned_data
            return {'phase': 'feature_engineering', 'output': 'Projected'}
        if HAS_NUMPY:
            fm = np.mean(self.features, axis=0)
            return {'phase': 'feature_engineering', 'output': f'Feature mean_norm={np.linalg.norm(fm):.4f}'}
        return {'phase': 'feature_engineering', 'output': 'Feature analysis'}

    def _step_model_training(self):
        if self.target is None:
            if HAS_NUMPY:
                nf = self.features.shape[1]
                np.random.seed(42 + self.cycle)
                tw = np.random.randn(nf) * 0.5
                self.target = self.features @ tw + np.random.randn(self.features.shape[0]) * 0.1
                if self.weights is None:
                    self.weights = np.zeros(nf, dtype=np.float64)
                    self.bias = 0.0
                    return {'phase': 'model_training', 'output': f'Cold start: {nf} weights, {self.features.shape[0]:,} samples'}
                else:
                    if len(self.weights) != nf:
                        nw = np.zeros(nf, dtype=np.float64)
                        cn = min(len(self.weights), nf)
                        nw[:cn] = self.weights[:cn]
                        self.weights = nw
                    return {'phase': 'model_training', 'output': f'Warm start (cycle {self.cycle}): {self.features.shape[0]:,} samples'}
            if self.weights is None:
                self.weights = [0.0] * self.n_components
            self.target = [0.0] * len(self.features)
            return {'phase': 'model_training', 'output': f'Ready (cycle {self.cycle})'}
        if HAS_NUMPY:
            self.epochs_completed += 1
            pred = self.features @ self.weights + self.bias
            err = pred - self.target
            n = len(self.target)
            loss = float(np.mean(err ** 2))
            self.train_losses.append(loss)
            gw = (2.0 / n) * (self.features.T @ err)
            gb = (2.0 / n) * np.sum(err)
            gn = float(np.linalg.norm(gw))
            self.gradient_norms.append(gn)
            self.weights -= self.learning_rate * gw
            self.bias -= self.learning_rate * gb
            return {'phase': 'model_training', 'output': f'Epoch {self.epochs_completed}: loss={loss:.6f}, grad={gn:.6f}'}
        self.epochs_completed += 1
        self.train_losses.append(1.0 / self.epochs_completed)
        return {'phase': 'model_training', 'output': f'Epoch {self.epochs_completed}'}

    def _step_evaluation(self):
        if HAS_NUMPY:
            pred = self.features @ self.weights + self.bias
            err = pred - self.target
            mse = float(np.mean(err ** 2))
            ss_res = float(np.sum(err ** 2))
            ss_tot = float(np.sum((self.target - np.mean(self.target)) ** 2))
            r2 = 1.0 - (ss_res / max(ss_tot, 1e-8))
            self.mse_history.append(mse)
            self.r2_history.append(r2)
            self.best_mse = min(self.best_mse, mse)
            self.best_r2 = max(self.best_r2, r2)
            n = self.features.shape[0]
            fs = n // 5
            if fs > 0:
                fi = len(self.cv_scores) % 5
                s, e = fi * fs, (fi + 1) * fs
                vm = float(np.mean((self.features[s:e] @ self.weights + self.bias - self.target[s:e]) ** 2))
                self.cv_scores.append(vm)
            return {'phase': 'evaluation', 'output': f'MSE={mse:.6f}, R\\u00b2={r2:.4f}, CV={len(self.cv_scores)}'}
        self.mse_history.append(1.0 / max(1, len(self.mse_history) + 1))
        self.r2_history.append(0.5)
        return {'phase': 'evaluation', 'output': f'Eval {len(self.mse_history)}'}

    def estimate_memory_mb(self):
        total = 0
        if HAS_NUMPY:
            for c in self.raw_chunks:
                total += c.nbytes
            for arr in [self.raw_data, self.cleaned_data, self.cov_matrix, self.eigenvectors, self.features, self.weights, self.target]:
                if arr is not None:
                    total += arr.nbytes
        else:
            total = self.total_rows * self.num_features * 8 * 3
        total += (len(self.train_losses) + len(self.gradient_norms) + len(self.mse_history) + len(self.r2_history) + len(self.cv_scores)) * 8
        return total / (1024 * 1024)


def run_jupyter_workload(num_cells, num_features, chunk_size, duration, working_dir):
    duration_str = f"{duration}s" if duration > 0 else "infinite"
    print(f"[Jupyter] Starting pipeline simulation (duration={duration_str})")
    print(f"[Jupyter] Phases: {' -> '.join(PipelineState.PHASES)}")

    pipeline = PipelineState(num_features=num_features, chunk_size=chunk_size)
    create_ready_signal(working_dir)

    start_time = time.time()
    last_report_time = start_time
    phase_start_time = start_time

    while True:
        if check_restore_complete(working_dir):
            elapsed = time.time() - start_time
            print(f"[Jupyter] Restore detected - checkpoint_flag removed")
            print(f"[Jupyter] === STATE SUMMARY (lost on restart) ===")
            print(f"[Jupyter]   Cycles completed: {pipeline.cycle}")
            print(f"[Jupyter]   Phase: {pipeline.current_phase} (step {pipeline.phase_step})")
            print(f"[Jupyter]   Cells: {pipeline.cells_executed}")
            print(f"[Jupyter]   Data this cycle: {pipeline.total_rows:,} rows")
            print(f"[Jupyter]   Data all cycles: {pipeline.total_rows_all_cycles:,} rows")
            print(f"[Jupyter]   Outliers removed: {pipeline.outliers_removed}")
            print(f"[Jupyter]   PCA components: {pipeline.n_components}")
            print(f"[Jupyter]   Epochs (all cycles): {pipeline.epochs_completed}")
            if pipeline.train_losses:
                print(f"[Jupyter]   Final loss: {pipeline.train_losses[-1]:.6f}")
            if pipeline.r2_history:
                print(f"[Jupyter]   Best R\\u00b2: {pipeline.best_r2:.4f}")
            print(f"[Jupyter]   CV folds: {len(pipeline.cv_scores)}")
            print(f"[Jupyter]   Memory: {pipeline.estimate_memory_mb():.2f} MB")
            print(f"[Jupyter]   ALL intermediate results LOST on restart")
            print(f"[Jupyter] ==========================================")
            sys.exit(0)

        elapsed = time.time() - start_time
        if duration > 0 and elapsed >= duration:
            time.sleep(1)
            continue
        if num_cells > 0 and pipeline.cells_executed >= num_cells:
            time.sleep(1)
            continue

        if duration > 0 and pipeline.current_phase_idx < len(PipelineState.PHASES):
            pe = time.time() - phase_start_time
            cd = duration if pipeline.cycle == 0 else duration * 0.5
            pb = cd * PipelineState.PHASE_WEIGHTS[pipeline.current_phase_idx]
            if pe >= pb and pipeline.phase_step >= 2:
                pipeline.advance_phase()
                phase_start_time = time.time()

        result = pipeline.step()

        current_time = time.time()
        if current_time - last_report_time >= 5.0:
            elapsed = current_time - start_time
            mem = pipeline.estimate_memory_mb()
            print(f"[Jupyter] [{result['phase']}] Cell {pipeline.cells_executed}: "
                  f"{result['output']} | mem={mem:.1f}MB, elapsed={elapsed:.0f}s")
            last_report_time = current_time

        time.sleep(0.1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--num-cells', type=int, default=0)
    parser.add_argument('--num-features', type=int, default=20)
    parser.add_argument('--chunk-size', type=int, default=500)
    parser.add_argument('--duration', type=int, default=0)
    parser.add_argument('--working_dir', type=str, default='.')
    args = parser.parse_args()
    run_jupyter_workload(args.num_cells, args.num_features, args.chunk_size, args.duration, args.working_dir)


if __name__ == '__main__':
    main()
'''


class JupyterWorkload(BaseWorkload):
    """
    Jupyter notebook pipeline simulation workload.

    Simulates a sequential data science pipeline:
    - data_generation -> data_cleaning -> feature_engineering -> model_training -> evaluation
    - Each phase depends on previous phase results
    - All intermediate state is in-memory and lost on restart

    Represents:
    - Interactive Jupyter data science sessions
    - ML experiment notebooks
    - Research computing environments
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.num_cells = config.get('num_cells', 0)
        self.num_features = config.get('num_features', 20)
        self.chunk_size = config.get('chunk_size', 500)
        self.duration = config.get('duration', 0)

    def get_standalone_script_name(self) -> str:
        return 'jupyter_standalone.py'

    def get_standalone_script_content(self) -> str:
        return JUPYTER_STANDALONE_SCRIPT

    def get_command(self) -> str:
        cmd = f"python3 {self.get_standalone_script_name()}"
        cmd += f" --num-cells {self.num_cells}"
        cmd += f" --num-features {self.num_features}"
        cmd += f" --chunk-size {self.chunk_size}"
        cmd += f" --duration {self.duration}"
        cmd += f" --working_dir {self.working_dir}"
        return cmd

    def get_dependencies(self) -> list[str]:
        return ['numpy']

    def validate_config(self) -> bool:
        if self.num_features <= 0:
            raise ValueError("num_features must be positive")
        if self.chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        return True

    def estimate_memory_mb(self) -> float:
        # Rough estimate based on features and expected data size
        return max(50, self.num_features * self.chunk_size * 8 * 10 / (1024 * 1024))


WorkloadFactory.register('jupyter', JupyterWorkload)
