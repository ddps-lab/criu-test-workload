"""
XGBoost CPU Training Workload Wrapper

Control node wrapper for XGBoost gradient boosted tree training.
Matches ML training workloads from Can't Be Late (NSDI 2024).
"""

from typing import Dict, Any
from .base_workload import BaseWorkload, WorkloadFactory


XGBOOST_STANDALONE_SCRIPT = '''#!/usr/bin/env python3
"""XGBoost CPU Training - Auto-generated standalone script"""

import time
import os
import sys
import argparse

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

try:
    import xgboost as xgb
    HAS_XGB = True
except ImportError:
    HAS_XGB = False


def create_ready_signal(working_dir='.'):
    ready_path = os.path.join(working_dir, 'checkpoint_ready')
    with open(ready_path, 'w') as f:
        f.write(f'ready:{os.getpid()}\\n')
    print(f"[XGBoost] Checkpoint ready (PID: {os.getpid()})")


def check_restore_complete(working_dir='.'):
    flag_path = os.path.join(working_dir, 'checkpoint_flag')
    return not os.path.exists(flag_path)


def load_synthetic_dataset(num_samples, num_features, num_classes, seed):
    np.random.seed(seed)
    X = np.random.randn(num_samples, num_features).astype(np.float32)
    w = np.random.randn(num_features)
    scores = X @ w
    if num_classes == 2:
        y = (scores > 0).astype(np.int32)
    else:
        percentiles = np.linspace(0, 100, num_classes + 1)[1:-1]
        thresholds = np.percentile(scores, percentiles)
        y = np.digitize(scores, thresholds).astype(np.int32)
    return X, y


def load_covtype_dataset(dataset_path=None):
    if dataset_path and os.path.exists(dataset_path):
        data = np.loadtxt(dataset_path, delimiter=',')
        return data[:, :-1].astype(np.float32), (data[:, -1] - 1).astype(np.int32)
    try:
        from sklearn.datasets import fetch_covtype
        data = fetch_covtype()
        return data.data.astype(np.float32), (data.target - 1).astype(np.int32)
    except ImportError:
        print("[XGBoost] ERROR: Cannot load Covertype. Provide --dataset-path or install scikit-learn")
        sys.exit(1)


def load_higgs_dataset(dataset_path):
    if not dataset_path or not os.path.exists(dataset_path):
        print(f"[XGBoost] ERROR: Higgs dataset not found at {dataset_path}")
        sys.exit(1)
    data = np.loadtxt(dataset_path, delimiter=',', dtype=np.float32)
    return data[:, 1:], data[:, 0].astype(np.int32)


def run_xgboost_workload(dataset='synthetic', dataset_path=None, num_samples=100000,
                         num_features=50, num_classes=2, seed=42, num_rounds=0,
                         max_depth=6, learning_rate=0.1, num_threads=1, duration=0,
                         working_dir='.'):
    if not HAS_NUMPY:
        print("[XGBoost] ERROR: NumPy not installed")
        sys.exit(1)
    if not HAS_XGB:
        print("[XGBoost] ERROR: XGBoost not installed")
        sys.exit(1)

    print(f"[XGBoost] Starting training (dataset={dataset}, depth={max_depth}, lr={learning_rate})")

    if dataset == 'synthetic':
        X, y = load_synthetic_dataset(num_samples, num_features, num_classes, seed)
    elif dataset == 'covtype':
        X, y = load_covtype_dataset(dataset_path)
        num_classes = len(np.unique(y))
    elif dataset == 'higgs':
        X, y = load_higgs_dataset(dataset_path)
        num_classes = 2
    else:
        print(f"[XGBoost] ERROR: Unknown dataset: {dataset}")
        sys.exit(1)

    print(f"[XGBoost] Dataset: {X.shape[0]} samples, {X.shape[1]} features, {num_classes} classes")
    print(f"[XGBoost] Data memory: {(X.nbytes + y.nbytes) / (1024*1024):.2f} MB")

    dtrain = xgb.DMatrix(X, label=y, nthread=num_threads)

    if num_classes <= 2:
        objective = 'binary:logistic'
        eval_metric = 'logloss'
    else:
        objective = 'multi:softmax'
        eval_metric = 'mlogloss'

    params = {'max_depth': max_depth, 'eta': learning_rate, 'nthread': num_threads,
              'objective': objective, 'eval_metric': eval_metric, 'seed': seed, 'verbosity': 0}
    if num_classes > 2:
        params['num_class'] = num_classes

    create_ready_signal(working_dir)

    model = None
    round_num = 0
    max_r = num_rounds if num_rounds > 0 else 1000000
    start_time = time.time()
    last_report = start_time
    eval_results = []

    while round_num < max_r:
        if check_restore_complete(working_dir):
            elapsed = time.time() - start_time
            print(f"[XGBoost] Restore detected")
            print(f"[XGBoost] === STATE SUMMARY ===")
            print(f"[XGBoost]   Rounds: {round_num}")
            if eval_results:
                print(f"[XGBoost]   Last {eval_metric}: {eval_results[-1]:.6f}")
            print(f"[XGBoost]   ALL model state LOST on restart")
            print(f"[XGBoost]   Elapsed: {elapsed:.1f}s")
            print(f"[XGBoost] =========================")
            sys.exit(0)

        elapsed = time.time() - start_time
        if duration > 0 and elapsed >= duration:
            time.sleep(1)
            continue

        evals_result = {}
        model = xgb.train(params, dtrain, num_boost_round=1, xgb_model=model,
                          evals=[(dtrain, 'train')], evals_result=evals_result, verbose_eval=False)
        round_num += 1

        if evals_result and 'train' in evals_result:
            metric_key = list(evals_result['train'].keys())[0]
            eval_results.append(evals_result['train'][metric_key][-1])

        current_time = time.time()
        if current_time - last_report >= 5.0:
            eval_str = f", {eval_metric}={eval_results[-1]:.6f}" if eval_results else ""
            print(f"[XGBoost] Round {round_num}{eval_str}, elapsed={current_time - start_time:.0f}s")
            last_report = current_time

    while True:
        if check_restore_complete(working_dir):
            print(f"[XGBoost] Restore detected after training")
            sys.exit(0)
        time.sleep(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', type=str, choices=['synthetic','covtype','higgs'], default='synthetic')
    parser.add_argument('--dataset-path', type=str, default=None)
    parser.add_argument('--num-samples', type=int, default=100000)
    parser.add_argument('--num-features', type=int, default=50)
    parser.add_argument('--num-classes', type=int, default=2)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--num-rounds', type=int, default=0)
    parser.add_argument('--max-depth', type=int, default=6)
    parser.add_argument('--learning-rate', type=float, default=0.1)
    parser.add_argument('--num-threads', type=int, default=1)
    parser.add_argument('--duration', type=int, default=0)
    parser.add_argument('--working_dir', type=str, default='.')
    args = parser.parse_args()
    run_xgboost_workload(args.dataset, args.dataset_path, args.num_samples, args.num_features,
                         args.num_classes, args.seed, args.num_rounds, args.max_depth,
                         args.learning_rate, args.num_threads, args.duration, args.working_dir)


if __name__ == '__main__':
    main()
'''


class XGBoostWorkload(BaseWorkload):
    """
    XGBoost CPU training workload.

    Runs gradient boosted tree training using XGBoost. Matches ML training
    workloads from Can't Be Late (NSDI 2024). Tree-based ML produces
    different dirty page patterns than neural network backprop (ml_training).

    Dirty page pattern:
    - DMatrix (training data): large read-mostly region
    - Gradient/hessian arrays: dirty every round
    - Tree model: grows incrementally

    Requirements (must be pre-installed in AMI):
    - xgboost: pip install xgboost
    - numpy: pip install numpy
    - scikit-learn (optional, for covtype dataset): pip install scikit-learn
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.dataset = config.get('dataset', 'synthetic')
        self.dataset_path = config.get('dataset_path', None)
        self.num_samples = config.get('num_samples', 100000)
        self.num_features = config.get('num_features', 50)
        self.num_classes = config.get('num_classes', 2)
        self.seed = config.get('seed', 42)
        self.num_rounds = config.get('num_rounds', 0)
        self.max_depth = config.get('max_depth', 6)
        self.learning_rate = config.get('learning_rate', 0.1)
        self.num_threads = config.get('num_threads', 1)
        self.duration = config.get('duration', 0)

    def get_standalone_script_name(self) -> str:
        return 'xgboost_standalone.py'

    def get_standalone_script_content(self) -> str:
        return XGBOOST_STANDALONE_SCRIPT

    def get_command(self) -> str:
        cmd = f"python3 {self.get_standalone_script_name()}"
        cmd += f" --dataset {self.dataset}"
        if self.dataset_path:
            cmd += f" --dataset-path {self.dataset_path}"
        if self.dataset == 'synthetic':
            cmd += f" --num-samples {self.num_samples}"
            cmd += f" --num-features {self.num_features}"
            cmd += f" --num-classes {self.num_classes}"
        cmd += f" --seed {self.seed}"
        cmd += f" --num-rounds {self.num_rounds}"
        cmd += f" --max-depth {self.max_depth}"
        cmd += f" --learning-rate {self.learning_rate}"
        cmd += f" --num-threads {self.num_threads}"
        cmd += f" --duration {self.duration}"
        cmd += f" --working_dir {self.working_dir}"
        return cmd

    def get_dependencies(self) -> list[str]:
        return ['xgboost', 'numpy']

    def validate_config(self) -> bool:
        if self.dataset not in ('synthetic', 'covtype', 'higgs'):
            raise ValueError(f"Unknown dataset: {self.dataset}")
        if self.dataset == 'synthetic':
            if self.num_samples <= 0:
                raise ValueError("num_samples must be positive")
            if self.num_features <= 0:
                raise ValueError("num_features must be positive")
        if self.max_depth <= 0:
            raise ValueError("max_depth must be positive")
        if self.learning_rate <= 0:
            raise ValueError("learning_rate must be positive")
        return True

    def estimate_memory_mb(self) -> float:
        if self.dataset == 'synthetic':
            # float32 data + int32 labels + DMatrix overhead (~1.5x)
            data_bytes = self.num_samples * self.num_features * 4 + self.num_samples * 4
            return (data_bytes * 2.5) / (1024 * 1024)
        elif self.dataset == 'covtype':
            # ~581K samples * 54 features * 4 bytes * 2.5x overhead
            return (581012 * 54 * 4 * 2.5) / (1024 * 1024)
        elif self.dataset == 'higgs':
            # ~11M samples * 28 features * 4 bytes * 2.5x overhead
            return (11000000 * 28 * 4 * 2.5) / (1024 * 1024)
        return 100  # conservative default


WorkloadFactory.register('xgboost', XGBoostWorkload)
