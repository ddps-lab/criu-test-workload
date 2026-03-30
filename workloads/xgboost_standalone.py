#!/usr/bin/env python3
"""
XGBoost CPU Training Standalone Workload (CRIU Checkpoint)

This script runs XGBoost gradient boosted tree training for CRIU checkpoint testing.
Single Python process, in-process computation (no child processes).

Supports multiple dataset options:
  - synthetic: NumPy-generated random classification data (deterministic via --seed)
  - covtype: Covertype dataset (~581K samples, 54 features)
  - higgs: Higgs boson dataset (~11M samples, 28 features, requires local file)

Usage:
    # Synthetic dataset (default)
    python3 xgboost_standalone.py --dataset synthetic --num-samples 100000 --duration 300

    # Real dataset
    python3 xgboost_standalone.py --dataset covtype --duration 300

    # Higgs (must pre-download)
    python3 xgboost_standalone.py --dataset higgs --dataset-path /data/HIGGS.csv --duration 300

Checkpoint Protocol:
    1. Loads dataset and creates DMatrix
    2. Creates 'checkpoint_ready' file with PID
    3. Runs iterative boosting rounds
    4. Checks 'checkpoint_flag' to detect restore completion

Dirty page pattern:
    - DMatrix: large read-mostly region (training data)
    - Gradient/hessian arrays: dirty every round
    - Tree model: grows with each round
    - Different from PyTorch NN backprop (tree-based vs gradient descent)

Scenario:
    - ML training jobs (Can't Be Late, NSDI 2024 comparison)
    - Batch ML pipelines
    - AutoML hyperparameter search
"""

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


def create_ready_signal(working_dir: str = '.'):
    """Create checkpoint ready signal file."""
    ready_path = os.path.join(working_dir, 'checkpoint_ready')
    with open(ready_path, 'w') as f:
        f.write(f'ready:{os.getpid()}\n')
    print(f"[XGBoost] Checkpoint ready signal created (PID: {os.getpid()})")


def check_restore_complete(working_dir: str = '.') -> bool:
    """Check if restore is complete (checkpoint_flag removed)."""
    flag_path = os.path.join(working_dir, 'checkpoint_flag')
    return not os.path.exists(flag_path)


def load_synthetic_dataset(num_samples: int, num_features: int, num_classes: int, seed: int):
    """Generate synthetic classification dataset (deterministic)."""
    print(f"[XGBoost] Generating synthetic dataset: {num_samples} samples, {num_features} features, {num_classes} classes, seed={seed}")
    np.random.seed(seed)

    X = np.random.randn(num_samples, num_features).astype(np.float32)

    # Create labels with some structure (not purely random)
    # Use a random hyperplane to define class boundaries
    w = np.random.randn(num_features)
    scores = X @ w
    if num_classes == 2:
        y = (scores > 0).astype(np.int32)
    else:
        percentiles = np.linspace(0, 100, num_classes + 1)[1:-1]
        thresholds = np.percentile(scores, percentiles)
        y = np.digitize(scores, thresholds).astype(np.int32)

    return X, y


def load_covtype_dataset(dataset_path: str = None):
    """Load Covertype dataset."""
    if dataset_path and os.path.exists(dataset_path):
        print(f"[XGBoost] Loading Covertype from {dataset_path}")
        data = np.loadtxt(dataset_path, delimiter=',')
        X = data[:, :-1].astype(np.float32)
        y = (data[:, -1] - 1).astype(np.int32)  # 1-indexed to 0-indexed
        return X, y

    # Try sklearn
    try:
        from sklearn.datasets import fetch_covtype
        print(f"[XGBoost] Fetching Covertype via sklearn...")
        data = fetch_covtype()
        X = data.data.astype(np.float32)
        y = (data.target - 1).astype(np.int32)  # 1-indexed to 0-indexed
        return X, y
    except ImportError:
        pass

    print("[XGBoost] ERROR: Cannot load Covertype dataset.")
    print("[XGBoost] Either provide --dataset-path or install scikit-learn: pip install scikit-learn")
    sys.exit(1)


def load_higgs_dataset(dataset_path: str):
    """Load Higgs boson dataset from CSV."""
    if not dataset_path or not os.path.exists(dataset_path):
        print(f"[XGBoost] ERROR: Higgs dataset not found at {dataset_path}")
        print("[XGBoost] Download: https://archive.ics.uci.edu/ml/machine-learning-databases/00280/HIGGS.csv.gz")
        sys.exit(1)

    print(f"[XGBoost] Loading Higgs dataset from {dataset_path}...")
    data = np.loadtxt(dataset_path, delimiter=',', dtype=np.float32)
    y = data[:, 0].astype(np.int32)
    X = data[:, 1:]
    return X, y


def run_xgboost_workload(
    dataset: str = 'synthetic',
    dataset_path: str = None,
    num_samples: int = 100000,
    num_features: int = 50,
    num_classes: int = 2,
    seed: int = 42,
    num_rounds: int = 0,
    max_depth: int = 6,
    learning_rate: float = 0.1,
    num_threads: int = 1,
    duration: int = 0,
    working_dir: str = '.',
    keep_running: bool = False,
):
    """
    XGBoost training workload.

    Args:
        dataset: Dataset type (synthetic/covtype/higgs)
        dataset_path: Path to dataset file (for covtype/higgs)
        num_samples: Number of samples for synthetic dataset
        num_features: Number of features for synthetic dataset
        num_classes: Number of classes for synthetic dataset
        seed: Random seed for reproducibility
        num_rounds: Max boosting rounds (0 = duration-based)
        max_depth: Tree max depth
        learning_rate: Learning rate (eta)
        num_threads: Number of threads
        duration: Duration in seconds (0 = use num_rounds)
        working_dir: Working directory for signal files
    """
    if not HAS_NUMPY:
        print("[XGBoost] ERROR: NumPy not installed. Run: pip install numpy")
        sys.exit(1)
    if not HAS_XGB:
        print("[XGBoost] ERROR: XGBoost not installed. Run: pip install xgboost")
        sys.exit(1)

    duration_str = f"{duration}s" if duration > 0 else f"{num_rounds} rounds"
    print(f"[XGBoost] Starting XGBoost training workload")
    print(f"[XGBoost] Config: dataset={dataset}, max_depth={max_depth}, lr={learning_rate}, "
          f"threads={num_threads}, duration={duration_str}, seed={seed}")
    print(f"[XGBoost] Working directory: {working_dir}")
    os.makedirs(working_dir, exist_ok=True)

    # Load dataset
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

    data_mb = (X.nbytes + y.nbytes) / (1024 * 1024)
    print(f"[XGBoost] Data memory: {data_mb:.2f} MB")

    # Create DMatrix
    print(f"[XGBoost] Creating DMatrix...")
    dtrain = xgb.DMatrix(X, label=y, nthread=num_threads)

    dmatrix_mb = data_mb * 1.5  # DMatrix overhead estimate
    print(f"[XGBoost] DMatrix created (estimated {dmatrix_mb:.1f} MB)")

    # XGBoost parameters
    if num_classes <= 2:
        objective = 'binary:logistic'
        eval_metric = 'logloss'
    else:
        objective = 'multi:softmax'
        eval_metric = 'mlogloss'

    params = {
        'max_depth': max_depth,
        'eta': learning_rate,
        'nthread': num_threads,
        'objective': objective,
        'eval_metric': eval_metric,
        'seed': seed,
        'verbosity': 0,
    }
    if num_classes > 2:
        params['num_class'] = num_classes

    print(f"[XGBoost] Params: {params}")

    # Signal ready for checkpoint
    create_ready_signal(working_dir)

    # Training loop
    model = None
    round_num = 0
    metric_printed = False
    max_rounds = num_rounds if num_rounds > 0 else 1000000
    start_time = time.time()
    last_report_time = start_time
    eval_results = []

    print(f"[XGBoost] Starting training...")

    while round_num < max_rounds:
        # Check restore
        if not keep_running and check_restore_complete(working_dir):
            elapsed = time.time() - start_time
            print(f"[XGBoost] Restore detected - checkpoint_flag removed")
            print(f"[XGBoost] === STATE SUMMARY (lost on restart) ===")
            print(f"[XGBoost]   Rounds completed: {round_num}")
            if eval_results:
                print(f"[XGBoost]   Last {eval_metric}: {eval_results[-1]:.6f}")
            print(f"[XGBoost]   Model trees: {round_num} (ALL lost on restart)")
            print(f"[XGBoost]   Training history length: {len(eval_results)}")
            print(f"[XGBoost]   Elapsed time: {elapsed:.1f}s")
            print(f"[XGBoost] ==========================================")
            rounds_per_sec = round_num / elapsed if elapsed > 0 else 0
            print(f"[METRIC] throughput {rounds_per_sec:.4f} rounds/s")
            sys.exit(0)

        # Duration check
        elapsed = time.time() - start_time
        if duration > 0 and elapsed >= duration:
            if keep_running:
                rounds_per_sec = round_num / elapsed if elapsed > 0 else 0
                print(f"[XGBoost] Duration {duration}s reached, exiting")
                print(f"[METRIC] throughput {rounds_per_sec:.4f} rounds/s")
                sys.exit(0)
            if not metric_printed:
                rounds_per_sec = round_num / elapsed if elapsed > 0 else 0
                print(f"[METRIC] throughput {rounds_per_sec:.4f} rounds/s")
                metric_printed = True
            time.sleep(1)
            continue

        # Train one round
        round_start = time.time()
        evals_result = {}
        model = xgb.train(
            params,
            dtrain,
            num_boost_round=1,
            xgb_model=model,
            evals=[(dtrain, 'train')],
            evals_result=evals_result,
            verbose_eval=False,
        )
        round_num += 1

        # Record evaluation
        if evals_result and 'train' in evals_result:
            metric_key = list(evals_result['train'].keys())[0]
            eval_val = evals_result['train'][metric_key][-1]
            eval_results.append(eval_val)

        round_elapsed = time.time() - round_start

        # Progress report
        current_time = time.time()
        if current_time - last_report_time >= 5.0:
            total_elapsed = current_time - start_time
            eval_str = f", {eval_metric}={eval_results[-1]:.6f}" if eval_results else ""
            remaining = f", remaining={duration - total_elapsed:.0f}s" if duration > 0 else ""
            print(f"[XGBoost] Round {round_num}: time={round_elapsed:.3f}s{eval_str}, "
                  f"elapsed={total_elapsed:.0f}s{remaining}")
            last_report_time = current_time

    # If we reach max_rounds, exit or wait for checkpoint
    if keep_running:
        elapsed = time.time() - start_time
        rounds_per_sec = round_num / elapsed if elapsed > 0 else 0
        print(f"[XGBoost] Reached {max_rounds} rounds, exiting")
        print(f"[METRIC] throughput {rounds_per_sec:.4f} rounds/s")
        sys.exit(0)
    print(f"[XGBoost] Reached {max_rounds} rounds, waiting for checkpoint...")
    while True:
        if check_restore_complete(working_dir):
            elapsed = time.time() - start_time
            print(f"[XGBoost] Restore detected after training complete")
            print(f"[XGBoost] === STATE SUMMARY (lost on restart) ===")
            print(f"[XGBoost]   Rounds completed: {round_num}")
            if eval_results:
                print(f"[XGBoost]   Final {eval_metric}: {eval_results[-1]:.6f}")
            print(f"[XGBoost]   Elapsed time: {elapsed:.1f}s")
            print(f"[XGBoost] ==========================================")
            rounds_per_sec = round_num / elapsed if elapsed > 0 else 0
            print(f"[METRIC] throughput {rounds_per_sec:.4f} rounds/s")
            sys.exit(0)
        time.sleep(1)


def main():
    parser = argparse.ArgumentParser(
        description="XGBoost CPU training workload for CRIU checkpoint testing"
    )
    parser.add_argument(
        '--dataset',
        type=str,
        choices=['synthetic', 'covtype', 'higgs'],
        default='synthetic',
        help='Dataset type (default: synthetic)'
    )
    parser.add_argument(
        '--dataset-path',
        type=str,
        default=None,
        help='Path to dataset file (for covtype/higgs)'
    )
    parser.add_argument(
        '--num-samples',
        type=int,
        default=100000,
        help='Number of samples for synthetic dataset (default: 100000)'
    )
    parser.add_argument(
        '--num-features',
        type=int,
        default=50,
        help='Number of features for synthetic dataset (default: 50)'
    )
    parser.add_argument(
        '--num-classes',
        type=int,
        default=2,
        help='Number of classes for synthetic dataset (default: 2)'
    )
    parser.add_argument(
        '--seed',
        type=int,
        default=42,
        help='Random seed for reproducibility (default: 42)'
    )
    parser.add_argument(
        '--num-rounds',
        type=int,
        default=0,
        help='Max boosting rounds (0 = duration-based, default: 0)'
    )
    parser.add_argument(
        '--max-depth',
        type=int,
        default=6,
        help='Tree max depth (default: 6)'
    )
    parser.add_argument(
        '--learning-rate',
        type=float,
        default=0.1,
        help='Learning rate / eta (default: 0.1)'
    )
    parser.add_argument(
        '--num-threads',
        type=int,
        default=1,
        help='Number of threads (default: 1)'
    )
    parser.add_argument(
        '--duration',
        type=int,
        default=0,
        help='Duration in seconds (0 = use --num-rounds, default: 0)'
    )
    parser.add_argument(
        '--working_dir',
        type=str,
        default='.',
        help='Working directory for signal files'
    )
    parser.add_argument(
        '--keep-running',
        action='store_true',
        help='Keep running after restore (ignore checkpoint_flag removal)'
    )

    args = parser.parse_args()

    run_xgboost_workload(
        dataset=args.dataset,
        dataset_path=args.dataset_path,
        num_samples=args.num_samples,
        num_features=args.num_features,
        num_classes=args.num_classes,
        seed=args.seed,
        num_rounds=args.num_rounds,
        max_depth=args.max_depth,
        learning_rate=args.learning_rate,
        num_threads=args.num_threads,
        duration=args.duration,
        working_dir=args.working_dir,
        keep_running=args.keep_running,
    )


if __name__ == '__main__':
    main()
