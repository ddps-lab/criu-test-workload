#!/usr/bin/env python3
"""
ML Training Standalone Workload (PyTorch CPU)

This script simulates a machine learning training workload using PyTorch.
It trains a simple neural network on synthetic data, representing
long-running ML training jobs that can benefit from checkpointing.

Usage:
    python3 ml_training_standalone.py --model-size medium --batch-size 64 --epochs 100

Checkpoint Protocol:
    1. Creates 'checkpoint_ready' file when model is initialized
    2. Checks 'checkpoint_flag' to detect restore completion
    3. Continues training from current epoch after restore
    4. Exits gracefully when checkpoint_flag is removed

Scenario:
    - Deep learning training jobs
    - Model fine-tuning
    - Hyperparameter search
    - Long-running ML experiments
"""

import time
import os
import sys
import argparse
import random

try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    HAS_TORCH = True
except ImportError:
    HAS_TORCH = False


def create_ready_signal(working_dir: str = '.'):
    """Create checkpoint ready signal file."""
    ready_path = os.path.join(working_dir, 'checkpoint_ready')
    with open(ready_path, 'w') as f:
        f.write(f'ready:{os.getpid()}\n')
    print(f"[MLTrain] Checkpoint ready signal created (PID: {os.getpid()})")


def check_restore_complete(working_dir: str = '.') -> bool:
    """Check if restore is complete (checkpoint_flag removed)."""
    flag_path = os.path.join(working_dir, 'checkpoint_flag')
    return not os.path.exists(flag_path)


class SimpleNN(nn.Module):
    """Simple feedforward neural network."""

    def __init__(self, input_size: int, hidden_sizes: list, output_size: int):
        super().__init__()
        layers = []
        prev_size = input_size

        for hidden_size in hidden_sizes:
            layers.append(nn.Linear(prev_size, hidden_size))
            layers.append(nn.ReLU())
            layers.append(nn.BatchNorm1d(hidden_size))
            layers.append(nn.Dropout(0.2))
            prev_size = hidden_size

        layers.append(nn.Linear(prev_size, output_size))
        self.network = nn.Sequential(*layers)

    def forward(self, x):
        return self.network(x)


def get_model_config(model_size: str) -> dict:
    """Get model configuration based on size."""
    configs = {
        'small': {
            'input_size': 256,
            'hidden_sizes': [512, 256, 128],
            'output_size': 10,
            'dataset_size': 10000,
        },
        'medium': {
            'input_size': 512,
            'hidden_sizes': [1024, 512, 256, 128],
            'output_size': 100,
            'dataset_size': 50000,
        },
        'large': {
            'input_size': 1024,
            'hidden_sizes': [2048, 1024, 512, 256, 128],
            'output_size': 1000,
            'dataset_size': 100000,
        },
    }
    return configs.get(model_size, configs['medium'])


def generate_synthetic_data(config: dict, batch_size: int):
    """Generate synthetic training data."""
    X = torch.randn(config['dataset_size'], config['input_size'])
    y = torch.randint(0, config['output_size'], (config['dataset_size'],))

    dataset = torch.utils.data.TensorDataset(X, y)
    dataloader = torch.utils.data.DataLoader(
        dataset, batch_size=batch_size, shuffle=True
    )
    return dataloader


def run_ml_training_workload(
    model_size: str = 'medium',
    batch_size: int = 64,
    epochs: int = 0,  # 0 = infinite
    learning_rate: float = 0.001,
    working_dir: str = '.',
    dataset_size: int = None  # Override dataset size
):
    """
    Main ML training workload.

    Args:
        model_size: Model size ('small', 'medium', 'large')
        batch_size: Training batch size
        epochs: Number of epochs (0 for infinite)
        learning_rate: Optimizer learning rate
        working_dir: Working directory for signal files
        dataset_size: Override default dataset size (None = use model default)
    """
    if not HAS_TORCH:
        print("[MLTrain] ERROR: PyTorch not installed. Please install with: pip3 install torch")
        sys.exit(1)

    # Force CPU mode
    device = torch.device('cpu')

    print(f"[MLTrain] Starting ML training workload")
    print(f"[MLTrain] Config: model_size={model_size}, batch_size={batch_size}, epochs={epochs or 'infinite'}")
    print(f"[MLTrain] Device: {device}")
    print(f"[MLTrain] Working directory: {working_dir}")

    # Get model configuration
    config = get_model_config(model_size)

    # Override dataset size if specified
    if dataset_size is not None:
        config['dataset_size'] = dataset_size
        print(f"[MLTrain] Dataset size overridden to: {dataset_size}")

    print(f"[MLTrain] Model config: input={config['input_size']}, hidden={config['hidden_sizes']}, output={config['output_size']}")
    print(f"[MLTrain] Dataset size: {config['dataset_size']}")

    # Create model
    print(f"[MLTrain] Creating model...")
    model = SimpleNN(
        config['input_size'],
        config['hidden_sizes'],
        config['output_size']
    ).to(device)

    # Count parameters
    num_params = sum(p.numel() for p in model.parameters())
    print(f"[MLTrain] Model parameters: {num_params:,}")

    # Create optimizer and loss function
    optimizer = optim.Adam(model.parameters(), lr=learning_rate)
    criterion = nn.CrossEntropyLoss()

    # Generate synthetic data
    print(f"[MLTrain] Generating synthetic data...")
    dataloader = generate_synthetic_data(config, batch_size)

    # Signal ready for checkpoint
    create_ready_signal(working_dir)

    epoch = 0
    total_batches = 0
    training_start_time = time.time()

    while True:
        # Check if restore completed
        if check_restore_complete(working_dir):
            print(f"[MLTrain] Restore detected - checkpoint_flag removed")
            training_duration = time.time() - training_start_time
            print(f"[MLTrain] Training summary:")
            print(f"[MLTrain]   Epochs completed: {epoch}")
            print(f"[MLTrain]   Total batches: {total_batches}")
            print(f"[MLTrain]   Training time: {training_duration:.2f}s")
            print("[MLTrain] Workload complete, exiting")
            sys.exit(0)

        # Check epoch limit
        if epochs > 0 and epoch >= epochs:
            time.sleep(1)
            continue

        epoch += 1
        epoch_start_time = time.time()
        epoch_loss = 0.0
        batch_count = 0

        model.train()
        for batch_idx, (data, target) in enumerate(dataloader):
            data, target = data.to(device), target.to(device)

            optimizer.zero_grad()
            output = model(data)
            loss = criterion(output, target)
            loss.backward()
            optimizer.step()

            epoch_loss += loss.item()
            batch_count += 1
            total_batches += 1

        epoch_duration = time.time() - epoch_start_time
        avg_loss = epoch_loss / batch_count

        print(f"[MLTrain] Epoch {epoch}: loss={avg_loss:.4f}, time={epoch_duration:.2f}s, batches={batch_count}")


def main():
    parser = argparse.ArgumentParser(
        description="ML training workload for CRIU checkpoint testing"
    )
    parser.add_argument(
        '--model-size',
        type=str,
        default='medium',
        choices=['small', 'medium', 'large'],
        help='Model size (default: medium)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=64,
        help='Training batch size (default: 64)'
    )
    parser.add_argument(
        '--epochs',
        type=int,
        default=0,
        help='Number of epochs, 0 for infinite (default: 0)'
    )
    parser.add_argument(
        '--learning-rate',
        type=float,
        default=0.001,
        help='Learning rate (default: 0.001)'
    )
    parser.add_argument(
        '--working_dir',
        type=str,
        default='.',
        help='Working directory for signal files'
    )
    parser.add_argument(
        '--dataset-size',
        type=int,
        default=None,
        help='Override dataset size (default: depends on model-size)'
    )

    args = parser.parse_args()

    run_ml_training_workload(
        model_size=args.model_size,
        batch_size=args.batch_size,
        epochs=args.epochs,
        learning_rate=args.learning_rate,
        working_dir=args.working_dir,
        dataset_size=args.dataset_size
    )


if __name__ == '__main__':
    main()
