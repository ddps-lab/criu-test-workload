"""
ML Training Workload Wrapper (PyTorch CPU)

Control node wrapper for the ML training workload.
Simulates long-running machine learning training jobs.
"""

from typing import Dict, Any
from .base_workload import BaseWorkload, WorkloadFactory


ML_TRAINING_STANDALONE_SCRIPT = '''#!/usr/bin/env python3
"""
ML Training Standalone Workload
Auto-generated - do not edit directly
"""

import time
import os
import sys
import argparse

try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    HAS_TORCH = True
except ImportError:
    HAS_TORCH = False


def create_ready_signal(working_dir: str = '.'):
    ready_path = os.path.join(working_dir, 'checkpoint_ready')
    with open(ready_path, 'w') as f:
        f.write(f'ready:{os.getpid()}\\n')
    print(f"[MLTrain] Checkpoint ready signal created (PID: {os.getpid()})")


def check_restore_complete(working_dir: str = '.') -> bool:
    flag_path = os.path.join(working_dir, 'checkpoint_flag')
    return not os.path.exists(flag_path)


class SimpleNN(nn.Module):
    def __init__(self, input_size, hidden_sizes, output_size):
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


def get_model_config(model_size):
    configs = {
        'small': {'input_size': 256, 'hidden_sizes': [512, 256, 128], 'output_size': 10, 'dataset_size': 10000},
        'medium': {'input_size': 512, 'hidden_sizes': [1024, 512, 256, 128], 'output_size': 100, 'dataset_size': 50000},
        'large': {'input_size': 1024, 'hidden_sizes': [2048, 1024, 512, 256, 128], 'output_size': 1000, 'dataset_size': 100000},
    }
    return configs.get(model_size, configs['medium'])


def generate_synthetic_data(config, batch_size):
    X = torch.randn(config['dataset_size'], config['input_size'])
    y = torch.randint(0, config['output_size'], (config['dataset_size'],))
    dataset = torch.utils.data.TensorDataset(X, y)
    return torch.utils.data.DataLoader(dataset, batch_size=batch_size, shuffle=True)


def run_ml_training_workload(model_size, batch_size, epochs, learning_rate, working_dir, dataset_size=None):
    if not HAS_TORCH:
        print("[MLTrain] ERROR: PyTorch not installed")
        sys.exit(1)

    device = torch.device('cpu')
    print(f"[MLTrain] Starting ML training workload")
    print(f"[MLTrain] Config: model_size={model_size}, batch_size={batch_size}")

    config = get_model_config(model_size)
    if dataset_size is not None:
        config['dataset_size'] = dataset_size
        print(f"[MLTrain] Dataset size overridden to: {dataset_size}")
    model = SimpleNN(config['input_size'], config['hidden_sizes'], config['output_size']).to(device)
    num_params = sum(p.numel() for p in model.parameters())
    print(f"[MLTrain] Model parameters: {num_params:,}")

    optimizer = optim.Adam(model.parameters(), lr=learning_rate)
    criterion = nn.CrossEntropyLoss()
    dataloader = generate_synthetic_data(config, batch_size)

    create_ready_signal(working_dir)

    epoch = 0
    total_batches = 0

    while True:
        if check_restore_complete(working_dir):
            print(f"[MLTrain] Restore detected")
            print(f"[MLTrain] Epochs completed: {epoch}, Batches: {total_batches}")
            sys.exit(0)

        if epochs > 0 and epoch >= epochs:
            time.sleep(1)
            continue

        epoch += 1
        epoch_start = time.time()
        epoch_loss = 0.0
        batch_count = 0

        model.train()
        for data, target in dataloader:
            data, target = data.to(device), target.to(device)
            optimizer.zero_grad()
            output = model(data)
            loss = criterion(output, target)
            loss.backward()
            optimizer.step()
            epoch_loss += loss.item()
            batch_count += 1
            total_batches += 1

        print(f"[MLTrain] Epoch {epoch}: loss={epoch_loss/batch_count:.4f}, time={time.time()-epoch_start:.2f}s")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--model-size', type=str, default='medium', choices=['small', 'medium', 'large'])
    parser.add_argument('--batch-size', type=int, default=64)
    parser.add_argument('--epochs', type=int, default=0)
    parser.add_argument('--learning-rate', type=float, default=0.001)
    parser.add_argument('--working_dir', type=str, default='.')
    parser.add_argument('--dataset-size', type=int, default=None)

    args = parser.parse_args()
    run_ml_training_workload(args.model_size, args.batch_size, args.epochs, args.learning_rate, args.working_dir, args.dataset_size)


if __name__ == '__main__':
    main()
'''


class MLTrainingWorkload(BaseWorkload):
    """
    ML Training workload using PyTorch (CPU only).

    Simulates:
    - Deep learning training jobs
    - Model fine-tuning
    - Hyperparameter search
    - Long-running ML experiments

    Uses CPU for compatibility across cloud instances.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.model_size = config.get('model_size', 'medium')
        self.batch_size = config.get('batch_size', 64)
        self.epochs = config.get('epochs', 0)
        self.learning_rate = config.get('learning_rate', 0.001)
        self.dataset_size = config.get('dataset_size', None)

    def get_standalone_script_name(self) -> str:
        return 'ml_training_standalone.py'

    def get_standalone_script_content(self) -> str:
        return ML_TRAINING_STANDALONE_SCRIPT

    def get_command(self) -> str:
        cmd = f"python3 {self.get_standalone_script_name()}"
        cmd += f" --model-size {self.model_size}"
        cmd += f" --batch-size {self.batch_size}"
        cmd += f" --epochs {self.epochs}"
        cmd += f" --learning-rate {self.learning_rate}"
        cmd += f" --working_dir {self.working_dir}"
        if self.dataset_size is not None:
            cmd += f" --dataset-size {self.dataset_size}"
        return cmd

    def get_dependencies(self) -> list[str]:
        return ['torch']

    def validate_config(self) -> bool:
        if self.model_size not in ['small', 'medium', 'large']:
            raise ValueError(f"Invalid model_size: {self.model_size}")
        if self.batch_size <= 0:
            raise ValueError(f"batch_size must be positive")
        return True


WorkloadFactory.register('ml_training', MLTrainingWorkload)
