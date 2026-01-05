#!/usr/bin/env python3
"""
Quick experiment runner script.

This is a convenience wrapper around experiments/baseline_experiment.py
that can be run from the criu_workload directory root.

Usage:
    # Run with environment variables
    export SOURCE_NODE_IP="10.0.1.10"
    export DEST_NODE_IP="10.0.1.11"
    python3 run_experiment.py

    # Run with explicit IPs
    python3 run_experiment.py --source-ip 10.0.1.10 --dest-ip 10.0.1.11

    # Run with a specific config
    python3 run_experiment.py -c config/experiments/lazy_pages.yaml
"""

import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from experiments.baseline_experiment import main

if __name__ == '__main__':
    sys.exit(main())
