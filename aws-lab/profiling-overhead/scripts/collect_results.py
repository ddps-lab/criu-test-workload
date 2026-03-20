#!/usr/bin/env python3
"""Collect experiment results from S3 and generate summary table."""
import json
import os
import subprocess
import sys

S3_BUCKET = sys.argv[1] if len(sys.argv) > 1 else "ddps-criu-experiments"
LOCAL_DIR = "./results"

# Download all results
subprocess.run(["aws", "s3", "sync", f"s3://{S3_BUCKET}/overhead/", LOCAL_DIR])

# Parse and summarize
experiments = {}
for name in sorted(os.listdir(LOCAL_DIR)):
    if not name.endswith('.json'):
        continue
    with open(f"{LOCAL_DIR}/{name}") as f:
        data = json.load(f)
    experiments[name] = data.get('summary_table', 'NO TABLE')
    print(f"\n=== {name} ===")
    print(data.get('summary_table', 'NO TABLE'))
