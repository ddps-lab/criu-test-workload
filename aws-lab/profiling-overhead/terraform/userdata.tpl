#!/bin/bash
set -ex
exec > /var/log/experiment.log 2>&1

EXPERIMENT_NAME="${experiment_name}"
WORKLOAD="${workload}"
CONFIGS="${configs}"
EXTRA_ARGS="${extra_args}"
S3_BUCKET="${s3_bucket}"
REGION="${region}"
WORKLOAD_DIR="/opt/criu_workload"
OUTPUT_FILE="$${WORKLOAD_DIR}/results/$${EXPERIMENT_NAME}.json"
mkdir -p $${WORKLOAD_DIR}/results

echo "Starting experiment: $EXPERIMENT_NAME"
echo "Workload: $WORKLOAD"
echo "Configs: $CONFIGS"
echo "Extra: $EXTRA_ARGS"

cd $WORKLOAD_DIR

# Run experiment
sudo python3 experiments/measure_overhead.py \
    --workload $WORKLOAD \
    $EXTRA_ARGS \
    --duration 60 \
    --repeats 10 \
    --configs $CONFIGS \
    --output $OUTPUT_FILE \
    --working-dir /tmp/overhead_$EXPERIMENT_NAME

echo "Experiment complete, uploading to S3..."

# Upload result to S3
aws s3 cp $OUTPUT_FILE s3://$S3_BUCKET/overhead/$EXPERIMENT_NAME.json --region $REGION

# Upload experiment log
aws s3 cp /var/log/experiment.log s3://$S3_BUCKET/logs/$EXPERIMENT_NAME.log --region $REGION

echo "Upload complete, shutting down..."
sudo shutdown -h now
