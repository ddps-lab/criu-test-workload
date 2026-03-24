#!/bin/bash
set -ex
exec > /var/log/experiment.log 2>&1

EXPERIMENT_NAME="${experiment_name}"
WORKLOAD="${workload}"
CONFIGS="${configs}"
EXTRA_ARGS="${extra_args}"
S3_BUCKET="${s3_bucket}"
REGION="${region}"
DURATION="${duration}"
REPEATS="${repeats}"
WORKLOAD_DIR="/opt/criu_workload"

echo "Starting experiment: $EXPERIMENT_NAME"
echo "Workload: $WORKLOAD, Configs: $CONFIGS, Duration: $DURATION, Repeats: $REPEATS"

# Fresh clone (ensures latest code)
cd /tmp
rm -rf $WORKLOAD_DIR
git clone --depth 1 https://github.com/ddps-lab/criu-test-workload.git $WORKLOAD_DIR
cd $WORKLOAD_DIR/tools/dirty_tracker_c && make 2>&1 | tail -1
cd $WORKLOAD_DIR
mkdir -p results

# Stop system redis if running
sudo systemctl stop redis-server 2>/dev/null || true

OUTPUT_FILE="$${WORKLOAD_DIR}/results/$${EXPERIMENT_NAME}.json"

# Run experiment
sudo python3 experiments/measure_overhead.py \
    --workload $WORKLOAD \
    $EXTRA_ARGS \
    --duration $DURATION \
    --repeats $REPEATS \
    --configs $CONFIGS \
    --output $OUTPUT_FILE \
    --working-dir /tmp/overhead_$EXPERIMENT_NAME

echo "Experiment complete, uploading to S3..."
/usr/local/bin/aws s3 cp $OUTPUT_FILE s3://$S3_BUCKET/overhead/$EXPERIMENT_NAME.json --region $REGION
/usr/local/bin/aws s3 cp /var/log/experiment.log s3://$S3_BUCKET/logs/$EXPERIMENT_NAME.log --region $REGION

echo "Upload complete, shutting down..."
sudo shutdown -h now
