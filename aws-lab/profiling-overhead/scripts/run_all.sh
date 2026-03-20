#!/bin/bash
set -e

echo "=== Step 1: Deploy infrastructure ==="
cd terraform
terraform init
terraform apply -auto-approve

echo "=== Step 2: Wait for experiments ==="
echo "12 instances launched. Each will auto-terminate after ~1.5 hours."
echo "Monitor: aws ec2 describe-instances --filters Name=tag:Name,Values=criu-exp-*"
echo ""
echo "Wait for all instances to terminate (~1.5 hours)..."
while true; do
    RUNNING=$(aws ec2 describe-instances \
        --filters "Name=tag:Name,Values=criu-exp-*" "Name=instance-state-name,Values=running,pending" \
        --query "Reservations[*].Instances[*].InstanceId" --output text | wc -w)
    echo "Running instances: $RUNNING"
    [ "$RUNNING" -eq 0 ] && break
    sleep 60
done

echo "=== Step 3: Collect results ==="
cd ../scripts
python3 collect_results.py

echo "=== Step 4: Cleanup ==="
cd ../terraform
terraform destroy -auto-approve

echo "=== DONE ==="
