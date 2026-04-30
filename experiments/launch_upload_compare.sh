#!/bin/bash
# launch_upload_compare.sh — measure dump+upload time for two paths:
#   (A) CRIU --s3-direct-upload   (serial single-stream multipart, current default)
#   (B) CRIU dumps to local disk + `aws s3 cp --recursive` (CLI parallel multipart)
#
# Spawns 1 m5.8xlarge, runs the target workload under both paths, uploads
# a summary.ndjson, self-terminates (trap ensures no leak on failure).
#
# Usage:  bash launch_upload_compare.sh
# Env:    AMI_ID, BUCKET, REGION, WORKLOAD (default mc-4gb)

set -e

AMI_ID="${AMI_ID:-ami-0fd8cddbe746f93aa}"
INSTANCE_TYPE="m5.8xlarge"
KEY_NAME="mhsong-ddps-oregon"
SG="sg-0eb08e8fa10cb3031"
SUBNET="subnet-09c8aacd484cac3e2"
IAM_PROFILE="mhsong-ec2-admin"
REGION="${REGION:-us-west-2}"
SSH_KEY="$HOME/.ssh/mhsong-ddps-oregon.pem"
BUCKET="${BUCKET:-mhsong-criu-checkpoints}"
CRIU_SRC="${CRIU_SRC:-/spot_kubernetes/criu_build/criu-s3}"
WORKLOAD="${WORKLOAD:-mc-4gb}"

case "$WORKLOAD" in
    mc-4gb) TYPE=memcached; PFX=memcached-4gb; EXTRA="--memcached-memory 4096 --record-count 3100000 --ycsb-threads 4" ;;
    mc-1gb) TYPE=memcached; PFX=memcached-1gb; EXTRA="--memcached-memory 1024 --record-count 773000 --ycsb-threads 4" ;;
    *) echo "ERROR: unsupported WORKLOAD=$WORKLOAD (only mc-1gb / mc-4gb wired)"; exit 1 ;;
esac

AWS_KEY=$(aws configure get aws_access_key_id)
AWS_SECRET=$(aws configure get aws_secret_access_key)
[ -x "${CRIU_SRC}/criu/criu" ] || { echo "ERROR: ${CRIU_SRC}/criu/criu not built"; exit 1; }

TS=$(date -u +%Y%m%dT%H%M%SZ)

echo "=========================================="
echo " Upload-path compare ($WORKLOAD)"
echo " criu: $(${CRIU_SRC}/criu/criu --version 2>&1 | grep -i gitid)"
echo "=========================================="

IID=$(aws ec2 run-instances --region $REGION --image-id $AMI_ID --instance-type $INSTANCE_TYPE \
    --count 1 --key-name $KEY_NAME --security-group-ids $SG --subnet-id $SUBNET \
    --iam-instance-profile Name=$IAM_PROFILE \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":120,"VolumeType":"gp3"}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=bench-upload-compare}]" \
    --query 'Instances[0].InstanceId' --output text)
aws ec2 wait instance-running --region $REGION --instance-ids $IID
IP=$(aws ec2 describe-instances --region $REGION --instance-ids $IID \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
echo "Instance: $IID @ $IP"

for i in $(seq 1 40); do
    ssh -i $SSH_KEY -o ConnectTimeout=3 -o StrictHostKeyChecking=no ubuntu@$IP "echo ok" 2>/dev/null && break
    sleep 5
done

ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "
    cd /opt/criu_workload && git pull origin main -q
    ssh-keygen -t rsa -N '' -f ~/.ssh/id_rsa -q <<< y >/dev/null 2>&1 || true
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
    ssh-keyscan -H 127.0.0.1 >> ~/.ssh/known_hosts 2>/dev/null
    cp ~/.ssh/id_rsa ~/.ssh/id_ed25519 2>/dev/null || true
" 2>/dev/null

scp -i $SSH_KEY -o StrictHostKeyChecking=no "${CRIU_SRC}/criu/criu" \
    ubuntu@$IP:/tmp/criu.phase6-compression >/dev/null
for f in experiments/baseline_experiment.py lib/checkpoint.py lib/criu_utils.py \
         lib/lazy_mode.py lib/hot_vma.py experiments/dump_all_workloads.sh \
         config/default.yaml; do
    scp -i $SSH_KEY -o StrictHostKeyChecking=no "/spot_kubernetes/criu_workload/$f" \
        ubuntu@$IP:/tmp/$(basename $f) >/dev/null
done

# ---------- per-instance env (local expansion) ----------
ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "cat > /tmp/upload_env.sh" <<ENVFILE
export AWS_ACCESS_KEY_ID='${AWS_KEY}'
export AWS_SECRET_ACCESS_KEY='${AWS_SECRET}'
export AWS_DEFAULT_REGION='${REGION}'
export BUCKET='${BUCKET}'
export REGION='${REGION}'
export TS='${TS}'
export WORKLOAD='${WORKLOAD}'
export TYPE='${TYPE}'
export PFX='${PFX}'
export EXTRA='${EXTRA}'
ENVFILE

# ---------- driver (remote expansion only, via quoted heredoc) ----------
ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "cat > /tmp/driver.sh" <<'DRIVER'
#!/bin/bash
set +e
source /tmp/upload_env.sh

LOG=/tmp/driver.log
OUTDIR=/tmp/upload_compare_${TS}
mkdir -p "$OUTDIR"

self_terminate() {
    echo "=== [$(date +%H:%M:%S)] terminating ===" >> "$LOG"
    # Ship whatever we have before dying.
    aws s3 cp --recursive "$OUTDIR" "s3://$BUCKET/bench-upload-compare/$TS/" \
        --region "$REGION" --only-show-errors 2>/dev/null
    TOKEN=$(curl -sX PUT http://169.254.169.254/latest/api/token \
        -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')
    INSTANCE_ID=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" \
        http://169.254.169.254/latest/meta-data/instance-id)
    aws ec2 terminate-instances --instance-ids "$INSTANCE_ID" --region "$REGION" >/dev/null 2>&1
}
trap self_terminate EXIT

echo "=== install criu + patches ===" | tee -a "$LOG"
sudo install -m 0755 /tmp/criu.phase6-compression /usr/local/sbin/criu
cd /opt/criu_workload
sudo install -m 0644 -o ubuntu -g ubuntu /tmp/baseline_experiment.py experiments/baseline_experiment.py
sudo install -m 0644 -o ubuntu -g ubuntu /tmp/checkpoint.py         lib/checkpoint.py
sudo install -m 0644 -o ubuntu -g ubuntu /tmp/criu_utils.py         lib/criu_utils.py
sudo install -m 0644 -o ubuntu -g ubuntu /tmp/lazy_mode.py          lib/lazy_mode.py
sudo install -m 0644 -o ubuntu -g ubuntu /tmp/hot_vma.py            lib/hot_vma.py
sudo install -m 0644 -o ubuntu -g ubuntu /tmp/default.yaml          config/default.yaml
sudo install -m 0755 -o ubuntu -g ubuntu /tmp/dump_all_workloads.sh experiments/dump_all_workloads.sh

run_variant() {
    local mode=$1   # "direct" or "local"
    echo ""                                                    | tee -a "$LOG"
    echo "=== [$(date +%H:%M:%S)] variant=$mode ==="           | tee -a "$LOG"

    local s3_prefix="bench-upload-compare-${WORKLOAD}-${TS}-${mode}"
    aws s3 rm "s3://$BUCKET/$s3_prefix/" --recursive --quiet 2>/dev/null || true
    sudo rm -rf /tmp/criu_checkpoint /tmp/dump_${WORKLOAD}.* /tmp/dirty_pattern.json 2>/dev/null || true

    local t0
    t0=$(date +%s.%N)

    if [ "$mode" = "direct" ]; then
        sudo -E env AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
            python3 experiments/baseline_experiment.py \
            --config config/experiments/memcached_lazy_prefetch.yaml \
            --source-ip 127.0.0.1 --dest-ip 127.0.0.1 \
            --workload $TYPE $EXTRA \
            --s3-direct-upload \
            --s3-type standard \
            --s3-upload-bucket "$BUCKET" \
            --s3-prefix "$s3_prefix" \
            --s3-region "$REGION" \
            --s3-access-key "$AWS_ACCESS_KEY_ID" \
            --s3-secret-key "$AWS_SECRET_ACCESS_KEY" \
            --lazy-mode lazy-prefetch \
            --track-dirty-pages --dirty-tracker c --dirty-track-interval 5000 \
            --no-cleanup \
            -o "$OUTDIR/direct_result.json" > "$OUTDIR/direct.log" 2>&1 || true
        local t1; t1=$(date +%s.%N)
        python3 - <<PYEOF >> "$OUTDIR/summary.ndjson"
import json, os, re
d0, d1 = float("$t0"), float("$t1")
log = open("$OUTDIR/direct.log").read()
m = re.search(r"Final dump completed in ([\d.]+)s", log)
final = float(m.group(1)) if m else None
import subprocess
sz = subprocess.run(
    "aws s3 ls s3://$BUCKET/$s3_prefix/ --recursive --region $REGION --summarize",
    shell=True, capture_output=True, text=True).stdout
tb = next((int(l.split()[-1]) for l in sz.splitlines() if 'Total Size' in l), None)
print(json.dumps({
    "variant": "direct", "workload": "$WORKLOAD",
    "s3_prefix": "$s3_prefix",
    "wall_s": round(d1 - d0, 2),
    "final_dump_s": final,
    "upload_s_separate": None,
    "total_bytes": tb,
}))
PYEOF
    else
        # "local" mode: no S3 direct upload; CRIU writes to local disk, then aws s3 cp.
        sudo -E env AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
            python3 experiments/baseline_experiment.py \
            --config config/experiments/memcached_lazy_prefetch.yaml \
            --source-ip 127.0.0.1 --dest-ip 127.0.0.1 \
            --workload $TYPE $EXTRA \
            --lazy-mode lazy-prefetch \
            --track-dirty-pages --dirty-tracker c --dirty-track-interval 5000 \
            --no-cleanup \
            -o "$OUTDIR/local_result.json" > "$OUTDIR/local.log" 2>&1 || true
        local t1; t1=$(date +%s.%N)

        local dump_dir
        dump_dir=$(sudo ls -td /tmp/criu_checkpoint/*/ 2>/dev/null | head -1)
        echo "local dump dir: $dump_dir" | tee -a "$LOG"

        local t_up0 t_up1
        t_up0=$(date +%s.%N)
        sudo aws s3 cp --recursive "$dump_dir" "s3://$BUCKET/$s3_prefix/" \
            --region "$REGION" --only-show-errors 2>&1 | tee -a "$OUTDIR/local_upload.log"
        t_up1=$(date +%s.%N)

        python3 - <<PYEOF >> "$OUTDIR/summary.ndjson"
import json, os, re, subprocess
d0, d1   = float("$t0"),   float("$t1")
u0, u1   = float("$t_up0"), float("$t_up1")
log = open("$OUTDIR/local.log").read()
m = re.search(r"Final dump completed in ([\d.]+)s", log)
final = float(m.group(1)) if m else None
sz = subprocess.run(
    "aws s3 ls s3://$BUCKET/$s3_prefix/ --recursive --region $REGION --summarize",
    shell=True, capture_output=True, text=True).stdout
tb = next((int(l.split()[-1]) for l in sz.splitlines() if 'Total Size' in l), None)
print(json.dumps({
    "variant": "local", "workload": "$WORKLOAD",
    "s3_prefix": "$s3_prefix",
    "wall_s": round(u1 - d0, 2),
    "final_dump_s": final,
    "upload_s_separate": round(u1 - u0, 2),
    "total_bytes": tb,
}))
PYEOF
    fi
}

run_variant direct
run_variant local

cat "$OUTDIR/summary.ndjson" >> "$LOG"
DRIVER

ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "
    chmod +x /tmp/driver.sh
    nohup bash /tmp/driver.sh > /tmp/driver_outer.log 2>&1 &
    disown
    echo PID \$!
"
echo ""
echo "Follow:  ssh -i $SSH_KEY ubuntu@$IP 'tail -f /tmp/driver_outer.log'  # $IID"
echo "Results: s3://$BUCKET/bench-upload-compare/$TS/summary.ndjson"
