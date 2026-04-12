#!/bin/bash
# Page-server (source-coupled lazy restore) probe.
#
# Spawns ONE source instance and ONE destination instance, runs the workload
# on source, dumps with --lazy-mode=live-migration, starts criu page-server
# on source, then runs criu restore + lazy-pages daemon on destination that
# pulls pages from the source page-server. The wall-clock from "lazy-pages
# daemon start" until "lazy-pages daemon exit" is the page-server transfer
# time.
#
# Usage:
#   bash launch_pageserver_probe.sh --workload matmul
#   bash launch_pageserver_probe.sh --workload matmul --keep-instance
#
# This is a 1-shot probe (no repeat) intended for the matmul-first sanity
# check, then to run once per each of the 10 workloads to populate Table X
# in the paper.
#
# Notes
# -----
# - Page-server requires the source process to remain alive during the
#   entire restore. We do NOT auto-terminate the source until the lazy-pages
#   daemon on destination exits.
# - The lazy-pages daemon on destination exits when every IOV has been
#   transferred. We treat that exit as "transfer complete".
# - Cross-instance SSH must work in both directions (source<->dest). The
#   source instance is bootstrapped with our standard SSH self-key script.

set -e

AMI_ID="${AMI_ID:-ami-0fd8cddbe746f93aa}"
INSTANCE_TYPE="m5.8xlarge"
KEY_NAME="mhsong-ddps-oregon"
SG="sg-0eb08e8fa10cb3031"
SUBNET="subnet-09c8aacd484cac3e2"
IAM_PROFILE="mhsong-ec2-admin"
REGION="us-west-2"
SSH_KEY="$HOME/.ssh/mhsong-ddps-oregon.pem"
KEEP_INSTANCE=0
WORKLOAD=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --workload)      WORKLOAD="$2"; shift 2 ;;
        --keep-instance) KEEP_INSTANCE=1; shift ;;
        *) echo "Unknown: $1"; exit 1 ;;
    esac
done
[ -z "$WORKLOAD" ] && { echo "ERROR: --workload required"; exit 1; }

AWS_KEY=$(aws configure get aws_access_key_id)
AWS_SECRET=$(aws configure get aws_secret_access_key)
[ -z "$AWS_KEY" ] && { echo "AWS credentials missing"; exit 1; }

# Workload-specific args (matches launch_experiment.sh table).
case "$WORKLOAD" in
    matmul)      EXTRA_ARGS="--matrix-size 2048" ;;
    dataproc)    EXTRA_ARGS="--num-rows 1500000 --num-cols 60 --batch-size 1000" ;;
    ml_training) EXTRA_ARGS="--model-size large --dataset-size 50000" ;;
    xgboost)     EXTRA_ARGS="--dataset synthetic --num-samples 7000000 --num-features 100 --num-threads 3" ;;
    redis)       EXTRA_ARGS="--record-count 5000000 --ycsb-threads 4 --ycsb-workload a" ;;
    memcached)   EXTRA_ARGS="--memcached-memory 11264 --record-count 8500000 --ycsb-threads 4" ;;
    mc1gb)       WORKLOAD=memcached; EXTRA_ARGS="--memcached-memory 1024 --record-count 773000 --ycsb-threads 4" ;;
    mc4gb)       WORKLOAD=memcached; EXTRA_ARGS="--memcached-memory 4096 --record-count 3100000 --ycsb-threads 4" ;;
    mc8gb)       WORKLOAD=memcached; EXTRA_ARGS="--memcached-memory 8192 --record-count 6200000 --ycsb-threads 4" ;;
    mc16gb)      WORKLOAD=memcached; EXTRA_ARGS="--memcached-memory 16384 --record-count 12400000 --ycsb-threads 4" ;;
    *) echo "Unknown workload: $WORKLOAD"; exit 1 ;;
esac

echo "=========================================="
echo " Page-server probe: $WORKLOAD"
echo " AMI: $AMI_ID  Region: $REGION"
echo "=========================================="

INSTANCE_IDS=$(aws ec2 run-instances \
    --region $REGION --image-id $AMI_ID --instance-type $INSTANCE_TYPE \
    --count 2 \
    --key-name $KEY_NAME --security-group-ids $SG \
    --subnet-id $SUBNET --iam-instance-profile Name=$IAM_PROFILE \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":100,"VolumeType":"gp3"}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=criu-pageserver-${WORKLOAD}}]" \
    --query 'Instances[*].InstanceId' --output text)

read -r SRC_ID DST_ID <<< "$INSTANCE_IDS"
echo "src=$SRC_ID  dst=$DST_ID"
aws ec2 wait instance-running --region $REGION --instance-ids $INSTANCE_IDS

SRC_IP=$(aws ec2 describe-instances --region $REGION --instance-ids $SRC_ID --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
DST_IP=$(aws ec2 describe-instances --region $REGION --instance-ids $DST_ID --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
SRC_PRIV=$(aws ec2 describe-instances --region $REGION --instance-ids $SRC_ID --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text)
DST_PRIV=$(aws ec2 describe-instances --region $REGION --instance-ids $DST_ID --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text)
aws ec2 create-tags --region $REGION --resources $SRC_ID --tags "Key=Role,Value=source" 2>/dev/null || true
aws ec2 create-tags --region $REGION --resources $DST_ID --tags "Key=Role,Value=dest" 2>/dev/null || true
echo "src $SRC_IP ($SRC_PRIV)  dst $DST_IP ($DST_PRIV)"

wait_ssh() {
    local ip=$1
    for i in $(seq 1 30); do
        ssh -i $SSH_KEY -o ConnectTimeout=3 -o StrictHostKeyChecking=no ubuntu@$ip "echo ok" 2>/dev/null && return 0
        sleep 5
    done
    return 1
}
wait_ssh $SRC_IP || { echo "src ssh timeout"; exit 1; }
wait_ssh $DST_IP || { echo "dst ssh timeout"; exit 1; }

# Bootstrap SSH key on both sides + cross-host trust (so the framework's
# paramiko / criu page-server channel can connect from src to dst and back).
for ip in $SRC_IP $DST_IP; do
    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$ip "
        cd /opt/criu_workload && git pull origin main -q
        ssh-keygen -t rsa -N '' -f ~/.ssh/id_rsa -q <<< y >/dev/null 2>&1 || true
        cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
        cp ~/.ssh/id_rsa ~/.ssh/id_ed25519 2>/dev/null || true
    " 2>/dev/null
done

# Trust src→dst and dst→src using each side's freshly generated keypair.
SRC_PUB=$(ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$SRC_IP "cat ~/.ssh/id_rsa.pub")
DST_PUB=$(ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$DST_IP "cat ~/.ssh/id_rsa.pub")
ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$DST_IP "echo '$SRC_PUB' >> ~/.ssh/authorized_keys"
ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$SRC_IP "echo '$DST_PUB' >> ~/.ssh/authorized_keys"
ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$SRC_IP "ssh-keyscan -H $DST_PRIV >> ~/.ssh/known_hosts 2>/dev/null"
ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$DST_IP "ssh-keyscan -H $SRC_PRIV >> ~/.ssh/known_hosts 2>/dev/null"

# Driver runs on the SOURCE instance via baseline_experiment.py with
# --lazy-mode live-migration. The framework handles starting page-server on
# source and lazy-pages daemon on dest.  We capture the elapsed time from
# the framework's own metrics file and from the lazy-pages daemon log.
ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$SRC_IP "cat > /tmp/probe.sh" <<DRIVER
#!/bin/bash
set +e
export AWS_ACCESS_KEY_ID='${AWS_KEY}'
export AWS_SECRET_ACCESS_KEY='${AWS_SECRET}'
cd /opt/criu_workload

KEEP_INSTANCE=${KEEP_INSTANCE}
WORKLOAD="${WORKLOAD}"
SRC_ID="${SRC_ID}"
DST_ID="${DST_ID}"
TS=\$(date +%Y%m%d_%H%M%S)
OUTDIR=/tmp/results/pageserver_\${WORKLOAD}_\${TS}
mkdir -p \$OUTDIR

echo "=== [\$(date +%H:%M:%S)] page-server probe: \$WORKLOAD ==="

# duration=86400 keeps YCSB run alive past wall-clock; wait_before_dump=120
# matches the standard restore experiment so the workload state is comparable.
sudo -E python3 -u experiments/baseline_experiment.py \\
    --config config/experiments/memcached_lazy_prefetch.yaml \\
    --source-ip 127.0.0.1 --dest-ip $DST_PRIV \\
    --ssh-user ubuntu --workload \$WORKLOAD \\
    --lazy-mode live-migration \\
    --wait-before-dump 120 --duration 86400 \\
    $EXTRA_ARGS \\
    -o \$OUTDIR/pageserver.json \\
    > \$OUTDIR/baseline_experiment.log 2>&1
RC=\$?
echo "baseline_experiment exit=\$RC"

# Pull CRIU artefacts that the framework leaves behind on each side.
mkdir -p \$OUTDIR/source_logs \$OUTDIR/dest_logs
sudo cp /tmp/criu_checkpoint/1/criu-page-server.log \$OUTDIR/source_logs/ 2>/dev/null || true
sudo cp /tmp/criu_checkpoint/1/criu-dump.log         \$OUTDIR/source_logs/ 2>/dev/null || true
scp -i ~/.ssh/id_rsa -o StrictHostKeyChecking=no \\
    ubuntu@$DST_PRIV:/tmp/criu_checkpoint/1/criu-restore.log    \$OUTDIR/dest_logs/ 2>/dev/null || true
scp -i ~/.ssh/id_rsa -o StrictHostKeyChecking=no \\
    ubuntu@$DST_PRIV:/tmp/criu_checkpoint/1/criu-lazy-pages.log \$OUTDIR/dest_logs/ 2>/dev/null || true
sudo chown -R ubuntu:ubuntu \$OUTDIR

echo "=== framework metrics ==="
cat \$OUTDIR/pageserver.json 2>/dev/null | python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
    print(json.dumps({
        'final_dump':  d.get('final_dump',    {}).get('duration'),
        'transfer':    d.get('transfer',      {}).get('duration'),
        'restore':     d.get('restore',       {}).get('duration'),
        'lazy_pages':  d.get('lazy_pages',    {}).get('duration'),
        'criu_metrics_keys': list((d.get('criu_metrics') or {}).keys()),
    }, indent=2))
except Exception as e:
    print(f'parse error: {e}')
"

# Upload all artefacts to S3.
S3_DEST="s3://mhsong-criu-results/pageserver_\${WORKLOAD}/\${TS}/"
aws s3 sync \$OUTDIR \$S3_DEST --region us-west-2 --quiet
echo "uploaded → \$S3_DEST"

# Auto-terminate both instances unless explicitly told to keep.
if [ "\$KEEP_INSTANCE" = "0" ]; then
    echo "Auto-terminating src=\$SRC_ID dst=\$DST_ID"
    aws ec2 terminate-instances --region us-west-2 \\
        --instance-ids \$SRC_ID \$DST_ID 2>&1 | tail -5
fi
DRIVER

ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$SRC_IP "
    chmod +x /tmp/probe.sh
    nohup bash /tmp/probe.sh > /tmp/probe.log 2>&1 &
    disown
    echo started PID \$!
"

echo ""
echo "=========================================="
echo " Probe launched"
echo "=========================================="
echo "  src: ssh -i $SSH_KEY ubuntu@$SRC_IP 'tail -f /tmp/probe.log'"
echo "  dst: ssh -i $SSH_KEY ubuntu@$DST_IP 'tail -f /tmp/criu_checkpoint/1/criu-lazy-pages.log'"
echo
if [ "$KEEP_INSTANCE" -eq 0 ]; then
    echo "Instances will auto-terminate after S3 upload."
else
    echo "--keep-instance set: src=$SRC_ID dst=$DST_ID will remain."
fi
