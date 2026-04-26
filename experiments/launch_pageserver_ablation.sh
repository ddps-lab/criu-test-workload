#!/bin/bash
# launch_pageserver_ablation.sh — TCP page-server ablation across multiple
# workloads. Spawns one PAIR (source + destination) of m5.8xlarge per
# workload, runs --lazy-mode live-migration end-to-end REPEAT times, and
# self-terminates both instances on completion.
#
# Page-server flow per repeat:
#   1. Source: start workload (memcached load, etc.)
#   2. Source: criu dump --lazy-pages with page-server enabled
#   3. Dest:   criu lazy-pages connecting to source page-server
#   4. Dest:   criu restore --lazy-pages
#   5. Cleanup both sides, next repeat
#
# Usage:
#   bash launch_pageserver_ablation.sh --all                  # all 9 workloads
#   bash launch_pageserver_ablation.sh matmul redis mc-16gb   # subset
#
# Env: REPEAT (default 5), AMI_ID, BUCKET, REGION
set -e

AMI_ID="${AMI_ID:-ami-0697e5cc271d1da64}"  # criu-workload-v6-20260425
INSTANCE_TYPE="m5.8xlarge"
KEY_NAME="mhsong-ddps-oregon"
SG="sg-0eb08e8fa10cb3031"
SUBNET="subnet-09c8aacd484cac3e2"
IAM_PROFILE="mhsong-ec2-admin"
REGION="us-west-2"
SSH_KEY="$HOME/.ssh/mhsong-ddps-oregon.pem"
REPEAT="${REPEAT:-5}"
BUCKET="${BUCKET:-mhsong-criu-results}"
WORKLOADS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --all)    WORKLOADS=(matmul dataproc ml-training xgboost redis mc-1gb mc-4gb mc-8gb mc-16gb); shift ;;
        --repeat) REPEAT="$2"; shift 2 ;;
        *)        WORKLOADS+=("$1"); shift ;;
    esac
done

[ ${#WORKLOADS[@]} -gt 0 ] || { echo "ERROR: provide --all or one or more workloads"; exit 1; }

AWS_KEY=$(aws configure get aws_access_key_id)
AWS_SECRET=$(aws configure get aws_secret_access_key)
[ -n "$AWS_KEY" ] || { echo "ERROR: aws creds missing"; exit 1; }

# workload → (criu workload type, extra args). Same table as launch_dump.sh
# / dump_all_workloads.sh — kept in sync so configs match.
workload_config() {
    case "$1" in
        matmul)      echo "matmul|--matrix-size 25000" ;;
        dataproc)    echo "dataproc|--num-rows 17000000 --num-cols 60 --batch-size 1000" ;;
        ml-training) echo "ml_training|--model-size large --dataset-size 2000000" ;;
        xgboost)     echo "xgboost|--dataset synthetic --num-samples 7000000 --num-features 100 --num-threads 3" ;;
        redis)       echo "redis|--record-count 5000000 --ycsb-threads 4 --ycsb-workload a" ;;
        mc-1gb)      echo "memcached|--memcached-memory 1024 --record-count 773000 --ycsb-threads 4" ;;
        mc-4gb)      echo "memcached|--memcached-memory 4096 --record-count 3100000 --ycsb-threads 4" ;;
        mc-8gb)      echo "memcached|--memcached-memory 8192 --record-count 6200000 --ycsb-threads 4 --ycsb-workload a" ;;
        mc-16gb)     echo "memcached|--memcached-memory 16384 --record-count 12400000 --ycsb-threads 4" ;;
        *) return 1 ;;
    esac
}

N=${#WORKLOADS[@]}
echo "=========================================="
echo " Page-server ablation: $N workloads, REPEAT=$REPEAT"
echo " AMI: $AMI_ID"
echo " (each pair = 2 m5.8xlarge instances)"
for w in "${WORKLOADS[@]}"; do echo "   $w"; done
echo "=========================================="

# Spawn 2*N instances at once (count*N would be cheaper than per-pair launches).
TOTAL=$((N * 2))
INSTANCE_IDS=$(aws ec2 run-instances \
    --region $REGION --image-id $AMI_ID --instance-type $INSTANCE_TYPE \
    --count $TOTAL --key-name $KEY_NAME \
    --security-group-ids $SG --subnet-id $SUBNET \
    --iam-instance-profile Name=$IAM_PROFILE \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":120,"VolumeType":"gp3"}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=criu-pageserver}]" \
    --query 'Instances[*].InstanceId' --output text)

echo "Instances: $INSTANCE_IDS"
aws ec2 wait instance-running --region $REGION --instance-ids $INSTANCE_IDS

# Split instances into pairs by index. First N = sources, next N = destinations.
ALL_IDS=($INSTANCE_IDS)
declare -a SRC_IDS DST_IDS SRC_IPS DST_IPS SRC_PRIVS DST_PRIVS
for i in $(seq 0 $((N - 1))); do
    SRC_IDS[$i]=${ALL_IDS[$i]}
    DST_IDS[$i]=${ALL_IDS[$((i + N))]}
done

for i in $(seq 0 $((N - 1))); do
    SRC_IPS[$i]=$(aws ec2 describe-instances --region $REGION --instance-ids ${SRC_IDS[$i]} \
        --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
    DST_IPS[$i]=$(aws ec2 describe-instances --region $REGION --instance-ids ${DST_IDS[$i]} \
        --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
    SRC_PRIVS[$i]=$(aws ec2 describe-instances --region $REGION --instance-ids ${SRC_IDS[$i]} \
        --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text)
    DST_PRIVS[$i]=$(aws ec2 describe-instances --region $REGION --instance-ids ${DST_IDS[$i]} \
        --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text)
done

wait_ssh() {
    local ip=$1
    for i in $(seq 1 30); do
        ssh -i $SSH_KEY -o ConnectTimeout=3 -o StrictHostKeyChecking=no ubuntu@$ip "echo ok" 2>/dev/null && return 0
        sleep 5
    done
    return 1
}

for i in $(seq 0 $((N - 1))); do
    WL=${WORKLOADS[$i]}
    cfg=$(workload_config "$WL") || { echo "skip unknown $WL"; continue; }
    IFS='|' read -r WTYPE EXTRA_ARGS <<< "$cfg"
    SRC_IP=${SRC_IPS[$i]}; DST_IP=${DST_IPS[$i]}
    SRC_PRIV=${SRC_PRIVS[$i]}; DST_PRIV=${DST_PRIVS[$i]}
    SRC_ID=${SRC_IDS[$i]}; DST_ID=${DST_IDS[$i]}

    echo ""
    echo "--- $WL  src=$SRC_IP ($SRC_ID)  dst=$DST_IP ($DST_ID) ---"

    aws ec2 create-tags --region $REGION --resources $SRC_ID \
        --tags "Key=Experiment,Value=pageserver-${WL}-src" "Key=Role,Value=source" 2>/dev/null || true
    aws ec2 create-tags --region $REGION --resources $DST_ID \
        --tags "Key=Experiment,Value=pageserver-${WL}-dst" "Key=Role,Value=destination" 2>/dev/null || true

    wait_ssh $SRC_IP || { echo "ERROR: src ssh timeout"; continue; }
    wait_ssh $DST_IP || { echo "ERROR: dst ssh timeout"; continue; }

    # Per-pair SSH bootstrap: cross-host trust + relaxed StrictHostKeyChecking
    # so paramiko / criu page-server flows don't fail on stale known_hosts.
    for ip in $SRC_IP $DST_IP; do
        ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$ip "
            rm -f ~/.ssh/known_hosts
            cat > ~/.ssh/config <<'EOF'
Host *
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
    LogLevel ERROR
EOF
            chmod 600 ~/.ssh/config
            ssh-keygen -t rsa -N '' -f ~/.ssh/id_rsa -q <<< y >/dev/null 2>&1 || true
            cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
            cp ~/.ssh/id_rsa ~/.ssh/id_ed25519 2>/dev/null || true
            sudo mkdir -p /root/.ssh
            sudo cp ~/.ssh/id_rsa /root/.ssh/id_rsa
            sudo cp ~/.ssh/id_rsa.pub /root/.ssh/id_rsa.pub
            sudo cp ~/.ssh/config /root/.ssh/config
            sudo chmod 600 /root/.ssh/id_rsa /root/.ssh/config
            sudo chown -R root:root /root/.ssh
        " 2>/dev/null
    done
    SRC_PUB=$(ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$SRC_IP "cat ~/.ssh/id_rsa.pub")
    DST_PUB=$(ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$DST_IP "cat ~/.ssh/id_rsa.pub")
    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$DST_IP "echo '$SRC_PUB' >> ~/.ssh/authorized_keys"
    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$SRC_IP "echo '$DST_PUB' >> ~/.ssh/authorized_keys"

    # Sync the workload tree to BOTH sides (rsync pattern from launch_dump.sh).
    for ip in $SRC_IP $DST_IP; do
        rsync -a -e "ssh -i $SSH_KEY -o StrictHostKeyChecking=no" \
            --exclude '__pycache__' --exclude '*.pyc' --exclude '.git' \
            /spot_kubernetes/criu_workload/lib \
            /spot_kubernetes/criu_workload/workloads \
            /spot_kubernetes/criu_workload/experiments \
            /spot_kubernetes/criu_workload/config \
            /spot_kubernetes/criu_workload/tools \
            ubuntu@$ip:/tmp/criu_workload_sync/ >/dev/null
    done

    # Driver on SOURCE: loops REPEAT times calling baseline_experiment.py
    # with --lazy-mode live-migration. Auto-terminates both instances at end.
    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$SRC_IP "cat > /tmp/driver.sh" <<DRIVER
#!/bin/bash
set +e
export AWS_ACCESS_KEY_ID='${AWS_KEY}'
export AWS_SECRET_ACCESS_KEY='${AWS_SECRET}'

echo "=== install criu_workload tree (lib workloads experiments config tools) ==="
for d in lib workloads experiments config tools; do
    if [ -d /tmp/criu_workload_sync/\$d ]; then
        sudo rsync -a --chown=ubuntu:ubuntu /tmp/criu_workload_sync/\$d/ /opt/criu_workload/\$d/
    fi
done
sudo chmod +x /opt/criu_workload/experiments/*.sh 2>/dev/null || true
cd /opt/criu_workload

# Mirror the same install on the destination (driver is on source but
# baseline_experiment.py paramiko-deploys workload code via SFTP; we
# still want the standalone scripts present on dest).
ssh -o StrictHostKeyChecking=no ubuntu@${DST_PRIV} "
    sudo rsync -a --chown=ubuntu:ubuntu /tmp/criu_workload_sync/lib/ /opt/criu_workload/lib/
    sudo rsync -a --chown=ubuntu:ubuntu /tmp/criu_workload_sync/workloads/ /opt/criu_workload/workloads/
    sudo rsync -a --chown=ubuntu:ubuntu /tmp/criu_workload_sync/experiments/ /opt/criu_workload/experiments/
    sudo rsync -a --chown=ubuntu:ubuntu /tmp/criu_workload_sync/config/ /opt/criu_workload/config/
    sudo rsync -a --chown=ubuntu:ubuntu /tmp/criu_workload_sync/tools/ /opt/criu_workload/tools/
    sudo chmod +x /opt/criu_workload/experiments/*.sh 2>/dev/null || true
" 2>&1 || true

TS=\$(date +%Y%m%d_%H%M%S)
OUTDIR=/tmp/results/pageserver_${WL}_\${TS}
mkdir -p \$OUTDIR

echo "=== [\$(date +%H:%M:%S)] page-server ablation: ${WL} (REPEAT=${REPEAT}) ==="

for run in \$(seq 1 ${REPEAT}); do
    echo ""
    echo "--- ${WL} run \$run/${REPEAT} (\$(date +%H:%M:%S)) ---"

    # Bump ns_last_pid on both sides to avoid PID collisions.
    sudo sh -c 'echo 10000 > /proc/sys/kernel/ns_last_pid' 2>/dev/null || true
    ssh -o StrictHostKeyChecking=no ubuntu@${DST_PRIV} \\
        "sudo sh -c 'echo 10000 > /proc/sys/kernel/ns_last_pid'" 2>/dev/null || true

    # Cleanup any leftover state from a previous repeat — process tree,
    # checkpoint dirs, lingering memcached/redis daemons.
    sudo pkill -9 -f "_standalone\.py|memcached -m|redis-server|criu page-server|criu lazy-pages" 2>/dev/null || true
    ssh -o StrictHostKeyChecking=no ubuntu@${DST_PRIV} \\
        "sudo pkill -9 -f '_standalone\.py|memcached -m|redis-server|criu lazy-pages|criu restore'" 2>/dev/null || true
    sudo rm -rf /tmp/criu_checkpoint 2>/dev/null
    ssh -o StrictHostKeyChecking=no ubuntu@${DST_PRIV} "sudo rm -rf /tmp/criu_checkpoint" 2>/dev/null
    sleep 2

    sudo -E python3 -u experiments/baseline_experiment.py \\
        --config config/experiments/memcached_lazy_prefetch.yaml \\
        --source-ip ${SRC_PRIV} --dest-ip ${DST_PRIV} \\
        --ssh-user ubuntu --workload ${WTYPE} \\
        --lazy-mode live-migration \\
        --wait-before-dump 90 --duration 86400 \\
        ${EXTRA_ARGS} \\
        -o \$OUTDIR/run\${run}.json \\
        > \$OUTDIR/run\${run}.log 2>&1
    echo "run \$run exit=\$?"

    # Pull artefacts produced by this repeat (overwrites; the JSON above
    # is what we'll aggregate).
    sudo cp /tmp/criu_checkpoint/1/criu-page-server.log \$OUTDIR/run\${run}_page-server.log 2>/dev/null || true
    sudo cp /tmp/criu_checkpoint/1/criu-dump.log         \$OUTDIR/run\${run}_dump.log         2>/dev/null || true
    scp -i ~/.ssh/id_rsa -o StrictHostKeyChecking=no \\
        ubuntu@${DST_PRIV}:/tmp/criu_checkpoint/1/criu-restore.log    \$OUTDIR/run\${run}_restore.log    2>/dev/null || true
    scp -i ~/.ssh/id_rsa -o StrictHostKeyChecking=no \\
        ubuntu@${DST_PRIV}:/tmp/criu_checkpoint/1/criu-lazy-pages.log \$OUTDIR/run\${run}_lazy-pages.log 2>/dev/null || true
done

sudo chown -R ubuntu:ubuntu \$OUTDIR

# Quick stdout summary so the driver log shows the per-rep duration.
echo "=== summary ==="
for run in \$(seq 1 ${REPEAT}); do
    python3 -c "
import json
try:
    d = json.load(open('\$OUTDIR/run\${run}.json'))
    lp = d.get('criu_metrics', {}).get('lazy_pages', {})
    rs = d.get('criu_metrics', {}).get('restore', {})
    fd = d.get('final_dump', {})
    print('  run \${run}: dump=%.2fs daemon=%.2fs faults=%s' % (
        fd.get('duration', -1) or -1,
        lp.get('daemon_duration_s', -1) or -1,
        lp.get('uffd_faults','?')))
except Exception as e:
    print('  run \${run}: parse failed (\${e})')
" 2>&1
done

# Upload all artefacts to S3 — single canonical location per (workload, ts).
S3_DEST="s3://${BUCKET}/pageserver_${WL}_raw/\${TS}/"
aws s3 sync \$OUTDIR \$S3_DEST --region us-west-2 --quiet
echo "uploaded → \$S3_DEST"

# Auto-terminate BOTH instances now that the dest no longer needs the
# source page-server. The driver runs from src; terminate dst first so
# its SSH session quiesces, then src terminates itself.
echo "Auto-terminating dst=${DST_ID} src=${SRC_ID}"
aws ec2 terminate-instances --region us-west-2 --instance-ids ${DST_ID} ${SRC_ID} 2>&1 | tail -5
DRIVER

    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$SRC_IP "
        chmod +x /tmp/driver.sh
        nohup bash /tmp/driver.sh > /tmp/driver.log 2>&1 &
        disown
        echo PID \$!
    " 2>&1
    echo "   $WL launched on $SRC_IP (driver) → s3://${BUCKET}/pageserver_${WL}_raw/"
done

echo ""
echo "=========================================="
echo " $N pairs launched ($TOTAL instances, each pair auto-terminates)"
for i in $(seq 0 $((N - 1))); do
    echo "  ${WORKLOADS[$i]}: ssh -i $SSH_KEY ubuntu@${SRC_IPS[$i]} 'tail -f /tmp/driver.log'   # src=${SRC_IDS[$i]} dst=${DST_IDS[$i]}"
done
