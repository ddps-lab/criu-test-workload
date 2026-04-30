#!/bin/bash
# launch_workload_sweep.sh — parameter sweep for workload CPU/mem profiling.
# Spawns one m5.xlarge per (tag, workload, param) triple. Each instance
# runs the same driver as launch_workload_profile.sh but with the override
# (TAG, RUNNER, EXTRA_ARGS) passed via env.
#
# Used to tune matmul / dataproc / ml-training parameters so the "each
# workload uses ≥70% of m5.xlarge" design target holds.
#
# Usage:  bash launch_workload_sweep.sh

set -e

AMI_ID="${AMI_ID:-ami-0fd8cddbe746f93aa}"
INSTANCE_TYPE="${INSTANCE_TYPE:-m5.xlarge}"
KEY_NAME="mhsong-ddps-oregon"
SG="sg-0eb08e8fa10cb3031"
SUBNET="subnet-09c8aacd484cac3e2"
IAM_PROFILE="mhsong-ec2-admin"
REGION="${REGION:-us-west-2}"
SSH_KEY="$HOME/.ssh/mhsong-ddps-oregon.pem"
BUCKET="${BUCKET:-mhsong-criu-checkpoints}"
DURATION="${DURATION_SEC:-600}"

TS=$(date -u +%Y%m%dT%H%M%SZ)

# Config list: tag | runner script | extra args
# tag is used as S3 path suffix so multiple configs per workload coexist.
CONFIGS=(
    "memcached-m8gb|memcached_standalone.py|--memory-mb 8192 --record-count 6200000 --ycsb-threads 4 --ycsb-workload a"
    "redis-r5M|redis_standalone.py|--record-count 5000000 --ycsb-threads 4 --ycsb-workload a"
    "xgboost-s7M|xgboost_standalone.py|--dataset synthetic --num-samples 7000000 --num-features 100 --num-threads 3"
)

AWS_KEY=$(aws configure get aws_access_key_id)
AWS_SECRET=$(aws configure get aws_secret_access_key)

echo "=========================================="
echo " Workload parameter sweep on $INSTANCE_TYPE"
echo " TS: $TS"
echo " ${#CONFIGS[@]} configs × ${DURATION}s sampling"
echo "=========================================="

wait_ssh() {
    for i in $(seq 1 40); do
        ssh -i $SSH_KEY -o ConnectTimeout=3 -o StrictHostKeyChecking=no ubuntu@$1 "echo ok" 2>/dev/null && return 0
        sleep 5
    done
    return 1
}

for entry in "${CONFIGS[@]}"; do
    IFS='|' read -r TAG RUNNER EXTRA <<< "$entry"

    echo ""
    echo "--- spawning $TAG ---"
    IID=$(aws ec2 run-instances \
        --region $REGION --image-id $AMI_ID --instance-type $INSTANCE_TYPE \
        --count 1 --key-name $KEY_NAME \
        --security-group-ids $SG --subnet-id $SUBNET \
        --iam-instance-profile Name=$IAM_PROFILE \
        --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":120,"VolumeType":"gp3"}}]' \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=workload-sweep},{Key=Experiment,Value=sweep-${TAG}}]" \
        --query 'Instances[0].InstanceId' --output text)
    aws ec2 wait instance-running --region $REGION --instance-ids "$IID"
    IP=$(aws ec2 describe-instances --region $REGION --instance-ids "$IID" \
        --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
    echo "  $TAG -> $IID @ $IP"

    if ! wait_ssh $IP; then
        echo "  SSH timeout — terminating $IID"
        aws ec2 terminate-instances --region $REGION --instance-ids "$IID" >/dev/null 2>&1
        continue
    fi

    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "
        cd /opt/criu_workload && git pull origin main -q 2>/dev/null || true
    " 2>/dev/null

    # Ship the locally-modified workload runner (covers uncommitted fixes
    # like the dataproc redundant .astype() bug). Falls back gracefully if
    # RUNNER filename isn't in workloads/.
    LOCAL_RUNNER="/spot_kubernetes/criu_workload/workloads/$RUNNER"
    if [ -f "$LOCAL_RUNNER" ]; then
        scp -i $SSH_KEY -o StrictHostKeyChecking=no "$LOCAL_RUNNER" \
            ubuntu@$IP:/tmp/$RUNNER >/dev/null
        ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "
            sudo install -m 0644 -o ubuntu -g ubuntu /tmp/$RUNNER \
                /opt/criu_workload/workloads/$RUNNER
        " 2>/dev/null
    fi

    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "cat > /tmp/profile_env.sh" <<ENVFILE
export AWS_ACCESS_KEY_ID='${AWS_KEY}'
export AWS_SECRET_ACCESS_KEY='${AWS_SECRET}'
export AWS_DEFAULT_REGION='${REGION}'
export BUCKET='${BUCKET}'
export REGION='${REGION}'
export TS='${TS}'
export WL='${TAG}'
export RUNNER='${RUNNER}'
export EXTRA_ARGS='${EXTRA}'
export DURATION='${DURATION}'
ENVFILE

    # Same driver template as launch_workload_profile.sh — already hardened
    # with process-tree RSS + OUTDIR/WL export + self_terminate trap.
    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "cat > /tmp/driver.sh" <<'DRIVER'
#!/bin/bash
set +e
source /tmp/profile_env.sh

OUTDIR=/tmp/profile_${WL}_${TS}
mkdir -p "$OUTDIR"
LOG="$OUTDIR/driver.log"

self_terminate() {
    echo "=== [$(date +%H:%M:%S)] terminating ===" >> "$LOG"
    aws s3 cp --recursive "$OUTDIR" "s3://$BUCKET/bench-workload-sweep/$TS/$WL/" \
        --region "$REGION" --only-show-errors 2>/dev/null
    TOKEN=$(curl -sX PUT http://169.254.169.254/latest/api/token \
        -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')
    INSTANCE_ID=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" \
        http://169.254.169.254/latest/meta-data/instance-id)
    aws ec2 terminate-instances --instance-ids "$INSTANCE_ID" --region "$REGION" >/dev/null 2>&1
}
trap self_terminate EXIT

echo "=== [$(date +%H:%M:%S)] start $WL ===" > "$LOG"
nproc > "$OUTDIR/cpu_count.txt" 2>&1
free -m > "$OUTDIR/free_mem.txt" 2>&1

cd /opt/criu_workload
WORKDIR=/tmp/profile_${WL}_workdir
mkdir -p "$WORKDIR"

setsid python3 -u workloads/${RUNNER} ${EXTRA_ARGS} --working_dir "$WORKDIR" \
    > "$OUTDIR/workload.log" 2>&1 &
WL_PID=$!
echo "workload PID=$WL_PID cmd=workloads/${RUNNER} ${EXTRA_ARGS}" >> "$LOG"

READY=0
for i in $(seq 1 60); do
    if [ -f "$WORKDIR/checkpoint_ready" ]; then READY=1; break; fi
    if ! kill -0 "$WL_PID" 2>/dev/null; then
        echo "ERROR: workload exited during warmup" >> "$LOG"
        exit 1
    fi
    sleep 5
done
[ "$READY" -eq 0 ] && echo "WARN: checkpoint_ready never appeared" >> "$LOG"

SAMPLE_INTERVAL=5
N_SAMPLES=$(( DURATION / SAMPLE_INTERVAL ))
echo "sampling $N_SAMPLES x ${SAMPLE_INTERVAL}s" >> "$LOG"

top -b -d "$SAMPLE_INTERVAL" -n "$N_SAMPLES" -p "$WL_PID" > "$OUTDIR/top.log" 2>&1 &
TOP_PID=$!

(
    echo "ts,cpu_user_pct,cpu_sys_pct,cpu_iowait_pct,mem_used_pct,mem_cached_pct"
    PREV_TOTAL=0; PREV_USER=0; PREV_SYS=0; PREV_IOWAIT=0
    while kill -0 "$TOP_PID" 2>/dev/null; do
        read -r cpu user nice sys idle iowait irq softirq rest < /proc/stat
        TOTAL=$(( user + nice + sys + idle + iowait + irq + softirq ))
        if [ "$PREV_TOTAL" -ne 0 ]; then
            DT=$(( TOTAL - PREV_TOTAL ))
            DU=$(( user - PREV_USER ))
            DS=$(( sys - PREV_SYS ))
            DI=$(( iowait - PREV_IOWAIT ))
            if [ "$DT" -gt 0 ]; then
                MEM_TOTAL=$(awk '/^MemTotal:/{print $2}' /proc/meminfo)
                MEM_FREE=$(awk '/^MemFree:/{print $2}' /proc/meminfo)
                MEM_BUFF=$(awk '/^Buffers:/{print $2}' /proc/meminfo)
                MEM_CACHED=$(awk '/^Cached:/{print $2}' /proc/meminfo)
                MEM_SLAB=$(awk '/^SReclaimable:/{print $2}' /proc/meminfo)
                MEM_USED=$(( MEM_TOTAL - MEM_FREE - MEM_BUFF - MEM_CACHED - MEM_SLAB ))
                python3 - <<PY
MT, MU, MC = $MEM_TOTAL, $MEM_USED, $MEM_CACHED
DT, DU, DS, DI = $DT, $DU, $DS, $DI
import time
print(f"{int(time.time())},{DU*100/DT:.1f},{DS*100/DT:.1f},{DI*100/DT:.1f},{MU*100/MT:.1f},{MC*100/MT:.1f}")
PY
            fi
        fi
        PREV_TOTAL=$TOTAL; PREV_USER=$user; PREV_SYS=$sys; PREV_IOWAIT=$iowait
        sleep "$SAMPLE_INTERVAL"
    done
) > "$OUTDIR/system.csv" &

# Process tree RSS: main_vmrss / tree_vmrss / biggest process RSS.
(
    echo "ts,main_vmrss_kb,tree_vmrss_kb,biggest_pid,biggest_rss_kb,biggest_comm"
    while kill -0 "$TOP_PID" 2>/dev/null && kill -0 "$WL_PID" 2>/dev/null; do
        PIDS=("$WL_PID"); QUEUE=("$WL_PID")
        while [ ${#QUEUE[@]} -gt 0 ]; do
            next=${QUEUE[0]}; QUEUE=("${QUEUE[@]:1}")
            for c in $(pgrep -P "$next" 2>/dev/null); do
                PIDS+=("$c"); QUEUE+=("$c")
            done
        done
        TREE=0; MAIN=0; BPID=0; BRSS=0; BCOMM="?"
        for pid in "${PIDS[@]}"; do
            r=$(awk '/^VmRSS:/{print $2}' /proc/$pid/status 2>/dev/null)
            [ -z "$r" ] && continue
            TREE=$((TREE + r))
            [ "$pid" = "$WL_PID" ] && MAIN=$r
            if [ "$r" -gt "$BRSS" ]; then
                BRSS=$r; BPID=$pid
                BCOMM=$(awk '/^Name:/{print $2}' /proc/$pid/status 2>/dev/null)
            fi
        done
        echo "$(date +%s),$MAIN,$TREE,$BPID,$BRSS,${BCOMM:-?}"
        sleep "$SAMPLE_INTERVAL"
    done
) > "$OUTDIR/process.csv" &

wait "$TOP_PID" 2>/dev/null

OUTDIR="$OUTDIR" WL="$WL" RUNNER="$RUNNER" EXTRA="$EXTRA_ARGS" python3 - > "$OUTDIR/summary.json" <<'PYEOF'
import csv, json, os, statistics as stat
from collections import Counter
OUTDIR = os.environ["OUTDIR"]
out = {"tag": os.environ["WL"],
       "runner": os.environ["RUNNER"],
       "extra_args": os.environ["EXTRA"],
       "nproc": int(open(f"{OUTDIR}/cpu_count.txt").read().strip())}
sys_rows = list(csv.DictReader(open(f"{OUTDIR}/system.csv")))
out["n_samples"] = len(sys_rows)
def col(key): return [float(r[key]) for r in sys_rows if r.get(key)]
cpu_u = col("cpu_user_pct"); cpu_s = col("cpu_sys_pct"); mem = col("mem_used_pct")
if cpu_u and cpu_s:
    tot = [u + v for u, v in zip(cpu_u, cpu_s)]
    out["cpu_user_pct_mean"]      = round(stat.mean(cpu_u), 1)
    out["cpu_user_plus_sys_mean"] = round(stat.mean(tot), 1)
    out["cpu_user_plus_sys_p95"]  = round(sorted(tot)[int(0.95*len(tot))], 1)
if mem:
    out["mem_used_pct_mean"] = round(stat.mean(mem), 1)
    out["mem_used_pct_peak"] = round(max(mem), 1)

proc_rows = list(csv.DictReader(open(f"{OUTDIR}/process.csv")))
main_rss = [int(r["main_vmrss_kb"]) for r in proc_rows if r["main_vmrss_kb"].isdigit()]
tree_rss = [int(r["tree_vmrss_kb"]) for r in proc_rows if r["tree_vmrss_kb"].isdigit()]
if main_rss:
    out["main_vmrss_mb_peak"] = round(max(main_rss) / 1024, 1)
    out["main_vmrss_mb_mean"] = round(stat.mean(main_rss) / 1024, 1)
if tree_rss:
    out["tree_vmrss_mb_peak"] = round(max(tree_rss) / 1024, 1)
    out["tree_vmrss_mb_mean"] = round(stat.mean(tree_rss) / 1024, 1)
biggest = [r for r in proc_rows if r.get("biggest_comm", "?") not in ("?", "")]
if biggest:
    c = Counter(r["biggest_comm"] for r in biggest)
    out["dominant_process"] = c.most_common(1)[0][0]
    rss_vals = [int(r["biggest_rss_kb"]) for r in biggest
                if r["biggest_comm"] == out["dominant_process"]
                and r["biggest_rss_kb"].isdigit()]
    if rss_vals:
        out["dominant_rss_mb_peak"] = round(max(rss_vals) / 1024, 1)
        out["dominant_rss_mb_mean"] = round(stat.mean(rss_vals) / 1024, 1)

# ≥70% threshold on m5.xlarge = CPU ≥ 70% user+sys OR tree_rss ≥ 11.2 GB
cpu_ok = out.get("cpu_user_plus_sys_mean", 0) >= 70
mem_ok = (out.get("tree_vmrss_mb_mean", 0) >= 11.2 * 1024)
out["over_70pct_threshold"] = cpu_ok or mem_ok
out["cpu_ok"] = cpu_ok
out["mem_ok"] = mem_ok
print(json.dumps(out, indent=2))
PYEOF

kill "$WL_PID" 2>/dev/null
sleep 2
DRIVER

    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "
        chmod +x /tmp/driver.sh
        nohup bash /tmp/driver.sh > /tmp/driver_outer.log 2>&1 &
        disown
    "
    echo "  launched: ssh -i $SSH_KEY ubuntu@$IP 'tail -f /tmp/driver_outer.log'  # $IID"
done

echo ""
echo "=========================================="
echo " ${#CONFIGS[@]} configs launched (auto-terminate)"
echo " Results: s3://$BUCKET/bench-workload-sweep/$TS/<tag>/summary.json"
echo "=========================================="
