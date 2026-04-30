#!/bin/bash
# launch_workload_profile.sh — measure actual CPU + memory utilisation of
# each workload on m5.xlarge (4 vCPU, 16 GB RAM), the paper's reference
# instance. Six workloads run in parallel, one per m5.xlarge; each records
# `top -b` samples + /proc/<pid>/status snapshots for 10 minutes of steady
# state (after warmup), then uploads and self-terminates.
#
# Usage:  bash launch_workload_profile.sh
#            [--workloads 'matmul dataproc ml-training xgboost redis mc-11gb']
# Env:    BUCKET, REGION, DURATION_SEC (default 600)

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

DEFAULT_WLS="matmul dataproc ml-training xgboost redis mc-11gb"
WORKLOADS=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --workloads) WORKLOADS="$2"; shift 2 ;;
        *) echo "unknown arg: $1"; exit 1 ;;
    esac
done
WORKLOADS="${WORKLOADS:-$DEFAULT_WLS}"

workload_runner() {
    case "$1" in
        matmul)       echo "matmul_standalone.py|--matrix-size 2048" ;;
        dataproc)     echo "dataproc_standalone.py|--num-rows 1500000 --num-cols 60 --batch-size 1000" ;;
        ml-training)  echo "ml_training_standalone.py|--model-size large --dataset-size 50000" ;;
        xgboost)      echo "xgboost_standalone.py|--dataset synthetic --num-samples 7000000 --num-features 100 --num-threads 3" ;;
        redis)        echo "redis_standalone.py|--record-count 5000000 --ycsb-threads 4 --ycsb-workload a" ;;
        mc-11gb)      echo "memcached_standalone.py|--memcached-memory 11264 --record-count 8500000 --ycsb-threads 4" ;;
        *) return 1 ;;
    esac
}

AWS_KEY=$(aws configure get aws_access_key_id)
AWS_SECRET=$(aws configure get aws_secret_access_key)

TS=$(date -u +%Y%m%dT%H%M%SZ)
echo "=========================================="
echo " Workload profile on m5.xlarge (4 vCPU, 16 GB)"
echo " Workloads: $WORKLOADS"
echo " Sampling duration: ${DURATION}s"
echo "=========================================="

wait_ssh() {
    for i in $(seq 1 40); do
        ssh -i $SSH_KEY -o ConnectTimeout=3 -o StrictHostKeyChecking=no ubuntu@$1 "echo ok" 2>/dev/null && return 0
        sleep 5
    done
    return 1
}

# Spawn one instance per workload sequentially. This avoids array/IFS bugs
# that multi-instance launches hit; the serial cost is trivial (~30s each)
# because the real work runs in parallel after all drivers are kicked off.
for wl in $WORKLOADS; do
    cfg=$(workload_runner "$wl") || { echo "ERROR: unknown workload $wl"; continue; }
    IFS='|' read -r RUNNER EXTRA_ARGS <<< "$cfg"

    echo ""
    echo "--- spawning $wl ---"
    IID=$(aws ec2 run-instances \
        --region $REGION --image-id $AMI_ID --instance-type $INSTANCE_TYPE \
        --count 1 --key-name $KEY_NAME \
        --security-group-ids $SG --subnet-id $SUBNET \
        --iam-instance-profile Name=$IAM_PROFILE \
        --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":120,"VolumeType":"gp3"}}]' \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=workload-profile},{Key=Experiment,Value=profile-${wl}}]" \
        --query 'Instances[0].InstanceId' --output text)
    aws ec2 wait instance-running --region $REGION --instance-ids "$IID"
    IP=$(aws ec2 describe-instances --region $REGION --instance-ids "$IID" \
        --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
    echo "  $wl -> $IID @ $IP"

    if ! wait_ssh $IP; then
        echo "  SSH timeout — terminating $IID"
        aws ec2 terminate-instances --region $REGION --instance-ids "$IID" >/dev/null 2>&1
        continue
    fi

    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "
        cd /opt/criu_workload && git pull origin main -q 2>/dev/null || true
    " 2>/dev/null

    # Ship per-instance env via plain shell-variable file. Quoted heredoc
    # below (<<'DRIVER') keeps the rest verbatim so remote $vars don't get
    # eaten by the local shell.
    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "cat > /tmp/profile_env.sh" <<ENVFILE
export AWS_ACCESS_KEY_ID='${AWS_KEY}'
export AWS_SECRET_ACCESS_KEY='${AWS_SECRET}'
export AWS_DEFAULT_REGION='${REGION}'
export BUCKET='${BUCKET}'
export REGION='${REGION}'
export TS='${TS}'
export WL='${wl}'
export RUNNER='${RUNNER}'
export EXTRA_ARGS='${EXTRA_ARGS}'
export DURATION='${DURATION}'
ENVFILE

    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "cat > /tmp/driver.sh" <<'DRIVER'
#!/bin/bash
set +e
source /tmp/profile_env.sh

OUTDIR=/tmp/profile_${WL}_${TS}
mkdir -p "$OUTDIR"
LOG="$OUTDIR/driver.log"

# Always terminate on exit (success, error, or abort) so we never leak
# instances on partial failures.
self_terminate() {
    echo "=== [$(date +%H:%M:%S)] terminating ===" >> "$LOG"
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
cat /proc/meminfo | head -5 > "$OUTDIR/meminfo_start.txt" 2>&1

cd /opt/criu_workload
WORKDIR=/tmp/profile_${WL}_workdir
mkdir -p "$WORKDIR"

# Launch workload in background (setsid so it survives SSH session end).
setsid python3 -u workloads/${RUNNER} ${EXTRA_ARGS} --working_dir "$WORKDIR" \
    > "$OUTDIR/workload.log" 2>&1 &
WL_PID=$!
echo "workload PID=$WL_PID" >> "$LOG"

# Wait for readiness marker; workload writes checkpoint_ready once steady.
READY=0
for i in $(seq 1 60); do
    if [ -f "$WORKDIR/checkpoint_ready" ]; then READY=1; break; fi
    if ! kill -0 "$WL_PID" 2>/dev/null; then
        echo "ERROR: workload exited during warmup" >> "$LOG"
        exit 1
    fi
    sleep 5
done
if [ "$READY" -eq 0 ]; then
    echo "WARN: checkpoint_ready never appeared (workload may not be steady)" >> "$LOG"
fi

SAMPLE_INTERVAL=5
N_SAMPLES=$(( DURATION / SAMPLE_INTERVAL ))
echo "sampling $N_SAMPLES x ${SAMPLE_INTERVAL}s" >> "$LOG"

# `top -b` captures per-process CPU/mem (plus header for overall).
top -b -d "$SAMPLE_INTERVAL" -n "$N_SAMPLES" -p "$WL_PID" > "$OUTDIR/top.log" 2>&1 &
TOP_PID=$!

# System-wide CPU/mem delta samples derived from /proc/stat + /proc/meminfo.
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

# Per-process VM stats (main PID + process tree).
# Tree sum matters for workloads that spawn server processes (redis-server,
# memcached); main-PID alone would miss the real memory consumer.
#
# main_vmrss_kb  : just WL_PID (python driver)
# tree_vmrss_kb  : WL_PID + all descendants summed
# biggest_pid    : PID with the largest RSS in the tree (often the real worker)
# biggest_rss_kb : RSS of biggest_pid
(
    echo "ts,main_vmrss_kb,tree_vmrss_kb,biggest_pid,biggest_rss_kb,biggest_comm"
    while kill -0 "$TOP_PID" 2>/dev/null && kill -0 "$WL_PID" 2>/dev/null; do
        # Collect WL_PID + all descendants (breadth-first via pgrep -P).
        PIDS=("$WL_PID")
        QUEUE=("$WL_PID")
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

# Build summary JSON. Run python with OUTDIR + WL explicitly exported so
# the heredoc script can read them from os.environ. (Without the prefix,
# OUTDIR is a shell-local var and python sees nothing → empty summary.)
OUTDIR="$OUTDIR" WL="$WL" python3 - > "$OUTDIR/summary.json" <<'PYEOF'
import csv, json, os, statistics as stat
OUTDIR = os.environ["OUTDIR"]
wl     = os.environ["WL"]
out = {"workload": wl,
       "nproc": int(open(f"{OUTDIR}/cpu_count.txt").read().strip())}

sys_rows = list(csv.DictReader(open(f"{OUTDIR}/system.csv")))
out["n_samples"] = len(sys_rows)
def col(key):
    return [float(r[key]) for r in sys_rows if r.get(key)]
cpu_u = col("cpu_user_pct"); cpu_s = col("cpu_sys_pct")
mem   = col("mem_used_pct")
if cpu_u and cpu_s:
    tot = [u + v for u, v in zip(cpu_u, cpu_s)]
    out["cpu_user_pct_mean"]       = round(stat.mean(cpu_u), 1)
    out["cpu_user_plus_sys_mean"]  = round(stat.mean(tot), 1)
    out["cpu_user_plus_sys_p95"]   = round(sorted(tot)[int(0.95*len(tot))], 1)
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
# Record which single process was the largest RSS consumer — this is the
# "real" workload process for redis/memcached, where WL_PID is only a thin
# python driver.
biggest = [r for r in proc_rows if r.get("biggest_comm", "?") not in ("?", "")]
if biggest:
    # Mode of (comm) to find the dominant process
    from collections import Counter
    c = Counter(r["biggest_comm"] for r in biggest)
    out["dominant_process"] = c.most_common(1)[0][0]
    rss_vals = [int(r["biggest_rss_kb"]) for r in biggest
                if r["biggest_comm"] == out["dominant_process"]
                and r["biggest_rss_kb"].isdigit()]
    if rss_vals:
        out["dominant_rss_mb_peak"] = round(max(rss_vals) / 1024, 1)
        out["dominant_rss_mb_mean"] = round(stat.mean(rss_vals) / 1024, 1)

out["cpu_used_pct"] = out.get("cpu_user_plus_sys_mean", 0)
out["mem_used_pct"] = out.get("mem_used_pct_mean",       0)
out["over_70pct_threshold"] = (out["cpu_used_pct"] >= 70
                               or out["mem_used_pct"] >= 70)
print(json.dumps(out, indent=2))
PYEOF

cat "$OUTDIR/summary.json" >> "$LOG"

kill "$WL_PID" 2>/dev/null
sleep 2

aws s3 cp --recursive "$OUTDIR" "s3://$BUCKET/bench-workload-profile/$TS/$WL/" \
    --region "$REGION" --only-show-errors
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
echo " $N workloads launched (auto-terminate)"
echo " Results: s3://$BUCKET/bench-workload-profile/$TS/<workload>/summary.json"
echo "=========================================="
