#!/bin/bash
# launch_curlm_bench.sh — single-shot EC2 spot benchmark for CURLM upload_pool.
#
# Spins up 1 m5.8xlarge spot in us-west-2, installs the new CURLM-enabled
# criu binary, allocates a 2 GB bytearray workload, then dumps it four
# times with --upload-workers 1 / 4 / 8 / 16. Reports wall time,
# pages-1.img size on S3, and effective MB/s for each. Self-terminates.
#
# Env:
#   CRIU_SRC (default /spot_kubernetes/criu_build/criu-s3)
#   WL_SIZE_GB (default 2)
#   BUCKET (default mhsong-criu-checkpoints)
#   REPEATS (default 2)
set -e

AMI_ID="${AMI_ID:-ami-0fd8cddbe746f93aa}"
INSTANCE_TYPE="${INSTANCE_TYPE:-m5.8xlarge}"
KEY_NAME="mhsong-ddps-oregon"
SG="sg-0eb08e8fa10cb3031"
SUBNET="subnet-09c8aacd484cac3e2"
IAM_PROFILE="mhsong-ec2-admin"
REGION="us-west-2"
SSH_KEY="$HOME/.ssh/mhsong-ddps-oregon.pem"
CRIU_SRC="${CRIU_SRC:-/spot_kubernetes/criu_build/criu-s3}"
BUCKET="${BUCKET:-mhsong-criu-checkpoints}"
WL_SIZE_GB="${WL_SIZE_GB:-2}"
REPEATS="${REPEATS:-2}"
PREFIX_BASE="curlm-bench"

[ -x "${CRIU_SRC}/criu/criu" ] || { echo "ERROR: ${CRIU_SRC}/criu/criu not built"; exit 1; }

AWS_KEY=$(aws configure get aws_access_key_id)
AWS_SECRET=$(aws configure get aws_secret_access_key)
[ -n "$AWS_KEY" ] || { echo "ERROR: aws creds missing"; exit 1; }

echo "=========================================="
echo " CURLM bench (spot m5.8xlarge, us-west-2)"
echo " Workload: ${WL_SIZE_GB} GB bytearray"
echo " Workers tested: 1 4 8 16 (x$REPEATS each)"
echo " criu: $(${CRIU_SRC}/criu/criu --version 2>&1 | grep -i gitid)"
echo "=========================================="

# Spot request, fall back to on-demand if spot capacity unavailable.
# Spot interruption is annoying for a ~15 min bench; on-demand at
# $1.53/hr * 0.25h ~= $0.40 is acceptable.
RES=$(aws ec2 run-instances \
    --region $REGION --image-id $AMI_ID --instance-type $INSTANCE_TYPE \
    --count 1 --key-name $KEY_NAME \
    --security-group-ids $SG --subnet-id $SUBNET \
    --iam-instance-profile Name=$IAM_PROFILE \
    --instance-market-options 'MarketType=spot,SpotOptions={SpotInstanceType=one-time,InstanceInterruptionBehavior=terminate}' \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":120,"VolumeType":"gp3"}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=curlm-bench},{Key=Experiment,Value=curlm-bench}]" \
    --query 'Instances[0].InstanceId' --output text 2>/dev/null) || \
RES=$(aws ec2 run-instances \
    --region $REGION --image-id $AMI_ID --instance-type $INSTANCE_TYPE \
    --count 1 --key-name $KEY_NAME \
    --security-group-ids $SG --subnet-id $SUBNET \
    --iam-instance-profile Name=$IAM_PROFILE \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":120,"VolumeType":"gp3"}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=curlm-bench},{Key=Experiment,Value=curlm-bench}]" \
    --query 'Instances[0].InstanceId' --output text)

IID=$RES
echo "Instance: $IID"
aws ec2 wait instance-running --region $REGION --instance-ids $IID
IP=$(aws ec2 describe-instances --region $REGION --instance-ids $IID \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
echo "IP: $IP"

# Wait SSH
for i in $(seq 1 40); do
    ssh -i $SSH_KEY -o ConnectTimeout=3 -o StrictHostKeyChecking=no ubuntu@$IP "echo ok" 2>/dev/null && break
    sleep 5
done

echo "   uploading criu binary..."
scp -i $SSH_KEY -o StrictHostKeyChecking=no \
    "${CRIU_SRC}/criu/criu" ubuntu@$IP:/tmp/criu.curlm >/dev/null

# Build driver
cat > /tmp/curlm_driver.sh <<DRIVER
#!/bin/bash
set +e
export AWS_ACCESS_KEY_ID='${AWS_KEY}'
export AWS_SECRET_ACCESS_KEY='${AWS_SECRET}'
export AWS_DEFAULT_REGION='${REGION}'
BUCKET='${BUCKET}'
REGION='${REGION}'
PREFIX_BASE='${PREFIX_BASE}'
WL_SIZE_GB='${WL_SIZE_GB}'
REPEATS='${REPEATS}'

echo "=== install criu ==="
sudo install -m 0755 /tmp/criu.curlm /usr/local/sbin/criu
/usr/local/sbin/criu --version

# Write smoketest workload
cat > /tmp/smoketest.py <<'EOF'
import os, time, sys
# Mix of pseudo-random (~45%) and compressible pattern (~55%) so zstd-1
# ratio approximates real CRIU workloads (YCSB/string ~0.55-0.60,
# float ML ~0.90). All-zeros bytearray would have compressed to near-
# zero and made compressed-upload throughput meaningless to measure.
N = int(sys.argv[1]) * 1024 * 1024 * 1024  # GB
half = N // 2
buf = bytearray(os.urandom(half) + b"ABCDEFGH_HELLO_WORLD_SPOT_CRIU_" * (half // 30 + 1))
buf = buf[:N]
for i in range(0, N, 4096):
    buf[i] ^= 0x1
print(f"PID={os.getpid()} allocated {N/1024/1024/1024:.2f} GB (mixed)", flush=True)
while True:
    time.sleep(1)
EOF

# Run one dump with given workers
run_one() {
    local W=\$1
    local tag=\$2

    # Spawn workload via systemd-run (detached, null stdio)
    sudo systemctl stop smoke-wl 2>/dev/null
    sudo systemctl reset-failed smoke-wl 2>/dev/null
    sleep 1
    sudo systemd-run --unit=smoke-wl --service-type=simple --no-block \\
        --property=StandardOutput=null --property=StandardError=null --property=StandardInput=null \\
        python3 /tmp/smoketest.py \$WL_SIZE_GB >/dev/null 2>&1

    # Wait for workload to reach target RSS
    local target_kb=\$((WL_SIZE_GB * 1024 * 1024 - 50000))
    for i in \$(seq 1 120); do
        sleep 2
        local WL_PID=\$(systemctl show -p MainPID --value smoke-wl 2>/dev/null)
        [ "\$WL_PID" = "0" ] && continue
        local RSS=\$(sudo awk '/VmRSS/{print \$2}' /proc/\$WL_PID/status 2>/dev/null)
        [ "\${RSS:-0}" -ge "\$target_kb" ] && { echo "wl-ready pid=\$WL_PID rss=\${RSS}KB"; break; }
    done

    local WL_PID=\$(systemctl show -p MainPID --value smoke-wl)
    local RSS=\$(sudo awk '/VmRSS/{print \$2}' /proc/\$WL_PID/status 2>/dev/null)
    sudo rm -rf /tmp/dump_curlm/*
    sudo mkdir -p /tmp/dump_curlm

    local T0=\$(date +%s.%N)
    sudo /usr/local/sbin/criu dump \\
        -D /tmp/dump_curlm -t \$WL_PID --track-mem \\
        --log-file /tmp/dump_curlm/criu-dump.log -v3 \\
        --object-storage-upload \\
        --object-storage-endpoint-url https://s3.us-west-2.amazonaws.com \\
        --object-storage-bucket \$BUCKET \\
        --object-storage-object-prefix \${PREFIX_BASE}-\${tag}/ \\
        --aws-access-key \$AWS_ACCESS_KEY_ID \\
        --aws-secret-key \$AWS_SECRET_ACCESS_KEY \\
        --aws-region \$REGION \\
        --upload-workers \$W >/dev/null 2>&1
    local RC=\$?
    local T1=\$(date +%s.%N)
    local DUR=\$(python3 -c "print(round(\$T1 - \$T0, 3))")
    local SZ=\$(aws s3 ls s3://\$BUCKET/\${PREFIX_BASE}-\${tag}/pages-1.img --region \$REGION 2>/dev/null | awk '{print \$3}')
    local MBPS="-"
    if [ -n "\$SZ" ] && [ "\$(python3 -c 'print(1 if '\$DUR'>0 else 0)')" = "1" ]; then
        MBPS=\$(python3 -c "print(round((\$SZ/1024/1024)/\$DUR, 1))")
    fi
    echo "RESULT workers=\$W rc=\$RC pid=\$WL_PID rss=\${RSS}KB dur=\${DUR}s pages1=\${SZ}B mbps=\${MBPS}"
    # Clean up S3 prefix after recording
    aws s3 rm s3://\$BUCKET/\${PREFIX_BASE}-\${tag}/ --region \$REGION --recursive --quiet >/dev/null 2>&1 &
}

# Raw with variable part size (w=16 fixed — known throughput peak on m5.8xlarge).
run_one_partsize() {
    local P=\$1
    local tag=\$2

    sudo systemctl stop smoke-wl 2>/dev/null
    sudo systemctl reset-failed smoke-wl 2>/dev/null
    sleep 1
    sudo systemd-run --unit=smoke-wl --service-type=simple --no-block \\
        --property=StandardOutput=null --property=StandardError=null --property=StandardInput=null \\
        python3 /tmp/smoketest.py \$WL_SIZE_GB >/dev/null 2>&1
    local target_kb=\$((WL_SIZE_GB * 1024 * 1024 - 50000))
    for i in \$(seq 1 120); do
        sleep 2
        local WL_PID=\$(systemctl show -p MainPID --value smoke-wl 2>/dev/null)
        [ "\$WL_PID" = "0" ] && continue
        local RSS=\$(sudo awk '/VmRSS/{print \$2}' /proc/\$WL_PID/status 2>/dev/null)
        [ "\${RSS:-0}" -ge "\$target_kb" ] && break
    done
    local WL_PID=\$(systemctl show -p MainPID --value smoke-wl)
    local RSS=\$(sudo awk '/VmRSS/{print \$2}' /proc/\$WL_PID/status 2>/dev/null)
    sudo rm -rf /tmp/dump_curlm/*; sudo mkdir -p /tmp/dump_curlm

    local T0=\$(date +%s.%N)
    sudo /usr/local/sbin/criu dump -D /tmp/dump_curlm -t \$WL_PID --track-mem \\
        --log-file /tmp/dump_curlm/criu-dump.log -v3 \\
        --object-storage-upload \\
        --object-storage-endpoint-url https://s3.us-west-2.amazonaws.com \\
        --object-storage-bucket \$BUCKET \\
        --object-storage-object-prefix \${PREFIX_BASE}-\${tag}/ \\
        --aws-access-key \$AWS_ACCESS_KEY_ID --aws-secret-key \$AWS_SECRET_ACCESS_KEY --aws-region \$REGION \\
        --upload-workers 16 --upload-part-mb \$P >/dev/null 2>&1
    local RC=\$?
    local T1=\$(date +%s.%N)
    local DUR=\$(python3 -c "print(round(\$T1 - \$T0, 3))")
    local SZ=\$(aws s3 ls s3://\$BUCKET/\${PREFIX_BASE}-\${tag}/pages-1.img --region \$REGION 2>/dev/null | awk '{print \$3}')
    local MBPS="-"
    if [ -n "\$SZ" ] && [ "\$(python3 -c 'print(1 if '\$DUR'>0 else 0)')" = "1" ]; then
        MBPS=\$(python3 -c "print(round((\$SZ/1024/1024)/\$DUR, 1))")
    fi
    echo "RESULT PARTSIZE p=\${P}MB rc=\$RC dur=\${DUR}s mbps=\${MBPS}"
    aws s3 rm s3://\$BUCKET/\${PREFIX_BASE}-\${tag}/ --region \$REGION --recursive --quiet >/dev/null 2>&1 &
}

for W in 1 4 8 16 32 64; do
    for R in \$(seq 1 \$REPEATS); do
        echo "--- [\$(date +%H:%M:%S)] workers=\$W run=\$R ---"
        run_one \$W "w\${W}-r\${R}"
    done
done

# Part size sweep at w=16 (raw). 8/16/32/64/128 MB.
for P in 8 16 32 64 128; do
    for R in \$(seq 1 \$REPEATS); do
        echo "--- [\$(date +%H:%M:%S)] partsize=\${P}MB run=\$R ---"
        run_one_partsize \$P "p\${P}-r\${R}"
    done
done

# Auto-detect: no --upload-workers flag passed → criu reads NIC via IMDSv2
# and computes ideal worker count. Tag logs so we can compare against
# explicit w=8 / w=16 / w=32 baselines.
run_one_auto() {
    local tag="\$1"

    sudo systemctl stop smoke-wl 2>/dev/null
    sudo systemctl reset-failed smoke-wl 2>/dev/null
    sleep 1
    sudo systemd-run --unit=smoke-wl --service-type=simple --no-block \\
        --property=StandardOutput=null --property=StandardError=null --property=StandardInput=null \\
        python3 /tmp/smoketest.py \$WL_SIZE_GB >/dev/null 2>&1

    local target_kb=\$((WL_SIZE_GB * 1024 * 1024 - 50000))
    for i in \$(seq 1 120); do
        sleep 2
        local WL_PID=\$(systemctl show -p MainPID --value smoke-wl 2>/dev/null)
        [ "\$WL_PID" = "0" ] && continue
        local RSS=\$(sudo awk '/VmRSS/{print \$2}' /proc/\$WL_PID/status 2>/dev/null)
        [ "\${RSS:-0}" -ge "\$target_kb" ] && break
    done

    local WL_PID=\$(systemctl show -p MainPID --value smoke-wl)
    local RSS=\$(sudo awk '/VmRSS/{print \$2}' /proc/\$WL_PID/status 2>/dev/null)
    sudo rm -rf /tmp/dump_curlm/*; sudo mkdir -p /tmp/dump_curlm

    local T0=\$(date +%s.%N)
    sudo /usr/local/sbin/criu dump \\
        -D /tmp/dump_curlm -t \$WL_PID --track-mem \\
        --log-file /tmp/dump_curlm/criu-dump.log -v3 \\
        --object-storage-upload \\
        --object-storage-endpoint-url https://s3.us-west-2.amazonaws.com \\
        --object-storage-bucket \$BUCKET \\
        --object-storage-object-prefix \${PREFIX_BASE}-\${tag}/ \\
        --aws-access-key \$AWS_ACCESS_KEY_ID \\
        --aws-secret-key \$AWS_SECRET_ACCESS_KEY \\
        --aws-region \$REGION >/dev/null 2>&1
    local RC=\$?
    local T1=\$(date +%s.%N)
    local DUR=\$(python3 -c "print(round(\$T1 - \$T0, 3))")
    local SZ=\$(aws s3 ls s3://\$BUCKET/\${PREFIX_BASE}-\${tag}/pages-1.img --region \$REGION 2>/dev/null | awk '{print \$3}')
    local MBPS="-"
    if [ -n "\$SZ" ] && [ "\$(python3 -c 'print(1 if '\$DUR'>0 else 0)')" = "1" ]; then
        MBPS=\$(python3 -c "print(round((\$SZ/1024/1024)/\$DUR, 1))")
    fi
    local WCHOSEN=\$(grep -oP 'upload_workers auto: \K\d+' /tmp/dump_curlm/criu-dump.log 2>/dev/null | head -1)
    echo "RESULT AUTO workers=\${WCHOSEN:-?} rc=\$RC dur=\${DUR}s pages1=\${SZ}B mbps=\${MBPS}"
    aws s3 rm s3://\$BUCKET/\${PREFIX_BASE}-\${tag}/ --region \$REGION --recursive --quiet >/dev/null 2>&1 &
}

for R in \$(seq 1 \$REPEATS); do
    echo "--- [\$(date +%H:%M:%S)] AUTO run=\$R ---"
    run_one_auto "auto-r\${R}"
done

# Compressed scenarios: same CURLM workers, +zstd-1 compression.
# Measures whether compress_pipeline (with the CURLM integration) now
# keeps up with upload on real S3.
run_one_compress() {
    local W=\$1
    local tag=\$2

    sudo systemctl stop smoke-wl 2>/dev/null
    sudo systemctl reset-failed smoke-wl 2>/dev/null
    sleep 1
    sudo systemd-run --unit=smoke-wl --service-type=simple --no-block \\
        --property=StandardOutput=null --property=StandardError=null --property=StandardInput=null \\
        python3 /tmp/smoketest.py \$WL_SIZE_GB >/dev/null 2>&1

    local target_kb=\$((WL_SIZE_GB * 1024 * 1024 - 50000))
    for i in \$(seq 1 120); do
        sleep 2
        local WL_PID=\$(systemctl show -p MainPID --value smoke-wl 2>/dev/null)
        [ "\$WL_PID" = "0" ] && continue
        local RSS=\$(sudo awk '/VmRSS/{print \$2}' /proc/\$WL_PID/status 2>/dev/null)
        [ "\${RSS:-0}" -ge "\$target_kb" ] && break
    done

    local WL_PID=\$(systemctl show -p MainPID --value smoke-wl)
    local RSS=\$(sudo awk '/VmRSS/{print \$2}' /proc/\$WL_PID/status 2>/dev/null)
    sudo rm -rf /tmp/dump_curlm/*
    sudo mkdir -p /tmp/dump_curlm

    local T0=\$(date +%s.%N)
    sudo /usr/local/sbin/criu dump \\
        -D /tmp/dump_curlm -t \$WL_PID --track-mem \\
        --log-file /tmp/dump_curlm/criu-dump.log -v3 \\
        --object-storage-upload \\
        --object-storage-endpoint-url https://s3.us-west-2.amazonaws.com \\
        --object-storage-bucket \$BUCKET \\
        --object-storage-object-prefix \${PREFIX_BASE}-\${tag}/ \\
        --aws-access-key \$AWS_ACCESS_KEY_ID \\
        --aws-secret-key \$AWS_SECRET_ACCESS_KEY \\
        --aws-region \$REGION \\
        --compress --upload-workers \$W >/dev/null 2>&1
    local RC=\$?
    local T1=\$(date +%s.%N)
    local DUR=\$(python3 -c "print(round(\$T1 - \$T0, 3))")
    local SZ=\$(aws s3 ls s3://\$BUCKET/\${PREFIX_BASE}-\${tag}/pages-1.img --region \$REGION 2>/dev/null | awk '{print \$3}')
    local MBPS_OUT="-"
    local MBPS_IN="-"
    if [ -n "\$SZ" ] && [ "\$(python3 -c 'print(1 if '\$DUR'>0 else 0)')" = "1" ]; then
        MBPS_OUT=\$(python3 -c "print(round((\$SZ/1024/1024)/\$DUR, 1))")
        # input throughput = RSS / DUR (roughly, includes overhead)
        MBPS_IN=\$(python3 -c "print(round((\$RSS/1024)/\$DUR, 1))")
    fi
    echo "RESULT COMP workers=\$W rc=\$RC pid=\$WL_PID rss=\${RSS}KB dur=\${DUR}s pages1=\${SZ}B out_mbps=\${MBPS_OUT} in_mbps=\${MBPS_IN}"
    aws s3 rm s3://\$BUCKET/\${PREFIX_BASE}-\${tag}/ --region \$REGION --recursive --quiet >/dev/null 2>&1 &
}

for W in 1 4 8 16 32 64; do
    for R in \$(seq 1 \$REPEATS); do
        echo "--- [\$(date +%H:%M:%S)] COMPRESSED workers=\$W run=\$R ---"
        run_one_compress \$W "comp-w\${W}-r\${R}"
    done
done

# Reference baseline: aws s3 cp of a same-size file.
# This is the "ideal" upper bound — a stateless tool with no CRIU, no
# pagemap, no parasite, just raw data pushed to the same S3 endpoint.
echo ""
echo "=== aws s3 cp reference (\${WL_SIZE_GB} GB, tmpfs source) ==="
# Use tmpfs so disk read never bottlenecks aws-cli. m5.8xlarge has
# 128 GB RAM; a 2 GB /tmp source already lives in page cache after dd
# but tmpfs makes it explicit + repeatable.
sudo mkdir -p /mnt/awsref
sudo mount -t tmpfs -o size=\$((WL_SIZE_GB + 1))G none /mnt/awsref 2>/dev/null
REF_FILE=/mnt/awsref/ref_\${WL_SIZE_GB}gb.bin
sudo dd if=/dev/urandom of=\$REF_FILE bs=1M count=\$((WL_SIZE_GB * 1024)) status=none
REF_BYTES=\$(stat -c%s \$REF_FILE)
echo "ref file: \${REF_BYTES} bytes (on tmpfs)"

# Classic s3transfer (default). This is what we've been comparing against.
for R in \$(seq 1 \$REPEATS); do
    T0=\$(date +%s.%N)
    aws s3 cp \$REF_FILE "s3://\$BUCKET/\${PREFIX_BASE}-awscli-r\${R}/ref.bin" \\
        --region \$REGION --only-show-errors
    RC=\$?
    T1=\$(date +%s.%N)
    DUR=\$(python3 -c "print(round(\$T1 - \$T0, 3))")
    MBPS=\$(python3 -c "print(round((\$REF_BYTES/1024/1024)/\$DUR, 1))")
    echo "RESULT awscli run=\$R rc=\$RC dur=\${DUR}s bytes=\$REF_BYTES mbps=\$MBPS"
    aws s3 rm "s3://\$BUCKET/\${PREFIX_BASE}-awscli-r\${R}/" --region \$REGION --recursive --quiet >/dev/null 2>&1 &
done

# CRT client: need awscrt python package; only aws-cli v2 bundles CRT support
# through 'preferred_transfer_client crt'. Install awscrt if missing, then
# enable CRT and re-run cp. If install fails (e.g., offline or arch mismatch)
# we just skip the CRT comparison.
echo "=== aws s3 cp via CRT (if available) ==="
if pip3 show awscrt >/dev/null 2>&1 || pip3 install --quiet --break-system-packages awscrt >/dev/null 2>&1; then
    aws configure set default.s3.preferred_transfer_client crt
    for R in \$(seq 1 \$REPEATS); do
        T0=\$(date +%s.%N)
        aws s3 cp \$REF_FILE "s3://\$BUCKET/\${PREFIX_BASE}-awscli-crt-r\${R}/ref.bin" \\
            --region \$REGION --only-show-errors
        RC=\$?
        T1=\$(date +%s.%N)
        DUR=\$(python3 -c "print(round(\$T1 - \$T0, 3))")
        MBPS=\$(python3 -c "print(round((\$REF_BYTES/1024/1024)/\$DUR, 1))")
        echo "RESULT awscli_crt run=\$R rc=\$RC dur=\${DUR}s bytes=\$REF_BYTES mbps=\$MBPS"
        aws s3 rm "s3://\$BUCKET/\${PREFIX_BASE}-awscli-crt-r\${R}/" --region \$REGION --recursive --quiet >/dev/null 2>&1 &
    done
    aws configure set default.s3.preferred_transfer_client classic 2>/dev/null
else
    echo "RESULT awscli_crt unavailable (awscrt package install failed)"
fi

sudo rm -f \$REF_FILE
sudo umount /mnt/awsref 2>/dev/null

echo "=== [\$(date +%H:%M:%S)] DONE — uploading log + self-terminating ==="
# Copy driver log to S3 so we can read results after instance terminates.
aws s3 cp /tmp/curlm_driver.log "s3://\$BUCKET/\${PREFIX_BASE}-log/driver.log" \\
    --region \$REGION >/dev/null 2>&1

TOKEN=\$(curl -sX PUT http://169.254.169.254/latest/api/token \\
    -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')
INSTANCE_ID=\$(curl -s -H "X-aws-ec2-metadata-token: \$TOKEN" \\
    http://169.254.169.254/latest/meta-data/instance-id)
aws ec2 terminate-instances --instance-ids \$INSTANCE_ID --region $REGION
DRIVER

scp -i $SSH_KEY -o StrictHostKeyChecking=no /tmp/curlm_driver.sh ubuntu@$IP:/tmp/curlm_driver.sh >/dev/null
ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "
    chmod +x /tmp/curlm_driver.sh
    nohup bash /tmp/curlm_driver.sh > /tmp/curlm_driver.log 2>&1 &
    disown
    echo PID \$!
"

echo ""
echo "=========================================="
echo "CURLM bench launched on $IP ($IID)"
echo "Tail log: ssh -i $SSH_KEY ubuntu@$IP 'tail -f /tmp/curlm_driver.log'"
echo "=========================================="
