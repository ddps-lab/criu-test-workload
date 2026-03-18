# AMI Setup Guide for CRIU Workload Experiments

이 가이드는 CRIU 체크포인트/마이그레이션 실험을 위한 AMI 구성 방법을 설명합니다.

## 요구사항

| 구성요소 | 버전 | 용도 |
|---------|------|------|
| Ubuntu | 22.04 LTS | Base OS |
| CRIU | ddps-lab/criu-s3 | Checkpoint/Restore (S3 streaming 지원) |
| Python | 3.10+ | Workload 실행 |
| Redis | 7.0+ | Redis workload |
| FFmpeg | 5.0+ | Video workload |
| NumPy | 1.24+ | matmul, dataproc, xgboost |
| PyTorch | 2.0+ | ml_training |
| XGBoost | 2.0+ | xgboost |
| Memcached | 1.6+ | memcached workload |
| p7zip-full | - | 7zip workload |
| YCSB | 0.17.0 | redis/memcached YCSB 모드 |
| Java (JRE) | 11+ | YCSB 실행 |
| Python 2.7 | 2.7.18 | YCSB bin/ycsb 스크립트 |

## 빠른 설치

### AMI 구성 (프로덕션)

```bash
# AMI용 전체 설치 (root 권한, CRIU 빌드 포함)
chmod +x scripts/ami_setup.sh
sudo ./scripts/ami_setup.sh
```

전체 설치 스크립트: [scripts/ami_setup.sh](scripts/ami_setup.sh)

### 로컬 개발 환경

```bash
# 로컬 개발용 의존성 설치 (CRIU 빌드 제외, dirty tracking + checkpoint 프로토콜 테스트용)
chmod +x scripts/setup_local.sh
./scripts/setup_local.sh              # 전체 설치 (YCSB, PyTorch 포함)
./scripts/setup_local.sh --minimal    # 최소 설치 (YCSB, PyTorch 제외)
./scripts/setup_local.sh --check      # 설치 상태만 확인
```

### 외부 데이터셋 다운로드

```bash
# XGBoost 워크로드용 외부 데이터셋 (선택사항)
chmod +x scripts/download_datasets.sh
./scripts/download_datasets.sh                     # 전체 다운로드 (Higgs 7.5GB + Covtype 76MB)
./scripts/download_datasets.sh --dataset higgs      # Higgs만 (~2.6GB CSV)
./scripts/download_datasets.sh --dataset covtype    # Covtype만 (~76MB)
./scripts/download_datasets.sh --output-dir ~/data  # 커스텀 저장 경로
./scripts/download_datasets.sh --check              # 다운로드 상태 확인

# 사용 예시
python3 workloads/xgboost_standalone.py --dataset higgs --dataset-path /data/HIGGS.csv --duration 300
python3 workloads/xgboost_standalone.py --dataset covtype --dataset-path /data/covtype.data --duration 300
```

**참고**: synthetic 데이터셋은 `--seed` 옵션으로 deterministic 생성되므로 외부 다운로드 불필요.

---

## 수동 설치 (단계별)

### 1. 기본 시스템 설정

```bash
#!/bin/bash

set -e

echo "=== CRIU Workload AMI Setup ==="

# 시스템 업데이트
sudo apt-get update
sudo apt-get upgrade -y

# 기본 도구 설치
sudo apt-get install -y \
    build-essential \
    git \
    curl \
    wget \
    htop \
    iotop \
    sysstat \
    net-tools
```

### 2. CRIU 설치

```bash
# CRIU 의존성 (libcurl 포함 - S3 streaming용)
sudo apt-get install -y \
    libprotobuf-dev \
    libprotobuf-c-dev \
    protobuf-c-compiler \
    protobuf-compiler \
    python3-protobuf \
    libcap-dev \
    libnl-3-dev \
    libnet1-dev \
    libaio-dev \
    libgnutls28-dev \
    libcurl4-openssl-dev \
    pkg-config

# CRIU 빌드 및 설치 (ddps-lab/criu-s3 - S3 streaming 지원)
cd /tmp
git clone https://github.com/ddps-lab/criu-s3.git
cd criu-s3

make -j$(nproc)
sudo make install

# 설치 확인
criu --version
```

### 3. Python 환경 설정

```bash
# Python 및 pip
sudo apt-get install -y python3 python3-pip python3-venv

# 필수 Python 패키지
pip3 install --break-system-packages \
    numpy \
    redis \
    paramiko \
    pyyaml \
    boto3 \
    scp \
    xgboost

# PyTorch (CPU only - 용량 줄이기 위해)
pip3 install --break-system-packages \
    torch --index-url https://download.pytorch.org/whl/cpu
```

### 4. Redis, Memcached, 7zip, Java 설치

```bash
# Redis 설치
sudo apt-get install -y redis-server

# Memcached 설치
sudo apt-get install -y memcached

# 7zip 설치
sudo apt-get install -y p7zip-full

# Java (YCSB 실행에 필요)
sudo apt-get install -y default-jre-headless

# 시스템 서비스 비활성화 (워크로드에서 직접 실행)
sudo systemctl stop redis-server && sudo systemctl disable redis-server
sudo systemctl stop memcached && sudo systemctl disable memcached
```

### 5. FFmpeg 설치

```bash
sudo apt-get install -y ffmpeg
```

### 5.5 YCSB 설치

```bash
YCSB_VERSION="0.17.0"
curl -sSL -O "https://github.com/brianfrankcooper/YCSB/releases/download/${YCSB_VERSION}/ycsb-${YCSB_VERSION}.tar.gz"
tar xf "ycsb-${YCSB_VERSION}.tar.gz"
sudo mv "ycsb-${YCSB_VERSION}" /opt/ycsb
rm -f "ycsb-${YCSB_VERSION}.tar.gz"

# YCSB bin/ycsb는 Python 2 스크립트이므로 python2 설치 필요
# Ubuntu 24.04+에서는 소스 빌드 또는 pyenv 사용
# shebang 패치: #!/usr/bin/env python → #!/usr/bin/env python2
sudo sed -i '1s|#!/usr/bin/env python$|#!/usr/bin/env python2|' /opt/ycsb/bin/ycsb
```

### 6. 커널 설정 (CRIU 호환성)

```bash
# CRIU 호환 커널 파라미터
sudo tee /etc/sysctl.d/99-criu.conf << EOF
# CRIU 호환성을 위한 설정
kernel.ns_last_pid = 0
kernel.unprivileged_userns_clone = 1
EOF

sudo sysctl --system

# ptrace 권한 설정
echo 0 | sudo tee /proc/sys/kernel/yama/ptrace_scope
```

### 7. 전체 설치 스크립트

```bash
#!/bin/bash
# full_ami_setup.sh

set -e

echo "=== Starting CRIU Workload AMI Setup ==="
echo "Date: $(date)"

# 1. System update
echo "[1/6] Updating system..."
sudo apt-get update && sudo apt-get upgrade -y

# 2. Install dependencies
echo "[2/6] Installing dependencies..."
sudo apt-get install -y \
    build-essential git curl wget htop \
    libprotobuf-dev libprotobuf-c-dev protobuf-c-compiler \
    protobuf-compiler python3-protobuf libcap-dev \
    libnl-3-dev libnet1-dev libaio-dev libgnutls28-dev \
    pkg-config python3 python3-pip redis-server ffmpeg

# 3. Build CRIU
echo "[3/6] Building CRIU..."
cd /tmp
if [ ! -d "criu" ]; then
    git clone https://github.com/checkpoint-restore/criu.git
fi
cd criu
git checkout v3.19
make clean || true
make -j$(nproc)
sudo make install

# 4. Install Python packages
echo "[4/6] Installing Python packages..."
pip3 install --break-system-packages numpy redis paramiko pyyaml
pip3 install --break-system-packages torch --index-url https://download.pytorch.org/whl/cpu

# 5. Configure system
echo "[5/6] Configuring system..."
sudo systemctl stop redis-server
sudo systemctl disable redis-server

sudo tee /etc/sysctl.d/99-criu.conf << EOF
kernel.ns_last_pid = 0
kernel.unprivileged_userns_clone = 1
EOF
sudo sysctl --system

# 6. Verify installation
echo "[6/6] Verifying installation..."
echo "CRIU version: $(criu --version)"
echo "Python version: $(python3 --version)"
echo "Redis version: $(redis-server --version)"
echo "FFmpeg version: $(ffmpeg -version 2>&1 | head -1)"
python3 -c "import numpy; print(f'NumPy version: {numpy.__version__}')"
python3 -c "import torch; print(f'PyTorch version: {torch.__version__}')"
python3 -c "import redis; print(f'redis-py version: {redis.__version__}')"

echo ""
echo "=== AMI Setup Complete ==="
echo "Workloads ready: memory, matmul, redis, ml_training, video, dataproc, xgboost, memcached, 7zip"
```

## 워크로드별 요구사항

| 워크로드 | 필수 패키지 | 체크포인트 대상 | CRIU 플래그 |
|---------|------------|---------------|-------------|
| memory | (없음) | Python 프로세스 | `--shell-job` |
| matmul | numpy | Python 프로세스 | `--shell-job` |
| redis | redis-server, redis-py, YCSB+Java | **redis-server 프로세스** | `--shell-job --tcp-established` |
| ml_training | torch | Python 프로세스 | `--shell-job` |
| video | ffmpeg | **ffmpeg 프로세스** | `--shell-job` |
| dataproc | numpy | Python 프로세스 | `--shell-job` |
| xgboost | xgboost, numpy | Python 프로세스 | `--shell-job` |
| memcached | memcached, YCSB+Java | **memcached 프로세스** | `--shell-job --tcp-established` |
| 7zip | p7zip-full | **7z 프로세스** | `--shell-job` |

## AMI 생성 절차

### AWS Console

1. EC2 인스턴스 시작 (Ubuntu 22.04)
2. SSH 접속 후 `full_ami_setup.sh` 실행
3. 인스턴스 중지
4. Actions → Image and templates → Create image
5. AMI 이름: `criu-workload-ubuntu22.04-v1`

### AWS CLI

```bash
# 인스턴스에서 스크립트 실행 후
aws ec2 create-image \
    --instance-id i-1234567890abcdef0 \
    --name "criu-workload-ubuntu22.04-v1" \
    --description "CRIU workload experiment AMI with Redis, PyTorch, NumPy" \
    --no-reboot
```

## Terraform 연동

`aws-lab/main.tf`에서 AMI ID 설정:

```hcl
variable "workload_ami" {
  description = "AMI ID for workload nodes"
  default     = "ami-xxxxxxxxxxxxxxxxx"  # 생성한 AMI ID
}

resource "aws_instance" "az_a_instances" {
  ami           = var.workload_ami
  instance_type = var.instance_type
  # ...
}
```

## 설치 확인 테스트

```bash
# CRIU 테스트
criu check

# Redis 테스트
redis-server --port 6380 --daemonize yes
redis-cli -p 6380 ping  # PONG
redis-cli -p 6380 shutdown

# Python 패키지 테스트
python3 -c "
import numpy as np
import torch
import redis
import xgboost
print('All packages OK')
"

# YCSB 테스트
/opt/ycsb/bin/ycsb 2>&1 | head -1  # usage 출력 확인

# 7zip 테스트
7z --help 2>&1 | head -1
```

## 문제 해결

### CRIU 권한 오류

```bash
# capabilities 확인
sudo setcap cap_checkpoint_restore+eip $(which criu)

# 또는 root로 실행
sudo criu dump ...
```

### Redis 포트 충돌

```bash
# 사용 중인 포트 확인
sudo lsof -i :6379

# 프로세스 종료
sudo kill $(sudo lsof -t -i :6379)
```

### PyTorch 메모리 부족

```bash
# CPU only 버전 설치 (GPU 버전보다 작음)
pip3 uninstall torch
pip3 install torch --index-url https://download.pytorch.org/whl/cpu
```
