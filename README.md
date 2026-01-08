# CRIU 워크로드 실험 프레임워크

CRIU(Checkpoint/Restore In Userspace) 기반 라이브 마이그레이션 실험을 위한 재사용 가능한 모듈형 프레임워크입니다. 프로세스 마이그레이션, 체크포인트 최적화, 클라우드 스팟 인스턴스 데드라인 연구를 위해 설계되었습니다.

## 목차

- [아키텍처](#아키텍처)
- [빠른 시작](#빠른-시작)
- [워크로드 시나리오](#워크로드-시나리오)
  - [1. 메모리 할당](#1-메모리-할당-워크로드)
  - [2. 행렬 곱셈](#2-행렬-곱셈-워크로드)
  - [3. Redis 인메모리 DB](#3-redis-인메모리-데이터베이스-워크로드)
  - [4. ML 학습 (PyTorch)](#4-ml-학습-워크로드-pytorch)
  - [5. 비디오 처리](#5-비디오-처리-워크로드)
  - [6. 데이터 처리 (Pandas 유사)](#6-데이터-처리-워크로드-pandas-유사)
- [설정](#설정)
- [체크포인트 전략](#체크포인트-전략)
- [AWS Lab 연동](#aws-lab-연동)
- [새 워크로드 추가](#새-워크로드-추가)
- [메트릭 출력](#메트릭-출력)
- [연구 배경](#연구-배경)

## 아키텍처

```
criu_workload/
├── lib/                          # 코어 라이브러리 (컨트롤 노드 전용)
│   ├── config.py                 # YAML 설정 + 환경변수 치환
│   ├── checkpoint.py             # SSH 기반 CRIU 작업 + 로그 수집
│   ├── transfer.py               # 체크포인트 전송 (rsync/S3/EFS/EBS)
│   ├── timing.py                 # 메트릭 수집
│   └── criu_utils.py             # 메인 오케스트레이터
├── workloads/                    # 워크로드 구현체
│   ├── base_workload.py          # 추상 베이스 클래스
│   ├── *_standalone.py           # 독립 실행 스크립트 (워크로드 노드 배포용)
│   └── *_workload.py             # 컨트롤 노드 래퍼
├── config/                       # 설정 파일
│   ├── default.yaml              # 기본 설정
│   ├── experiments/              # 실험별 설정
│   └── workloads/                # 워크로드별 설정
├── experiments/                  # 실험 스크립트
│   └── baseline_experiment.py    # 메인 실험 실행기
├── results/                      # 실험 결과 (자동 생성)
│   └── <workload>_<timestamp>/
│       ├── source/               # Source 노드 로그
│       │   ├── criu-*.log        # CRIU 작업 로그
│       │   ├── workload_status_pre_dump.txt
│       │   └── workload_stdout_pre_dump.log*
│       ├── dest/                 # Destination 노드 로그
│       │   ├── criu-*.log
│       │   ├── workload_status_post_restore.txt
│       │   └── workload_stdout_post_restore.log*
│       └── metrics.json          # 실험 메트릭
└── run_experiment.py             # 빠른 실행 스크립트
```

### 코어 라이브러리 상세

#### checkpoint.py
CRIU 작업과 로그 수집을 담당하는 핵심 모듈:
- **SSH 기반 원격 실행**: Paramiko를 사용하여 source/dest 노드에서 CRIU 명령 실행
- **워크로드 배포**: SCP를 통해 독립 스크립트를 워크로드 노드에 배포
- **프로세스 관리**: 워크로드 시작, PID 추적, 체크포인트 준비 시그널 대기
- **CRIU 작업**:
  - Pre-dump: 반복적 메모리 스냅샷 (변경된 페이지만)
  - Dump: 최종 체크포인트 생성
  - Page server: 네트워크 기반 메모리 페이지 전송
  - Restore: 체크포인트에서 프로세스 복원
  - Lazy pages: 온디맨드 페이지 로딩
- **로그 수집**:
  - CRIU 작업 로그 (pre-dump, dump, restore, page-server, lazy-pages)
  - 워크로드 상태 정보 (프로세스 목록, 메모리 사용량)
  - 워크로드 stdout/stderr 캡처 (strace 사용)
  - 실험 실패 시에도 로그 수집 보장

#### transfer.py
체크포인트 데이터 전송을 담당:
- **rsync**: SSH 기반 빠른 파일 동기화 (기본값)
- **S3**: AWS S3를 통한 체크포인트 저장/복원
- **EFS**: AWS EFS 공유 파일시스템 (크로스-AZ 지원)
- **EBS**: EBS 볼륨 detach/attach 패턴
- 전송 시간, 크기, 처리량 메트릭 수집

#### config.py
설정 관리 및 환경변수 치환:
- YAML 파일 로드 및 파싱
- 환경변수 치환 (`${VAR_NAME}`)
- `servers.yaml`과 `default.yaml` 병합
- 명령줄 인자 우선순위 처리

#### timing.py
실험 메트릭 수집 및 저장:
- 각 단계별 시간 측정 (컨텍스트 매니저 사용)
- 메트릭 계산 (평균, 합계, 처리량)
- JSON 형식으로 메트릭 저장

#### criu_utils.py
전체 실험 오케스트레이션:
- 워크로드 배포 및 시작
- 체크포인트 전략 실행 (predump/full)
- 전송 및 복원
- 로그 수집 및 정리
- 에러 처리 및 복구

### 노드 분리 아키텍처

프레임워크는 3-노드 아키텍처를 사용합니다:

| 노드 타입 | 역할 | 구성 요소 |
|-----------|------|-----------|
| **컨트롤 노드** (Bastion) | 실험 오케스트레이션 | 전체 라이브러리, 설정, 실험 스크립트 |
| **소스 노드** | 워크로드 실행, 체크포인트 생성 | 독립 스크립트 + 의존성 패키지 |
| **목적지 노드** | 워크로드 복원 | 독립 스크립트 + 의존성 패키지 |

워크로드 노드에는 SCP로 독립 스크립트만 배포됩니다 - 라이브러리 의존성이 필요 없습니다.

## 빠른 시작

### 1. 환경 설정

Terraform으로 인프라를 배포하면 `config/servers.yaml`이 자동 생성됩니다. 별도의 환경 설정 없이 바로 실험을 실행할 수 있습니다.

```bash
# AWS Lab에서는 자동 설정됨 - 바로 실험 실행 가능
cd /opt/criu_workload
python3 run_experiment.py
```

수동 설정이 필요한 경우:

```bash
# 방법 1: 환경 변수 직접 설정
export SOURCE_NODE_IP="10.0.1.10"
export DEST_NODE_IP="10.0.1.11"

# 방법 2: AWS Lab 환경 변수 사용 (bastion에서)
source ~/.bashrc
export SOURCE_NODE_IP=$(echo $AZ_A_INSTANCES_IP | cut -d' ' -f1)
export DEST_NODE_IP=$(echo $AZ_A_INSTANCES_IP | cut -d' ' -f2)
```

### 2. 실험 실행

```bash
cd /spot_kubernetes/criu_workload

# 기본 실행 (memory 워크로드)
python3 run_experiment.py

# 워크로드 타입 지정
python3 run_experiment.py --workload matmul

# 커스텀 설정으로 실행
python3 run_experiment.py --workload memory --mb-size 512 --max-memory 8192

# 설정 파일 사용
python3 run_experiment.py -c config/experiments/lazy_pages.yaml
```

### 3. 결과 확인

```bash
cat metrics.json | python3 -m json.tool
```

---

## 로그 수집 및 디버깅

### 자동 로그 수집

모든 실험은 자동으로 상세한 로그를 수집하여 `results/` 디렉토리에 저장합니다:

```bash
results/
└── <workload>_<timestamp>/
    ├── source/                       # Source 노드 로그
    │   ├── 1/                        # 체크포인트 디렉토리
    │   ├── criu-pre-dump.log         # Pre-dump CRIU 로그
    │   ├── criu-dump.log             # Final dump CRIU 로그
    │   ├── criu-page-server.log      # Page server 로그
    │   ├── workload_status_pre_dump.txt         # 프로세스 상태
    │   ├── workload_stdout_pre_dump.log         # 워크로드 출력
    │   ├── workload_stdout_pre_dump.log.raw     # 원본 strace 출력
    │   └── workload_stdout_pre_dump.log.info    # 로그 수집 메타데이터
    ├── dest/                         # Destination 노드 로그
    │   ├── 1/                        # 복원된 체크포인트
    │   ├── criu-restore.log          # Restore CRIU 로그
    │   ├── criu-lazy-pages.log       # Lazy pages 로그 (해당 시)
    │   ├── workload_status_post_restore.txt     # 복원 후 프로세스 상태
    │   ├── workload_stdout_post_restore.log     # 복원 후 워크로드 출력
    │   ├── workload_stdout_post_restore.log.raw
    │   └── workload_stdout_post_restore.log.info
    └── metrics.json                  # 실험 메트릭
```

### 워크로드 stdout/stderr 캡처

워크로드의 stdout/stderr는 `/dev/null`로 리다이렉트되지만, strace를 사용하여 write 시스템 콜을 캡처합니다:

- **pre_dump 단계**: 워크로드 시작부터 dump 직전까지의 모든 출력 캡처
- **post_restore 단계**: 복원 후 6초간의 출력 캡처
- **파싱**: strace raw 출력을 파싱하여 읽기 쉬운 형식으로 변환
- **메타데이터**: `.info` 파일에 수집 과정 상세 정보 저장

### 로그 확인

```bash
# 실험 실행
python3 run_experiment.py --workload matmul

# 결과 확인
cd results/matmul_<timestamp>

# Source 노드 워크로드 출력 확인
cat source/workload_stdout_pre_dump.log

# Destination 노드 복원 후 출력 확인
cat dest/workload_stdout_post_restore.log

# CRIU dump 로그 확인
cat source/criu-dump.log

# 프로세스 상태 확인
cat source/workload_status_pre_dump.txt
```

### 실패 시 로그 수집

실험이 실패하거나 예외가 발생해도 로그는 자동으로 수집됩니다:
- 실패한 실험의 결과 디렉토리는 `_failed` 또는 `_exception` 접미사가 붙습니다
- 실패 지점까지의 모든 로그가 수집됩니다
- CRIU 에러 메시지는 해당 `.log` 파일에서 확인 가능합니다

---

## 워크로드 시나리오

### 1. 메모리 할당 워크로드

**시나리오**: 점진적 메모리 할당으로 다양한 메모리 사용량에서의 체크포인트 크기 테스트

**사용 사례**:
- 메모리 집약적 배치 처리
- 캐시 서비스 워밍업
- Pre-dump 효율성 테스트 (증가하는 메모리)

**명령어**:
```bash
# 기본: 256MB 블록, 5초 간격, 최대 8GB
python3 run_experiment.py --workload memory

# 소형 워크로드 (1GB)
python3 run_experiment.py --workload memory --mb-size 128 --max-memory 1024 --interval 2

# 대형 워크로드 + lazy pages (8GB)
python3 run_experiment.py --workload memory --mb-size 512 --max-memory 8192 --lazy-pages

# 커스텀 설정
python3 run_experiment.py --workload memory \
  --mb-size 256 \
  --interval 5 \
  --max-memory 4096 \
  --predump-iterations 8 \
  --predump-interval 10
```

**파라미터**:
| 파라미터 | 기본값 | 설명 |
|----------|--------|------|
| `--mb-size` | 256 | 메모리 블록 크기 (MB) |
| `--interval` | 5.0 | 할당 간격 (초) |
| `--max-memory` | 8192 | 최대 메모리 (MB) |

**예상 메모리 사용량**: `mb_size * (경과시간 / interval)` (최대값까지)

---

### 2. 행렬 곱셈 워크로드

**시나리오**: 연속적인 행렬 곱셈으로 HPC/과학 계산 워크로드 시뮬레이션

**사용 사례**:
- 과학 계산 작업
- HPC 배치 처리
- 수치 시뮬레이션
- CPU 집약적 계산

**명령어**:
```bash
# 기본: 2048x2048 행렬
python3 run_experiment.py --workload matmul

# 대형 행렬 (더 많은 메모리, 긴 계산 시간)
python3 run_experiment.py --workload matmul --matrix-size 4096

# 빠른 반복
python3 run_experiment.py --workload matmul --matrix-size 1024 --interval 0.5
```

**파라미터**:
| 파라미터 | 기본값 | 설명 |
|----------|--------|------|
| `--matrix-size` | 2048 | 정방 행렬 크기 (NxN) |
| `--iterations` | 0 | 반복 횟수 (0=무한) |
| `--interval` | 1.0 | 반복 간 최소 간격 (초) |

**메모리 사용량**: `~3 * matrix_size² * 8 bytes` (예: 2048x2048 = ~96MB)

**의존성**: `numpy`

---

### 3. Redis 인메모리 데이터베이스 워크로드

**시나리오**: 실제 redis-server를 사용하여 인메모리 키-값 데이터베이스 워크로드 시뮬레이션 + 지속적인 읽기/쓰기 작업

**사용 사례**:
- Redis 캐싱 레이어
- 인메모리 세션 저장소
- 실시간 데이터 캐시
- 키-값 데이터베이스
- 실제 Redis 프로세스의 체크포인트/복원 테스트

**명령어**:
```bash
# 기본: 100K 키, 1KB 값
python3 run_experiment.py --workload redis

# 대형 캐시 (1M 키, 4KB 값)
python3 run_experiment.py --workload redis --num-keys 1000000 --value-size 4096

# 커스텀 Redis 포트
python3 run_experiment.py --workload redis --redis-port 6380 --num-keys 100000
```

**파라미터**:
| 파라미터 | 기본값 | 설명 |
|----------|--------|------|
| `--redis-port` | 6379 | Redis 서버 포트 |
| `--num-keys` | 100000 | 키 개수 |
| `--value-size` | 1024 | 값 크기 (바이트) |

**메모리 사용량**: `~num_keys * (overhead + value_size)` + Redis 오버헤드

**특징**:
- 실제 redis-server 프로세스 사용 (TCP 소켓 포함)
- 복원 후 데이터 무결성 검증 (체크섬 비교)
- Python redis 클라이언트로 작업 수행
- TCP 소켓을 통한 실제 네트워크 통신
- CRIU --tcp-established 플래그 필요

**의존성**: `redis-server`, `redis` (Python 패키지)

---

### 4. ML 학습 워크로드 (PyTorch)

**시나리오**: 합성 데이터에서 forward/backward pass를 수행하는 머신러닝 모델 학습 시뮬레이션

**사용 사례**:
- 딥러닝 학습 작업
- 모델 파인튜닝
- 하이퍼파라미터 탐색
- 장시간 ML 실험

**명령어**:
```bash
# 기본: medium 모델
python3 run_experiment.py --workload ml_training

# small 모델 (빠른 반복)
python3 run_experiment.py --workload ml_training --model-size small --batch-size 128

# large 모델
python3 run_experiment.py --workload ml_training --model-size large --batch-size 32
```

**파라미터**:
| 파라미터 | 기본값 | 설명 |
|----------|--------|------|
| `--model-size` | medium | 모델 크기: small, medium, large |
| `--batch-size` | 64 | 학습 배치 크기 |
| `--epochs` | 0 | 에폭 수 (0=무한) |
| `--learning-rate` | 0.001 | 학습률 |
| `--dataset-size` | model-size 기본값 | 데이터셋 크기 (모델 기본값 덮어쓰기) |

**모델 크기별 구성**:
| 크기 | 입력 | 히든 레이어 | 출력 | 데이터셋 | 파라미터 수 |
|------|------|-------------|------|----------|-------------|
| small | 256 | [512, 256, 128] | 10 | 10K | ~200K |
| medium | 512 | [1024, 512, 256, 128] | 100 | 50K | ~1M |
| large | 1024 | [2048, 1024, 512, 256, 128] | 1000 | 100K | ~5M |

**의존성**: `torch`

---

### 5. 비디오 처리 워크로드

**시나리오**: 실제 ffmpeg를 사용한 비디오 인코딩/디코딩 작업, 합성 비디오 프레임 처리

**사용 사례**:
- 비디오 트랜스코딩 작업
- 라이브 스트리밍 처리
- 비디오 편집 배치 작업
- 미디어 인코딩 파이프라인

**명령어**:
```bash
# 기본: 1080p @ 30fps
python3 run_experiment.py --workload video

# 4K 비디오 처리
python3 run_experiment.py --workload video --resolution 3840x2160 --fps 30

# 빠른 720p 처리
python3 run_experiment.py --workload video --resolution 1280x720 --fps 60
```

**파라미터**:
| 파라미터 | 기본값 | 설명 |
|----------|--------|------|
| `--resolution` | 1920x1080 | 비디오 해상도 (WxH) |
| `--fps` | 30 | 초당 프레임 수 |
| `--duration` | 300 | 지속 시간 (초, file 모드용) |
| `--video-mode` | live | 비디오 출력 모드 (file/live) |

**일반 해상도**:
| 이름 | 해상도 | 프레임 크기 |
|------|--------|-------------|
| 720p | 1280x720 | 2.6 MB |
| 1080p | 1920x1080 | 5.9 MB |
| 4K | 3840x2160 | 23.7 MB |

**의존성**: `ffmpeg` (필수)

**특징**:
- 실제 ffmpeg 프로세스 사용 (Python wrapper + ffmpeg 프로세스 트리)
- testsrc2 필터로 합성 비디오 생성
- file 모드: 단일 MP4 파일 생성
- live 모드: segment 파일 생성 (CRIU 복원 시 권장)

---

### 6. 데이터 처리 워크로드 (Pandas 유사)

**시나리오**: Pandas/Spark와 유사한 대규모 데이터셋에서의 ETL 및 데이터 처리 작업 시뮬레이션

**사용 사례**:
- ETL 파이프라인
- 데이터 웨어하우스 작업
- 배치 분석 작업
- 데이터 변환 워크플로우

**명령어**:
```bash
# 기본: 100만 행, 50 컬럼
python3 run_experiment.py --workload dataproc

# 대규모 데이터셋
python3 run_experiment.py --workload dataproc --num-rows 10000000 --num-cols 100

# 빠른 작업
python3 run_experiment.py --workload dataproc --num-rows 100000 --interval 0.1
```

**파라미터**:
| 파라미터 | 기본값 | 설명 |
|----------|--------|------|
| `--num-rows` | 1000000 | 데이터셋 행 수 |
| `--num-cols` | 50 | 데이터셋 컬럼 수 |
| `--operations` | 0 | 작업 수 (0=무한) |
| `--interval` | 1.0 | 작업 간 간격 (초) |

**수행 작업**:
| 작업 | 설명 |
|------|------|
| filter | 컬럼 임계값으로 행 필터링 |
| aggregate | 컬럼 통계 계산 (평균, 표준편차, 합계) |
| sort | 컬럼별 정렬 |
| group_aggregate | 카테고리별 그룹화 및 집계 |
| join | 조인 작업 |
| transform | 컬럼 정규화 |
| window | 롤링/윈도우 작업 |

**메모리 사용량**: `~num_rows * num_cols * 8 bytes`

**의존성**: `numpy` (pandas는 선택사항)

---

## 설정

설정은 두 개의 파일로 분리되어 있습니다:

| 파일 | 용도 | 생성 방법 |
|------|------|----------|
| `config/default.yaml` | 실험 설정 (워크로드, 체크포인트 전략 등) | 수동 편집 |
| `config/servers.yaml` | 노드 IP 정보 | Terraform 자동 생성 |

### 서버 설정 (servers.yaml)

Terraform으로 인프라를 배포하면 `config/servers.yaml`이 자동 생성됩니다. 실험 스크립트는 이 파일에서 노드 IP를 자동으로 읽어옵니다.

```yaml
# config/servers.yaml (Terraform이 자동 생성)
nodes:
  ssh_user: "ubuntu"
  ssh_key: "~/.ssh/id_ed25519"

  source:
    ip: "192.168.10.100"
    name: "az-a-node-0"

  destination:
    ip: "192.168.10.101"
    name: "az-a-node-1"

# 모든 사용 가능한 노드
all_nodes:
  az_a:
    - ip: "192.168.10.100"
      name: "az-a-node-0"
    - ip: "192.168.10.101"
      name: "az-a-node-1"
  az_c:
    - ip: "192.168.20.100"
      name: "az-c-node-0"
```

**참고**: `servers.yaml`이 없으면 `default.yaml`의 환경변수(`${SOURCE_NODE_IP}`, `${DEST_NODE_IP}`)를 사용합니다.

### 실험 설정 (default.yaml)

```yaml
# config/default.yaml
experiment:
  name: "default-experiment"
  workload_type: "memory"      # 워크로드 타입
  save_metrics: true

# 노드 설정 (servers.yaml이 없을 때 fallback)
nodes:
  ssh_user: "ubuntu"
  source:
    ip: "${SOURCE_NODE_IP}"    # 환경변수 사용
  destination:
    ip: "${DEST_NODE_IP}"

# 체크포인트 전략
checkpoint:
  strategy:
    mode: "predump"           # 또는 "full"
    predump_iterations: 8
    predump_interval: 10
    lazy_pages: false

# 전송 방법
transfer:
  method: "rsync"             # rsync, s3, efs, ebs
```

### 환경 변수

| 변수 | 설명 |
|------|------|
| `SOURCE_NODE_IP` | 소스 노드 IP 주소 |
| `DEST_NODE_IP` | 목적지 노드 IP 주소 |
| `AWS_S3_BUCKET` | S3 버킷 (체크포인트 저장용, 선택) |

---

## 체크포인트 전략

### 1. Pre-dump 전략 (권장)

반복적인 pre-dump로 최종 덤프 시간을 최소화합니다. 메모리 변경 사항을 점진적으로 캡처합니다.

```bash
python3 run_experiment.py \
  --strategy predump \
  --predump-iterations 8 \
  --predump-interval 10
```

**동작 원리**:
1. 10초마다 메모리 스냅샷 캡처 (pre-dump)
2. 각 pre-dump는 변경된 페이지만 캡처
3. 최종 덤프는 최소화됨 (마지막 변경 사항만)
4. 마이그레이션 다운타임 감소

**적합한 워크로드**: 메모리가 증가하거나 변경되는 워크로드 (memory, redis, ml_training)

### 2. Full Dump 전략

pre-dump 없이 단일 전체 체크포인트를 생성합니다.

```bash
python3 run_experiment.py --strategy full
```

**적합한 워크로드**: 작고 정적인 워크로드 또는 베이스라인 측정

### 3. Lazy Pages

복원 후 온디맨드로 페이지를 로딩합니다.

```bash
python3 run_experiment.py --lazy-pages
```

**동작 원리**:
1. 최소한의 페이지로 즉시 프로세스 복원
2. 나머지 페이지는 페이지 폴트 시 온디맨드로 가져옴
3. 복원 시간 단축, 복원 후 지연 시간 증가

**적합한 워크로드**: 빠른 복원이 중요한 대용량 메모리 워크로드

---

## AWS Lab 연동

### Bastion 노드에서 설정

```bash
# Terraform에서 설정된 환경 변수 로드
source ~/.bashrc

# 같은 AZ 내 마이그레이션
export SOURCE_NODE_IP=$(echo $AZ_A_INSTANCES_IP | cut -d' ' -f1)
export DEST_NODE_IP=$(echo $AZ_A_INSTANCES_IP | cut -d' ' -f2)

# 크로스-AZ 마이그레이션
export SOURCE_NODE_IP=$(echo $AZ_A_INSTANCES_IP | cut -d' ' -f1)
export DEST_NODE_IP=$(echo $AZ_C_INSTANCES_IP | cut -d' ' -f1)

# 실험 실행
cd /spot_kubernetes/criu_workload
python3 run_experiment.py --workload memory
```

### EFS를 사용한 체크포인트 전송

```bash
python3 run_experiment.py \
  -c config/experiments/efs_transfer.yaml \
  --source-ip $SOURCE_NODE_IP \
  --dest-ip $DEST_NODE_IP
```

### EBS를 사용한 체크포인트 전송

EBS 볼륨을 사용한 체크포인트 전송은 볼륨 detach/attach 패턴을 사용합니다. 설정 파일에서 `transfer.method: ebs`로 지정하면 됩니다.

---

## 새 워크로드 추가

### 1. 독립 스크립트 생성

```python
# workloads/myworkload_standalone.py
def create_ready_signal(working_dir):
    with open(f'{working_dir}/checkpoint_ready', 'w') as f:
        f.write(f'ready:{os.getpid()}\n')

def check_restore_complete(working_dir):
    return not os.path.exists(f'{working_dir}/checkpoint_flag')

def run_workload(working_dir, ...):
    # 초기화
    create_ready_signal(working_dir)

    while True:
        if check_restore_complete(working_dir):
            print("복원 완료")
            sys.exit(0)

        # 작업 수행...
```

### 2. 래퍼 클래스 생성

```python
# workloads/myworkload_workload.py
from .base_workload import BaseWorkload, WorkloadFactory

class MyWorkload(BaseWorkload):
    def get_standalone_script_name(self):
        return 'myworkload_standalone.py'

    def get_standalone_script_content(self):
        return STANDALONE_SCRIPT_CONTENT

    def get_command(self):
        return f"python3 {self.get_standalone_script_name()} --working_dir {self.working_dir}"

    def get_dependencies(self):
        return ['numpy']  # 필요한 패키지

WorkloadFactory.register('myworkload', MyWorkload)
```

### 3. __init__.py에 등록

```python
from .myworkload_workload import MyWorkload
```

---

## 메트릭 출력

실험 완료 후 출력되는 메트릭 예시:

```
============================================================
CRIU Experiment Metrics: baseline-predump
Workload Type: memory
============================================================

Pre-dump Iterations: 8
  Total time: 12.34s
  Average time: 1.54s
  Iteration 1: 1.23s
  Iteration 2: 1.45s
  ...

Final Dump:
  Duration: 2.15s

Transfer:
  Method: rsync
  Duration: 3.45s
  Size: 2048.00 MB
  Throughput: 593.62 MB/s

Restore:
  Duration: 1.23s

Total Experiment Duration: 95.67s
============================================================
```

---

## 연구 배경

이 프레임워크는 **Challenge C1: Deadline Violation** 연구를 위해 설계되었습니다 - CRIU 기반 라이브 마이그레이션이 클라우드 스팟 인스턴스 종료 데드라인 내에 완료될 수 있는지 테스트합니다:

| 클라우드 제공자 | 경고 시간 |
|----------------|-----------|
| AWS | 120초 |
| GCP | 30초 |

7개의 워크로드는 데드라인 제약 하에서 마이그레이션 성능을 평가하기 위한 현실적인 클라우드 워크로드 시나리오를 대표합니다.

---

## 워크로드 요약 테이블

| 워크로드 | 시나리오 | 메모리 패턴 | 의존성 | 특징 |
|----------|----------|-------------|--------|------|
| `memory` | 메모리 할당 | 증가형 | 없음 | Pre-dump 효율성 테스트에 최적 |
| `matmul` | HPC/과학 계산 | 정적 | numpy | CPU 집약적 계산 |
| `redis` | 실제 Redis 서버 | 초기화 후 정적 | redis-server, redis | TCP 소켓, 실제 프로세스 |
| `ml_training` | 딥러닝 학습 | 정적 | torch | GPU 사용 가능 |
| `video` | 비디오 트랜스코딩 | 정적 | ffmpeg | 실제 ffmpeg 프로세스, live/file 모드 |
| `dataproc` | ETL/배치 분석 | 초기화 후 정적 | numpy | Pandas/Spark 유사 작업 |
