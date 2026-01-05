# CRIU 워크로드 실험 프레임워크

CRIU(Checkpoint/Restore In Userspace) 기반 라이브 마이그레이션 실험을 위한 재사용 가능한 모듈형 프레임워크입니다. 프로세스 마이그레이션, 체크포인트 최적화, 클라우드 스팟 인스턴스 데드라인 연구를 위해 설계되었습니다.

## 목차

- [아키텍처](#아키텍처)
- [빠른 시작](#빠른-시작)
- [워크로드 시나리오](#워크로드-시나리오)
  - [1. 메모리 할당](#1-메모리-할당-워크로드)
  - [2. 행렬 곱셈](#2-행렬-곱셈-워크로드)
  - [3. Redis 유사 인메모리 DB](#3-redis-유사-인메모리-데이터베이스-워크로드)
  - [4. ML 학습 (PyTorch)](#4-ml-학습-워크로드-pytorch)
  - [5. Jupyter 노트북 시뮬레이션](#5-jupyter-노트북-시뮬레이션-워크로드)
  - [6. 비디오 처리](#6-비디오-처리-워크로드)
  - [7. 데이터 처리 (Pandas 유사)](#7-데이터-처리-워크로드-pandas-유사)
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
│   ├── checkpoint.py             # SSH 기반 CRIU 작업
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
└── run_experiment.py             # 빠른 실행 스크립트
```

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
cat experiment_metrics.json | python3 -m json.tool
```

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

### 3. Redis 유사 인메모리 데이터베이스 워크로드

**시나리오**: Redis/Memcached 유사 인메모리 키-값 저장소 시뮬레이션 + 지속적인 읽기/쓰기 작업

**사용 사례**:
- Redis/Memcached 캐싱 레이어
- 인메모리 세션 저장소
- 실시간 데이터 캐시
- 키-값 데이터베이스

**명령어**:
```bash
# 기본: 100K 키, 1KB 값, 80% 읽기
python3 run_experiment.py --workload redis

# 대형 캐시 (1M 키, 4KB 값)
python3 run_experiment.py --workload redis --num-keys 1000000 --value-size 4096

# 쓰기 중심 워크로드
python3 run_experiment.py --workload redis --num-keys 100000 --read-ratio 0.3
```

**파라미터**:
| 파라미터 | 기본값 | 설명 |
|----------|--------|------|
| `--num-keys` | 100000 | 키 개수 |
| `--value-size` | 1024 | 값 크기 (바이트) |
| `--ops-per-sec` | 1000 | 목표 초당 작업 수 |
| `--read-ratio` | 0.8 | 읽기 작업 비율 (0.0-1.0) |

**메모리 사용량**: `~num_keys * (12 + value_size) bytes`

**특징**:
- 복원 후 데이터 무결성 검증 (체크섬 비교)
- 히트율 통계
- 검증을 위한 결정적 값 생성

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

**모델 크기별 구성**:
| 크기 | 입력 | 히든 레이어 | 출력 | 데이터셋 | 파라미터 수 |
|------|------|-------------|------|----------|-------------|
| small | 256 | [512, 256, 128] | 10 | 10K | ~200K |
| medium | 512 | [1024, 512, 256, 128] | 100 | 50K | ~1M |
| large | 1024 | [2048, 1024, 512, 256, 128] | 1000 | 100K | ~5M |

**의존성**: `torch`

---

### 5. Jupyter 노트북 시뮬레이션 워크로드

**시나리오**: 다양한 셀 타입(import, 데이터 로드, 계산, 시각화, 모델 학습)으로 대화형 Jupyter 노트북 세션 시뮬레이션

**사용 사례**:
- 대화형 데이터 과학 세션
- 연구 컴퓨팅 환경
- 교육용 컴퓨팅 랩
- 탐색적 데이터 분석

**명령어**:
```bash
# 기본: 지속적 셀 실행
python3 run_experiment.py --workload jupyter

# 제한된 셀 수 + 빠른 실행
python3 run_experiment.py --workload jupyter --num-cells 100 --cell-interval 1.0

# 느린 대화형 세션 시뮬레이션
python3 run_experiment.py --workload jupyter --cell-interval 10.0
```

**파라미터**:
| 파라미터 | 기본값 | 설명 |
|----------|--------|------|
| `--num-cells` | 0 | 실행할 셀 수 (0=무한) |
| `--cell-interval` | 3.0 | 셀 실행 간격 (초) |

**셀 타입 분포**:
| 타입 | 확률 | 설명 |
|------|------|------|
| import | 5% | 라이브러리 임포트 |
| markdown | 15% | 문서화 셀 |
| data_load | 15% | 데이터 로딩 |
| computation | 35% | 수치 계산 |
| visualization | 15% | 시각화 생성 |
| model | 15% | 모델 학습 |

**의존성**: `numpy` (선택사항이지만 권장)

---

### 6. 비디오 처리 워크로드

**시나리오**: ffmpeg과 유사한 비디오 인코딩/디코딩 작업 시뮬레이션, 합성 비디오 프레임 처리

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
| `--duration` | 0 | 지속 시간 (초, 0=무한) |

**일반 해상도**:
| 이름 | 해상도 | 프레임 크기 |
|------|--------|-------------|
| 720p | 1280x720 | 2.6 MB |
| 1080p | 1920x1080 | 5.9 MB |
| 4K | 3840x2160 | 23.7 MB |

**의존성**: `numpy` (선택사항이지만 권장)

**특징**:
- I-프레임 및 P-프레임 시뮬레이션 (GOP 패턴)
- zlib을 사용한 압축
- 프레임 레이트 유지

---

### 7. 데이터 처리 워크로드 (Pandas 유사)

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

### 기본 설정

```yaml
# config/default.yaml
experiment:
  name: "default-experiment"
  workload_type: "memory"
  save_metrics: true

nodes:
  ssh_user: "ubuntu"
  source:
    ip: "${SOURCE_NODE_IP}"
  destination:
    ip: "${DEST_NODE_IP}"

checkpoint:
  strategy:
    mode: "predump"           # 또는 "full"
    predump_iterations: 8
    predump_interval: 10
    lazy_pages: false

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

EBS 볼륨 attach/detach 패턴은 `aws-lab/ebs_test/live_migration_with_ebs.py`를 참조하세요.

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

| 워크로드 | 시나리오 | 메모리 패턴 | 의존성 |
|----------|----------|-------------|--------|
| `memory` | 메모리 할당 | 증가형 | 없음 |
| `matmul` | HPC/과학 계산 | 정적 | numpy |
| `redis` | 캐싱/KV 저장소 | 초기화 후 정적 | 없음 |
| `ml_training` | 딥러닝 학습 | 정적 | torch |
| `jupyter` | 대화형 데이터 과학 | 증가형 | numpy (선택) |
| `video` | 비디오 트랜스코딩 | 정적 | numpy (선택) |
| `dataproc` | ETL/배치 분석 | 초기화 후 정적 | numpy |
