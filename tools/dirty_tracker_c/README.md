# C Dirty Tracker

## 개요

C dirty tracker는 Linux 6.7+의 `PAGEMAP_SCAN` ioctl과 userfaultfd write-protection(uffd-wp)을
결합하여 atomic dirty page tracking을 수행한다. Uffd-wp 등록을 위해 ptrace syscall injection을
사용하며, soft-dirty bit에 전혀 영향을 미치지 않는 독립 채널로 동작한다.

uffd-wp setup이 실패하면 에러로 종료한다 (soft-dirty fallback 없음).
CRIU의 `--track-mem`과 동시에 사용할 수 있다.

자식 프로세스(descendant)를 자동으로 발견하고 추적하며, `--exclude-pid`로 특정 PID를 제외할 수 있다.

기술적 상세 내용은 [HOT_PAGE_SKIP_DESIGN.md](../../../HOT_PAGE_SKIP_DESIGN.md)의
"1.2 Dirty Page 수집 메커니즘" 섹션을 참조한다.

## 빌드

```bash
cd criu_workload/tools/dirty_tracker_c
make
```

## 동작 모드

| 모드 | 설명 | Soft-dirty 영향 |
|------|------|-----------------|
| **기본 (uffd-wp)** | `PM_SCAN_WP_MATCHING`으로 atomic scan+clear | 없음 (`PAGE_IS_WRITTEN` 사용) |
| **--no-clear** | Soft-dirty bit scan-only, clear 없이 누적 | 없음 (읽기만) |
| **--dual-channel (-D)** | WP + soft-dirty 동시 수집 (sd는 누적) | 없음 |
| **-D --sd-clear** | WP + soft-dirty 동시 수집 (sd도 delta) | **있음** (`clear_refs` 호출) |

기본 모드와 `--dual-channel`은 CRIU의 soft-dirty tracking과 독립적으로 동작한다.
`--sd-clear`는 soft-dirty를 clear하므로 CRIU와 병행 사용 시 주의가 필요하다.

## 사용법

### 기본 사용 (uffd-wp)

```bash
sudo ./dirty_tracker -p <PID> -i 100 -d 10
sudo ./dirty_tracker -p <PID> -i 100 -d 10 -o result.json
```

### no-clear 모드 (dirty bit 누적)

```bash
sudo ./dirty_tracker -p <PID> -i 100 -d 10 --no-clear
```

### Dual-channel (WP + soft-dirty 비교)

```bash
# sd 누적 모드: wp는 delta, sd는 누적
sudo ./dirty_tracker -p <PID> -i 200 -d 10 -D -o dual.json

# sd delta 모드: 두 채널 모두 delta (CRIU 간섭 주의)
sudo ./dirty_tracker -p <PID> -i 200 -d 10 -D -S -o dual_clear.json
```

### dirty_track_only.py를 통한 사용

```bash
python3 experiments/dirty_track_only.py --workload matmul --duration 30 --dirty-tracker c
```

## CLI 옵션

| 옵션 | 설명 | 기본값 |
|------|------|--------|
| `-p, --pid PID` | 추적 대상 프로세스 PID | (필수) |
| `-i, --interval MS` | 샘플링 간격 (밀리초) | 100 |
| `-d, --duration SEC` | 추적 시간 (초) | 10 |
| `-o, --output FILE` | JSON 출력 파일 경로 | stdout |
| `-w, --workload NAME` | 워크로드 이름 (JSON에 기록) | "unknown" |
| `--no-clear` | dirty bit clear 없이 누적 (WP 미사용) | false |
| `-D, --dual-channel` | WP + soft-dirty 동시 수집 | false |
| `-S, --sd-clear` | dual-channel에서 soft-dirty도 매 scan마다 clear | false |
| `-C, --no-track-children` | 자식 프로세스 추적 비활성화 | false (기본 추적) |
| `-E, --exclude-pid PID` | 특정 PID 추적 제외 (반복 가능) | - |

## JSON 출력 형식

### 기본 모드 (단일 채널)

```json
{
  "samples": [
    {
      "timestamp_ms": 200.0,
      "dirty_pages": [{"addr": "0x...", "vma_type": "heap", ...}],
      "delta_dirty_count": 107,
      "pids_tracked": [12345]
    }
  ]
}
```

### Dual-channel 모드

```json
{
  "dual_channel": true,
  "samples": [
    {
      "timestamp_ms": 200.0,
      "wp_channel": {
        "dirty_pages": [...],
        "dirty_count": 107,
        "dirty_size_bytes": 438272
      },
      "sd_channel": {
        "dirty_pages": [...],
        "dirty_count": 3986,
        "dirty_size_bytes": 16326656
      },
      "pids_tracked": [12345]
    }
  ]
}
```

- `wp_channel`: 이번 interval에 write된 page (매 interval clear + re-protect)
- `sd_channel`: soft-dirty 누적 (기본) 또는 delta (`--sd-clear` 시)

## 테스트 결과

### Dual-channel --sd-clear (matmul 256x256, 5초, interval 200ms)

```
Sample 10: wp=111 sd=111 dirty pages
Sample 20: wp=111 sd=111 dirty pages
```

두 채널이 동일한 delta 결과를 보여, uffd-wp와 soft-dirty가 동일한 write를 추적함을 검증.

### Dual-channel 누적 비교 (matmul 256x256, 3초, interval 500ms)

```
Sample 0: wp=    0 (delta), sd=    0 (cumulative)
Sample 1: wp=  108 (delta), sd= 3986 (cumulative)
Sample 2: wp=  110 (delta), sd= 3986 (cumulative)
Sample 3: wp=  107 (delta), sd= 3986 (cumulative)
```

- WP: 매 interval ~107-110 pages (delta)
- SD: 3986 pages (clear 안 해서 누적, 변화 없음)

### Soft-dirty 독립성 검증

| 시점 | Soft-dirty pages |
|------|-----------------|
| WP tracker 실행 전 | 7,359 |
| WP tracker 5초 실행 후 | 7,359 (변화 없음) |

## 자식 프로세스 추적

기본적으로 root PID의 모든 descendant 프로세스를 자동 발견하고 추적한다.
`/proc/{pid}/task/{tid}/children`을 재귀적으로 탐색하여 BFS로 발견한다.

```bash
# 기본: 자식 프로세스 포함 추적
sudo ./dirty_tracker -p <PID> -d 10

# 자식 추적 비활성화
sudo ./dirty_tracker -p <PID> -d 10 --no-track-children

# 특정 PID 제외
sudo ./dirty_tracker -p <PID> -d 10 --exclude-pid 12345 --exclude-pid 12346
```

매 sample 수집 시:
1. `discover_descendants()`로 새 자식 프로세스 발견
2. 죽은 프로세스 제거 (`kill(pid, 0)` → `ESRCH`)
3. 각 프로세스의 dirty page를 aggregate하여 하나의 sample에 기록

JSON 출력의 `pids_tracked` 배열이 sample별로 추적된 PID를 포함하고,
`summary.total_pids_seen`에 전체 추적 이력이 기록된다.

## CRIU 호환성

### uffd-wp Cleanup

Tracker 종료 시 `cleanup_userfaultfd_wp_for_process()`가 자동 호출되어:
1. 대상 프로세스에 ptrace로 재접근
2. 모든 writable VMA에 대해 `UFFDIO_UNREGISTER` 실행
3. 대상 프로세스 내의 userfaultfd fd를 `close()`
4. 레지스터/명령어 복원 후 detach

이를 통해 CRIU dump 전에 VM_UFFD_WP 플래그와 uffd fd가 정리된다.

**주의사항:**
- SIGTERM/SIGINT → `stop_flag` → main loop 탈출 → `tracker_cleanup()` → cleanup 정상 동작
- SIGKILL로 tracker가 죽으면 cleanup 불가 → 대상 프로세스에 uffd fd와 VM_UFFD_WP이 남음
- 대상 프로세스가 이미 종료된 경우 cleanup을 건너뛰고 정상 반환

## 요구사항

- Linux 6.7+ (PAGEMAP_SCAN ioctl)
- root 권한 (ptrace + /proc/PID/pagemap 접근)
- x86_64 아키텍처 (ptrace injection은 x86_64 레지스터 규약 사용)
