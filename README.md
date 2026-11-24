# forecast-reliability-pipeline

아이와 야외 일정을 잡을 때, 몇 시간 전 예보까지 믿을 수 있을까?
기상청 단기예보와 실제 관측(ASOS)을 비교해 리드타임별 예보 신뢰도를 검증하는 데이터 파이프라인.

> 상태: 🚧 구축 중 — 예보 자동 수집 운영 중 (2026-11-16 02:15 KST 시작) · S3 → PostgreSQL Raw 적재 (2026-11-18) · Raw DQ 검사·대사 (2026-11-24)

## 예보 자동 수집

EventBridge Scheduler → Lambda → S3

| 항목 | 내용 |
|---|---|
| 수집 대상 | 기상청 단기예보, ASOS 관측소 4곳의 격자 ([docs/stations.md](docs/stations.md)) |
| 주기 | 하루 8회, 발표 15분 후 (Asia/Seoul) |
| Raw 저장 | `raw/vilage_fcst/base_date=YYYYMMDD/base_time=HHMM/grid=nx_ny.json` |
| 매니페스트 | 발표별 1개. 관측소별 성공 여부, totalCount, 수신 행 수, 오류 |

### 설계 포인트
- **관측소별 실패 격리:** 한 곳이 실패해도 나머지는 계속 수집하고, 결과는 매니페스트에 남긴 뒤 실행을 오류로 종료
- **건수 대사:** totalCount와 수신 행 수가 다르면 실패로 처리하고 Raw를 저장하지 않음
- **멱등성:** 같은 발표·격자는 같은 경로에 덮어씀. 재시도·재수집해도 결과가 같음
- **재수집:** 이벤트에 발표일자·시각을 넘기면 해당 발표를 다시 수집
- **실패 알림:** 비동기 재시도 2회 후에도 실패하면 메일 알림 (Lambda 실패 대상 → SNS)
- **최소 권한:** Lambda 실행 역할은 `raw/`·`manifest/` 쓰기와 알림 발행만 허용

## Raw 적재 (로컬 Airflow)

S3 → Airflow → PostgreSQL `raw` 스키마

| 항목 | 내용 |
|---|---|
| 오케스트레이션 | Airflow 3.3.2, LocalExecutor, Docker Compose |
| DB | PostgreSQL 16 — Airflow 메타 DB와 데이터용 `dw` DB 분리 |
| DAG | `load_raw_forecast` — 매시 실행, `catchup=False`, `max_active_runs=1` |

```
ensure_tables → find_pending → load_files → dq_checks → report
```

| task | 하는 일 |
|---|---|
| `ensure_tables` | `raw`·`dq` 스키마와 테이블 생성 (`IF NOT EXISTS`) |
| `find_pending` | S3 매니페스트와 `raw.load_log`를 비교해 아직 적재되지 않은 파일만 선택. 기간 파라미터(`reload_from`, `reload_to`)를 주면 해당 기간을 강제 재적재 |
| `load_files` | 파일(발표 회차 × 격자) 단위로 DELETE → INSERT → 건수 대사 → `load_log` 기록을 한 트랜잭션으로 처리 |
| `dq_checks` | DQ 규칙 6개를 실행해 결과를 `dq.check_result`에 기록 |
| `report` | 적재되지 않은 파일이 있거나 error 규칙이 실패하면 실행을 실패로 표시 |

### 테이블
| 테이블 | 내용 |
|---|---|
| `raw.vilage_fcst` | API 응답 항목 1개 = 1행. 값은 모두 문자열 그대로 저장 (형 변환은 다음 단계) |
| `raw.load_log` | 파일 1개당 1행: 기대 건수, 적재 건수, 상태(`loaded` / `count_mismatch` / `source_missing` / `error`), 수집 시각 |

### 설계 포인트
- **날짜가 아니라 "미적재 파일" 기준으로 적재:** 수집은 클라우드에서 계속 돌고 Airflow는 로컬에서 켤 때만 돌기 때문에, 언제 실행하든 빠진 파일만 채우도록 함
- **파일 단위 트랜잭션:** 적재 도중 실패해도 데이터가 반쯤 남지 않음. 다시 실행해도 결과가 같음 (멱등)
- **대사:** 매니페스트에 적힌 행 수와 DB에 실제로 들어간 행 수(`count(*)`)를 비교하고, 다르면 롤백한 뒤 `count_mismatch`로 기록
- **실패 격리:** 파일 하나가 실패해도 나머지는 계속 적재하고, 실패한 파일은 `load_log`에 남아 다음 실행에서 다시 시도
- **Raw는 받은 그대로:** 중복 등 내용 검증은 적재 단계에서 막지 않고 DQ 단계에서 보고하도록 분리

### 검증 결과
| 확인 | 결과 |
|---|---|
| 첫 적재 (2026-11-18) | 100개 파일(25회차 × 4지점) 모두 `loaded` |
| 대사 | `load_log` 적재 건수 합계 = `raw.vilage_fcst` 행 수 = 92,756 |
| 재실행 | 적재 대상 0건 |
| 하루치(11/16) 강제 재적재 | 32개 파일을 다시 적재한 뒤에도 전체 행 수 92,756 유지, 해당 날짜만 적재 시각 갱신 |
| 5일 미실행 후 기동 (2026-11-23) | 실행 1건으로 밀린 156개 파일 적재, 누적 256파일 대사 일치 (236,896행) |

## DQ 검사·대사

규칙 1개 = SQL 파일 1개 (`dags/sql/dq/`). 각 쿼리는 **위반한 행만 반환**하고, 0행이면 pass. 결과는 실행마다 `dq.check_result`에 누적한다 (규칙별 위반 건수와 사례 최대 5개).

### 규칙
| 규칙 | 검사 | 관점 | severity |
|---|---|---|---|
| `missing_manifest` | 발표 후 1시간 30분이 지났는데 매니페스트가 없는 회차 | 완전성 | error |
| `rows_by_base_time` | 같은 발표 시각의 평소 행 수(최빈값)와 다른 파일 | 일관성 | error |
| `fetched_before_issue` | 수집 시각이 발표 시각보다 이름 (시간대 오류) | 정확성 | error |
| `fetch_delay` | 발표 후 30분이 넘어서 수집 | 적시성 | warn |
| `duplicate_rows` | 같은 자연키가 2행 이상 | 유일성 | error |
| `raw_vs_load_log` | 파일 단위로 `load_log` 적재 건수 ≠ 실제 행 수 | 정합성 | error |

- **error**는 실행을 실패로 표시하고, **warn**은 기록만 한다. 조치가 필요 없는 항목까지 실패로 표시하면 실패 표시가 무시되기 때문
- 발표 시각별 행 수는 고정되어 있지 않다 (예: 02시 944행, 11시 835행, 17시 1052행). 그래서 고정 기대값 대신 발표 시각별 최빈값을 기준으로 삼는다

### 층별 분담
| 층 | 검사 | 도구 |
|---|---|---|
| Raw | 다 들어왔는지, 평소와 같은지, 제때 왔는지, 중복이 없는지, 적재 기록과 맞는지 | SQL 규칙 + `dq.check_result` |
| Core (예정) | 값이 유효한지 (범위, 허용값, NULL) | dbt tests |

### 검증 결과 (2026-11-23 ~ 24)
| 확인 | 결과 |
|---|---|
| 실데이터 | 6개 규칙 모두 pass. 규칙마다 같은 범위를 다른 방법으로 센 숫자로 0건을 확인 (예: 전체 행 수 = 고유 자연키 수) |
| 위반 탐지 | 규칙별로 위반을 만들어 탐지 확인. 5개는 `BEGIN … ROLLBACK`으로 DB에 흔적 없이, `duplicate_rows`는 테스트 행을 넣어 DAG 전체로 실행 → `report` 실패 표시 확인 후 테스트 행 삭제 |
| 실데이터에서 찾은 사례 | `fetch_delay`가 스케줄러 설정 전 수동 첫 수집(2026-11-15 23시)을 탐지 → 이 규칙은 스케줄 수집 이후만 검사하도록 범위를 정함 |

### 대응
누락 회차는 자동으로 탐지하고, 재수집은 수동으로 한다. 기준과 절차는 [docs/runbook.md](docs/runbook.md).

## 현재 한계
- 재수집은 수동. 단기예보 API가 지난 발표를 얼마나 보관하는지 확인하지 못함
- 적재 실패(`source_missing`)로 실행이 실패 표시되는 경로는 실제 실패 상황으로 확인하지 못함 (DQ 실패 경로로는 확인)
- `fetch_delay`는 수집이 늦었는지만 본다. 기상청 API가 발표 후 몇 분 뒤 데이터를 제공하는지는 확인하지 못함

## 로컬 실행
```bash
cp .env.example .env        # 값 채우기 (API 키, 버킷, Fernet 키, JWT 비밀값)
docker compose up -d
# http://localhost:8080
```

## 구조
```
collector/
  fetch_forecast.py      # 단기예보 API 호출 (로컬 실행 겸용)
  lambda_function.py     # Lambda 진입점: 관측소 4곳 수집, Raw·매니페스트 저장
dags/
  load_raw_forecast.py   # S3 → PostgreSQL Raw 적재 + DQ 검사
  smoke_test.py          # 환경 확인용
  sql/
    create_raw.sql       # raw 스키마 DDL
    create_dq.sql        # dq 스키마 DDL
    dq/                  # DQ 규칙 6개 (규칙 1개 = 파일 1개)
docker/
  postgres-init/         # dw DB·사용자 생성 (최초 기동 시 1회)
docker-compose.yaml      # Airflow + PostgreSQL
docs/
  stations.md            # 관측소 좌표와 격자
  runbook.md             # 운영 런북: 확인, 대응, 재수집, 재적재
```