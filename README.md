# forecast-reliability-pipeline

아이와 야외 일정을 잡을 때, 몇 시간 전 예보까지 믿을 수 있을까?
기상청 단기예보와 실제 관측(ASOS)을 비교해 리드타임별 예보 신뢰도를 검증하는 데이터 파이프라인.

> 상태: 🚧 구축 중 — 예보 자동 수집 운영 중 (2026-11-16 02:15 KST 시작) · S3 → PostgreSQL Raw 적재 DAG 동작 (2026-11-18)

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
ensure_tables → find_pending → load_files → report
```

| task | 하는 일 |
|---|---|
| `ensure_tables` | `raw` 스키마와 테이블 생성 (`IF NOT EXISTS`) |
| `find_pending` | S3 매니페스트와 `raw.load_log`를 비교해 아직 적재되지 않은 파일만 선택. 기간 파라미터(`reload_from`, `reload_to`)를 주면 해당 기간을 강제 재적재 |
| `load_files` | 파일(발표 회차 × 격자) 단위로 DELETE → INSERT → 건수 대사 → `load_log` 기록을 한 트랜잭션으로 처리 |
| `report` | 적재되지 않은 파일이 있으면 실행을 실패로 표시 |

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
- **Raw는 받은 그대로:** 중복 등 내용 검증은 적재 단계에서 막지 않고 별도 DQ 단계에서 보고하도록 분리

### 검증 결과 (2026-11-18)
| 확인 | 결과 |
|---|---|
| 첫 적재 | 100개 파일(25회차 × 4지점) 모두 `loaded` |
| 대사 | `load_log` 적재 건수 합계 = `raw.vilage_fcst` 행 수 = 92,756 |
| 재실행 | 적재 대상 0건 |
| 하루치(11/16) 강제 재적재 | 32개 파일을 다시 적재한 뒤에도 전체 행 수 92,756 유지, 해당 날짜만 적재 시각 갱신 |

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
  load_raw_forecast.py   # S3 → PostgreSQL Raw 적재
  smoke_test.py          # 환경 확인용
  sql/
    create_raw.sql       # raw 스키마 DDL
docker/
  postgres-init/         # dw DB·사용자 생성 (최초 기동 시 1회)
docker-compose.yaml      # Airflow + PostgreSQL
docs/
  stations.md            # 관측소 좌표와 격자
```