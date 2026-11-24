# 운영 런북

수집(Lambda)과 적재·검사(Airflow `load_raw_forecast`)를 운영할 때 확인하고 대응하는 절차.

## 1. 매일 확인

### 1-1. 실행 결과
Airflow UI → `load_raw_forecast` → 가장 최근 실행의 `report` task
- 초록색: 적재·DQ 모두 정상
- 빨간색: 적재되지 않은 파일이 있거나 error 규칙이 fail → 로그의 `RuntimeError` 메시지 확인

### 1-2. 최근 DQ 결과
```sql
SELECT check_name, severity, status, failed_count, sample
FROM dq.check_result
WHERE run_id = (SELECT run_id FROM dq.check_result ORDER BY checked_at DESC LIMIT 1)
ORDER BY check_name;
```

### 1-3. 규칙별 실패 이력
```sql
SELECT check_name,
       count(*) FILTER (WHERE status = 'fail') AS fail_runs,
       max(checked_at) FILTER (WHERE status = 'fail') AS last_fail
FROM dq.check_result
GROUP BY check_name
ORDER BY check_name;
```

### 1-4. 적재 상태
```sql
SELECT status, count(*) FROM raw.load_log GROUP BY status;
```

> 시각 컬럼은 UTC로 표시된다. KST로 보려면 `컬럼 AT TIME ZONE 'Asia/Seoul'`.

## 2. 대응표

### 2-1. 적재 상태 (`raw.load_log.status`)
| 상태 | 의미 | 조치 |
|---|---|---|
| `source_missing` | 수집 단계에서 실패해 S3에 파일이 없음 | 3. 재수집 |
| `count_mismatch` | S3 파일 행 수와 매니페스트 건수가 다름 (적재는 롤백됨) | S3 원본과 매니페스트 확인 → 3. 재수집 |
| `error` | 적재 중 예외 | `error` 컬럼 확인 → 원인 해결 후 다음 정시 실행이 자동 재시도 |

### 2-2. DQ 규칙 (`dq.check_result`)
| 규칙 | severity | 의미 | 원인 후보 | 조치 |
|---|---|---|---|---|
| `missing_manifest` | error | 발표 후 1시간 30분이 지났는데 매니페스트가 없는 회차 | 스케줄러 비활성, Lambda 호출 실패 | EventBridge 스케줄 상태와 Lambda 모니터링 확인 → 3. 재수집 |
| `rows_by_base_time` | error | 같은 발표 시각의 평소 행 수(최빈값)와 다른 파일 | 기상청 응답 일부 누락, 응답 형식 변경 | `sample`의 회차를 S3 원본에서 확인. 한 회차만 다르면 재수집, 여러 회차가 같은 새 값이면 형식 변경으로 판단하고 기록 |
| `fetched_before_issue` | error | 수집 시각이 발표 시각보다 이름 | 수집 코드의 시간대 계산 오류 | 수집 코드의 발표 시각 계산(KST) 확인 후 해당 회차 재수집 |
| `fetch_delay` | warn | 발표 후 30분이 넘어서 수집됨 (2026-11-16 02시 발표 이후만 검사) | Lambda 재시도 발생 | 기록만. 반복되면 Lambda 로그 확인 |
| `duplicate_rows` | error | Raw에 같은 자연키가 2행 이상 | 적재 로직 변경, 수동 INSERT | `sample`의 회차를 4. 재적재 (파일 단위 DELETE 후 INSERT로 정리됨) |
| `raw_vs_load_log` | error | 파일 단위로 `load_log` 적재 건수와 실제 행 수가 다름 | 적재 후 Raw를 수동으로 변경 | `sample`의 회차를 4. 재적재 |

## 3. 재수집 (Lambda 수동 호출)

대상: `missing_manifest`로 잡힌 회차, `source_missing`·`count_mismatch` 파일

```powershell
'{"base_date": "20261120", "base_time": "0200"}' | Out-File -Encoding ascii payload.json
aws lambda invoke --function-name frp-forecast-collector --invocation-type Event `
  --payload fileb://payload.json --region ap-northeast-2 --profile frp response.json
```
- 비동기 호출이라 즉시 `StatusCode: 202`만 돌아옴. 실제 결과는 S3 매니페스트로 확인
- 다음 정시 실행에서 `find_pending`이 새 매니페스트를 찾아 자동 적재 → `missing_manifest`가 pass로 바뀌는지 확인
- **재수집 가능 기간: 미확인.** 단기예보 API는 지난 발표를 오래 보관하지 않음 (2026-11-01 발표 호출은 실패함). 기간 밖 회차를 호출하면 재시도 후 실패 알림 메일이 옴
- 재수집할 수 없는 회차는 분석 결과의 한계로 README에 기록

## 4. 재적재 (S3 → Raw)

Airflow UI → `load_raw_forecast` → 트리거 → 파라미터
- `reload_from`: 시작일 `YYYYMMDD`
- `reload_to`: 종료일 `YYYYMMDD`

해당 기간의 파일을 상태와 관계없이 다시 적재한다 (파일 단위 DELETE → INSERT, 멱등).

확인:
```sql
SELECT base_date, count(*) AS files,
       max(loaded_at) AT TIME ZONE 'Asia/Seoul' AS last_loaded_kst
FROM raw.load_log
GROUP BY base_date
ORDER BY base_date;
```
재적재한 날짜만 `last_loaded_kst`가 갱신되고, 1-4의 적재 건수 합계와 Raw 행 수가 같으면 정상.

## 5. 알려진 예외

| 회차 | 내용 | 처리 |
|---|---|---|
| 2026-11-15 23시 (4파일) | 스케줄러 설정 전 수동 첫 수집. 발표 후 1시간 54분에 수집 | `fetch_delay` 검사 범위(2026-11-16 02시 발표부터) 밖. 데이터는 정상 |

## 6. 규칙 검증 방법

규칙 SQL을 고친 뒤, 위반을 일부러 만들었을 때 잡히는지 확인한다. `BEGIN … ROLLBACK`으로 실행하므로 DB에 흔적이 남지 않는다.

```powershell
$rule = Get-Content dags/sql/dq/<규칙>.sql -Raw
"BEGIN; <위반 만들기>; $rule; ROLLBACK;" | docker compose exec -T postgres psql -U dw -d dw
```

| 규칙 | 위반 만들기 | 기대 결과 |
|---|---|---|
| `missing_manifest` | `DELETE FROM raw.load_log WHERE base_date = '20261120' AND base_time = '0200'` | 해당 회차 1행 |
| `rows_by_base_time` | `UPDATE raw.load_log SET loaded_rows = loaded_rows - 1 WHERE base_date = '20261120' AND base_time = '0200' AND nx = '60' AND ny = '127'` | 1행 (943 vs 944) |
| `fetched_before_issue` | `UPDATE raw.load_log SET fetched_at = fetched_at - interval '9 hours' WHERE …(같은 파일)` | 1행 |
| `raw_vs_load_log` | `DELETE FROM raw.vilage_fcst WHERE ctid = (SELECT ctid FROM raw.vilage_fcst WHERE …(같은 파일) LIMIT 1)` | 1행 (944 vs 943) |

`duplicate_rows`와 `report` 실패 경로는 DAG 전체로 확인한다.
1. 한 행을 `s3_key = 'TEST_DUP'`로 복사해 INSERT
2. DAG 실행 → `duplicate_rows` fail, `report` 빨간색
3. `DELETE FROM raw.vilage_fcst WHERE s3_key = 'TEST_DUP'` → 다시 실행해 초록색 확인

마지막으로 1-4와 Raw 행 수가 원래와 같은지 확인한다.