# forecast-reliability-pipeline

아이와 야외 일정을 잡을 때, 몇 시간 전 예보까지 믿을 수 있을까?
기상청 단기예보와 실제 관측(ASOS)을 비교해 리드타임별 예보 신뢰도를 검증하는 데이터 파이프라인.

> 상태: 🚧 구축 중 — 예보 자동 수집 운영 중 (2026-11-16 02:15 KST 시작)

## 현재 구현: 예보 자동 수집

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

## 구조
```
collector/
  fetch_forecast.py    # 단기예보 API 호출 (로컬 실행 겸용)
  lambda_function.py   # Lambda 진입점: 관측소 4곳 수집, Raw·매니페스트 저장
docs/
  stations.md          # 관측소 좌표와 격자
```