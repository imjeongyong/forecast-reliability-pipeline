"""기상청 단기예보(getVilageFcst) 호출 — 로컬 1회 실행용"""
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

API_URL = "https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst"
KST = timezone(timedelta(hours=9))
BASE_HOURS = [2, 5, 8, 11, 14, 17, 20, 23]   # 단기예보 발표시각
AVAILABLE_DELAY = timedelta(minutes=10)       # 발표 후 API 제공까지 가정 (실측으로 확인 예정)
PAGE_SIZE = 1000


def latest_base_time(now):
    """now(KST) 기준으로 조회 가능한 최신 발표일자·발표시각"""
    t = now - AVAILABLE_DELAY
    for h in reversed(BASE_HOURS):
        if t.hour >= h:
            return t.strftime("%Y%m%d"), f"{h:02d}00"
    prev = t - timedelta(days=1)              # 02시 발표 전이면 전날 23시
    return prev.strftime("%Y%m%d"), "2300"


def fetch_vilage_fcst(service_key, base_date, base_time, nx, ny):
    """발표 1회 × 격자 1개의 예보 전체를 페이지를 나눠 받아 반환"""
    items = []
    page = 1
    while True:
        params = {
            "serviceKey": service_key,
            "pageNo": page,
            "numOfRows": PAGE_SIZE,
            "dataType": "JSON",
            "base_date": base_date,
            "base_time": base_time,
            "nx": nx,
            "ny": ny,
        }
        url = API_URL + "?" + urllib.parse.urlencode(params)
        with urllib.request.urlopen(url, timeout=10) as resp:
            body = resp.read().decode("utf-8")

        # 인증 오류 등은 JSON을 요청해도 XML로 오는 경우가 있음
        if not body.lstrip().startswith("{"):
            raise RuntimeError(f"JSON이 아닌 응답: {body[:300]}")

        data = json.loads(body)
        header = data["response"]["header"]
        if header["resultCode"] != "00":
            raise RuntimeError(f"API 오류 {header['resultCode']}: {header['resultMsg']}")

        result = data["response"]["body"]
        total = result["totalCount"]
        page_items = result["items"]["item"]
        items.extend(page_items)

        if len(items) >= total or not page_items:
            break
        page += 1

    return items, total


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    service_key = os.environ["KMA_SERVICE_KEY"]
    nx, ny = 60, 127                          # 테스트용 격자 (서울 중구·종로 일대)

    fetched_at = datetime.now(KST)
    base_date, base_time = latest_base_time(fetched_at)
    items, total = fetch_vilage_fcst(service_key, base_date, base_time, nx, ny)

    # Raw 저장: S3 키와 같은 구조 (발표일자/발표시각/격자)
    out = Path("data/raw") / base_date / base_time / f"{nx}_{ny}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "base_date": base_date,
        "base_time": base_time,
        "nx": nx,
        "ny": ny,
        "fetched_at": fetched_at.isoformat(),
        "total_count": total,
        "items": items,
    }
    out.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")

    # 점검 출력
    issued = datetime.strptime(base_date + base_time, "%Y%m%d%H%M")
    targets = sorted({i["fcstDate"] + i["fcstTime"] for i in items})
    last = datetime.strptime(targets[-1], "%Y%m%d%H%M")

    print(f"발표: {base_date} {base_time} / 격자: {nx},{ny}")
    print(f"totalCount={total}, 받은 행={len(items)}, 일치={total == len(items)}")
    print("category:", sorted({i["category"] for i in items}))
    print("PCP 값 종류:", sorted({i["fcstValue"] for i in items if i["category"] == "PCP"}))
    print(f"예보대상: {targets[0]} ~ {targets[-1]} (최대 리드타임 {(last - issued).total_seconds() / 3600:.0f}시간)")
    print(f"저장: {out}")
    