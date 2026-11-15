"""Lambda 진입점 — 발표 1회 × 관측소 4곳 수집 → S3 Raw 저장 + 매니페스트 기록"""
import json
import os
from datetime import datetime

import boto3

from fetch_forecast import KST, fetch_vilage_fcst, latest_base_time

STATIONS = [
    {"stn_id": 108, "name": "서울", "nx": 60, "ny": 127},
    {"stn_id": 112, "name": "인천", "nx": 54, "ny": 125},
    {"stn_id": 90, "name": "속초", "nx": 87, "ny": 142},
    {"stn_id": 203, "name": "이천", "nx": 69, "ny": 120},
]

s3 = boto3.client("s3")


def put_json(bucket, key, obj):
    """dict를 JSON으로 바꿔 S3에 저장 (같은 key면 덮어씀)"""
    body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")


def collect_one(service_key, bucket, base_date, base_time, stn):
    """격자 1곳 수집 → Raw 저장. 결과를 매니페스트 한 줄(dict)로 반환"""
    result = {"stn_id": stn["stn_id"], "nx": stn["nx"], "ny": stn["ny"],
              "status": "fail", "total_count": None, "rows": None, "error": None}
    try:
        items, total = fetch_vilage_fcst(service_key, base_date, base_time, stn["nx"], stn["ny"])
        result["total_count"] = total
        result["rows"] = len(items)
        if total != len(items):
            result["error"] = "count_mismatch"
            return result

        key = (f"raw/vilage_fcst/base_date={base_date}/base_time={base_time}"
               f"/grid={stn['nx']}_{stn['ny']}.json")
        put_json(bucket, key, {
            "base_date": base_date, "base_time": base_time,
            "stn_id": stn["stn_id"], "nx": stn["nx"], "ny": stn["ny"],
            "fetched_at": datetime.now(KST).isoformat(),
            "total_count": total, "items": items,
        })
        result["status"] = "success"
        result["raw_key"] = key
    except Exception as e:
        result["error"] = str(e)[:500]
    return result


def handler(event, context):
    service_key = os.environ["KMA_SERVICE_KEY"]
    bucket = os.environ["S3_BUCKET"]
    started_at = datetime.now(KST)

    # 재처리: event에 발표일자·시각이 있으면 그 발표를, 없으면 최신 발표를 수집
    event = event or {}
    if "base_date" in event and "base_time" in event:
        base_date, base_time = event["base_date"], event["base_time"]
    else:
        base_date, base_time = latest_base_time(started_at)

    results = [collect_one(service_key, bucket, base_date, base_time, stn) for stn in STATIONS]
    succeeded = sum(1 for r in results if r["status"] == "success")

    put_json(bucket, f"manifest/vilage_fcst/base_date={base_date}/base_time={base_time}.json", {
        "base_date": base_date, "base_time": base_time,
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(KST).isoformat(),
        "expected": len(STATIONS), "succeeded": succeeded,
        "results": results,
    })

    if succeeded < len(STATIONS):
        raise RuntimeError(f"수집 실패 {len(STATIONS) - succeeded}/{len(STATIONS)}건: {base_date} {base_time}")
    return {"base_date": base_date, "base_time": base_time, "succeeded": succeeded}


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    print(handler({}, None))
    