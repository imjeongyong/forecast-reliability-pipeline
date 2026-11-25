import json
import os
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from airflow.sdk import Param, dag, task

SQL_DIR = Path(__file__).parent / "sql"
KST = ZoneInfo("Asia/Seoul")
START_DATE = date(2026, 11, 15)
STAITIONS = ["108", "112", "90", "203"]
API_URL = "https://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList"

@dag(
    dag_id="load_raw_asos",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries":2},
    tags=["raw", "asos"],
    params={
        "recollector_from": Param(None, type=["null", "string"], description="재수집 시작일 YYYYMMDD"),
        "recollector_to": Param(None, type=["null", "string"], description="재수집 종료일 YYYYMMDD"),
    },
)
def load_raw_asos():

    @task
    def ensure_tables():
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id="dw_postgres")
        hook.run((SQL_DIR / "create_raw.sql").read_text(encoding="utf-8"))
        print("raw 테이블 확인 완료")

    @task
    def collect_missing(params=None):
        import boto3

        bucket = os.environ["S3_BUCKET"]
        service_key = os.environ["KMA_SERVICE_KEY"]
        s3 = boto3.client("s3")

        yesterday = datetime.now(KST).date() - timedelta(days=1)
        start, end = params.get("recollect_from"), params.get("recollect_to")
        force = bool(start and end)

        done = set()
        if not force:
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix="manifest/asos_hourly/"):
                for obj in page.get("Contents", []):
                    m = json.loads(s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read())
                    if m["succeeded"] == m["expected"]:
                        done.add(m["obs_date"])

        targets = []
        d = START_DATE
        while d <= yesterday:
            ymd = d.strftime("%Y%m%d")
            if (force and start <= ymd <= end) or (not force and ymd not in done):
                targets.append(ymd)
            d += timedelta(days=1)

        for ymd in targets:
            results = []
            for stn in STAITIONS:
                r = {"stn_id": stn, "status": "fail", "rows": None, "raw_key": None, "error": None}
                try:
                    query = {
                        "serviceKey": service_key, "pageNo": 1, "numOfRows": 100, "dataType": "JSON",
                        "dataCd": "ASOS", "dateCd": "HR",
                        "startDt": ymd, "startHh": "00", "endDt": ymd, "endHh": "23",
                        "stnIds": stn,
                    }
                    url = API_URL + "?" + urllib.parse.urlencode(query)
                    with urllib.request.urlopen(url, timeout=10) as resp:
                        data = json.loads(resp.read().decode("utf-8"))

                    header = data["response"]["header"]
                    if header["resultCode"] != "00":
                        raise RuntimeError(f"API {header['resultCode']} {header['resultMsg']}")
                    items = data["response"]["body"]["items"]["item"]
                    if len(items) != 24:
                        raise RuntimeError(f"rows {len(items)} != 24")

                    raw_key = f"raw/asos_hourly/obs_date={ymd}/stn={stn}.json"
                    raw = {
                                "obs_date": ymd, "stn_id": stn,
                                "fetched_at": datetime.now(KST).isoformat(),
                                "total_count": data["response"]["body"]["totalCount"],
                                "items": items,
                            }
                    s3.put_object(Bucket=bucket, Key=raw_key, Body=json.dumps(raw, ensure_ascii=False).encode("utf-8"),
                                ContentType="application/json")
                    r.update(status="success", rows=len(items), raw_key=raw_key)
                except Exception as e:
                    r["error"] = str(e)[:500]
                results.append(r)

            manifest = {
                "obs_date": ymd,
                "expected": len(STAITIONS),
                "succeeded": sum(1 for r in results if r["status"] == "success"),
                "results": results,
            }
            s3.put_object(Bucket=bucket, Key=f"manifest/asos_hourly/obs_date={ymd}.json",
                          Body=json.dumps(manifest, ensure_ascii=False).encode("utf-8"),
                          ContentType="application/json")
            print(f"{ymd}: {manifest['succeeded']}/{manifest['expected']}")

        print(f"수집 대상 {len(targets)}일 (강제 재수집 {force})")
        return len(targets)

    ensure_tables() >> collect_missing()

load_raw_asos()

            
