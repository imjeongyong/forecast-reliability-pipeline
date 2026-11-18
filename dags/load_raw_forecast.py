import json
import os

from datetime import datetime
from pathlib import Path

from airflow.sdk import Param, dag, task

SQL_DIR = Path(__file__).parent / "sql"


@dag(
    dag_id="load_raw_forecast",
    schedule="@hourly",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2},
    tags=["raw"],
    params={
        "reload_from": Param(None, type=["null", "string"], description="재적재 시작일 YYYYMMDD"),
        "reload_to": Param(None, type=["null", "string"], description="재적재 종료일 YYYYMMDD"),
    },
)
def load_raw_forecast():

    @task
    def ensure_tables():

        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id="dw_postgres")
        hook.run((SQL_DIR / "create_raw.sql").read_text(encoding="utf-8"))
        print("raw 테이블 확인 완료")

    @task
    def find_pending(params=None):
        import boto3
        from airflow.providers.postgres.hooks.postgres import PostgresHook


        start, end = params.get("reload_from"), params.get("reload_to")
        force = bool(start and end)

        hook = PostgresHook(postgres_conn_id="dw_postgres")
        rows = hook.get_records(
            "SELECT base_date, base_time, nx, ny FROM raw.load_log WHERE status = 'loaded'"
        )
        loaded = {tuple(r) for r in rows}

        bucket = os.environ["S3_BUCKET"]
        s3 = boto3.client("s3")
        paginator = s3.get_paginator("list_objects_v2")

        pending = []
        for page in paginator.paginate(Bucket=bucket, Prefix="manifest/vilage_fcst/"):
            for obj in page.get("Contents", []):
                body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
                m = json.loads(body)

                if force and not (start <= m["base_date"] <= end):
                    continue

                for r in m["results"]:
                    key = (m["base_date"], m["base_time"], str(r["nx"]), str(r["ny"]))
                    if not force and key in loaded:
                        continue
                    pending.append({
                        "base_date": key[0], "base_time": key[1], "nx": key[2], "ny": key[3],
                        "status": r["status"], "raw_key": r.get("raw_key"), "rows": r.get("rows"),
                    })

        print(f"이미 적재 {len(loaded)}건 / 적재 대상 {len(pending)}건 / 강제 재적재 {force}")
        return pending

    ensure_tables() >> find_pending()

load_raw_forecast()
