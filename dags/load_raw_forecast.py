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
        for name in ["create_raw.sql", "create_dq.sql"]:
                hook.run((SQL_DIR / name).read_text(encoding="utf-8"))
        print("raw, dq 테이블 확인 완료")

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

    @task
    def load_files(pending):
        import boto3
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        LOG_SQL = """
            INSERT INTO raw.load_log
                (base_date, base_time, nx, ny, s3_key, expected_rows, loaded_rows,
                status, error, fetched_at, loaded_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (base_date, base_time, nx, ny) DO UPDATE SET
                s3_key = EXCLUDED.s3_key,
                expected_rows = EXCLUDED.expected_rows,
                loaded_rows = EXCLUDED.loaded_rows,
                status = EXCLUDED.status,
                error = EXCLUDED.error,
                fetched_at = EXCLUDED.fetched_at,
                loaded_at = now()
        """
        WHERE_FILE = "WHERE base_date = %s AND base_time = %s AND nx = %s AND ny = %s"

        bucket = os.environ["S3_BUCKET"]
        s3 = boto3.client("s3")
        conn = PostgresHook(postgres_conn_id="dw_postgres").get_conn()

        def write_log(p, loaded_rows, status, error=None, fetched_at=None):
            with conn.cursor() as cur:
                cur.execute(LOG_SQL, (p["base_date"], p["base_time"], p["nx"], p["ny"],
                                      p["raw_key"], p["rows"], loaded_rows,
                                      status, error, fetched_at))

        results = []
        for p in pending:
            file_key = (p["base_date"], p["base_time"], p["nx"], p["ny"])
            try:
                if p["status"] != "success":
                    write_log(p, None, "source_missing")
                    conn.commit()
                    results.append("source_missing")
                    continue

                body = s3.get_object(Bucket=bucket, Key=p["raw_key"])["Body"].read()
                raw = json.loads(body)
                values = [
                    (i["baseDate"], i["baseTime"], str(i["nx"]), str(i["ny"]), i["category"],
                     i["fcstDate"], i["fcstTime"], i["fcstValue"], p["raw_key"])
                    for i in raw["items"]
                ]

                with conn.cursor() as cur:
                    cur.execute("DELETE FROM raw.vilage_fcst " + WHERE_FILE, file_key)
                    cur.executemany(
                        "INSERT INTO raw.vilage_fcst (base_date, base_time, nx, ny, category, "
                        " fcst_date, fcst_time, fcst_value, s3_key)"
                        " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        values,
                    )
                    cur.execute("SELECT count(*) FROM raw.vilage_fcst " + WHERE_FILE, file_key)
                    loaded = cur.fetchone()[0]

                if loaded == p["rows"]:
                    write_log(p, loaded, "loaded", fetched_at=raw.get("fetched_at"))
                    conn.commit()
                    results.append("loaded")
                else:
                    conn.rollback()
                    write_log(p, loaded, "count_mismatch",
                              error=f"expected {p['rows']}, loaded {loaded}",
                              fetched_at=raw.get("fetched_at"))
                    conn.commit()
                    results.append("count_mismatch")

            except Exception as e:
                conn.rollback()
                write_log(p, None, "error", error=str(e)[:500])    
                conn.commit()
                results.append("error")

        conn.close()
        summary = {s: results.count(s) for s in set(results)}
        print(f"처리 {len(results)}건: {summary}")
        return summary

    @task(retries=0)
    def report(summary):
        print(f"적재 결과: {summary}")
        bad = {k: v for k, v in summary.items() if k != "loaded"}
        if bad:
            raise RuntimeError(f"적재되지 않은 파일 {sum(bad.values())}건: {bad}")

    pending = find_pending()
    ensure_tables() >> pending
    report(load_files(pending))

load_raw_forecast()
