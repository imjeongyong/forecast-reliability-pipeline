from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task

SQL_DIR = Path(__file__).parent / "sql"


@dag(
    dag_id="load_raw_forecast",
    schedule="@hourly",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2},
    tags=["raw"],
)
def load_raw_forecast():

    @task
    def ensure_tables():
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id="dw_postgres")
        hook.run((SQL_DIR / "create_raw.sql").read_text(encoding="utf-8"))
        print("raw 테이블 확인 완료")

    ensure_tables()

load_raw_forecast()
