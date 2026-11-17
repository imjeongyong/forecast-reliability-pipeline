"""시험용 DAG: 태스크 실행, S3읽기, XCom 전달 확인"""
import os
from datetime import datetime

from airflow.sdk import dag, task

@dag(
    dag_id="smoke_test",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["smoke"],
)
def smoke_test():

    @task
    def list_manifests():
        import boto3

        bucket = os.environ["S3_BUCKET"]
        s3 = boto3.client("s3")
        resp = s3.list_objects_v2(Bucket=bucket, Prefix="manifest/vilage_fcst/")
        keys = [obj["Key"] for obj in resp.get("Contents", [])]
        print(f"매니페스트 {len(keys)}개, 가장 최근: {max(keys) if keys else '없음'}")
        return len(keys)

    @task
    def report(count):
        print(f"전달받은 매니페스트 개수: {count}")

    report(list_manifests())

smoke_test()
