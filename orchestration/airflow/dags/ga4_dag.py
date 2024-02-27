from pendulum import datetime
from airflow import DAG
from airflow.models.param import Param
from cosmos.config import RenderConfig
from cosmos import DbtTaskGroup
from cosmos.operators import DbtDocsOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor

from dbt_config import project_config, profile_config  # type:ignore
from dbt_upload_docs import upload_docs  # type:ignore

import logging
import re
from datetime import datetime, timedelta


def replace_hyphens(date_string):
    pattern = r"(\d{4})-(\d{2})-(\d{2})"
    new_string = re.sub(pattern, r"\1\2\3", date_string)
    return new_string


def get_yesterday():
    now = datetime.now()
    yesterday = now - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y%m%d")
    return yesterday_str


with DAG(
    dag_id="ga4_dag",
    start_date=datetime(2022, 11, 27),
    schedule="@daily",
    tags=["dbt", "ga4"],
    catchup=False,
    params={
        "execution_date": Param(
            description="Execution date for dbt run",
            type="string",
            default=get_yesterday(),
        ),
        "start_date": Param(
            default="notset", description="Execution date for dbt run", type="string"
        ),
        "end_date": Param(
            default="notset", description="Execution date for dbt run", type="string"
        ),
    },
) as dag:
    ga4_table_existence_sensor = BigQueryTableExistenceSensor(
        task_id="ga4_table_existence_sensor",
        project_id="cadumagalhaes",
        dataset_id="analytics_317251007",
        table_id="events_{{ params.execution_date }}",
        poke_interval=60,
        timeout=60 * 60 * 8,
        gcp_conn_id="dbt_file_connection",
    )

    dbt_ga = DbtTaskGroup(
        group_id="google_analytics_4",
        project_config=project_config,
        profile_config=profile_config,
        render_config=RenderConfig(select=["path:models/googleanalytics"]),
        operator_args={
            "vars": {
                "execution_date": "{{ params.execution_date }}",
                "start_date": "{{ params.start_date }}",
                "end_date": "{{ params.end_date }}",
            }
        },
    )

    render_dbt_docs = DbtDocsOperator(
        task_id="render_dbt_docs",
        profile_config=profile_config,
        project_dir="/opt/airflow/dbt_project",
        callback=upload_docs,
    )

    ga4_table_existence_sensor >> dbt_ga >> render_dbt_docs
