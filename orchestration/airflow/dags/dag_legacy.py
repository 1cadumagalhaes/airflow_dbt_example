from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# A DAG represents a workflow, a collection of tasks
with DAG(
    dag_id="demo", start_date=datetime(2023, 12, 1), schedule_interval="@daily"
) as dag:
    # Tasks are represented as operators
    hello = BashOperator(task_id="hello", bash_command="echo hello")

    def airflow():
        print("airflow")

    python_airflow = PythonOperator(task_id="airflow", python_callable=airflow)

    # Set dependencies between tasks
    hello >> python_airflow
