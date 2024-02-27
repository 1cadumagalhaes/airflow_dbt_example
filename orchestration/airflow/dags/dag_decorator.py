from pendulum import datetime
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator


@dag(
    dag_id="demo_decorator",
    schedule_interval="@daily",
    start_date=datetime(2023, 12, 1),
)
def demo():
    hello = BashOperator(task_id="hello", bash_command="echo hello")

    @task()
    def airflow():
        print("airflow")

    hello >> airflow()


demo_dag = demo()
