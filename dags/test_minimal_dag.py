"""
Minimal test DAG with zero project dependencies.
Used to verify that the scheduler discovers and displays DAGs.
If this DAG appears in the UI but crypto_pipeline_* do not, the issue is with the main DAG file.
"""
from datetime import datetime
from airflow.sdk import dag, task


@dag(
    dag_id="test_minimal_dag",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["test"],
)
def test_minimal_dag():
    @task
    def hello():
        print("Hello from minimal test DAG")

    hello()


test_minimal_dag()
