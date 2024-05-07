import logging

try:
    from airflow.operators.bash_operator import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator

from common import DEFAULT_ARGS, DEFAULT_TAGS, create_dag

logger = logging.getLogger(__name__)


job_id = "build-accountant-reports"
dag_id = f"monthly-{job_id}"
tags = [*DEFAULT_TAGS, f"job:{job_id}", "schedule:monthly", "automated"]
manually_dag_id = f"manually-{job_id}"
manually_tags = [*DEFAULT_TAGS, f"job:{job_id}", "manually"]

args = {
    **DEFAULT_ARGS,
}

tasks = [
    {"name": "run-accountant-models", "args": "dbt-postgres:run_balance_model"},
]

dag = create_dag(dag_id, tags, args, tasks, schedule_interval="0 23 L * *")
manually_dag = create_dag(manually_dag_id, manually_tags, args, tasks, schedule_interval=None)
