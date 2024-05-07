import logging

try:
    from airflow.operators.bash_operator import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator

from common import DEFAULT_ARGS, DEFAULT_TAGS, create_dag

logger = logging.getLogger(__name__)


job_id = "elt-bizflycloud-revenue"
dag_id = f"daily-{job_id}"
tags = [*DEFAULT_TAGS, f"job:{job_id}", "schedule:daily", "automated"]
manually_dag_id = f"manually-{job_id}"
manually_tags = [*DEFAULT_TAGS, f"job:{job_id}", "manually"]

args = {
    **DEFAULT_ARGS,
}

tasks = [
    {"name": "Sync-Bizfly-CRM", "args": "tap-bizflycrm target-postgres"},
    {"name": "Sync-Payment-Request", "args": "tap-payment-request target-postgres"},
    {"name": "Sync-Admin-Data", "args": "tap-admin target-postgres"},
    {"name": "Sync-Billing-DB", "args": "tap-postgres target-postgres"},
    {"name": "Sync-V4-Invoices", "args": "tap-billing target-postgres"},
    {"name": "Daily-Snapshot", "args": "dbt-postgres:daily_snapshot"},
    {"name": "Daily-Run", "args": "dbt-postgres:daily_run"},
]

dag = create_dag(dag_id, tags, args, tasks, schedule_interval="0 19 * * *")
manually_dag = create_dag(manually_dag_id, manually_tags, args, tasks, schedule_interval=None)
