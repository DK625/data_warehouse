import logging
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

try:
    from airflow.operators.bash_operator import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator

from common import DEFAULT_ARGS, DEFAULT_TAGS, create_dag, PROJECT_ROOT, MELTANO_BIN, ENV_KEY
from utils import reset_cursor, dt_to_utc
from dateutil import parser

logger = logging.getLogger(__name__)

job_id = "sync-balances-every-day"
automated_dag_id = f"automated-{job_id}"
automated_tags = [*DEFAULT_TAGS, f"job:{job_id}", "schedule:monthly", "automated"]
manually_dag_id = f"manually-{job_id}"
manually_tags = [*DEFAULT_TAGS, f"job:{job_id}", "manually"]

args = {
    **DEFAULT_ARGS,
}

tasks = [
    {"name": "sync-balance", "args": "tap-balance target-postgres"},
]

STATE_ID = f"{ENV_KEY}:tap-balance-to-target-postgres"


def reset_balance_cursor(cursor):
    raw_cursor = cursor
    if not raw_cursor:
        logger.warning("Cursor value is not provided. Skipping cursor reset.")
        return
    try:
        dt = parser.parse(raw_cursor)
    except ValueError:
        raise ValueError("Cursor is not a valid date")
    dt = dt_to_utc(dt)
    cursor = dt.isoformat()
    reset_cursor(state_id=STATE_ID, meltano_bin=MELTANO_BIN, working_dir=PROJECT_ROOT, new_cursor=cursor)


cursor_value = (datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(hours=7) - timedelta(
    days=1)).isoformat()

reset_cursor_task = PythonOperator(
    task_id="reset-cursor",
    python_callable=reset_balance_cursor,
    provide_context=True,
    op_kwargs={"cursor": cursor_value}
)
manually_reset_cursor_task = PythonOperator(
    task_id="reset-cursor",
    python_callable=reset_balance_cursor,
    provide_context=True,
)

automated_dag = create_dag(
    automated_dag_id, automated_tags, args, tasks, schedule_interval="0 19 * * *", prev_operators=[reset_cursor_task]
)
manual_dag = create_dag(
    manually_dag_id, manually_tags, args, tasks, schedule_interval=None, prev_operators=[manually_reset_cursor_task]
)
