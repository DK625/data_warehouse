import logging

from airflow.operators.python import PythonOperator

try:
    from airflow.operators.bash_operator import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator

from common import DEFAULT_ARGS, DEFAULT_TAGS, create_dag, PROJECT_ROOT, MELTANO_BIN
from utils import reset_all_cursor, dt_to_utc
from dateutil import parser

logger = logging.getLogger(__name__)


manually_dag_id = f"manually-reset-all-state"
manually_tags = [
    *DEFAULT_TAGS,
    f"job:reset-all-state",
    "manually"
]

args = {
    **DEFAULT_ARGS,
}


def reset_all_state_cursor(**kwargs):
    raw_cursor = kwargs.get("params", {}).get("cursor")
    if not raw_cursor:
        raise ValueError("Cursor is required")
    try:
        dt = parser.parse(raw_cursor)
    except ValueError:
        raise ValueError("Cursor is not a valid date")
    dt = dt_to_utc(dt)
    cursor = dt.isoformat()

    reset_all_cursor(meltano_bin=MELTANO_BIN, working_dir=PROJECT_ROOT, new_cursor=cursor)


manually_reset_cursor_task = PythonOperator(
    task_id="reset-cursor",
    python_callable=reset_all_state_cursor,
    provide_context=True,
)


manual_dag = create_dag(manually_dag_id, manually_tags, args, tasks=[], schedule_interval=None,
                        prev_operators=[manually_reset_cursor_task])
