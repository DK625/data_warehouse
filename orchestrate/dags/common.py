import http.client
import logging
import os
import urllib.parse

from airflow import DAG

try:
    from airflow.operators.bash_operator import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "catchup": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(1970, 1, 1),
    "concurrency": 1,
}

DEFAULT_TAGS = ["meltano"]
PROJECT_ROOT = os.getenv("MELTANO_PROJECT_ROOT", os.getcwd())
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
ENV_KEY = os.getenv("MELTANO_ENVIRONMENT", "dev")
MELTANO_BIN = ".meltano/run/bin"

# Suppress info-level log from CLI when polling Meltano CLI for schedule updates.
SCHEDULE_POLLING_LOG_LEVEL = "warning"


if not Path(PROJECT_ROOT).joinpath(MELTANO_BIN).exists():
    logger.warning(
        f"A symlink to the 'meltano' executable could not be found at '{MELTANO_BIN}'. Falling back on expecting it "
        f"to be in the PATH instead. "
    )
    MELTANO_BIN = "meltano"


def _send_telegram_message(message):
    message = urllib.parse.quote(message)
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning(
            "Telegram bot token and/or chat ID not set. Skipping sending Telegram message."
        )
        return

    conn = http.client.HTTPSConnection("api.telegram.org")
    endpoint = f"/bot{TELEGRAM_BOT_TOKEN}/sendMessage?chat_id={TELEGRAM_CHAT_ID}&text={message}&parse_mode=Markdown"
    logger.info(f"Sending Telegram message")
    conn.request("GET", endpoint)
    response = conn.getresponse()
    result = response.read()
    logger.info(f"Sent Telegram message: {result}")


def on_failed_dag_handler(context):
    logger.error(f"DAG failed: {context}")
    dag = context.get("dag_run")
    utc_now = datetime.now(tz=timezone.utc)
    message = f"""
‼️ DAG `{dag.dag_id}`
Time: `{utc_now.isoformat()}`
State: `{dag.state}`
Start Date: `{dag.start_date.isoformat()}`
End Date: `{dag.end_date.isoformat()}`
    """
    _send_telegram_message(message)


def on_success_dag_handler(context):
    logger.info(f"DAG success: {context}")
    dag = context.get("dag_run")
    utc_now = datetime.now(tz=timezone.utc)
    message = f"""
✅ DAG `{dag.dag_id}`
Time: `{utc_now.isoformat()}`
State: `{dag.state}`
Start Date: `{dag.start_date.isoformat()}`
End Date: `{dag.end_date.isoformat()}`
    """
    _send_telegram_message(message)


def create_dag(dag_id, tags, args, tasks, schedule_interval="0 19 * * *", prev_operators=None):
    previous_operator = None
    previous_operators = []
    if prev_operators:
        previous_operators = prev_operators
    with DAG(
            dag_id,
            tags=tags,
            catchup=False,
            default_args=args,
            schedule_interval=schedule_interval,
            max_active_runs=1,
            on_failure_callback=on_failed_dag_handler,
            on_success_callback=on_success_dag_handler,
            is_paused_upon_creation=False,
            start_date=args["start_date"],
    ) as dag:
        print("Dag start_date: ", dag.start_date)
        for op in previous_operators:
            if previous_operator:
                op.set_upstream(previous_operator)
            previous_operator = op
            dag.add_task(op)
        for idx, task in enumerate(tasks):
            task_id = task["name"]
            task_args = task["args"]
            operator = BashOperator(
                task_id=task_id,
                bash_command=f"cd {PROJECT_ROOT}; {MELTANO_BIN} run {task_args}",
                dag=dag,
            )
            if previous_operator:
                operator.set_upstream(previous_operator)
            previous_operator = operator
            logger.info(
                f"Spun off task '{task_args}' of schedule '{task_id}': {task_args}"
            )
    return dag
