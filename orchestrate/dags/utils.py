import json
import subprocess
from datetime import datetime
import logging

import pytz


logger = logging.getLogger(__name__)


def run_command(cmd, **kwargs):
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, check=True, **kwargs)
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {e}")
        print(f"stdout: {e.stdout.decode('utf-8')}")
        print(f"stderr: {e.stderr.decode('utf-8')}")
        raise
    else:
        return result.stdout.decode("utf-8").strip()


def get_start_of_month():
    local_now = datetime.now(tz=pytz.timezone("Asia/Ho_Chi_Minh"))
    start_month = local_now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return start_month.astimezone(pytz.utc)


def reset_cursor(state_id, working_dir, meltano_bin, new_cursor):
    logger.info("Resetting state: %s to cursor: %s", state_id, new_cursor)
    old_state = run_command(f"{meltano_bin} state get {state_id}", cwd=working_dir)
    logger.info(f"Old state: {old_state}")
    try:
        state = json.loads(old_state)
    except json.JSONDecodeError:
        print(f"Error loading state: {old_state}")
        raise
    if not state:
        logger.info("State is empty")
        return None
    for k in state["singer_state"]["bookmarks"]:
        v = state["singer_state"]["bookmarks"][k]
        replication_method = v.get("last_replication_method", "FULL_TABLE")
        if replication_method != "INCREMENTAL":
            continue
        v["replication_key_value"] = new_cursor
    new_state = json.dumps(state)
    run_command(f"{meltano_bin} state set --force {state_id} '{new_state}'", cwd=working_dir)
    logger.info("Reset state: %s to cursor: %s", state_id, new_cursor)


def reset_all_cursor(working_dir, meltano_bin, new_cursor):
    state_list = run_command(f"{meltano_bin} state list", cwd=working_dir)
    logger.info("State list: %s", state_list)
    for state_id in state_list.split("\n"):
        if not state_id:
            continue
        reset_cursor(state_id, working_dir, meltano_bin, new_cursor)


def dt_to_utc(dt):
    if dt.tzinfo is not None:
        return dt.astimezone(pytz.utc)
    return pytz.utc.localize(dt)


def change_tz(dt, tz):
    if dt.tzinfo is not None:
        return dt.astimezone(pytz.utc).astimezone(tz)
    return pytz.utc.localize(dt).astimezone(tz)


def aware_to_naive(dt):
    if dt.tzinfo is None:
        return dt
    return dt.replace(tzinfo=None)
