#!/usr/bin/env python3
"""Monitor and recover every Vegasbark scan workflow run."""
import base64
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

OWNER = "MraJJqq"
DOG_REPO = "Dog"
BARK_REPO = "Vegasbark"
WORKFLOW = "scan.yml"
BRANCH = "main"
TZ = ZoneInfo("Asia/Shanghai")
UTC = ZoneInfo("UTC")
SLOTS = ("08:02", "10:02", "16:02", "20:02", "22:02")
API = "https://api.github.com"
TOKEN = os.environ["DOG_GITHUB_TOKEN"]
LIMIT_SECONDS = 360
RECENT_SECONDS = 48 * 3600


def api(method, path, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(
        API + path, data=data, method=method,
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
            "User-Agent": "Dog-Watchdog/2.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=25) as response:
            raw = response.read()
            return response.status, json.loads(raw.decode()) if raw else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode(errors="replace")
        if exc.code == 404:
            return 404, {}
        raise RuntimeError(f"GitHub API {exc.code}: {raw[:500]}") from exc


def parse_time(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def load_state():
    status, body = api("GET", f"/repos/{OWNER}/{DOG_REPO}/contents/state.json?ref={BRANCH}")
    if status == 404:
        return {"runs": {}, "slots": {}}, None
    content = base64.b64decode(body["content"]).decode()
    state = json.loads(content)
    state.setdefault("runs", {})
    state.setdefault("slots", {})
    return state, body["sha"]


def save_state(state, sha):
    content = json.dumps(state, ensure_ascii=False, indent=2) + "\n"
    payload = {
        "message": "Dog: monitor and recover all Bark scans",
        "content": base64.b64encode(content.encode()).decode(),
        "branch": BRANCH,
    }
    if sha:
        payload["sha"] = sha
    api("PUT", f"/repos/{OWNER}/{DOG_REPO}/contents/state.json", payload)


def get_runs():
    runs = []
    for page in range(1, 11):
        _, body = api(
            "GET",
            f"/repos/{OWNER}/{BARK_REPO}/actions/workflows/{WORKFLOW}/runs?per_page=100&page={page}",
        )
        batch = body.get("workflow_runs", [])
        runs.extend(batch)
        if len(batch) < 100:
            break
    return runs


def is_scan_run(run):
    # Push-triggered runs only warm the cache; they do not perform a Bark scan.
    return run.get("event") in ("schedule", "workflow_dispatch")


def dispatch_recovery(task_id):
    api(
        "POST",
        f"/repos/{OWNER}/{BARK_REPO}/actions/workflows/{WORKFLOW}/dispatches",
        {"ref": BRANCH, "inputs": {"manual": "false", "task_id": task_id, "source": "scheduled"}},
    )
    print(f"Recovery scan dispatched: {task_id}")


def cancel_run(run_id):
    api("POST", f"/repos/{OWNER}/{BARK_REPO}/actions/runs/{run_id}/cancel")


def rerun(run_id):
    api("POST", f"/repos/{OWNER}/{BARK_REPO}/actions/runs/{run_id}/rerun")


def monitor_run(run, state, now):
    run_id = str(run["id"])
    record = state["runs"].setdefault(run_id, {})
    attempt = int(run.get("run_attempt") or 1)
    status = run.get("status")
    conclusion = run.get("conclusion")
    created = parse_time(run["created_at"])
    started_value = run.get("run_started_at")
    started = parse_time(started_value) if started_value else created
    age = (now - (started if status == "in_progress" else created)).total_seconds()

    if status == "completed":
        if conclusion == "success":
            record.update({"attempt": attempt, "status": "completed", "conclusion": "success"})
            return
        last_retry_attempt = int(record.get("retry_attempt", 0))
        retry_requested_at = record.get("retry_requested_at")
        retry_wait = (now - parse_time(retry_requested_at)).total_seconds() if retry_requested_at else LIMIT_SECONDS + 1
        if last_retry_attempt == attempt and retry_wait < LIMIT_SECONDS:
            return
        rerun(run["id"])
        record.update({
            "attempt": attempt, "status": "retry_requested",
            "retry_attempt": attempt, "retry_requested_at": now.isoformat(),
            "conclusion": conclusion,
        })
        print(f"Run {run_id} attempt {attempt} failed ({conclusion}); full workflow rerun requested.")
        return

    if status in ("queued", "waiting", "pending", "requested"):
        if age >= LIMIT_SECONDS or status == "queued":
            if record.get("cancel_requested_attempt") != attempt:
                cancel_run(run["id"])
                record.update({
                    "attempt": attempt, "status": "cancel_requested",
                    "cancel_requested_attempt": attempt,
                    "cancel_requested_at": now.isoformat(),
                })
                print(f"Run {run_id} has not started; cancellation requested for recovery.")
        return

    if status == "in_progress":
        if age > LIMIT_SECONDS and record.get("cancel_requested_attempt") != attempt:
            cancel_run(run["id"])
            record.update({
                "attempt": attempt, "status": "cancel_requested",
                "cancel_requested_attempt": attempt,
                "cancel_requested_at": now.isoformat(),
            })
            print(f"Run {run_id} exceeded 6 minutes; cancellation requested.")
        else:
            record.update({"attempt": attempt, "status": "running"})
        return

    # Unknown non-terminal statuses are retained and checked again next cycle.
    record.update({"attempt": attempt, "status": status or "unknown"})


def slot_recovery(runs, state, now):
    for day in (now.date(), now.date() - timedelta(days=1)):
        for value in SLOTS:
            hour, minute = map(int, value.split(":"))
            slot = datetime(day.year, day.month, day.day, hour, minute, tzinfo=TZ)
            slot_key = slot.strftime("%Y-%m-%d %H:%M")
            if slot_key in state["slots"] or (now - slot).total_seconds() < LIMIT_SECONDS:
                continue
            if (now - slot).total_seconds() > 24 * 3600:
                continue
            found = False
            task_id = f"auto:{slot:%Y-%m-%d}:{slot:%H:%M}"
            for run in runs:
                if run.get("event") == "workflow_dispatch":
                    title = (run.get("display_title") or run.get("name") or "")
                    if task_id in title:
                        found = True
                        break
                if run.get("event") != "schedule":
                    continue
                created = parse_time(run["created_at"])
                delta = (created - slot.astimezone(UTC)).total_seconds()
                if -120 <= delta <= 2 * 3600 - 120:
                    found = True
                    break
            if not found:
                dispatch_recovery(task_id)
                state["slots"][slot_key] = {
                    "status": "recovery_dispatched",
                    "task_id": task_id,
                    "dispatched_at": now.isoformat(),
                }


def main():
    now = datetime.now(UTC)
    state, sha = load_state()
    runs = [run for run in get_runs() if is_scan_run(run)]
    cutoff = now - timedelta(seconds=RECENT_SECONDS)

    # Monitor every active Bark scan, including older runs, and all recent terminal runs.
    for run in runs:
        if run.get("status") != "completed" or parse_time(run["created_at"]) >= cutoff:
            monitor_run(run, state, now)

    # Detect a missed native scheduled scan and start one recovery scan.
    slot_recovery(runs, state, now)

    # Keep state bounded without dropping active runs or the recent recovery history.
    active_ids = {
        str(run["id"]) for run in runs
        if run.get("status") != "completed" or parse_time(run["created_at"]) >= cutoff
    }
    state["runs"] = {
        key: value for key, value in state["runs"].items()
        if key in active_ids or (
            value.get("status") == "completed"
            and key in {str(r["id"]) for r in runs if parse_time(r["created_at"]) >= cutoff}
        )
    }
    state["slots"] = {
        key: value for key, value in state["slots"].items()
        if (now - parse_time(value["dispatched_at"])).total_seconds() <= 48 * 3600
    }
    save_state(state, sha)
    print(f"Dog checked {len(runs)} Bark scan runs.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Dog check failed: {exc}", file=sys.stderr)
        sys.exit(1)
