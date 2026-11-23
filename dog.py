#!/usr/bin/env python3
"""GitHub-only watchdog for Vegasbark automatic scans."""
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
SLOTS = ("08:02", "10:02", "16:02", "20:02", "22:02")
API = "https://api.github.com"
TOKEN = os.environ["DOG_GITHUB_TOKEN"]


def api(method, path, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(
        API + path,
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
            "User-Agent": "Dog-Watchdog/1.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=25) as response:
            raw = response.read()
            return response.status, (json.loads(raw.decode()) if raw else {})
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode(errors="replace")
        if exc.code == 404:
            return 404, {}
        raise RuntimeError(f"GitHub API {exc.code}: {raw[:500]}") from exc


def load_state():
    status, body = api("GET", f"/repos/{OWNER}/{DOG_REPO}/contents/state.json?ref={BRANCH}")
    if status == 404:
        return {}, None
    content = base64.b64decode(body["content"]).decode()
    return json.loads(content), body["sha"]


def save_state(state, sha):
    content = json.dumps(state, ensure_ascii=False, indent=2) + "\n"
    payload = {
        "message": "Dog: update automatic scan state",
        "content": base64.b64encode(content.encode()).decode(),
        "branch": BRANCH,
    }
    if sha:
        payload["sha"] = sha
    api("PUT", f"/repos/{OWNER}/{DOG_REPO}/contents/state.json", payload)


def latest_slot(now):
    candidates = []
    for day in (now.date() - timedelta(days=1), now.date()):
        for value in SLOTS:
            hour, minute = map(int, value.split(":"))
            candidate = datetime(day.year, day.month, day.day, hour, minute, tzinfo=TZ)
            if candidate <= now:
                candidates.append(candidate)
    return max(candidates) if candidates else None


def get_runs():
    _, body = api(
        "GET",
        f"/repos/{OWNER}/{BARK_REPO}/actions/workflows/{WORKFLOW}/runs?per_page=100",
    )
    return body.get("workflow_runs", [])


def find_task_run(runs, task_id, dispatch_time=None, slot_time=None):
    matches = []
    for run in runs:
        name = run.get("name", "") or ""
        run_name = run.get("display_title", "") or run.get("run_name", "") or ""
        created = datetime.fromisoformat(run["created_at"].replace("Z", "+00:00"))
        if task_id in name or task_id in run_name:
            if dispatch_time:
                lower = datetime.fromisoformat(dispatch_time).timestamp() - 90
                if created.timestamp() < lower:
                    continue
            matches.append(run)
        elif slot_time and run.get("event") == "schedule":
            slot_utc = slot_time.astimezone(ZoneInfo("UTC"))
            delta = (created - slot_utc).total_seconds()
            if -120 <= delta <= 4 * 60 * 60:
                matches.append(run)
    if not matches:
        return None
    return max(matches, key=lambda item: item["created_at"])


def dispatch(task_id, now_iso, state, sha):
    # Persist intent before dispatch so a Dog interruption cannot lose the task.
    state.update({
        "task_id": task_id,
        "status": "dispatch_pending",
        "run_id": None,
        "dispatch_requested_at": now_iso,
        "updated_at": now_iso,
    })
    save_state(state, sha)
    api(
        "POST",
        f"/repos/{OWNER}/{BARK_REPO}/actions/workflows/{WORKFLOW}/dispatches",
        {
            "ref": BRANCH,
            "inputs": {
                "manual": "false",
                "task_id": task_id,
                "retry_stage": "full",
                "source": "scheduled",
            },
        },
    )
    print(f"Dispatched full automatic task: {task_id}")


def main():
    now = datetime.now(TZ)
    slot = latest_slot(now)
    if slot is None:
        print("No automatic scan slot has occurred yet.")
        return

    task_id = f"auto:{slot:%Y-%m-%d}:{slot:%H:%M}"
    state, sha = load_state()
    if state.get("task_id") != task_id:
        state = {
            "task_id": task_id,
            "slot": slot.isoformat(),
            "status": "pending",
            "run_id": None,
            "dispatch_requested_at": None,
            "updated_at": now.isoformat(),
        }
        save_state(state, sha)
        sha = None  # Contents API returned a new commit; reload SHA before the next write.
        state, sha = load_state()

    if state.get("status") == "completed":
        print(f"{task_id}: already completed.")
        return

    runs = get_runs()
    run = None
    if state.get("run_id"):
        _, run = api("GET", f"/repos/{OWNER}/{BARK_REPO}/actions/runs/{state['run_id']}")
    else:
        run = find_task_run(
            runs,
            task_id,
            dispatch_time=state.get("dispatch_requested_at"),
            slot_time=slot if not state.get("dispatch_requested_at") else None,
        )

    if run:
        state["run_id"] = run["id"]
        state["run_created_at"] = run.get("created_at")
        status = run.get("status")
        if status == "completed":
            if run.get("conclusion") == "success":
                state.update({"status": "completed", "completed_at": now.isoformat(), "updated_at": now.isoformat()})
                save_state(state, sha)
                print(f"{task_id}: completed successfully.")
                return
            state.update({"status": "failed", "last_conclusion": run.get("conclusion"), "run_id": None})
            save_state(state, sha)
            state, sha = load_state()
            dispatch(task_id, now.isoformat(), state, sha)
            return

        if status == "in_progress":
            started = run.get("run_started_at") or run.get("created_at")
            started_at = datetime.fromisoformat(started.replace("Z", "+00:00"))
            elapsed = (now.astimezone(ZoneInfo("UTC")) - started_at).total_seconds()
            if elapsed > 360:
                api("POST", f"/repos/{OWNER}/{BARK_REPO}/actions/runs/{run['id']}/cancel")
                state.update({"status": "cancel_requested", "updated_at": now.isoformat()})
                save_state(state, sha)
                print(f"{task_id}: run exceeded 6 minutes; cancellation requested.")
                return
            state.update({"status": "running", "updated_at": now.isoformat()})
            save_state(state, sha)
            print(f"{task_id}: running ({int(elapsed)} seconds).")
            return

        if status == "queued":
            state.update({"status": "queued", "updated_at": now.isoformat()})
            save_state(state, sha)
            print(f"{task_id}: queued; waiting without cancelling another task.")
            return

    # A cancelled or vanished run is restarted as one complete scan + push.
    requested = state.get("dispatch_requested_at")
    if requested:
        requested_at = datetime.fromisoformat(requested)
        if (now - requested_at).total_seconds() < 360:
            print(f"{task_id}: dispatch is being registered by GitHub.")
            return

    dispatch(task_id, now.isoformat(), state, sha)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Dog check failed: {exc}", file=sys.stderr)
        sys.exit(1)
