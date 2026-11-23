#!/usr/bin/env python3
"""Monitor and recover every Vegasbark scan workflow run."""
import base64
import json
import os
import sys
import urllib.error
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
TOKEN = os.getenv("DOG_GITHUB_TOKEN", "")
LIMIT_SECONDS = 360
RECENT_SECONDS = 48 * 3600
SLOT_TOLERANCE_SECONDS = 30 * 60


def api(method, path, payload=None):
    if not TOKEN:
        raise RuntimeError("Missing DOG_GITHUB_TOKEN")
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(
        API + path, data=data, method=method,
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
            "User-Agent": "Dog-Watchdog/3.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as response:
            raw = response.read()
            return response.status, json.loads(raw.decode()) if raw else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode(errors="replace")
        if exc.code == 404:
            return 404, {}
        raise RuntimeError(f"GitHub API {exc.code}: {raw[:500]}") from exc


def parse_time(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def iso_now(now):
    return now.astimezone(UTC).isoformat()


def normalize_state(raw):
    """Migrate both current and legacy state.json layouts to one schema."""
    if not isinstance(raw, dict):
        raw = {}
    state = {
        "schema_version": 2,
        "runs": raw.get("runs") if isinstance(raw.get("runs"), dict) else {},
        "slots": raw.get("slots") if isinstance(raw.get("slots"), dict) else {},
    }
    # Preserve a legacy in-flight slot instead of silently losing it.
    legacy_task = raw.get("task_id")
    legacy_slot = raw.get("slot")
    legacy_time = raw.get("dispatch_requested_at") or raw.get("updated_at")
    if legacy_task and legacy_slot and legacy_time:
        try:
            slot_dt = datetime.fromisoformat(legacy_slot).astimezone(TZ)
            key = slot_dt.strftime("%Y-%m-%d %H:%M")
            state["slots"].setdefault(key, {
                "status": "recovery_dispatched",
                "task_id": legacy_task,
                "dispatched_at": legacy_time,
            })
        except (TypeError, ValueError):
            pass
    return state


def load_state():
    status, body = api("GET", f"/repos/{OWNER}/{DOG_REPO}/contents/state.json?ref={BRANCH}")
    if status == 404:
        return normalize_state({}), None
    content = base64.b64decode(body["content"]).decode()
    return normalize_state(json.loads(content)), body["sha"]


def save_state(state, sha):
    state["schema_version"] = 2
    content = json.dumps(state, ensure_ascii=False, indent=2) + "\n"
    payload = {
        "message": "Dog: enforce six-minute Bark monitoring and recovery",
        "content": base64.b64encode(content.encode()).decode(),
        "branch": BRANCH,
    }
    if sha:
        payload["sha"] = sha
    api("PUT", f"/repos/{OWNER}/{DOG_REPO}/contents/state.json", payload)


def _list_runs(query):
    runs = []
    page = 1
    while True:
        _, body = api(
            "GET",
            f"/repos/{OWNER}/{BARK_REPO}/actions/workflows/{WORKFLOW}/runs?per_page=100&page={page}&{query}",
        )
        batch = body.get("workflow_runs", [])
        runs.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    return runs


def get_runs(now):
    """Fetch recent terminal runs and every active queued/running scan."""
    cutoff = now - timedelta(seconds=RECENT_SECONDS)
    merged = {}
    # A date filter keeps the completed-run history bounded without truncating
    # old active runs, which are queried separately below.
    recent_query = f"created=%3E{cutoff.strftime('%Y-%m-%d')}"
    for run in _list_runs(recent_query):
        merged[str(run["id"])] = run
    for status in ("queued", "in_progress"):
        for run in _list_runs(f"status={status}"):
            merged[str(run["id"])] = run
    return list(merged.values())


def is_scan_run(run):
    # Push-triggered runs only warm the cache; they are not Bark scan tasks.
    return run.get("event") in ("schedule", "workflow_dispatch")


def dispatch_recovery(task_id):
    api(
        "POST",
        f"/repos/{OWNER}/{BARK_REPO}/actions/workflows/{WORKFLOW}/dispatches",
        {"ref": BRANCH, "inputs": {"manual": False, "task_id": task_id, "source": "scheduled"}},
    )
    print(f"Recovery scan dispatched: {task_id}")


def cancel_run(run_id):
    api("POST", f"/repos/{OWNER}/{BARK_REPO}/actions/runs/{run_id}/cancel")


def rerun(run_id):
    api("POST", f"/repos/{OWNER}/{BARK_REPO}/actions/runs/{run_id}/rerun")


def _request_cancel(run, record, attempt, now, reason):
    last = record.get("cancel_requested_at")
    if last and record.get("cancel_requested_attempt") == attempt:
        if (now - parse_time(last)).total_seconds() < LIMIT_SECONDS:
            return None
    record.update({
        "attempt": attempt, "status": "cancel_requested",
        "cancel_requested_attempt": attempt, "cancel_requested_at": iso_now(now),
    })
    print(f"Run {run['id']} {reason}; cancellation queued.")
    return ("cancel", run["id"])

def monitor_run(run, state, now):
    run_id = str(run["id"])
    record = state["runs"].setdefault(run_id, {})
    attempt = int(run.get("run_attempt") or 1)
    status = run.get("status")
    conclusion = run.get("conclusion")
    previous_attempt = int(record.get("attempt", 0))
    if previous_attempt != attempt:
        record["attempt"] = attempt
        for key in ("cancel_requested_at", "cancel_requested_attempt",
                    "retry_requested_at", "retry_requested_attempt",
                    "queued_since", "running_since"):
            record.pop(key, None)

    if status == "completed":
        if conclusion == "success":
            record.update({"attempt": attempt, "status": "completed",
                           "conclusion": "success",
                           "completed_at": run.get("updated_at") or iso_now(now)})
            return None
        retry_at = record.get("retry_requested_at")
        retry_attempt = int(record.get("retry_requested_attempt", 0))
        if retry_at and retry_attempt == attempt:
            if (now - parse_time(retry_at)).total_seconds() < LIMIT_SECONDS:
                return None
        record.update({"attempt": attempt, "status": "retry_requested",
                       "retry_requested_attempt": attempt,
                       "retry_requested_at": iso_now(now), "conclusion": conclusion})
        print(f"Run {run_id} attempt {attempt} failed ({conclusion}); rerun queued.")
        return ("rerun", run["id"])

    if status in ("queued", "waiting", "pending", "requested"):
        if not record.get("queued_since"):
            queued_at = run.get("created_at") if attempt == 1 else None
            record["queued_since"] = queued_at or iso_now(now)
        age = (now - parse_time(record["queued_since"])).total_seconds()
        if age >= LIMIT_SECONDS:
            return _request_cancel(run, record, attempt, now,
                                   "has remained unstarted for six minutes")
        record.update({"attempt": attempt, "status": "queued"})
        return None

    if status == "in_progress":
        started_value = run.get("run_started_at")
        if started_value and attempt == 1:
            started = parse_time(started_value)
        else:
            if not record.get("running_since"):
                record["running_since"] = started_value or iso_now(now)
            started = parse_time(record["running_since"])
        age = (now - started).total_seconds()
        if age >= LIMIT_SECONDS:
            return _request_cancel(run, record, attempt, now,
                                   "has run for more than six minutes")
        record.update({"attempt": attempt, "status": "running"})
        return None

    record.update({"attempt": attempt, "status": status or "unknown"})
    return None

def _slot_key(slot):
    return slot.strftime("%Y-%m-%d %H:%M")


def _task_id(slot):
    return f"auto:{slot:%Y-%m-%d}:{slot:%H:%M}"


def _run_matches_slot(run, slot, task_id):
    if run.get("event") == "workflow_dispatch":
        title = " ".join(str(run.get(k) or "") for k in ("display_title", "name"))
        return task_id in title
    if run.get("event") == "schedule":
        try:
            delta = (parse_time(run["created_at"]) - slot.astimezone(UTC)).total_seconds()
            return -SLOT_TOLERANCE_SECONDS <= delta <= SLOT_TOLERANCE_SECONDS
        except (KeyError, TypeError, ValueError):
            return False
    return False


def slot_recovery(runs, state, now):
    pending_dispatches = []
    for day in (now.date(), now.date() - timedelta(days=1)):
        for value in SLOTS:
            hour, minute = map(int, value.split(":"))
            slot = datetime(day.year, day.month, day.day, hour, minute, tzinfo=TZ)
            elapsed = (now - slot).total_seconds()
            if elapsed < LIMIT_SECONDS or elapsed > 24 * 3600:
                continue
            key = _slot_key(slot)
            task_id = _task_id(slot)
            matching = next((r for r in runs if _run_matches_slot(r, slot, task_id)), None)
            record = state["slots"].setdefault(key, {})
            if matching:
                record.update({"status": "run_found", "task_id": task_id,
                               "run_id": matching["id"], "last_seen_at": iso_now(now)})
                continue
            dispatched_at = record.get("dispatched_at")
            if dispatched_at and (now - parse_time(dispatched_at)).total_seconds() < LIMIT_SECONDS:
                continue
            # Persist intent before dispatch so an unrecorded API action cannot duplicate.
            record.update({"status": "dispatch_pending", "task_id": task_id,
                           "dispatched_at": iso_now(now)})
            pending_dispatches.append(task_id)
    return pending_dispatches

def prune_state(state, runs, now):
    cutoff = now - timedelta(seconds=RECENT_SECONDS)
    keep_run_ids = set()
    for run in runs:
        try:
            if run.get("status") != "completed" or parse_time(run["created_at"]) >= cutoff:
                keep_run_ids.add(str(run["id"]))
        except (KeyError, TypeError, ValueError):
            keep_run_ids.add(str(run.get("id")))
    state["runs"] = {k: v for k, v in state["runs"].items() if k in keep_run_ids}

    kept_slots = {}
    for key, value in state["slots"].items():
        try:
            slot_time = datetime.strptime(key, "%Y-%m-%d %H:%M").replace(tzinfo=TZ)
            if (now - slot_time).total_seconds() <= 48 * 3600:
                kept_slots[key] = value
        except ValueError:
            continue
    state["slots"] = kept_slots


def main():
    now = datetime.now(UTC)
    state, sha = load_state()
    runs = [run for run in get_runs(now) if is_scan_run(run)]
    pending_actions = []
    for run in runs:
        if run.get("status") != "completed" or parse_time(run["created_at"]) >= now - timedelta(seconds=RECENT_SECONDS):
            action = monitor_run(run, state, now)
            if action:
                pending_actions.append(action)
    pending_dispatches = slot_recovery(runs, state, now)
    prune_state(state, runs, now)
    # Persist all intents before issuing side-effecting GitHub API calls.
    save_state(state, sha)
    for action, run_id in pending_actions:
        if action == "cancel":
            cancel_run(run_id)
        elif action == "rerun":
            rerun(run_id)
    for task_id in pending_dispatches:
        dispatch_recovery(task_id)
    print(f"Dog checked {len(runs)} Bark scan runs.")

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Dog check failed: {exc}", file=sys.stderr)
        sys.exit(1)
