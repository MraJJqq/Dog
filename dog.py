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
WORKFLOW = "bark-scan.yml"
LEGACY_WORKFLOW = "scan.yml"
WORKFLOWS = (WORKFLOW, LEGACY_WORKFLOW)
BRANCH = "main"
TZ = ZoneInfo("Asia/Shanghai")
UTC = ZoneInfo("UTC")
SLOTS = ("08:02", "10:02", "16:02", "20:02", "22:02")
CRON_TO_SLOT = {"2 0 * * *": "08:02", "2 2 * * *": "10:02", "2 8 * * *": "16:02", "2 12 * * *": "20:02", "2 14 * * *": "22:02"}
API = "https://api.github.com"
TOKEN = os.getenv("DOG_GITHUB_TOKEN", "")
LIMIT_SECONDS = 360
RECENT_SECONDS = 48 * 3600
STATE_CONTENT = None


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
    global STATE_CONTENT
    status, body = api("GET", f"/repos/{OWNER}/{DOG_REPO}/contents/state.json?ref={BRANCH}")
    if status == 404:
        STATE_CONTENT = None
        return normalize_state({}), None
    content = base64.b64decode(body["content"]).decode()
    STATE_CONTENT = content
    return normalize_state(json.loads(content)), body["sha"]


def save_state(state, sha):
    global STATE_CONTENT
    state["schema_version"] = 2
    content = json.dumps(state, ensure_ascii=False, indent=2) + "\n"
    if content == STATE_CONTENT:
        return
    payload = {
        "message": "Dog: enforce six-minute Bark monitoring and recovery",
        "content": base64.b64encode(content.encode()).decode(),
        "branch": BRANCH,
    }
    if sha:
        payload["sha"] = sha
    api("PUT", f"/repos/{OWNER}/{DOG_REPO}/contents/state.json", payload)
    STATE_CONTENT = content


def _list_runs(query, workflow=WORKFLOW):
    runs = []
    page = 1
    while True:
        _, body = api(
            "GET",
            f"/repos/{OWNER}/{BARK_REPO}/actions/workflows/{workflow}/runs?per_page=100&page={page}&{query}",
        )
        batch = body.get("workflow_runs", [])
        runs.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    return runs


def _list_status_runs(status, start_date, end_date, workflow=WORKFLOW):
    query = f"status={status}&created={start_date.isoformat()}..{end_date.isoformat()}"
    batch = _list_runs(query, workflow)
    # GitHub caps each filtered search at 1,000 results. Split the date range
    # recursively so no failed run is hidden behind that cap.
    if len(batch) < 1000 or start_date >= end_date:
        return batch
    midpoint = start_date + (end_date - start_date) // 2
    return (
        _list_status_runs(status, start_date, midpoint, workflow)
        + _list_status_runs(status, midpoint + timedelta(days=1), end_date, workflow)
    )


def get_runs(now):
    """Fetch recent runs, every active run, and every failed/cancelled scan."""
    merged = {}
    cutoff = now - timedelta(seconds=RECENT_SECONDS)
    recent_query = f"created=%3E{cutoff.strftime('%Y-%m-%d')}"
    start_date = datetime(2008, 1, 1).date()
    end_date = now.date()

    for workflow in WORKFLOWS:
        for run in _list_runs(recent_query, workflow):
            merged[str(run["id"])] = run

        # GitHub's run-list status filter accepts workflow statuses, not
        # conclusion names. Fetch completed runs and retain every non-success.
        for run in _list_status_runs("completed", start_date, end_date, workflow):
            if run.get("conclusion") != "success":
                merged[str(run["id"])] = run

        for status in ("queued", "in_progress", "waiting", "pending", "requested"):
            for run in _list_status_runs(status, start_date, end_date, workflow):
                merged[str(run["id"])] = run
    return list(merged.values())


def is_scan_run(run):
    # Push-triggered runs only warm the cache; they are not Bark scan tasks.
    return run.get("event") in ("schedule", "workflow_dispatch")


def dispatch_task(task_id, source, manual, restore_run_id=None):
    inputs = {"manual": manual, "task_id": task_id, "source": source}
    if source == "scheduled":
        parts = task_id.split(":", 2)
        slot = parts[2] if len(parts) == 3 else ""
        schedule_keys = {
            "08:02": "2 0 * * *",
            "10:02": "2 2 * * *",
            "16:02": "2 8 * * *",
            "20:02": "2 12 * * *",
            "22:02": "2 14 * * *",
        }
        schedule_key = schedule_keys.get(slot)
        if not schedule_key:
            raise RuntimeError(f"Unknown scheduled slot in recovery task: {task_id}")
        inputs["schedule_key"] = schedule_key
    if restore_run_id is not None:
        inputs["restore_run_id"] = str(restore_run_id)
    api("POST", f"/repos/{OWNER}/{BARK_REPO}/actions/workflows/{WORKFLOW}/dispatches",
        {"ref": BRANCH, "inputs": inputs})
    print(f"Retry scan dispatched: {task_id} ({source})")


def dispatch_recovery(task_id):
    dispatch_task(task_id, "scheduled", False)


def cancel_run(run_id):
    api("POST", f"/repos/{OWNER}/{BARK_REPO}/actions/runs/{run_id}/cancel")


def _scheduled_task_id(created_at, schedule_cron=None):
    created = parse_time(created_at).astimezone(TZ)
    if schedule_cron in CRON_TO_SLOT:
        hour, minute = map(int, CRON_TO_SLOT[schedule_cron].split(":"))
        slot = datetime(created.year, created.month, created.day, hour, minute, tzinfo=TZ)
        if slot > created:
            slot -= timedelta(days=1)
        return f"auto:{slot:%Y-%m-%d}:{slot:%H:%M}"
    candidates = []
    for offset in (0, 1):
        day = created.date() - timedelta(days=offset)
        for value in SLOTS:
            hour, minute = map(int, value.split(":"))
            slot = datetime(day.year, day.month, day.day, hour, minute, tzinfo=TZ)
            if slot <= created:
                candidates.append(slot)
    if not candidates:
        return None
    slot = max(candidates)
    return f"auto:{slot:%Y-%m-%d}:{slot:%H:%M}"


def _task_descriptor(run):
    event = run.get("event")
    if event == "schedule":
        title = run.get("display_title") or run.get("name") or ""
        schedule_cron = next((cron for cron in CRON_TO_SLOT if cron in title), None)
        task_id = _scheduled_task_id(run.get("created_at", ""), schedule_cron)
        return (task_id, "scheduled", False) if task_id else None
    if event == "workflow_dispatch":
        title = run.get("display_title") or ""
        prefix = "Vegasbark "
        task_id = title[len(prefix):] if title.startswith(prefix) else str(run["id"])
        if not task_id:
            task_id = str(run["id"])
        if task_id.startswith("auto:"):
            return task_id, "scheduled", False
        return task_id, "manual", True
    return None


def _has_newer_task_run(run, runs):
    descriptor = _task_descriptor(run)
    if not descriptor:
        return False
    task_id = descriptor[0]
    created = parse_time(run["created_at"])
    for other in runs:
        if str(other.get("id")) == str(run.get("id")):
            continue
        other_descriptor = _task_descriptor(other)
        if not other_descriptor or other_descriptor[0] != task_id:
            continue
        if parse_time(other["created_at"]) > created:
            return True
    return False


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

def monitor_run(run, state, now, retry_descriptor=None):
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
        if not retry_descriptor:
            record.update({"attempt": attempt, "status": "retry_unroutable",
                           "conclusion": conclusion})
            return None
        task_id, source, manual = retry_descriptor
        record.update({"attempt": attempt, "status": "retry_requested",
                       "retry_requested_attempt": attempt,
                       "retry_requested_at": iso_now(now),
                       "retry_task_id": task_id, "conclusion": conclusion})
        print(f"Run {run_id} attempt {attempt} failed ({conclusion}); replacement dispatch queued.")
        return ("dispatch_retry", task_id, source, manual, run["id"])

    if status in ("queued", "waiting", "pending", "requested"):
        if not record.get("queued_since"):
            queued_at = run.get("created_at") if attempt == 1 else run.get("updated_at")
            record["queued_since"] = queued_at or iso_now(now)
        age = (now - parse_time(record["queued_since"])).total_seconds()
        if age >= LIMIT_SECONDS:
            return _request_cancel(run, record, attempt, now,
                                   "has remained unstarted for six minutes")
        record.update({"attempt": attempt, "status": "queued"})
        return None

    if status == "in_progress":
        started_value = run.get("run_started_at")
        if started_value:
            started = parse_time(started_value)
        else:
            if not record.get("running_since"):
                record["running_since"] = run.get("updated_at") or iso_now(now)
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
    descriptor = _task_descriptor(run)
    if descriptor:
        return descriptor[0] == task_id
    if run.get("event") == "schedule":
        try:
            slot_index = SLOTS.index(slot.strftime("%H:%M"))
            if slot_index + 1 < len(SLOTS):
                next_hour, next_minute = map(int, SLOTS[slot_index + 1].split(":"))
                next_slot = slot.replace(hour=next_hour, minute=next_minute)
            else:
                next_day = slot + timedelta(days=1)
                next_slot = next_day.replace(hour=8, minute=2)
            interval = (next_slot - slot).total_seconds()
            delta = (parse_time(run["created_at"]) - slot.astimezone(UTC)).total_seconds()
            # Non-overlapping slot window: from this scheduled slot
            # up to, but not including, the next configured slot.
            return 0 <= delta < interval
        except (KeyError, TypeError, ValueError):
            return False
    return False


def slot_recovery(runs, state, now):
    """Keep every due slot pending until its scan has a confirmed successful run."""
    pending_dispatches = []
    # Register today's and yesterday's due slots; older pending slots remain in state.
    for day in (now.date(), now.date() - timedelta(days=1)):
        for value in SLOTS:
            hour, minute = map(int, value.split(":"))
            slot = datetime(day.year, day.month, day.day, hour, minute, tzinfo=TZ)
            if (now - slot).total_seconds() >= LIMIT_SECONDS:
                state["slots"].setdefault(_slot_key(slot), {"task_id": _task_id(slot)})

    for key, record in list(state["slots"].items()):
        try:
            slot = datetime.strptime(key, "%Y-%m-%d %H:%M").replace(tzinfo=TZ)
        except ValueError:
            continue
        if (now - slot).total_seconds() < LIMIT_SECONDS:
            continue
        task_id = record.get("task_id") or _task_id(slot)
        if record.get("status") == "completed_success":
            continue
        matching = next((r for r in runs if _run_matches_slot(r, slot, task_id)), None)
        if matching:
            if matching.get("status") == "completed" and matching.get("conclusion") == "success":
                record.update({"status": "completed_success", "task_id": task_id,
                               "run_id": matching["id"]})
            else:
                record.update({"status": "run_found", "task_id": task_id,
                               "run_id": matching["id"]})
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
            if state["runs"].get(str(run["id"]), {}).get("status") == "superseded":
                continue
            if run.get("status") != "completed" or run.get("conclusion") != "success" or parse_time(run["created_at"]) >= cutoff:
                keep_run_ids.add(str(run["id"]))
        except (KeyError, TypeError, ValueError):
            keep_run_ids.add(str(run.get("id")))
    state["runs"] = {k: v for k, v in state["runs"].items() if k in keep_run_ids}

    kept_slots = {}
    for key, value in state["slots"].items():
        try:
            slot_time = datetime.strptime(key, "%Y-%m-%d %H:%M").replace(tzinfo=TZ)
            age = (now - slot_time).total_seconds()
            if value.get("status") != "completed_success" or age <= 48 * 3600:
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
        if run.get("status") == "completed" and run.get("conclusion") != "success":
            if _has_newer_task_run(run, runs):
                state["runs"].setdefault(str(run["id"]), {}).update({
                    "status": "superseded", "conclusion": run.get("conclusion"),
                    "attempt": int(run.get("run_attempt") or 1),
                })
                continue
            descriptor = _task_descriptor(run)
        else:
            descriptor = None

        if run.get("status") != "completed" or run.get("conclusion") != "success" or parse_time(run["created_at"]) >= now - timedelta(seconds=RECENT_SECONDS):
            action = monitor_run(run, state, now, descriptor)
            if action:
                pending_actions.append(action)

    pending_dispatches = slot_recovery(runs, state, now)
    prune_state(state, runs, now)

    # Persist all retry/cancel/dispatch intents before side-effecting API calls.
    save_state(state, sha)
    for action in pending_actions:
        if action[0] == "cancel":
            cancel_run(action[1])
        elif action[0] == "dispatch_retry":
            dispatch_task(action[1], action[2], action[3], restore_run_id=action[4])
    for task_id in pending_dispatches:
        dispatch_recovery(task_id)

    print(f"Dog checked {len(runs)} Bark scan runs.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Dog check failed: {exc}", file=sys.stderr)
        sys.exit(1)
