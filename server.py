from collections import deque
from flask import Flask, request, jsonify
import subprocess
import sys
import threading
import time

app = Flask(__name__)

# --- CONFIGURATION ---
SECRET_TOKEN = "your_secret_password_123"
# ---------------------

EVENT_SCRIPT_MAP = {
    "pipeline_trigger": "run_ingestion.py",
    "sync_trigger": "run_syncing.py",
    "reconcile_trigger": "run_reconciliation.py",
}

GLOBAL_QUEUE_KEY = "__global__"

_queue_lock = threading.Lock()
_sheet_queues: dict[str, deque[dict]] = {}
_sheet_workers: dict[str, threading.Thread] = {}


def _first_present(data: dict, keys: list[str]):
    for key in keys:
        val = data.get(key)
        if isinstance(val, str):
            if val.strip():
                return val.strip()
        elif val not in (None, ""):
            return str(val)
    return None


def _build_cmd(script_name, target_client=None):
    cmd = [sys.executable, script_name]
    if target_client:
        cmd.extend(["--client", target_client])
    return cmd


def _run_script_blocking(script_name, target_client=None):
    """Runs a Python script and waits for completion."""
    cmd = _build_cmd(script_name, target_client=target_client)
    start_ts = time.time()
    try:
        print(f"   Starting {script_name} for '{target_client or 'ALL'}'...")
        completed = subprocess.run(cmd, check=False)
        elapsed = time.time() - start_ts
        print(
            f"   Finished {script_name} for '{target_client or 'ALL'}' "
            f"(exit={completed.returncode}, {elapsed:.1f}s)"
        )
    except Exception as e:
        print(f"   Error running {script_name}: {e}")


def _dequeue_job(sheet_key: str):
    with _queue_lock:
        queue = _sheet_queues.get(sheet_key)
        if not queue:
            return None
        return queue.popleft()


def _cleanup_worker(sheet_key: str):
    with _queue_lock:
        queue = _sheet_queues.get(sheet_key)
        if queue and len(queue) > 0:
            return False
        _sheet_workers.pop(sheet_key, None)
        _sheet_queues.pop(sheet_key, None)
        return True


def _sheet_worker_loop(sheet_key: str):
    print(f"[Queue] Worker started for sheet '{sheet_key}'")
    while True:
        job = _dequeue_job(sheet_key)
        if not job:
            if _cleanup_worker(sheet_key):
                print(f"[Queue] Worker stopped for sheet '{sheet_key}'")
                return
            continue

        print(
            f"[Queue] Running job {job['job_id']} | sheet={sheet_key} | "
            f"event={job['event_type']} | target={job['target_client'] or 'ALL'}"
        )
        _run_script_blocking(job["script_name"], target_client=job["target_client"])


def _enqueue_job(event_type: str, target_client: str | None, sheet_key: str | None):
    script_name = EVENT_SCRIPT_MAP[event_type]
    queue_key = sheet_key.strip() if isinstance(sheet_key, str) and sheet_key.strip() else GLOBAL_QUEUE_KEY

    with _queue_lock:
        queue = _sheet_queues.setdefault(queue_key, deque())
        job_id = f"{queue_key}:{event_type}:{int(time.time() * 1000)}:{len(queue) + 1}"
        job = {
            "job_id": job_id,
            "event_type": event_type,
            "target_client": target_client,
            "sheet_key": queue_key,
            "script_name": script_name,
            "queued_at": time.time(),
        }
        queue.append(job)
        position = len(queue)

        worker = _sheet_workers.get(queue_key)
        if worker is None or not worker.is_alive():
            worker = threading.Thread(target=_sheet_worker_loop, args=(queue_key,), daemon=True)
            _sheet_workers[queue_key] = worker
            worker.start()

    return job_id, queue_key, position


@app.route('/webhook', methods=['POST'])
def webhook_listener():
    # 1. Security Check
    token = request.headers.get('X-My-Secret-Token')
    if token != SECRET_TOKEN:
        print("[WARN] Blocked unauthorized attempt.")
        return jsonify({"status": "error", "message": "Unauthorized"}), 401

    # 2. Parse Data
    data = request.json or {}
    event_type = data.get('event')
    sheet_key = _first_present(
        data,
        [
            "spreadsheet_id",
            "spreadsheetId",
            "spreadsheet",
            "sheet_id",
            "sheetId",
            "folder_id",
            "folderId",
            "folderid",
            "output_folder_id",
            "outputFolderId",
            "output_folder",
            "folder",
            "realm_id",
            "realmId",
        ],
    )
    target_client = _first_present(
        data,
        [
            "client",
            "client_name",
            "workspace",
            "target",
            "country",
        ],
    )

    print(f"\n[Webhook] event={event_type} | sheet_key={sheet_key} | target={target_client}")

    # 3. Handle Events by queueing per sheet key.
    if event_type in EVENT_SCRIPT_MAP:
        job_id, queue_key, position = _enqueue_job(event_type, target_client, sheet_key=sheet_key)
        return jsonify(
            {
                "status": "queued",
                "message": f"Queued {event_type} for {target_client or 'ALL'}",
                "job_id": job_id,
                "sheet_key": queue_key,
                "position_in_sheet_queue": position,
            }
        ), 200

    return jsonify({"status": "ignored", "message": "Unknown event type"}), 200


if __name__ == '__main__':
    print("-------------------------------------------------------")
    print("Server listening for Webhooks...")
    app.run(host='0.0.0.0', port=8000)
