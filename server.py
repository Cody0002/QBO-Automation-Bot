from flask import Flask, request, jsonify
import subprocess
import sys
import threading

app = Flask(__name__)

# --- CONFIGURATION ---
SECRET_TOKEN = "your_secret_password_123" 
# ---------------------

def run_script_in_background(script_name):
    """Runs a Python script independently."""
    try:
        print(f"   ▶️  Starting {script_name}...")
        subprocess.Popen([sys.executable, script_name]) 
    except Exception as e:
        print(f"   ❌ Error running {script_name}: {e}")

@app.route('/webhook', methods=['POST'])
def webhook_listener():
    # 1. Security Check
    token = request.headers.get('X-My-Secret-Token')
    if token != SECRET_TOKEN:
        print(f"[⚠️] Blocked unauthorized attempt.")
        return jsonify({"status": "error", "message": "Unauthorized"}), 401

    # 2. Parse Data
    data = request.json
    event_type = data.get('event')
    country = data.get('country', 'Unknown')
    
    print(f"\n[🔔] Incoming Webhook | Event: {event_type} | Country: {country}")

    # 3. Handle Events
    
    # --- CASE A: TRANSFORM / INGESTION ---
    if event_type == 'pipeline_trigger':
        thread = threading.Thread(target=run_script_in_background, args=("run_ingestion.py",))
        thread.start()
        return jsonify({"status": "success", "message": "Ingestion started"}), 200

    # --- CASE B: SYNCING ---
    elif event_type == 'sync_trigger':
        thread = threading.Thread(target=run_script_in_background, args=("run_syncing.py",))
        thread.start()
        return jsonify({"status": "success", "message": "Syncing started"}), 200

    # --- CASE C: RECONCILIATION ---
    elif event_type == 'reconcile_trigger':
        thread = threading.Thread(target=run_script_in_background, args=("run_reconciliation.py",))
        thread.start()
        return jsonify({"status": "success", "message": "Reconciliation started"}), 200

    return jsonify({"status": "ignored", "message": "Unknown event type"}), 200

if __name__ == '__main__':
    print("-------------------------------------------------------")
    print(f"🚀 Server listening for Webhooks...")
    app.run(host='0.0.0.0', port=5000)