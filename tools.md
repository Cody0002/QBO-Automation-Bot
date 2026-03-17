# QBO Automation Bot - Tools and Runbook

## 1. Environment and Setup

Project root:

- `d:\Projects\KZG\QBO-Automation-Bot`

Typical setup:

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

Required config:

- `config/secrets.env` with QBO credentials and optional overrides.
- Google auth files (`oauth_client_secret.json` + `token.json`) or `service_account.json`.

## 2. Main Entry Commands

### 2.1 Ingestion
Run all eligible clients:

```powershell
.\.venv\python.exe .\run_ingestion.py
```

Run one target:

```powershell
.\.venv\python.exe .\run_ingestion.py --client "KZDW"
```

Also supports `--client <realm_id>` and `--client <spreadsheet_id>`.

### 2.2 Sync
Run all:

```powershell
.\.venv\python.exe .\run_syncing.py
```

Run one target:

```powershell
.\.venv\python.exe .\run_syncing.py --client "KZDW"
```

### 2.3 Reconciliation
Run all:

```powershell
.\.venv\python.exe .\run_reconciliation.py
```

Run one target:

```powershell
.\.venv\python.exe .\run_reconciliation.py --client "KZDW"
```

### 2.4 Webhook server

```powershell
.\.venv\python.exe .\server.py
```

Endpoint:

- `POST /webhook`

Supported target fields in payload:

- `spreadsheet_id`, `spreadsheetId`
- `realm_id`, `realmId`
- `client`, `client_name`
- `workspace`, `target`
- fallback `country`

## 3. Sync Throttling Controls

Set in `config/secrets.env`:

```env
QBO_SYNC_PATCH_SIZE=10
QBO_SYNC_CALL_DELAY_SEC=0.35
QBO_SYNC_PATCH_DELAY_SEC=0.8
# Optional reconcile speed toggle:
RECONCILE_ENABLE_RAW_CHECK=1
```

Guidelines:

- If QBO rate-limit or transient errors increase, raise delays.
- If throughput is too slow and stable, reduce delays gradually.
- Keep patch size moderate to avoid large batch writes to sheets.
- Set `RECONCILE_ENABLE_RAW_CHECK=0` to skip raw-vs-transform checks and speed up reconciliation runs.

## 3.1 Single-instance safety lock

The runners use scoped locks:

- Per-client lock (allows parallel runs across different clients):
  - ingestion: `.locks/run_ingestion_client_<realm_id>.lock`
  - syncing: `.locks/run_syncing_client_<realm_id>.lock`
  - reconciliation: `.locks/run_reconciliation_client_<realm_id>.lock`
- ALL-dispatch lock (prevents duplicate "ALL" fan-out):
  - `.locks/run_ingestion_all_dispatch.lock`
  - `.locks/run_syncing_all_dispatch.lock`
  - `.locks/run_reconciliation_all_dispatch.lock`

Behavior:

- Concurrent user runs are allowed when they target different clients.
- Same stage + same client is blocked to avoid race conditions.
- Concurrent duplicate `ALL` dispatches are blocked.

## 4. Fast-Gating Behavior (Cost/Speed Optimization)

These scripts skip auth-heavy work when no action rows exist:

- `run_ingestion.py`: skips if no `Transform == READY`
- `run_syncing.py`: skips if no `QBO Sync == SYNC NOW`
- `run_reconciliation.py`: skips if no `QBO Reconcile == RECONCILE NOW`

This is intentional to reduce unnecessary QBO mapping fetches and auth churn.

## 5. Troubleshooting Playbook

### 5.1 "Why is it processing all workspaces?"
Check:

1. Webhook payload includes one of the supported target keys.
2. CLI uses `--client`.
3. Master sheet status and allow-list (`ALLOWED_QBO_WORKSPACES`) are correct.

### 5.2 "Rows not syncing"
Check Transform tabs:

1. `Remarks` contains `Ready to sync`.
2. Key IDs present:
   - Journal: `Journal No`
   - Expense: `Exp Ref. No`
   - Transfer: `Ref No`
3. Account mapping values exist in QBO.

### 5.3 "Currency/exchange issues"
Current behavior:

- Journal, Expense, Transfer all apply explicit FX for non-USD.
- Missing FX results in sync error by design.

Actions:

1. Verify transaction currency in Transform.
2. Verify exchange rate exists in QBO for TxnDate.
3. Re-run sync after fixing rates.

### 5.4 "Reconcile says not found but QBO has it"
Legacy cause:

- Old journal `QBO ID` looked like `2888.1`.

Now:

- Reconciler normalizes legacy suffix IDs.

If still failing:

1. Confirm date range month in control row.
2. Confirm doc number in sheet matches QBO.
3. Confirm realm/company is correct.

## 6. Useful Diagnostic Commands

List top-level files:

```powershell
Get-ChildItem -Name
```

Search key logic:

```powershell
rg -n "No READY rows|No SYNC NOW rows|No RECONCILE NOW rows" run_ingestion.py run_syncing.py run_reconciliation.py
rg -n "_attach_exchange_rate_if_needed|ExchangeRate" src\logic\syncing.py
rg -n "_normalize_qbo_id" src\logic\reconciler.py
```

Syntax check after edits:

```powershell
.\.venv\python.exe -m py_compile run_ingestion.py run_syncing.py run_reconciliation.py src\logic\transformer.py src\logic\syncing.py src\logic\reconciler.py
```

## 7. Recovery and Safe Re-run

When a sync/reconcile run partially fails:

1. Fix root cause (mapping, FX rate, source data, status column).
2. Set control row back to:
   - Sync: `SYNC NOW`
   - Reconcile: `RECONCILE NOW`
3. Re-run targeted client with `--client`.

For ingestion retry behavior:

- Existing output `ERROR` rows are detected and retried with preserved IDs.

## 8. Data Quality Rules to Respect

1. Keep `No` stable and numeric in raw data.
2. Avoid manual schema drift in output tabs.
3. Do not hand-edit generated `QBO ID` unless necessary.
4. Keep month/date formats consistent in control rows.

## 9. Maintenance Notes

1. Prefer additive compatibility fixes over destructive rewrites.
2. Preserve realm/workspace safety gates.
3. Keep docs synced with code when business rules change.
4. If introducing new webhook payload format, update `server.py` and this runbook.
