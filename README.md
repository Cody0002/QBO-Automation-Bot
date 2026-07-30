# Google Sheets -> Transform -> QBO Pipeline

This project runs a repeatable accounting pipeline:

1. Read **raw** data from Google Sheets (monthly tab).
2. Transform in Python (Pandas) into 3 outputs:
   - Journals
   - Expenses
   - Withdraw
3. Write outputs back to Sheets.
4. Push transformed records into QuickBooks Online (QBO).
5. Run reconciliation checks on a schedule (e.g., every 5 minutes).

## Folder Structure

```
config/
  settings.py
  secrets.env              # NOT committed
  service_account.json     # optional
  oauth_client_secret.json # optional
  token.json               # optional
logs/
src/
  connectors/
    gsheets_client.py
    qbo_client.py
  logic/
    transformer.py
    reconciler.py
  utils/
    logger.py
run_ingestion.py
run_reconciliation.py
```

## AI Codebase Graph

This repo has a Graphify knowledge graph in `graphify-out/`.

For AI-assisted architecture work, refactors, or coding optimization, ask the AI to read `graphify-out/GRAPH_REPORT.md` and use `graphify-out/graph.json` or `graphify query "<question>"` before changing code.

Client-specific logic differs across KZO, KZP, KZDW, S5, and UMBER. Future optimization should compare those branches in:

- `run_ingestion.py`
- `src/logic/raw_adapter.py`
- `src/logic/transformer.py`
- `src/logic/syncing.py`
- `src/logic/reconciler.py`

## Control Sheet (for users)

In your **Control** tab, users only fill these columns:

- `Country`
- `Spreadsheet_URL` (or spreadsheetId)
- `Tab Name` (monthly raw tab name, e.g. `2025-10`)
- `Month` (optional, for tracking)
- `Active` = TRUE/FALSE

The pipeline automatically updates:

- `Last Run At`
- `Last Processed Row` (based on `No.` column in raw data)

### Pending rows

A raw row is held in `Pending Amount Nos` (not pushed to QBO) when it has a `QBO Method`
but `USD - QBO` is 0. Once the amount is filled in, the next run picks the row up
automatically, even though its `No.` is already below `Last Processed Row`.

### KZDW temporary COY hold

`run_ingestion.py` has a manual switch for holding KZDW rows by `COY` value:

```python
KZDW_FORCED_PENDING_COY_VALUES: set[str] = set()
```

Any `COY` listed there (case-insensitive, whitespace-trimmed) stays in `Pending Amount Nos`
for KZDW regardless of amount or method, and is skipped by new/late-filled/retry processing.
Emptying the set releases those rows on the next run — they come back through the
late-filled path and post normally.

Status: **`COY = TD` is released and processing normally** (held from 2026-07-22, released
2026-07-30 after its posting logic was confirmed). To hold it again, set the value back to
`{"TD"}`.

### Monthly deployment (new month tab)
Create a new row in Control sheet with:
- same `Spreadsheet_URL`
- new `Tab Name` (new month)
- set `Last Processed Row` = 0
- `Active` = TRUE

No code change needed.

## Setup

### 1) Create venv & install
```bash
python -m venv .venv
source .venv/bin/activate   # (Windows: .venv\Scripts\activate)
pip install -r requirements.txt
```

### 2) Configure secrets
Copy `config/secrets.env` template and fill values.

### 3) Google Sheets auth
Recommended: OAuth (work account)

- Put your OAuth client secret JSON at: `config/oauth_client_secret.json`
- First run will open a browser login (or you can generate token.json on your laptop and copy it to server).
- Token is saved at `config/token.json`

Alternative: service account
- Put service account json at `config/service_account.json`
- Share the spreadsheet with that service account email

### 4) Run ingestion
```bash
python run_ingestion.py
```

### 5) Run reconciliation
```bash
python run_reconciliation.py
```

## Scheduling (every 5 minutes)

Use **cron** or **systemd timer**:

Cron example:
```bash
*/5 * * * * /path/to/.venv/bin/python /path/to/run_reconciliation.py >> /path/to/logs/cron.log 2>&1
```

## Where to plug your existing notebook logic
- Copy your cleaning/splitting logic from `Accounting_Clean.ipynb` into:
  - `src/logic/transformer.py :: transform_raw()`
- Copy your QBO payload mapping/push logic (from `API_QBO_flow.ipynb`) into:
  - a new module (recommended) `src/logic/qbo_mapper.py`
  - call `qbo.create_journal_entry(...)` / `qbo.create_purchase(...)` / `qbo.create_deposit(...)` in `run_ingestion.py`
