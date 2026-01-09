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
