# QBO Automation Bot 🤖💸

A robust **ETL (Extract, Transform, Load)** pipeline designed to automate accounting workflows for **cryptocurrency transactions**.

This system fetches transaction data from **Cregis**, transforms and cleans it in **Google Sheets**, synchronizes it with **QuickBooks Online (QBO)**, and performs automated **Reconciliation** to ensure financial accuracy.

---

## 🚀 Features

### 🔹 Ingestion (ETL)
- Fetches raw crypto transaction data from the **Cregis API**
- Cleans, filters, and formats data (e.g. Unix timestamp conversion)
- Uploads processed data to structured **Google Sheets**

### 🔹 Transformation
- Auto-categorizes transactions into:
  - Journal Entries
  - Expenses
  - Transfers
- Assigns **QBO Account IDs** automatically based on wallet mappings

### 🔹 Syncing
- Pushes validated data into **QuickBooks Online** via the Accounting API
- Prevents duplicate records
- Handles API rate limits safely

### 🔹 Reconciliation
- Compares **Google Sheets (source)** with **QBO (target)**
- Detects mismatches in:
  - Amount
  - Date
  - Memo
  - Account
- Updates the **Control Sheet** with pass/fail statuses

### 🔹 Webhook Server
- Flask-based server
- Listens for external triggers to run the pipeline automatically

---

## 📂 Project Structure

```text
.
├── config/
│   ├── secrets.env         # API keys & secrets (NOT COMMITTED)
│   ├── cregis_oa.json      # Google OAuth credentials
│   └── token.json          # Google Sheets access token
├── src/
│   ├── connectors/         # API integration clients
│   │   ├── gsheets_client.py
│   │   └── qbo_client.py
│   ├── logic/              # Core business logic
│   │   ├── transformer.py  # Data cleaning & categorization
│   │   ├── syncing.py      # QBO mapping & upload logic
│   │   └── reconciler.py   # Comparison & verification logic
│   └── utils/
│       └── logger.py       # Logging configuration
├── run_ingestion.py        # Cregis → Google Sheets
├── run_syncing.py          # Google Sheets → QBO
├── run_reconciliation.py   # Verify data accuracy
├── server.py               # Flask webhook listener
└── README.md
```

---

## 🛠️ Setup & Installation

### 1️⃣ Prerequisites
- Python 3.8+
- Google Cloud Project with Google Sheets API enabled
- QuickBooks Online Developer Account
- Git

### 2️⃣ Install Dependencies

```bash
pip install pandas flask requests python-dotenv \
google-api-python-client google-auth-httplib2 \
google-auth-oauthlib gspread pip-system-certs
```

> **Note:** `pip-system-certs` fixes SSL certificate issues commonly found on corporate Windows networks.

### 3️⃣ Environment Configuration

Create the following file:

```
config/secrets.env
```

⚠️ **Do NOT commit this file to GitHub**

```env
# --- Google Sheets Configuration ---
GSHEET_URL=https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID
CONTROL_SHEET_ID=YOUR_CONTROL_SHEET_ID
CONTROL_TAB_NAME=Control Panel
START_DATE=2025-01-01

# --- Cregis API Projects ---
PROJ1_NAME="Project Alpha"
PROJ1_ID=123456789
PROJ1_SECRET=abcdef123456

PROJ2_NAME="Project Beta"
PROJ2_ID=987654321
PROJ2_SECRET=654321fedcba

# --- QuickBooks Online (OAuth2) ---
QBO_CLIENT_ID=...
QBO_CLIENT_SECRET=...
QBO_REALM_ID=...
QBO_REFRESH_TOKEN=...
QBO_ENV=sandbox   # Change to 'production' for live data

# --- Webhook Security ---
SECRET_TOKEN=your_secure_password_123
```

---

## 🏃‍♂️ Usage

### 🔹 Option A: Manual Execution

#### Step 1: Fetch & Transform Data
Fetches data from Cregis and populates Google Sheets.

```bash
python run_ingestion.py
```

#### Step 2: Sync to QuickBooks
Reads Google Sheets and pushes transactions into QBO.

```bash
python run_syncing.py
```

#### Step 3: Reconciliation
Verifies that QBO data matches Google Sheets.

```bash
python run_reconciliation.py
```

### 🔹 Option B: Background Server (Webhook)

#### Start the Webhook Server

```bash
python server.py
```

#### Trigger Endpoint

- **URL:** `http://localhost:5000/webhook`
- **Method:** `POST`
- **Headers:**
  ```
  X-My-Secret-Token: <your_secure_password_123>
  ```
- **Body (JSON):**
  ```json
  {
    "event": "pipeline_trigger",
    "country": "TH"
  }
  ```

#### Supported Events
- `pipeline_trigger` — Run ingestion
- `sync_trigger` — Run QBO sync
- `reconcile_trigger` — Run reconciliation

---

## 📊 Google Sheets Control Layout

| Country | Last Processed Row | QBO Sync Status | Reconciliation Status | Last Run At          |
|---------|-------------------|-----------------|----------------------|---------------------|
| TH      | 150               | DONE            | Clean                | 2025-12-29 10:00    |
| VN      | 200               | ERROR           | Mismatch Found       | 2025-12-29 10:05    |

---

## 🛡️ Security Best Practices

- **Never commit secrets**
  - `.env`, `*.json`, `*.csv` excluded via `.gitignore`
- **Rotate keys immediately** if credentials are exposed
- **Change webhook tokens regularly**

---

## 🐛 Troubleshooting

### SSL Errors
- Ensure `pip-system-certs` is installed

### QBO Token Invalid
- Tokens refresh automatically
- If refresh fails:
  1. Generate a new refresh token via QBO Playground
  2. Update `config/secrets.env`

### Google Authentication Error
1. Delete `token.json`
2. Re-run the script locally
3. Complete browser-based OAuth authentication

---

## ✅ Status

Production-ready, modular, and designed for secure, auditable financial automation with QuickBooks Online.
