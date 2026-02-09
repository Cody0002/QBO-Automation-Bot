"""Project settings & constants.

Keep secrets in `config/secrets.env` (not committed).
"""

from __future__ import annotations

import os
from dataclasses import dataclass

# ---- Google Sheets (Control) ----
# Control spreadsheet includes list of source tabs to process.
CONTROL_SHEET_ID = os.getenv("CONTROL_SHEET_ID", "1dRaRVkxBGgRD8I2AYz2QSbG5h32MM7pN1W6fNqZ_XBw")  # e.g. 1AbC...
CONTROL_TAB_NAME = os.getenv("CONTROL_TAB_NAME", "JOBS CONTROL")

# UPDATED: Column names based on your request and screenshot
CTRL_COL_COUNTRY = "Country"
CTRL_COL_SOURCE_URL = "Source File"       # Was "Spreadsheet_URL"
CTRL_COL_TRANSFORM_URL = "Transform File" # NEW COLUMN
CTRL_COL_TAB_NAME = "Tab Name"
CTRL_COL_MONTH = "Month"
CTRL_COL_ACTIVE = "Transform"
CTRL_COL_QBO_SYNC = "QBO Sync"

CTRL_COL_LAST_RUN_AT = "Last Run At"
CTRL_COL_LAST_PROCESSED_ROW = "Last Processed Row"

# Raw data expectations
RAW_COL_NO = os.getenv("RAW_COL_NO", "No")  # incremental row id / sequence

# Output tabs (inside the *same* spreadsheet by default, configurable)
OUTPUT_TAB_JOURNALS = os.getenv("OUTPUT_TAB_JOURNALS", "Journals")
OUTPUT_TAB_EXPENSES = os.getenv("OUTPUT_TAB_EXPENSES", "Expenses")
OUTPUT_TAB_WITHDRAW = os.getenv("OUTPUT_TAB_WITHDRAW", "Transfers")

# Behavior
DEFAULT_BATCH_SIZE = int(os.getenv("DEFAULT_BATCH_SIZE", "5000"))

# ---- QBO ----
QBO_BASE_URL = os.getenv("QBO_BASE_URL", "https://quickbooks.api.intuit.com")
QBO_TOKEN_URL = os.getenv("QBO_TOKEN_URL", "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer")
QBO_MINOR_VERSION = os.getenv("QBO_MINOR_VERSION", "65")

# ---- Auth mode for Google Sheets ----
# Supported:
# - service_account: uses config/service_account.json
# - oauth: uses OAuth client secret + token.json (recommended for work account flow)
GSHEETS_AUTH_MODE = os.getenv("GSHEETS_AUTH_MODE", "oauth")

# ---- Master Sheet Configuration ----
# Replace with the ID of the sheet from your screenshot
MASTER_SHEET_ID = os.getenv("MASTER_SHEET_ID", "1fVAVZXosAIz-Je-04vJe80Ggr1iAYNqBxMM84-FVWlU") 
MASTER_TAB_NAME = os.getenv("MASTER_TAB_NAME", "Sheet1")

# Master Sheet Columns
MST_COL_CLIENT = "Client Name"
MST_COL_SHEET_ID = "Spreadsheet ID"
MST_COL_REALM_ID = "Realm ID"
MST_COL_STATUS = "Status"
MST_COL_OUTPUT = "Output Folder"
MST_COL_REFRESH_TOKEN = "Refresh Token"  # <--- NEW COLUMN

@dataclass(frozen=True)
class ControlRow:
    country: str
    spreadsheet_url: str
    tab_name: str
    month: str
    active: bool
    last_run_at: str | None
    last_processed_row: int | None
