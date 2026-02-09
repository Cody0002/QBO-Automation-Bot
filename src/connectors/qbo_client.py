from __future__ import annotations
import base64
import os
import time
import requests
import urllib.parse  # <--- ADD THIS IMPORT
import pandas as pd
from typing import Optional, Any, Dict
from config import settings
# Import GSheetsClient for type hinting only
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from src.connectors.gsheets_client import GSheetsClient

class QBOClient:
    """
    Smart QBO Client that manages its own authentication via the Master Sheet.
    """
    def __init__(self, gs_client: GSheetsClient = None):
        self.gs = gs_client
        self.client_id = os.getenv("QBO_CLIENT_ID", "")
        self.client_secret = os.getenv("QBO_CLIENT_SECRET", "")
        
        # State variables
        self.realm_id: str | None = None
        self.refresh_token: str | None = None
        self.access_token: str | None = None
        self.token_expiry: float = 0.0
        
        # Cache the Master Sheet row index for the current realm to speed up writes
        self._master_sheet_row_idx: int | None = None

    def set_company(self, realm_id: str):
        """
        Switches context to a specific company.
        Reads the latest Refresh Token from the Master Sheet.
        """
        self.realm_id = str(realm_id).strip()
        self.access_token = None # Clear old access token
        self.token_expiry = 0.0
        
        print(f"🔄 [QBOClient] Switching context to Realm ID: {self.realm_id}")
        self._load_auth_from_sheet()

    def _load_auth_from_sheet(self):
        """Finds the refresh token in the Master Sheet for the current Realm ID."""
        if not self.gs:
            raise ValueError("GSheetsClient not provided to QBOClient.")

        # Read Master Sheet (Columns: Client Name, Spreadsheet ID, Realm ID, ..., Refresh Token)
        df = self.gs.read_as_df(settings.MASTER_SHEET_ID, settings.MASTER_TAB_NAME)
        
        # Find the row with this Realm ID
        # Note: We assume Realm ID is unique per row
        mask = df[settings.MST_COL_REALM_ID].astype(str).str.strip() == self.realm_id
        if not mask.any():
            raise ValueError(f"Realm ID {self.realm_id} not found in Master Sheet!")

        row_data = df[mask].iloc[0]
        
        # Calculate Row Number (Dataframe index + 2 for header/0-index correction)
        self._master_sheet_row_idx = df.index[mask][0] + 2
        
        # Get Token
        self.refresh_token = str(row_data.get(settings.MST_COL_REFRESH_TOKEN, "")).strip()
        if not self.refresh_token:
            print(f"⚠️ Warning: No Refresh Token found for Realm {self.realm_id}")

    def _save_new_token_to_sheet(self, new_refresh_token: str):
        """Writes a rotated refresh token back to the Master Sheet."""
        if not self._master_sheet_row_idx:
            print("⚠️ Cannot save token: Master Sheet row unknown.")
            return

        print(f"💾 [QBOClient] Updating Refresh Token in Master Sheet (Row {self._master_sheet_row_idx})...")
        
        # We need to find the column index for "Refresh Token"
        # Ideally, cache this or look it up dynamically. 
        # For safety, we'll read the header row again or rely on settings if you made a constant.
        # Here is a robust way to do it with a cell update:
        
        # 1. Read headers to find column number
        headers = self.gs.read_as_df(settings.MASTER_SHEET_ID, settings.MASTER_TAB_NAME, header_row=1).columns.tolist()
        try:
            col_idx = headers.index(settings.MST_COL_REFRESH_TOKEN) + 1
        except ValueError:
            print(f"❌ Critical: Column '{settings.MST_COL_REFRESH_TOKEN}' not found in Master Sheet headers.")
            return

        # 2. Update Cell
        self.gs.update_cell(
            settings.MASTER_SHEET_ID, 
            settings.MASTER_TAB_NAME, 
            self._master_sheet_row_idx, 
            col_idx, 
            new_refresh_token
        )
        print("✅ Token saved.")

    def _basic_auth_header(self) -> str:
        auth_str = f"{self.client_id}:{self.client_secret}"
        return base64.b64encode(auth_str.encode()).decode()

    def refresh_access_token(self) -> str:
        """Exchanges refresh_token for access_token. Handles rotation automatically."""
        if not self.refresh_token:
            raise ValueError("Cannot refresh: No Refresh Token available.")

        headers = {
            "Authorization": f"Basic {self._basic_auth_header()}",
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = {"grant_type": "refresh_token", "refresh_token": self.refresh_token}
        
        resp = requests.post(settings.QBO_TOKEN_URL, headers=headers, data=data, timeout=60)
        if resp.status_code != 200:
            print(f"❌ QBO Auth Failed: {resp.text}")
            resp.raise_for_status()
            
        payload = resp.json()
        self.access_token = payload["access_token"]
        expires_in = int(payload.get("expires_in", 3600))
        self.token_expiry = time.time() + max(60, expires_in - 120)

        # --- AUTO-ROTATION LOGIC ---
        new_refresh_token = payload.get("refresh_token")
        if new_refresh_token and new_refresh_token != self.refresh_token:
            print("✨ QBO Rotated Refresh Token. Saving...")
            self.refresh_token = new_refresh_token
            self._save_new_token_to_sheet(new_refresh_token)
        # ---------------------------

        return self.access_token

    def get_access_token(self) -> str:
        if not self.access_token or time.time() >= self.token_expiry:
            return self.refresh_access_token()
        return self.access_token

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.get_access_token()}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _url(self, path: str) -> str:
        return f"{settings.QBO_BASE_URL}{path}"

    def _get(self, path: str) -> Dict[str, Any]:
        url = self._url(path)
        resp = requests.get(url, headers=self._headers(), timeout=60)
        resp.raise_for_status()
        return resp.json()

    def query(self, select_statement: str) -> list[Dict[str, Any]]:
        all_results = []
        start_position = 1
        max_results = 1000

        while True:
            paged_query = f"{select_statement} STARTPOSITION {start_position} MAXRESULTS {max_results}"
            encoded_query = urllib.parse.quote(paged_query)
            path = f"/v3/company/{self.realm_id}/query?query={encoded_query}&minorversion={settings.QBO_MINOR_VERSION}"
            
            data = self._get(path)
            query_response = data.get("QueryResponse", {})
            entities = []
            for key, value in query_response.items():
                if isinstance(value, list): entities.extend(value)
            
            if not entities: break
            all_results.extend(entities)
            start_position += len(entities)
            if len(entities) < max_results: break
                
        return all_results

    def post(self, path: str, json_body: Dict[str, Any]) -> Dict[str, Any]:
        url = self._url(path)
        resp = requests.post(url, headers=self._headers(), json=json_body, timeout=60)
        
        # --- NEW: DETAILED ERROR REPORTING ---
        if resp.status_code >= 400:
            print(f"❌ QBO API ERROR ({resp.status_code}): {resp.text}")
            
        resp.raise_for_status()
        return resp.json()

    def get_max_journal_number(self, prefix: str) -> int:
        query = f"SELECT DocNumber FROM JournalEntry WHERE DocNumber LIKE '{prefix}%' ORDER BY DocNumber DESC MAXRESULTS 1"
        try:
            results = self.query(query)
            if not results: return 0
            doc_num = str(results[0].get("DocNumber", ""))
            if doc_num.startswith(prefix):
                suffix = doc_num[len(prefix):]
                clean_suffix = "".join(filter(str.isdigit, suffix))
                return int(clean_suffix) if clean_suffix else 0
            return 0
        except Exception as e:
            print(f"⚠️ Failed to fetch max journal number: {e}")
            return 0