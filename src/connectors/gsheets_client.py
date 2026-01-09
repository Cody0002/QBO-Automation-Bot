from __future__ import annotations

import os
import re
import time
import random
import pandas as pd
from config import settings
from gspread.utils import rowcol_to_a1
from gspread.exceptions import APIError

# --- NEW IMPORTS FOR OAUTH REFRESHING ---
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import gspread
# ----------------------------------------

def _extract_sheet_id(url_or_id: str) -> str:
    if "docs.google.com" not in (url_or_id or ""):
        return (url_or_id or "").strip()
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url_or_id)
    if not m:
        raise ValueError(f"Cannot parse spreadsheetId from: {url_or_id}")
    return m.group(1)

def retry_with_backoff(retries=5, initial_delay=2.0):
    def decorator(func):
        def wrapper(*args, **kwargs):
            delay = initial_delay
            for i in range(retries):
                try:
                    return func(*args, **kwargs)
                except APIError as e:
                    if e.response.status_code in [429, 500, 502, 503]:
                        if i == retries - 1: raise e
                        sleep_time = delay + random.uniform(0, 1)
                        print(f"⚠️ Quota hit (429). Retrying in {sleep_time:.2f}s... (Attempt {i+1}/{retries})")
                        time.sleep(sleep_time)
                        delay *= 2
                    else:
                        raise e
                except Exception as e:
                    if "quota" in str(e).lower() or "rate limit" in str(e).lower():
                        time.sleep(delay)
                        delay *= 2
                    else:
                        raise e
            return func(*args, **kwargs)
        return wrapper
    return decorator

class GSheetsClient:
    """Google Sheets wrapper with Auto-Refresh OAuth."""

    def __init__(self):
        mode = (settings.GSHEETS_AUTH_MODE or "oauth").lower().strip()
        SCOPES = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]

        if mode == "service_account":
            from google.oauth2.service_account import Credentials as ServiceCreds
            creds = ServiceCreds.from_service_account_file(
                os.getenv("GOOGLE_SERVICE_ACCOUNT_PATH", "config/service_account.json"),
                scopes=SCOPES,
            )
            self.gc = gspread.authorize(creds)
        else:
            token_path = os.getenv("GOOGLE_OAUTH_TOKEN_PATH", "config/token.json")
            if not os.path.exists(token_path):
                raise FileNotFoundError(f"❌ token.json not found at {token_path}.")

            creds = Credentials.from_authorized_user_file(token_path, SCOPES)
            if not creds.valid:
                if creds.expired and creds.refresh_token:
                    print("🔄 OAuth Token expired. Refreshing now...")
                    try:
                        creds.refresh(Request())
                        with open(token_path, 'w') as token:
                            token.write(creds.to_json())
                        print("✅ Token refreshed and saved.")
                    except Exception as e:
                        print(f"❌ Failed to refresh token: {e}")
                        raise e
                else:
                    raise Exception("❌ Token is invalid. Re-generate token.json.")
            self.gc = gspread.authorize(creds)

    @retry_with_backoff()
    def open(self, spreadsheet_url_or_id: str):
        sid = _extract_sheet_id(spreadsheet_url_or_id)
        return self.gc.open_by_key(sid)

    @retry_with_backoff()
    def read_as_df(self, spreadsheet_url_or_id: str, tab_name: str, header_row: int = 1, value_render_option: str = 'FORMATTED_VALUE') -> pd.DataFrame:
        sh = self.open(spreadsheet_url_or_id)
        try:
            ws = sh.worksheet(tab_name)
        except Exception:
            print(f"⚠️ Warning: Tab '{tab_name}' not found. Returning empty DataFrame.")
            return pd.DataFrame()

        values = ws.get_all_values(value_render_option=value_render_option)
        if not values: return pd.DataFrame()

        header_idx = header_row - 1
        if header_idx >= len(values): return pd.DataFrame()

        header = values[header_idx]
        data = values[header_idx + 1 :]
        
        df = pd.DataFrame(data, columns=header)
        df = df.replace("", pd.NA).dropna(how="all")
        return df
    
    @retry_with_backoff()
    def read_as_df_sync(self, spreadsheet_url_or_id: str, tab_name: str) -> pd.DataFrame:
        sh = self.open(spreadsheet_url_or_id)
        try:
            ws = sh.worksheet(tab_name)
        except Exception:
            print(f"⚠️ Warning: Tab '{tab_name}' not found. Returning empty DataFrame.")
            return pd.DataFrame()
        values = ws.get_all_records()
        return pd.DataFrame(values)

    @retry_with_backoff()
    def create_spreadsheet(self, title: str) -> str:
        try:
            sh = self.gc.create(title)
            return f"https://docs.google.com/spreadsheets/d/{sh.id}"
        except Exception as e:
            print(f"❌ Failed to create spreadsheet '{title}': {e}")
            raise e

    @retry_with_backoff()
    def copy_permissions(self, source_id: str, target_id: str):
        try:
            source_sh = self.open(source_id)
            target_sh = self.open(target_id)
            permissions = source_sh.list_permissions()
            for p in permissions:
                email = p.get('emailAddress')
                role = p.get('role')
                if not email or "iam.gserviceaccount.com" in email: continue
                if role == 'owner': role = 'writer'
                print(f"   Sharing with {email} ({role})...")
                try:
                    target_sh.share(email, perm_type='user', role=role, notify=False)
                except Exception as share_err:
                    print(f"   ⚠️ Could not share with {email}: {share_err}")
        except Exception as e:
            print(f"⚠️ Warning: Could not copy permissions: {e}")
        
    @retry_with_backoff()
    def update_cell(self, spreadsheet_url_or_id: str, tab_name: str, row: int, col: int, value: str):
        sh = self.open(spreadsheet_url_or_id)
        ws = sh.worksheet(tab_name)
        cell_address = rowcol_to_a1(row, col)
        ws.update(range_name=cell_address, values=[[str(value)]], value_input_option="USER_ENTERED")

    @retry_with_backoff()
    def batch_update_cells(self, spreadsheet_url_or_id: str, tab_name: str, updates: list[dict]):
        if not updates: return
        sh = self.open(spreadsheet_url_or_id)
        ws = sh.worksheet(tab_name)
        batch_payload = []
        for u in updates:
            a1_notation = rowcol_to_a1(u['row'], u['col'])
            batch_payload.append({'range': a1_notation, 'values': [[str(u['val'])]]})
        ws.batch_update(batch_payload, value_input_option="USER_ENTERED")

    @retry_with_backoff()
    def delete_rows(self, spreadsheet_url_or_id: str, tab_name: str, row_indices: list[int]):
        if not row_indices: return
        sh = self.open(spreadsheet_url_or_id)
        try:
            ws = sh.worksheet(tab_name)
        except Exception: return
        sorted_rows = sorted(list(set(row_indices)), reverse=True)
        for row_num in sorted_rows:
            try: ws.delete_rows(row_num)
            except Exception: pass

    # --- NEW: Delete "Sheet1" Helper ---
    @retry_with_backoff()
    def cleanup_default_sheet(self, spreadsheet_url_or_id: str):
        """Deletes 'Sheet1' if other tabs exist."""
        sh = self.open(spreadsheet_url_or_id)
        worksheets = sh.worksheets()
        if len(worksheets) > 1:
            try:
                ws = sh.worksheet("Sheet1")
                sh.del_worksheet(ws)
                print("   🗑️ Deleted default 'Sheet1'")
            except Exception:
                pass # Sheet1 probably doesn't exist

    # --- UPDATED: COPY TEMPLATE FROM OTHER FILE ---
    @retry_with_backoff()
    def append_or_create_df(self, spreadsheet_url_or_id: str, tab_name: str, df: pd.DataFrame, 
                            template_tab_name: str | None = None, 
                            template_spreadsheet_id: str | None = None) -> None:
        if df is None or df.empty: return
        
        target_sh = self.open(spreadsheet_url_or_id)
        target_sid = target_sh.id

        # 1. Check if tab exists
        try:
            ws = target_sh.worksheet(tab_name)
            tab_exists = True
        except Exception:
            tab_exists = False

        df_export = df.astype(object).where(pd.notnull(df), None)
        data_values = df_export.values.tolist()

        # 2. If tab missing, create it (Using Template if provided)
        if not tab_exists:
            created_from_template = False
            
            if template_tab_name and template_spreadsheet_id:
                try:
                    # Open the Source (Control Sheet)
                    source_sh = self.open(template_spreadsheet_id)
                    source_ws = source_sh.worksheet(template_tab_name)
                    
                    # Copy to Target
                    print(f"   📋 Copying template '{template_tab_name}'...")
                    copy_res = source_ws.copy_to(target_sid)
                    
                    # Find the new sheet (it comes in as "Copy of...") and Rename it
                    new_sheet_id = copy_res['sheetId']
                    ws = target_sh.get_worksheet_by_id(new_sheet_id)
                    ws.update_title(tab_name)
                    
                    created_from_template = True
                    print(f"   ✅ Created '{tab_name}' from template.")
                except Exception as e:
                    print(f"   ⚠️ Template Copy Failed ({e}). Falling back to blank.")

            if not created_from_template:
                # Fallback: Create Blank
                rows = max(len(df) + 1, 100)
                cols = max(len(df.columns), 26)
                ws = target_sh.add_worksheet(title=tab_name, rows=rows, cols=cols)
                # Write Headers for blank sheet
                values = [df.columns.tolist()] + data_values
                ws.update("A1", values)
                return

        # 3. Append Data (Whether created from template or existing)
        ws.append_rows(data_values, value_input_option="USER_ENTERED")