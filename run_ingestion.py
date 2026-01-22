from __future__ import annotations

# --- FIX: USE WINDOWS SYSTEM CERTIFICATES ---
try:
    import pip_system_certs.wrappers
    pip_system_certs.wrappers.wrap_requests()
except ImportError:
    pass
# --------------------------------------------

from dotenv import load_dotenv
load_dotenv("config/secrets.env")

import os
from datetime import datetime
from typing import Any, Dict, List, Tuple
import pandas as pd
from config import settings
from src.connectors.gsheets_client import GSheetsClient
from src.connectors.qbo_client import QBOClient
from src.logic.syncing import QBOSync
from src.logic.transformer import transform_raw
from src.utils.logger import setup_logger

logger = setup_logger("ingestion")

def _now_iso_local() -> str:
    now = datetime.now().astimezone()
    z = now.strftime("%z") 
    try:
        offset_hour = int(z[:3]) 
        gmt_str = f"GMT{offset_hour:+}" 
    except:
        gmt_str = "GMT"
    return now.strftime(f"%Y-%m-%d %H:%M:%S ({gmt_str})")

def _batch_update_control(gs, sheet_id, tab_name, row_num, columns, updates_dict):
    headers = list(columns)
    for col_name, val in updates_dict.items():
        if col_name in headers:
            col_idx = headers.index(col_name) + 1
            gs.update_cell(sheet_id, tab_name, row_num, col_idx, str(val))

def format_month_name(date_str: str) -> str:
    if not date_str: return ""
    try:
        dt = pd.to_datetime(date_str)
        return dt.strftime("%b %y")
    except Exception:
        return date_str

def get_retry_context(gs: GSheetsClient, spreadsheet_url: str, tab_name: str, id_col_name: str) -> Tuple[List[int], Dict[int, str]]:
    try:
        df = gs.read_as_df_sync(spreadsheet_url, tab_name)
        if df.empty or "Remarks" not in df.columns or id_col_name not in df.columns:
            return [], {}
        
        error_mask = df["Remarks"].astype(str).str.contains("ERROR|Unbalanced", case=False, na=False)
        bad_rows = df[error_mask]
        if bad_rows.empty: return [], {}

        bad_ids = bad_rows[id_col_name].unique()
        rows_to_delete_mask = df[id_col_name].isin(bad_ids)
        target_df = df[rows_to_delete_mask].copy()
        if target_df.empty: return [], {}

        rows_to_delete = []
        existing_id_map = {}
        for idx, row in target_df.iterrows():
            rows_to_delete.append(idx + 2) 
            if "No" in row and pd.notna(row["No"]):
                try:
                    s_no = int(float(str(row["No"])))
                    existing_id_map[s_no] = str(row[id_col_name])
                except: pass

        return rows_to_delete, existing_id_map
    except Exception:
        return [], {}

def main():
    gs = GSheetsClient()

    logger.info("🔌 Connecting to QBO to fetch Account/Location Mappings...")
    try:
        qbo_client = QBOClient()
        temp_sync = QBOSync(qbo_client) 
        qbo_mappings = temp_sync.mappings 
        logger.info("✅ QBO Mappings fetched successfully.")
    except Exception as e:
        logger.error(f"❌ Failed to fetch QBO Mappings: {e}")
        return

    if not settings.CONTROL_SHEET_ID:
        raise ValueError("CONTROL_SHEET_ID is empty in config/secrets.env")

    logger.info(f"Connecting to Control Sheet: {settings.CONTROL_TAB_NAME}")
    try:
        ctrl_df = gs.read_as_df(settings.CONTROL_SHEET_ID, settings.CONTROL_TAB_NAME)
    except Exception as e:
        logger.error(f"Failed to read Control Tab: {e}")
        raise e

    if ctrl_df.empty: return

    COL_LAST_JV = "Last Journal No"
    COL_LAST_EXP = "Last Expense No"
    COL_LAST_TR = "Last Transfer No"
    COL_QBO_JV = "QBO Journal"
    COL_QBO_EXP = "QBO Expense"
    COL_QBO_TR = "QBO Transfer"

    for i, row in ctrl_df.iterrows():
        status_val = str(row.get(settings.CTRL_COL_ACTIVE, "")).strip()
        if status_val != 'READY': continue

        row_num = i + 2
        logger.info(f"🚀 Starting Row {row_num}: Status is READY -> Setting to PROCESSING")
        _batch_update_control(gs, settings.CONTROL_SHEET_ID, settings.CONTROL_TAB_NAME, row_num, ctrl_df.columns, {settings.CTRL_COL_ACTIVE: "PROCESSING"})

        try:
            country = str(row.get(settings.CTRL_COL_COUNTRY, "")).strip()
            source_url = str(row.get(settings.CTRL_COL_SOURCE_URL, "")).strip()
            transform_url = str(row.get(settings.CTRL_COL_TRANSFORM_URL, "")).strip()
            raw_tab_name = str(row.get(settings.CTRL_COL_TAB_NAME, "")).strip()
            raw_month = str(row.get(settings.CTRL_COL_MONTH, "")).strip()
            month = format_month_name(raw_month)

            # --- CREATE TRANSFORM FILE IF MISSING ---
            if not transform_url or len(transform_url) < 10:
                new_title = f"{country} QBO - {month}"
                logger.info(f"   ⚠️ No Transform File found. Creating: '{new_title}'...")
                try:
                    transform_url = gs.create_spreadsheet(new_title)
                    try:
                        new_file_id = transform_url.split("/d/")[1].split("/")[0]
                        logger.info("      Applying permissions...")
                        gs.copy_permissions(source_id=settings.CONTROL_SHEET_ID, target_id=new_file_id)
                    except Exception as pe:
                        logger.warning(f"      ⚠️ Permission copy warning: {pe}")

                    _batch_update_control(gs, settings.CONTROL_SHEET_ID, settings.CONTROL_TAB_NAME, row_num, ctrl_df.columns, {settings.CTRL_COL_TRANSFORM_URL: transform_url})
                    logger.info(f"   ✅ Created & Secured: {transform_url}")
                except Exception as e:
                    logger.error(f"   ❌ Failed to create spreadsheet: {e}")
                    raise e
            
            # --- PREPARE COUNTERS ---
            def get_int(val):
                try: return int(float(val))
                except: return 0

            last_processed = get_int(row.get(settings.CTRL_COL_LAST_PROCESSED_ROW, 0))
            sheet_last_jv = get_int(row.get(COL_LAST_JV, 0))
            
            logger.info(f"   🔎 Checking QBO for latest Journal Number (Prefix: KZO-JV)...")
            qbo_last_jv = qbo_client.get_max_journal_number("KZO-JV")
            final_start_jv = max(sheet_last_jv, qbo_last_jv)
            
            last_exp = get_int(row.get(COL_LAST_EXP, 0))
            last_tr = get_int(row.get(COL_LAST_TR, 0))

            tab_prefix = f"{country} {month}"
            tab_jv = f"{tab_prefix} - Journals"
            tab_exp = f"{tab_prefix} - Expenses"
            tab_tr = f"{tab_prefix} - Transfers"
        
            # --- RETRY CONTEXT ---
            preserved_ids = {'journals': {}, 'expenses': {}, 'transfers': {}}
            deletions = {}

            jv_del, jv_ids = get_retry_context(gs, transform_url, tab_jv, "Journal No")
            if jv_del:
                deletions[tab_jv] = jv_del
                preserved_ids['journals'] = jv_ids

            exp_del, exp_ids = get_retry_context(gs, transform_url, tab_exp, "Exp Ref. No")
            if exp_del:
                deletions[tab_exp] = exp_del
                preserved_ids['expenses'] = exp_ids

            tr_del, tr_ids = get_retry_context(gs, transform_url, tab_tr, "Ref No")
            if tr_del:
                deletions[tab_tr] = tr_del
                preserved_ids['transfers'] = tr_ids

            retry_nos = list(set(list(preserved_ids['journals'].keys()) + list(preserved_ids['expenses'].keys()) + list(preserved_ids['transfers'].keys())))

            # --- READ SOURCE & ALIGN COLUMNS ---
            logger.info(f"[{country}] Reading Source: {raw_tab_name}")
            try:
                raw_df = gs.read_as_df(source_url, raw_tab_name, header_row=1, value_render_option='UNFORMATTED_VALUE')
                print(raw_df.head(2))
            # 1. Manually assign columns to match the list you provided for the 2026 reporting transition
                raw_df.columns = [
                    "CO", "COY", "Date", "Category", "Type", "Item Description", 
                    "TrxHarsh", "Account Fr", "Account To", "Currency", "Amount Fr", 
                    "Currency To", "Amount To", "Budget", "USD - Raw", "USD - Actual", 
                    "USD - Loss", "USD - QBO", "Reclass", "QBO Method", 
                    "If Journal/Expense Method", "QBO Transfer Fr", "QBO Transfer To", 
                    "Check (Internal use)", "No"
                ]

                # 2. TYPE SAFETY FIX: Convert strings to numeric objects immediately
             # 2. THE CRITICAL FIX: Convert to Numeric Series immediately
                # We use errors='coerce' to turn text into NaN, then fillna(0) works on the resulting Series
                for col in ["No", "USD - QBO", "Amount Fr", "Amount To"]:
                    if col in raw_df.columns:
                        raw_df[col] = pd.to_numeric(raw_df[col], errors="coerce").fillna(0)

                print(raw_df.dtypes)
            except Exception as e:
                logger.error(f"Failed to read Source File or align columns: {e}")
                _batch_update_control(gs, settings.CONTROL_SHEET_ID, settings.CONTROL_TAB_NAME, row_num, ctrl_df.columns, {settings.CTRL_COL_ACTIVE: "ERROR (Read Source)"})
                continue
            
            if raw_df.empty:
                logger.info(f"[{country}] Raw tab empty.")
                _batch_update_control(gs, settings.CONTROL_SHEET_ID, settings.CONTROL_TAB_NAME, row_num, ctrl_df.columns, {settings.CTRL_COL_ACTIVE: "DONE"})
                continue

            # 3. ROBUST FILTERING: Ensure we can handle empty cells in the Check column
            # We force the column to string first so .str.contains works even on empty/null cells
            raw_df = raw_df[~raw_df["Check (Internal use)"].astype(str).str.contains("exclude", na=False, case=False)].copy()

            # 4. Filter for only new or retried rows
            new_df = raw_df[raw_df["No"] > last_processed].copy()
            retry_df = raw_df[raw_df["No"].isin(retry_nos)].copy()
            processing_df = pd.concat([new_df, retry_df]).drop_duplicates(subset=["No"])

            if processing_df.empty:
                logger.info(f"[{country}] No new rows.")
                _batch_update_control(gs, settings.CONTROL_SHEET_ID, settings.CONTROL_TAB_NAME, row_num, ctrl_df.columns, {settings.CTRL_COL_LAST_RUN_AT: _now_iso_local(), settings.CTRL_COL_ACTIVE: "DONE"})
                continue
            
            # Clean existing bad rows in Transform File
            for tab, rows in deletions.items():
                if rows: gs.delete_rows(transform_url, tab, rows)

            logger.info(f"[{country}] Transforming {len(processing_df)} rows...")
            
            # --- CALL TRANSFORMER ---
            print(processing_df.dtypes)
            result = transform_raw(processing_df, final_start_jv, last_exp, last_tr, qbo_mappings=qbo_mappings, existing_ids=preserved_ids)
            print("finished transform!")

            # --- CONVERT DATETIMES TO STRINGS FOR GOOGLE SHEETS ---
            # This prevents the "Timestamp is not JSON serializable" error
            for df_res in [result.journals, result.expenses, result.withdraw]:
                if not df_res.empty:
                    for col in df_res.select_dtypes(include=['datetime64', 'datetimetz']).columns:
                        df_res[col] = df_res[col].dt.strftime('%Y-%m-%d')

            # --- WRITE TO OUTPUT ---
            if not result.journals.empty:
                gs.append_or_create_df(transform_url, tab_jv, result.journals, template_tab_name="Sample - Journals", template_spreadsheet_id=settings.CONTROL_SHEET_ID)
            
            if not result.expenses.empty:
                gs.append_or_create_df(transform_url, tab_exp, result.expenses, template_tab_name="Sample - Expenses", template_spreadsheet_id=settings.CONTROL_SHEET_ID)
            
            if not result.withdraw.empty:
                gs.append_or_create_df(transform_url, tab_tr, result.withdraw, template_tab_name="Sample - Transfers", template_spreadsheet_id=settings.CONTROL_SHEET_ID)

            gs.cleanup_default_sheet(transform_url)

            # --- STATUS & UPDATE ---
            def check_status(df):
                if df.empty: return ""
                if "Remarks" in df.columns and df["Remarks"].astype(str).str.contains("ERROR", case=False, na=False).any(): return "ERROR"
                return "READY TO SYNC"

            status_jv = check_status(result.journals)
            status_exp = check_status(result.expenses)
            status_tr = check_status(result.withdraw)

            final_last_row = max(last_processed, result.max_row_processed) if result.max_row_processed else last_processed

            updates = {
                settings.CTRL_COL_LAST_PROCESSED_ROW: final_last_row,
                COL_LAST_JV: result.last_journal_no,
                COL_LAST_EXP: result.last_expense_no,
                COL_LAST_TR: result.last_withdraw_no,
                settings.CTRL_COL_LAST_RUN_AT: _now_iso_local(),
                settings.CTRL_COL_ACTIVE: "DONE"
            }
            if COL_QBO_JV in ctrl_df.columns: updates[COL_QBO_JV] = status_jv
            if COL_QBO_EXP in ctrl_df.columns: updates[COL_QBO_EXP] = status_exp
            if COL_QBO_TR in ctrl_df.columns: updates[COL_QBO_TR] = status_tr
            
            _batch_update_control(gs, settings.CONTROL_SHEET_ID, settings.CONTROL_TAB_NAME, row_num, ctrl_df.columns, updates)
            logger.info(f"[{country}] Process Complete. Statuses updated.")

        except Exception as e:
            logger.error(f"❌ Error processing row {row_num} ({country}): {e}")
            _batch_update_control(gs, settings.CONTROL_SHEET_ID, settings.CONTROL_TAB_NAME, row_num, ctrl_df.columns, {settings.CTRL_COL_ACTIVE: "ERROR"})
            continue

    logger.info("Job Finished.")

if __name__ == "__main__":
    main()