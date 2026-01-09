from __future__ import annotations
import os
from dotenv import load_dotenv

# --- FIX: USE WINDOWS SYSTEM CERTIFICATES ---
try:
    import pip_system_certs.wrappers
    pip_system_certs.wrappers.wrap_requests()
except ImportError:
    pass
# --------------------------------------------

load_dotenv("config/secrets.env")

import pandas as pd
from datetime import datetime
from config import settings
from src.connectors.gsheets_client import GSheetsClient
from src.connectors.qbo_client import QBOClient
from src.logic.syncing import QBOSync
from src.utils.logger import setup_logger

logger = setup_logger("syncing_runner")

def _now_iso_local() -> str:
    now = datetime.now().astimezone()
    z = now.strftime("%z") 
    try:
        offset_hour = int(z[:3]) 
        gmt_str = f"GMT{offset_hour:+}" 
    except:
        gmt_str = "GMT"
    return now.strftime(f"%Y-%m-%d %H:%M:%S ({gmt_str})")

def _batch_update_control(gs, row_num, updates):
    """Updates the Control Sheet status."""
    ctrl_df = gs.read_as_df(settings.CONTROL_SHEET_ID, settings.CONTROL_TAB_NAME)
    headers = list(ctrl_df.columns)
    batch_data = []
    
    for col_name, val in updates.items():
        if col_name in headers:
            col_idx = headers.index(col_name) + 1
            batch_data.append({'row': row_num, 'col': col_idx, 'val': str(val)})
            
    if batch_data:
        gs.batch_update_cells(settings.CONTROL_SHEET_ID, settings.CONTROL_TAB_NAME, batch_data)

def format_tab_name(country, raw_date_str, suffix):
    try:
        dt = pd.to_datetime(raw_date_str)
        month_label = dt.strftime("%b %y")
        return f"{country} QBO - {month_label} - {suffix}" # Matches naming convention
    except:
        # Fallback if date parse fails, though ingestion usually standardizes it
        return f"{country} {raw_date_str} - {suffix}"

def main():
    gs = GSheetsClient()
    qbo_api = QBOClient()
    
    logger.info(f"Connecting to Control Sheet: {settings.CONTROL_TAB_NAME}")
    try:
        ctrl_df = gs.read_as_df(settings.CONTROL_SHEET_ID, settings.CONTROL_TAB_NAME)
    except Exception as e:
        logger.error(f"Failed to read Control Tab: {e}")
        return

    if ctrl_df.empty: return

    sync_engine = QBOSync(client=qbo_api)

    # Define the detailed status columns
    COL_QBO_JV = "QBO Journal"
    COL_QBO_EXP = "QBO Expense"
    COL_QBO_TR = "QBO Transfer"

    for i, row in ctrl_df.iterrows():
        # --- 1. CHECK TRIGGER CONDITION: 'Sync now' ---
        status = str(row.get(settings.CTRL_COL_QBO_SYNC, "")).strip()
        if status != "SYNC NOW": 
            continue

        row_num = i + 2
        country = row.get(settings.CTRL_COL_COUNTRY)
        
        # --- 2. VALIDATION GATE: Check for ERRORs in detailed columns ---
        jv_status_curr = str(row.get(COL_QBO_JV, "")).strip().upper()
        exp_status_curr = str(row.get(COL_QBO_EXP, "")).strip().upper()
        tr_status_curr = str(row.get(COL_QBO_TR, "")).strip().upper()

        if "ERROR" in [jv_status_curr, exp_status_curr, tr_status_curr]:
            logger.warning(f"🚫 BLOCKED {country}: One or more files have errors.")
            _batch_update_control(gs, row_num, {settings.CTRL_COL_QBO_SYNC: "BLOCKED (Fix Errors)"})
            continue

        # --- 3. PROCEED IF SAFE ---
        # Look for the TRANSFORM FILE, not the source file
        url = row.get(settings.CTRL_COL_TRANSFORM_URL)
        if not url:
            logger.error(f"❌ {country}: No Transform File URL found.")
            _batch_update_control(gs, row_num, {settings.CTRL_COL_QBO_SYNC: "ERROR: No Transform File"})
            continue

        month_str = row.get(settings.CTRL_COL_MONTH)
        
        logger.info(f"🚀 Starting Sync for {country} (Row {row_num})")
        _batch_update_control(gs, row_num, {settings.CTRL_COL_QBO_SYNC: "PROCESSING"})

        has_global_error = False
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sync_msg = f"Sync at {timestamp}"

        # Initialize status updates for this row
        row_updates = {}

        try:
            # ====================================================
            # 1. SYNC JOURNALS
            # ====================================================
            try:
                # Assuming format: "PH QBO - Oct 25 - Journals"
                # If ingestion creates "PH Oct 25 - Journals", adjust format_tab_name logic or use exact logic from ingestion
                # Here we replicate ingestion logic: f"{country} {month} - Journals"
                dt_label = pd.to_datetime(month_str).strftime("%b %y")
                tab_name = f"{country} {dt_label} - {settings.OUTPUT_TAB_JOURNALS}"
                
                logger.info(f"   📂 Reading Journals: '{tab_name}'")
                
                # Check if tab exists before reading to avoid crashing
                try:
                    df_journals = gs.read_as_df_sync(url, tab_name)
                except:
                    df_journals = pd.DataFrame()

                if not df_journals.empty and "Remarks" in df_journals.columns:
                    # Filter for rows that are NOT already synced and NOT errors
                    to_sync = df_journals[~df_journals["Remarks"].str.contains("Sync at|ERROR", regex=True, na=False)]
                    
                    if not to_sync.empty:
                        grouped = to_sync.groupby("Journal No")
                        rem_col_idx = list(df_journals.columns).index("Remarks") + 1
                        
                        for journal_no, group_df in grouped:
                            batch_updates = []
                            if abs(group_df['Amount'].sum()) > 0.01:
                                msg = f"ERROR: Unbalanced {group_df['Amount'].sum()}"
                                for idx, _ in group_df.iterrows():
                                    batch_updates.append({'row': idx + 2, 'col': rem_col_idx, 'val': msg})
                                gs.batch_update_cells(url, tab_name, batch_updates)
                                has_global_error = True # Mark partial error
                                continue

                            try:
                                logger.info(f"   📤 Pushing Journal: {journal_no}")
                                sync_engine.push_journal(journal_no, group_df)
                                for idx, _ in group_df.iterrows():
                                    batch_updates.append({'row': idx + 2, 'col': rem_col_idx, 'val': sync_msg})
                                    
                            except Exception as je:
                                error_msg = f"ERROR: {str(je)}"[:500]
                                logger.error(f"      ❌ Failed Journal {journal_no}: {error_msg}")
                                has_global_error = True
                                for idx, _ in group_df.iterrows():
                                    batch_updates.append({'row': idx + 2, 'col': rem_col_idx, 'val': error_msg})

                            if batch_updates:
                                gs.batch_update_cells(url, tab_name, batch_updates)
                    
                    # If we finished processing without catastrophic failure (even if some rows failed), 
                    # we usually mark "SYNCED" unless it was totally empty. 
                    # User requested: "When sheets synced => write back SYNCED".
                    # If has_global_error is True, we might want to keep it as ERROR or PARTIAL.
                    # But if specific rows failed, the main status is usually "DONE" or "PARTIAL".
                    # For the individual column, let's set SYNCED if at least attempting worked.
                    row_updates[COL_QBO_JV] = "SYNCED"

            except Exception as e:
                logger.error(f"   ❌ Critical Journal Error: {e}")
                has_global_error = True
                row_updates[COL_QBO_JV] = "ERROR"

            # ====================================================
            # 2. SYNC EXPENSES
            # ====================================================
            try:
                tab_name = f"{country} {dt_label} - {settings.OUTPUT_TAB_EXPENSES}"
                logger.info(f"   📂 Reading Expenses: '{tab_name}'")
                
                try: df_expenses = gs.read_as_df_sync(url, tab_name)
                except: df_expenses = pd.DataFrame()

                if not df_expenses.empty and "Remarks" in df_expenses.columns:
                    to_sync = df_expenses[~df_expenses["Remarks"].str.contains("Sync at|ERROR", regex=True, na=False)]
                    rem_col_idx = list(df_expenses.columns).index("Remarks") + 1

                    batch_updates = []
                    for idx, r in to_sync.iterrows():
                        exp_ref = str(r.get("Exp Ref. No", "")).strip()
                        if not exp_ref: continue
                        try:
                            sync_engine.push_expense(exp_ref, r)
                            batch_updates.append({'row': idx + 2, 'col': rem_col_idx, 'val': sync_msg})
                        except Exception as ee:
                            error_msg = f"ERROR: {str(ee)}"[:500]
                            logger.error(f"      ❌ Failed Expense {exp_ref}: {error_msg}")
                            batch_updates.append({'row': idx + 2, 'col': rem_col_idx, 'val': error_msg})
                            has_global_error = True
                    
                    if batch_updates:
                        gs.batch_update_cells(url, tab_name, batch_updates)
                    
                    row_updates[COL_QBO_EXP] = "SYNCED"

            except Exception as e:
                logger.error(f"   ❌ Critical Expense Error: {e}")
                has_global_error = True
                row_updates[COL_QBO_EXP] = "ERROR"

            # ====================================================
            # 3. SYNC TRANSFERS
            # ====================================================
            try:
                tab_name = f"{country} {dt_label} - {settings.OUTPUT_TAB_WITHDRAW}"
                try: df_transfers = gs.read_as_df_sync(url, tab_name)
                except: df_transfers = pd.DataFrame()

                if not df_transfers.empty and "Remarks" in df_transfers.columns:
                    to_sync = df_transfers[~df_transfers["Remarks"].str.contains("Sync at|ERROR", regex=True, na=False)]
                    rem_col_idx = list(df_transfers.columns).index("Remarks") + 1
                    
                    batch_updates = []
                    for idx, r in to_sync.iterrows():
                        ref_no = str(r.get("Ref No", "")).strip()
                        if not ref_no: continue
                        try:
                            sync_engine.push_transfer(r)
                            batch_updates.append({'row': idx + 2, 'col': rem_col_idx, 'val': sync_msg})
                        except Exception as te:
                            error_msg = f"ERROR: {str(te)}"[:500]
                            logger.error(f"      ❌ Failed Transfer {ref_no}: {error_msg}")
                            batch_updates.append({'row': idx + 2, 'col': rem_col_idx, 'val': error_msg})
                            has_global_error = True
                    
                    if batch_updates:
                        gs.batch_update_cells(url, tab_name, batch_updates)
                    
                    row_updates[COL_QBO_TR] = "SYNCED"

            except Exception as e:
                logger.error(f"   ❌ Critical Transfer Error: {e}")
                has_global_error = True
                row_updates[COL_QBO_TR] = "ERROR"

            # ====================================================
            # FINAL STATUS UPDATE
            # ====================================================
            final_status = "DONE"
            if has_global_error:
                final_status = "PARTIAL ERROR"
                logger.warning(f"⚠️ {country} finished with some row-level errors.")
                
            now_str =  _now_iso_local()

            # Add global status to the update list
            row_updates[settings.CTRL_COL_QBO_SYNC] = final_status
            row_updates["Last Sync At"] = now_str

            # Perform the Batch Update for this Control Sheet Row
            _batch_update_control(gs, row_num, row_updates)
            
            logger.info(f"✅ {country} Sync Cycle Complete. Control Sheet Updated.")

        except Exception as e:
            logger.error(f"❌ {country} Global Failure: {e}")
            _batch_update_control(gs, row_num, {settings.CTRL_COL_QBO_SYNC: "ERROR"})

if __name__ == "__main__":
    main()