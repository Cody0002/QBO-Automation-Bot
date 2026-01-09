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

import pandas as pd
from datetime import datetime
from config import settings
from src.connectors.gsheets_client import GSheetsClient
from src.connectors.qbo_client import QBOClient
from src.logic.reconciler import Reconciler
from src.utils.logger import setup_logger

logger = setup_logger("reconciliation_runner")

def _batch_update_control(gs, row_num, updates):
    """Updates the Control Sheet with a dictionary of {ColName: Value}."""
    ctrl_df = gs.read_as_df(settings.CONTROL_SHEET_ID, settings.CONTROL_TAB_NAME)
    headers = list(ctrl_df.columns)
    batch_data = []
    
    for col_name, val in updates.items():
        if col_name in headers:
            col_idx = headers.index(col_name) + 1
            batch_data.append({'row': row_num, 'col': col_idx, 'val': str(val)})
            
    if batch_data:
        gs.batch_update_cells(settings.CONTROL_SHEET_ID, settings.CONTROL_TAB_NAME, batch_data)

def write_reconcile_results(gs, spreadsheet_url, tab_name, df, updates_list):
    """Writes the 'Reconcile Status' column to the detailed Transform sheet."""
    if not updates_list: return
    
    # 1. Determine Column Index for "Reconcile Status"
    target_col_name = "Reconcile Status"
    
    if target_col_name in df.columns:
        col_idx = df.columns.get_loc(target_col_name) + 1
    else:
        col_idx = len(df.columns) + 1
        gs.update_cell(spreadsheet_url, tab_name, 1, col_idx, target_col_name)
    
    # 2. Prepare Batch Update
    batch_payload = []
    for item in updates_list:
        sheet_row = item["row_idx"] + 2
        batch_payload.append({
            "row": sheet_row,
            "col": col_idx,
            "val": item["status"]
        })
        
    logger.info(f"   💾 Writing {len(batch_payload)} reconciliation updates to '{tab_name}'...")
    gs.batch_update_cells(spreadsheet_url, tab_name, batch_payload)

def main():
    gs = GSheetsClient()
    qbo_api = QBOClient()
    reconciler = Reconciler(qbo_api)
    
    logger.info("Reading Control Sheet...")
    try:
        ctrl_df = gs.read_as_df(settings.CONTROL_SHEET_ID, settings.CONTROL_TAB_NAME)
    except Exception as e:
        logger.error(f"Failed to read Control Tab: {e}")
        return

    if ctrl_df.empty: return

    # --- Column Definitions ---
    CTRL_COL_RECONCILE = "QBO Reconcile" 
    COL_QBO_JV = "QBO Journal"
    COL_QBO_EXP = "QBO Expense"
    COL_QBO_TR = "QBO Transfer"
    # COL_MONTH from Control Sheet (e.g., "2025-10-10")

    for i, row in ctrl_df.iterrows():
        status = str(row.get(CTRL_COL_RECONCILE, "")).strip()
        
        if status != "RECONCILE NOW": continue

        row_num = i + 2
        country = row.get(settings.CTRL_COL_COUNTRY)
        transform_url = row.get(settings.CTRL_COL_TRANSFORM_URL)
        month_str = str(row.get(settings.CTRL_COL_MONTH, ""))
        
        if not transform_url:
            logger.error(f"❌ {country}: No Transform File found to reconcile.")
            _batch_update_control(gs, row_num, {CTRL_COL_RECONCILE: "ERROR: No File"})
            continue

        if not month_str or month_str.lower() == "nan":
            logger.error(f"❌ {country}: No Month specified for reconciliation.")
            _batch_update_control(gs, row_num, {CTRL_COL_RECONCILE: "ERROR: No Month"})
            continue

        logger.info(f"⚖️  Starting Reconciliation for {country} (Month: {month_str})...")
        
        # Set status to RUNNING
        _batch_update_control(gs, row_num, {CTRL_COL_RECONCILE: "RUNNING..."})

        # Dictionary to hold all updates for this row
        row_updates = {}
        has_global_issues = False

        try:
            dt_label = pd.to_datetime(month_str).strftime("%b %y")

            # ====================================================
            # 1. RECONCILE JOURNALS
            # ====================================================
            tab_jv = f"{country} {dt_label} - {settings.OUTPUT_TAB_JOURNALS}"
            try:
                df_jv = gs.read_as_df_sync(transform_url, tab_jv)
                if not df_jv.empty:
                    # Pass month_str to Reconciler
                    res_jv = reconciler.reconcile_journals(df_jv, month_str)
                    write_reconcile_results(gs, transform_url, tab_jv, df_jv, res_jv)
                    
                    if any("Mismatch" in r["status"] or "Missing" in r["status"] for r in res_jv):
                        row_updates[COL_QBO_JV] = "QBO Mismatch"
                        has_global_issues = True
                    else:
                        row_updates[COL_QBO_JV] = "Matched"
                else:
                    pass 
            except Exception as e:
                logger.error(f"   ❌ Failed Journals: {e}")
                row_updates[COL_QBO_JV] = "ERROR"
                has_global_issues = True

            # ====================================================
            # 2. RECONCILE EXPENSES
            # ====================================================
            tab_exp = f"{country} {dt_label} - {settings.OUTPUT_TAB_EXPENSES}"
            try:
                df_exp = gs.read_as_df_sync(transform_url, tab_exp)
                if not df_exp.empty:
                    # Pass month_str to Reconciler
                    res_exp = reconciler.reconcile_expenses(df_exp, month_str)
                    write_reconcile_results(gs, transform_url, tab_exp, df_exp, res_exp)
                    
                    if any("Mismatch" in r["status"] or "Missing" in r["status"] for r in res_exp):
                        row_updates[COL_QBO_EXP] = "QBO Mismatch"
                        has_global_issues = True
                    else:
                        row_updates[COL_QBO_EXP] = "Matched"
            except Exception as e:
                logger.error(f"   ❌ Failed Expenses: {e}")
                row_updates[COL_QBO_EXP] = "ERROR"
                has_global_issues = True

            # ====================================================
            # 3. RECONCILE TRANSFERS
            # ====================================================
            tab_tr = f"{country} {dt_label} - {settings.OUTPUT_TAB_WITHDRAW}"
            try:
                df_tr = gs.read_as_df_sync(transform_url, tab_tr)
                if not df_tr.empty:
                    # Pass month_str to Reconciler
                    res_tr = reconciler.reconcile_transfers(df_tr, month_str)
                    write_reconcile_results(gs, transform_url, tab_tr, df_tr, res_tr)
                    
                    if any("Mismatch" in r["status"] or "Missing" in r["status"] for r in res_tr):
                        row_updates[COL_QBO_TR] = "QBO Mismatch"
                        has_global_issues = True
                    else:
                        row_updates[COL_QBO_TR] = "Matched"
            except Exception as e:
                logger.error(f"   ❌ Failed Transfers: {e}")
                row_updates[COL_QBO_TR] = "ERROR"
                has_global_issues = True

            # ====================================================
            # FINAL STATUS UPDATE
            # ====================================================
            final_status = "DONE (Issues Found)" if has_global_issues else "DONE (Clean)"
            
            row_updates[CTRL_COL_RECONCILE] = final_status
            row_updates["Last Sync At"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            _batch_update_control(gs, row_num, row_updates)
            
            logger.info(f"✅ Reconciliation Complete: {final_status}")

        except Exception as e:
            logger.error(f"❌ Global Reconciliation Error: {e}")
            _batch_update_control(gs, row_num, {CTRL_COL_RECONCILE: "ERROR"})

if __name__ == "__main__":
    main()