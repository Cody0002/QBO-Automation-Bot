from __future__ import annotations
try:
    import pip_system_certs.wrappers
    pip_system_certs.wrappers.wrap_requests()
except ImportError:
    pass

from dotenv import load_dotenv
load_dotenv("config/secrets.env")

import pandas as pd
from datetime import datetime
from config import settings
from src.connectors.gsheets_client import GSheetsClient
from src.connectors.qbo_client import QBOClient
from src.logic.reconciler import Reconciler
from src.utils.logger import setup_logger
from src.logic.raw_adapter import standardize_raw_df

logger = setup_logger("reconciliation_runner")

def _batch_update_control(gs, sheet_id, tab_name, row_num, columns, updates_dict):
    """Updates the Control Sheet."""
    headers = list(columns)
    batch_data = []
    for col_name, val in updates_dict.items():
        if col_name in headers:
            col_idx = headers.index(col_name) + 1
            batch_data.append({'row': row_num, 'col': col_idx, 'val': str(val)})
    if batch_data:
        gs.batch_update_cells(sheet_id, tab_name, batch_data)

def write_reconcile_results(gs, spreadsheet_url, tab_name, df, updates_list):
    """Writes status to the detailed Transform file."""
    if not updates_list: return
    target_col_name = "Reconcile Status"
    if target_col_name in df.columns:
        col_idx = df.columns.get_loc(target_col_name) + 1
    else:
        col_idx = len(df.columns) + 1
        gs.update_cell(spreadsheet_url, tab_name, 1, col_idx, target_col_name)
    
    batch_payload = []
    for item in updates_list:
        batch_payload.append({"row": item["row_idx"]+2, "col": col_idx, "val": item["status"]})
    gs.batch_update_cells(spreadsheet_url, tab_name, batch_payload)

def write_raw_check_results(gs, spreadsheet_url, tab_name, df, updates_list):
    """Writes 'Raw Status' to the Transform file."""
    if not updates_list: return
    target_col_name = "Raw Reconcile"
    
    # Check if column exists, if not add it
    if target_col_name in df.columns:
        col_idx = df.columns.get_loc(target_col_name) + 1
    else:
        col_idx = len(df.columns) + 1
        gs.update_cell(spreadsheet_url, tab_name, 1, col_idx, target_col_name)
    
    batch_payload = []
    for item in updates_list:
        # +2 offset for 0-index + header row
        batch_payload.append({"row": item["row_idx"]+2, "col": col_idx, "val": item["status"]})
    
    # print("Add Raw Reconcile")
    
    gs.batch_update_cells(spreadsheet_url, tab_name, batch_payload)
# ==============================================================================
# LOGIC: PROCESS ONE CLIENT
# ==============================================================================
def process_client_reconcile(gs: GSheetsClient, qbo_client: QBOClient, control_sheet_id: str, client_name: str):
    logger.info(f"📂 [{client_name}] Reading Control Sheet (ID: {control_sheet_id})...")
    
    try:
        ctrl_df = gs.read_as_df(control_sheet_id, settings.CONTROL_TAB_NAME)
    except Exception as e:
        logger.error(f"   ❌ [{client_name}] Failed to read Control Sheet: {e}")
        return
    
    if ctrl_df.empty: return

    # Initialize Reconciler
    reconciler = Reconciler(qbo_client)
    CTRL_COL_RECONCILE = "QBO Reconcile"
    COL_QBO_JV = "QBO Journal"
    COL_QBO_EXP = "QBO Expense"
    COL_QBO_TR = "QBO Transfer"

    raw_month = str(row.get(settings.CTRL_COL_MONTH, "")).strip()

    for i, row in ctrl_df.iterrows():
        status = str(row.get(CTRL_COL_RECONCILE, "")).strip()
        if status != "RECONCILE NOW": continue

        row_num = i + 2
        country = row.get(settings.CTRL_COL_COUNTRY)
        transform_url = row.get(settings.CTRL_COL_TRANSFORM_URL)
        month_str = str(row.get(settings.CTRL_COL_MONTH, ""))

        if not transform_url or not month_str or month_str.lower() == "nan":
            _batch_update_control(gs, control_sheet_id, settings.CONTROL_TAB_NAME, row_num, ctrl_df.columns, {CTRL_COL_RECONCILE: "ERROR: Missing Info"})
            continue

        logger.info(f"⚖️  [{client_name}] Reconciling {country}...")
        _batch_update_control(gs, control_sheet_id, settings.CONTROL_TAB_NAME, row_num, ctrl_df.columns, {CTRL_COL_RECONCILE: "RUNNING..."})

        row_updates = {}
        has_issue = False
        dt_label = pd.to_datetime(month_str).strftime("%b %y")
        
        # --- NEW: Fetch Raw Data for Comparison ---
        source_url = row.get(settings.CTRL_COL_SOURCE_URL)
        raw_tab_name = row.get(settings.CTRL_COL_TAB_NAME)
        
        raw_df = pd.DataFrame()
        
        try:
            if source_url and raw_tab_name:
                logger.info(f"   📥 [{client_name}] Fetching Raw Source for Validation...")
                # Read header_row=1 to match ingestion logic
                raw_df = gs.read_as_df(source_url, raw_tab_name, header_row=1, value_render_option='UNFORMATTED_VALUE')
                raw_df = standardize_raw_df(raw_df, client_name=client_name, raw_month=raw_month)
                # Apply Standard Columns
                raw_df = raw_df.iloc[:, :25]  # Keep first 25 columns

                raw_df.columns = [
                    "CO", "COY", "Date", "Category", "Type", "Item Description", 
                    "TrxHarsh", "Account Fr", "Account To", "Currency", "Amount Fr", 
                    "Currency To", "Amount To", "Budget", "USD - Raw", "USD - Actual", 
                    "USD - Loss", "USD - QBO", "Reclass", "QBO Method", 
                    "If Journal/Expense Method", "QBO Transfer Fr", "QBO Transfer To", 
                    "Check (Internal use)", "No"
                ]
                
                # 8. Numeric Cleanup
                for col in ["No", "USD - QBO", "Amount Fr", "Amount To"]:
                    if col in raw_df.columns:
                        raw_df[col] = pd.to_numeric(raw_df[col], errors="coerce").fillna(0)

        except Exception as e:
            logger.error(f"   ⚠️ Failed to read Raw Source: {e}")

        # 1. Reconcile Journals
        try:
            tab = f"{country} {dt_label} - {settings.OUTPUT_TAB_JOURNALS}"
            try: df = gs.read_as_df_sync(transform_url, tab)
            except: df = pd.DataFrame()

            if not df.empty:
                # A. QBO Reconcile (Existing)
                res_qbo = reconciler.reconcile_journals(df, month_str)
                write_reconcile_results(gs, transform_url, tab, df, res_qbo)
                
                # B. Raw vs Transform Reconcile (NEW)
                if not raw_df.empty:
                    # print("RUN")
                    res_raw = reconciler.reconcile_raw_vs_transform(raw_df, df, "JournalEntry")
                    write_raw_check_results(gs, transform_url, tab, df, res_raw)
                
                if any("Mismatch" in r["status"] or "Missing" in r["status"] for r in res_qbo):
                    row_updates[COL_QBO_JV] = "QBO MISMATCH"
                    has_issue = True
                else:
                    row_updates[COL_QBO_JV] = "SYNCED"
        except Exception as e:
            logger.error(f"   ❌ JV Reconcile Error: {e}")
            has_issue = True

        # 2. Reconcile Expenses
        try:
            tab = f"{country} {dt_label} - {settings.OUTPUT_TAB_EXPENSES}"
            try: df = gs.read_as_df_sync(transform_url, tab)
            except: df = pd.DataFrame()

            if not df.empty:
                # A. QBO Reconcile
                res_qbo = reconciler.reconcile_expenses(df, month_str)
                write_reconcile_results(gs, transform_url, tab, df, res_qbo)
                
                # B. Raw Check
                if not raw_df.empty:
                    res_raw = reconciler.reconcile_raw_vs_transform(raw_df, df, "Purchase")
                    write_raw_check_results(gs, transform_url, tab, df, res_raw)
                
                if any("Mismatch" in r["status"] or "Missing" in r["status"] for r in res_qbo):
                    row_updates[COL_QBO_EXP] = "QBO MISMATCH"
                    has_issue = True
                else:
                    row_updates[COL_QBO_EXP] = "SYNCED"
        except Exception as e:
            logger.error(f"   ❌ Exp Reconcile Error: {e}")
            has_issue = True

        # 3. Reconcile Transfers
        try:
            tab = f"{country} {dt_label} - {settings.OUTPUT_TAB_WITHDRAW}"
            try: df = gs.read_as_df_sync(transform_url, tab)
            except: df = pd.DataFrame()

            if not df.empty:
                # A. QBO Reconcile
                res_qbo = reconciler.reconcile_transfers(df, month_str)
                write_reconcile_results(gs, transform_url, tab, df, res_qbo)
                
                # B. Raw Check
                if not raw_df.empty:
                    res_raw = reconciler.reconcile_raw_vs_transform(raw_df, df, "Transfer")
                    write_raw_check_results(gs, transform_url, tab, df, res_raw)
                
                if any("Mismatch" in r["status"] or "Missing" in r["status"] for r in res_qbo):
                    row_updates[COL_QBO_TR] = "QBO MISMATCH"
                    has_issue = True
                else:
                    row_updates[COL_QBO_TR] = "SYNCED"
        except Exception as e:
            logger.error(f"   ❌ Trf Reconcile Error: {e}")
            has_issue = True

        final = "DONE (Issues Found)" if has_issue else "DONE"
        row_updates[CTRL_COL_RECONCILE] = final
        row_updates["Last Sync At"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        _batch_update_control(gs, control_sheet_id, settings.CONTROL_TAB_NAME, row_num, ctrl_df.columns, row_updates)
        logger.info(f"✅ [{client_name}] Reconcile Complete: {final}")


# ==============================================================================
# MAIN ENTRY POINT
# ==============================================================================
def main():
    gs = GSheetsClient()
    qbo_client = QBOClient(gs_client=gs)

    logger.info("🌍 Reading MASTER SHEET for Reconcile Jobs...")
    try:
        master_df = gs.read_as_df(settings.MASTER_SHEET_ID, settings.MASTER_TAB_NAME)
    except Exception as e:
        logger.error(f"❌ Critical: {e}")
        return

    for i, client_row in master_df.iterrows():
        client_name = str(client_row.get(settings.MST_COL_CLIENT, "Unknown"))
        if str(client_row.get(settings.MST_COL_STATUS, "")).strip().lower() != "active": continue

        sheet_id = str(client_row.get(settings.MST_COL_SHEET_ID, "")).strip()
        realm_id = str(client_row.get(settings.MST_COL_REALM_ID, "")).strip()

        if not sheet_id or not realm_id: continue

        print(f"\n🏢 RECONCILING CLIENT: {client_name} (Realm: {realm_id})")
        
        try:
            # 1. Switch Auth
            qbo_client.set_company(realm_id)
            # 2. Process
            process_client_reconcile(gs, qbo_client, sheet_id, client_name)
        except Exception as e:
            logger.error(f"❌ Failed client {client_name}: {e}")

if __name__ == "__main__":
    main()