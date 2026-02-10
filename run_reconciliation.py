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

        # 1. Reconcile Journals
        try:
            tab = f"{country} {dt_label} - {settings.OUTPUT_TAB_JOURNALS}"
            try: df = gs.read_as_df_sync(transform_url, tab)
            except: df = pd.DataFrame()

            if not df.empty:
                res = reconciler.reconcile_journals(df, month_str)
                write_reconcile_results(gs, transform_url, tab, df, res)
                
                if any("Mismatch" in r["status"] or "Missing" in r["status"] for r in res):
                    row_updates[COL_QBO_JV] = "QBO Mismatch"
                    has_issue = True
                else:
                    row_updates[COL_QBO_JV] = "Matched"
        except Exception as e:
            logger.error(f"   ❌ JV Reconcile Error: {e}")
            has_issue = True

        # 2. Reconcile Expenses
        try:
            tab = f"{country} {dt_label} - {settings.OUTPUT_TAB_EXPENSES}"
            try: df = gs.read_as_df_sync(transform_url, tab)
            except: df = pd.DataFrame()

            if not df.empty:
                res = reconciler.reconcile_expenses(df, month_str)
                write_reconcile_results(gs, transform_url, tab, df, res)
                
                if any("Mismatch" in r["status"] or "Missing" in r["status"] for r in res):
                    row_updates[COL_QBO_EXP] = "QBO Mismatch"
                    has_issue = True
                else:
                    row_updates[COL_QBO_EXP] = "Matched"
        except Exception as e:
            logger.error(f"   ❌ Exp Reconcile Error: {e}")
            has_issue = True

        # 3. Reconcile Transfers
        try:
            tab = f"{country} {dt_label} - {settings.OUTPUT_TAB_WITHDRAW}"
            try: df = gs.read_as_df_sync(transform_url, tab)
            except: df = pd.DataFrame()

            if not df.empty:
                res = reconciler.reconcile_transfers(df, month_str)
                write_reconcile_results(gs, transform_url, tab, df, res)
                
                if any("Mismatch" in r["status"] or "Missing" in r["status"] for r in res):
                    row_updates[COL_QBO_TR] = "QBO Mismatch"
                    has_issue = True
                else:
                    row_updates[COL_QBO_TR] = "Matched"
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