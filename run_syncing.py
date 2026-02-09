from __future__ import annotations
# --- FIX 1: Load Secrets ---
from dotenv import load_dotenv
load_dotenv("config/secrets.env")
# ---------------------------

try:
    import pip_system_certs.wrappers
    pip_system_certs.wrappers.wrap_requests()
except ImportError:
    pass

import pandas as pd
from datetime import datetime
from config import settings
from src.connectors.gsheets_client import GSheetsClient
from src.connectors.qbo_client import QBOClient
from src.logic.syncing import QBOSync
from src.utils.logger import setup_logger

logger = setup_logger("syncing_runner")

def _batch_update_control(gs, sheet_id, tab_name, row_num, columns, updates_dict):
    headers = list(columns)
    batch_data = []
    for col_name, val in updates_dict.items():
        if col_name in headers:
            col_idx = headers.index(col_name) + 1
            batch_data.append({'row': row_num, 'col': col_idx, 'val': str(val)})
    if batch_data:
        gs.batch_update_cells(sheet_id, tab_name, batch_data)

def _update_row_status_and_id(gs, spreadsheet_url, tab_name, updates: list):
    """
    Updates 'Remarks' AND 'QBO ID' columns.
    updates = [{'row_idx': 0, 'status': 'Synced', 'qbo_id': '123'}]
    """
    if not updates: return
    try:
        df_header = gs.read_as_df(spreadsheet_url, tab_name)
        headers = df_header.columns.tolist()

        # 1. Find or Define 'Remarks' Column
        if "Remarks" in headers:
            col_rem = headers.index("Remarks") + 1
        else:
            col_rem = len(headers) + 1
            # Ideally we would write the header here, but batch update handles value placement
        
        # 2. Find or Define 'QBO ID' Column
        # We append it at the end if it doesn't exist
        if "QBO ID" in headers:
            col_id = headers.index("QBO ID") + 1
        else:
            # If QBO ID doesn't exist, we put it after Remarks or at the very end
            col_id = col_rem + 1 if "Remarks" not in headers else len(headers) + 1
            # Optional: Write header if missing (gs.update_cell(..., 1, col_id, "QBO ID"))

        batch_payload = []
        for item in updates:
            # Update Status (Remarks)
            batch_payload.append({"row": item["row_idx"] + 2, "col": col_rem, "val": item["status"]})
            
            # Update QBO ID (Only if we have one)
            if item.get("qbo_id"):
                batch_payload.append({"row": item["row_idx"] + 2, "col": col_id, "val": str(item["qbo_id"])})
        
        gs.batch_update_cells(spreadsheet_url, tab_name, batch_payload)
    except Exception as e:
        logger.error(f"Failed to update status in sheet: {e}")

def process_client_sync(gs: GSheetsClient, qbo_client: QBOClient, control_sheet_id: str, client_name: str):
    logger.info(f"📂 [{client_name}] Processing Control Sheet...")
    try:
        ctrl_df = gs.read_as_df(control_sheet_id, settings.CONTROL_TAB_NAME)
    except Exception as e:
        logger.error(f"   ❌ [{client_name}] Failed to read Control Sheet: {e}")
        return

    if ctrl_df.empty: return

    sync_engine = QBOSync(client=qbo_client)
    
    COL_QBO_JV = "QBO Journal"
    COL_QBO_EXP = "QBO Expense"
    COL_QBO_TR = "QBO Transfer"

    for i, row in ctrl_df.iterrows():
        if str(row.get(settings.CTRL_COL_QBO_SYNC, "")).strip() != "SYNC NOW": continue
        
        row_num = i + 2
        country = str(row.get(settings.CTRL_COL_COUNTRY, "")).strip()
        transform_url = str(row.get(settings.CTRL_COL_TRANSFORM_URL, "")).strip()
        month_str = str(row.get(settings.CTRL_COL_MONTH, "")).strip()
        
        if not transform_url or not month_str: continue

        _batch_update_control(gs, control_sheet_id, settings.CONTROL_TAB_NAME, row_num, ctrl_df.columns, {settings.CTRL_COL_QBO_SYNC: "PROCESSING"})
        
        has_error = False
        jv_status, exp_status, tr_status = "Skipped", "Skipped", "Skipped"

        dt_label = pd.to_datetime(month_str).strftime("%b %y")
        tab_jv = f"{country} {dt_label} - Journals"
        tab_exp = f"{country} {dt_label} - Expenses"
        tab_tr = f"{country} {dt_label} - Transfers"

        # ---------------------------------------------------------
        # 1. SYNC JOURNALS
        # ---------------------------------------------------------
        try:
            logger.info(f"   Using Tab: {tab_jv}")
            try: df_jv = gs.read_as_df_sync(transform_url, tab_jv)
            except: df_jv = pd.DataFrame()

            if not df_jv.empty and "Remarks" in df_jv.columns:
                to_sync = df_jv[df_jv["Remarks"].astype(str).str.contains("Ready to sync", case=False, na=False)]
                
                if to_sync.empty:
                    jv_status = "SYNCED"
                else:
                    all_jv_nos = to_sync["Journal No"].unique().tolist()
                    existing_docs = sync_engine.get_existing_duplicates("JournalEntry", all_jv_nos)
                    
                    updates = []
                    section_fail = False
                    
                    for jv_no, group in to_sync.groupby("Journal No"):
                        if str(jv_no) in existing_docs:
                             for idx in group.index:
                                updates.append({"row_idx": idx, "status": "Skipped (Already in QBO)", "qbo_id": ""})
                             continue

                        try:
                            resp = sync_engine.push_journal(jv_no, group)
                            new_id = resp.get("JournalEntry", {}).get("Id", "")
                            msg = f"Synced" # Cleaner message, ID goes to own column
                        except Exception as e:
                            msg = f"ERROR: {str(e)}"
                            new_id = ""
                            has_error = True
                            section_fail = True
                        
                        for idx in group.index:
                            updates.append({"row_idx": idx, "status": msg, "qbo_id": new_id})
                    
                    _update_row_status_and_id(gs, transform_url, tab_jv, updates)
                    jv_status = "SYNC FAIL" if section_fail else "SYNCED"
        except Exception as e:
            logger.error(f"   ❌ Journal Sync Fail: {e}")
            has_error = True
            jv_status = "SYNC FAIL"

        # ---------------------------------------------------------
        # 2. SYNC EXPENSES
        # ---------------------------------------------------------
        try:
            logger.info(f"   Using Tab: {tab_exp}")
            try: df_exp = gs.read_as_df_sync(transform_url, tab_exp)
            except: df_exp = pd.DataFrame()

            if not df_exp.empty and "Remarks" in df_exp.columns:
                to_sync = df_exp[df_exp["Remarks"].astype(str).str.contains("Ready to sync", case=False, na=False)]
                
                if to_sync.empty:
                    exp_status = "SYNCED"
                else:
                    all_exp_nos = to_sync["Exp Ref. No"].unique().tolist()
                    existing_docs = sync_engine.get_existing_duplicates("Purchase", all_exp_nos)

                    updates = []
                    section_fail = False

                    for idx, row_data in to_sync.iterrows():
                        ref_no = str(row_data.get("Exp Ref. No", ""))
                        if ref_no in existing_docs:
                            updates.append({"row_idx": idx, "status": "Skipped (Already in QBO)", "qbo_id": ""})
                            continue

                        try:
                            resp = sync_engine.push_expense(ref_no, row_data)
                            new_id = resp.get("Purchase", {}).get("Id", "")
                            updates.append({"row_idx": idx, "status": "Synced", "qbo_id": new_id})
                        except Exception as e:
                            updates.append({"row_idx": idx, "status": f"ERROR: {str(e)}", "qbo_id": ""})
                            has_error = True
                            section_fail = True
                    
                    _update_row_status_and_id(gs, transform_url, tab_exp, updates)
                    exp_status = "SYNC FAIL" if section_fail else "SYNCED"
        except Exception as e:
            logger.error(f"   ❌ Expense Sync Fail: {e}")
            has_error = True
            exp_status = "SYNC FAIL"

        # ---------------------------------------------------------
        # 3. SYNC TRANSFERS
        # ---------------------------------------------------------
        try:
            logger.info(f"   Using Tab: {tab_tr}")
            try: df_tr = gs.read_as_df_sync(transform_url, tab_tr)
            except: df_tr = pd.DataFrame()

            if not df_tr.empty and "Remarks" in df_tr.columns:
                to_sync = df_tr[df_tr["Remarks"].astype(str).str.contains("Ready to sync", case=False, na=False)]
                
                if to_sync.empty:
                    tr_status = "SYNCED"
                else:
                    all_tr_nos = to_sync["Ref No"].unique().tolist()
                    existing_docs = sync_engine.get_existing_duplicates("Transfer", all_tr_nos)

                    updates = []
                    section_fail = False

                    for idx, row_data in to_sync.iterrows():
                        ref_no = str(row_data.get("Ref No", ""))
                        if ref_no in existing_docs:
                            updates.append({"row_idx": idx, "status": "Skipped (Already in QBO)", "qbo_id": ""})
                            continue

                        try:
                            resp = sync_engine.push_transfer(row_data)
                            new_id = resp.get("Transfer", {}).get("Id", "")
                            updates.append({"row_idx": idx, "status": "Synced", "qbo_id": new_id})
                        except Exception as e:
                            updates.append({"row_idx": idx, "status": f"ERROR: {str(e)}", "qbo_id": ""})
                            has_error = True
                            section_fail = True
                    
                    _update_row_status_and_id(gs, transform_url, tab_tr, updates)
                    tr_status = "SYNC FAIL" if section_fail else "SYNCED"
        except Exception as e:
            logger.error(f"   ❌ Transfer Sync Fail: {e}")
            has_error = True
            tr_status = "SYNC FAIL"

        final_status = "PARTIAL ERROR" if has_error else "DONE"
        
        update_payload = {
            settings.CTRL_COL_QBO_SYNC: final_status,
            "Last Sync At": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        if jv_status != "Skipped": update_payload[COL_QBO_JV] = jv_status
        if exp_status != "Skipped": update_payload[COL_QBO_EXP] = exp_status
        if tr_status != "Skipped": update_payload[COL_QBO_TR] = tr_status

        _batch_update_control(gs, control_sheet_id, settings.CONTROL_TAB_NAME, row_num, ctrl_df.columns, update_payload)
        logger.info(f"✅ [{client_name}] Sync Complete: {final_status}")

def main():
    gs = GSheetsClient()
    qbo_client = QBOClient(gs_client=gs)
    try:
        master_df = gs.read_as_df(settings.MASTER_SHEET_ID, settings.MASTER_TAB_NAME)
    except Exception as e:
        logger.error(f"❌ Critical: {e}")
        return

    for _, row in master_df.iterrows():
        if str(row.get(settings.MST_COL_STATUS, "")).strip().lower() != "active": continue
        client_name = row.get(settings.MST_COL_CLIENT)
        sheet_id = row.get(settings.MST_COL_SHEET_ID)
        realm_id = str(row.get(settings.MST_COL_REALM_ID)).strip()
        if not sheet_id or not realm_id: continue

        logger.info(f"🏢 STARTING SYNC FOR: {client_name} ({realm_id})")
        try:
            qbo_client.set_company(realm_id)
            process_client_sync(gs, qbo_client, sheet_id, client_name)
        except Exception as e:
            logger.error(f"❌ Auth/Sync failed for {client_name}: {e}")

if __name__ == "__main__":
    main()