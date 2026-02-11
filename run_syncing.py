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
    Updates:
    - Remarks
    - QBO ID (raw number)
    - QBO Link (raw URL)
    """
    if not updates:
        return

    try:
        df_header = gs.read_as_df(spreadsheet_url, tab_name)
        headers = df_header.columns.tolist()

        col_rem = headers.index("Remarks") + 1 if "Remarks" in headers else len(headers) + 1
        col_id  = headers.index("QBO ID") + 1 if "QBO ID" in headers else len(headers) + 1
        col_link = headers.index("QBO Link") + 1 if "QBO Link" in headers else len(headers) + 1

        batch_payload = []

        for item in updates:
            row_no = item["row_idx"] + 2

            # Remarks
            batch_payload.append({
                "row": row_no,
                "col": col_rem,
                "val": item["status"]
            })

            # QBO ID
            if item.get("qbo_id"):
                batch_payload.append({
                    "row": row_no,
                    "col": col_id,
                    "val": str(item["qbo_id"])
                })

            # QBO Link (RAW URL)
            if item.get("qbo_link"):
                batch_payload.append({
                    "row": row_no,
                    "col": col_link,
                    "val": item["qbo_link"]
                })

        gs.batch_update_cells(spreadsheet_url, tab_name, batch_payload)

    except Exception as e:
        logger.error(f"Failed to update status in sheet: {e}")

def process_client_sync(gs: GSheetsClient, qbo_client: QBOClient, control_sheet_id: str, client_name: str):
    BATCH_SIZE = 50  # Update the Google Sheet every 5 rows
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

                            qbo_link = sync_engine.build_qbo_url("JournalEntry", new_id) if new_id else ""

                            msg = f"Synced at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

                            for idx in group.index:
                                updates.append({
                                    "row_idx": idx,
                                    "status": msg,
                                    "qbo_id": new_id,
                                    "qbo_link": qbo_link
                                })
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
        # 2. SYNC EXPENSES (UPDATED WITH BATCHING)
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
                    total_rows = len(to_sync)

                    # Use enumerate to track progress count
                    for i, (idx, row_data) in enumerate(to_sync.iterrows()):
                        # LOG PROGRESS to Console
                        logger.info(f"   [Expense {i+1}/{total_rows}] Processing Ref: {row_data.get('Exp Ref. No')}...")

                        ref_no = str(row_data.get("Exp Ref. No", ""))
                        
                        # --- Logic: Check Duplicates ---
                        if ref_no in existing_docs:
                            updates.append({"row_idx": idx, "status": "Skipped (Already in QBO)", "qbo_id": "", "qbo_link": ""})
                        
                        # --- Logic: Push to QBO ---
                        else:
                            try:
                                resp = sync_engine.push_expense(ref_no, row_data)
                                new_id = resp.get("Purchase", {}).get("Id", "")
                                qbo_link = sync_engine.build_qbo_url("Purchase", new_id) if new_id else ""
                                msg = f"Synced at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

                                updates.append({
                                    "row_idx": idx,
                                    "status": msg,
                                    "qbo_id": new_id,
                                    "qbo_link": qbo_link
                                })
                            except Exception as e:
                                error_msg = f"ERROR: {str(e)}"
                                logger.error(f"      -> Failed: {error_msg}")
                                updates.append({"row_idx": idx, "status": error_msg, "qbo_id": "", "qbo_link": ""})
                                has_error = True
                                section_fail = True
                        
                        # --- NEW: BATCH UPDATE ---
                        # If we hit the batch size, write to Sheet immediately and clear memory
                        if len(updates) >= BATCH_SIZE:
                            logger.info(f"   >>> Flushing {len(updates)} updates to Sheet...")
                            _update_row_status_and_id(gs, transform_url, tab_exp, updates)
                            updates = [] # Clear the list for the next batch

                    # Flush any remaining updates after the loop finishes
                    if updates:
                        _update_row_status_and_id(gs, transform_url, tab_exp, updates)

                    exp_status = "SYNC FAIL" if section_fail else "SYNCED"
        except Exception as e:
            logger.error(f"   ❌ Expense Sync Fail: {e}")
            has_error = True
            exp_status = "SYNC FAIL"

        # ---------------------------------------------------------
        # 3. SYNC TRANSFERS (UPDATED WITH BATCHING)
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
                    total_rows = len(to_sync)

                    for i, (idx, row_data) in enumerate(to_sync.iterrows()):
                        logger.info(f"   [Transfer {i+1}/{total_rows}] Processing Ref: {row_data.get('Ref No')}...")

                        ref_no = str(row_data.get("Ref No", ""))

                        if ref_no in existing_docs:
                            updates.append({"row_idx": idx, "status": "Skipped (Already in QBO)", "qbo_id": "", "qbo_link": ""})
                        else:
                            try:
                                resp = sync_engine.push_transfer(row_data)
                                new_id = resp.get("Transfer", {}).get("Id", "")
                                qbo_link = sync_engine.build_qbo_url("Transfer", new_id) if new_id else ""
                                msg = f"Synced at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

                                updates.append({
                                    "row_idx": idx,
                                    "status": msg,
                                    "qbo_id": new_id,
                                    "qbo_link": qbo_link
                                })
                            except Exception as e:
                                error_msg = f"ERROR: {str(e)}"
                                logger.error(f"      -> Failed: {error_msg}")
                                updates.append({"row_idx": idx, "status": error_msg, "qbo_id": "", "qbo_link": ""})
                                has_error = True
                                section_fail = True

                        # --- NEW: BATCH UPDATE ---
                        if len(updates) >= BATCH_SIZE:
                            logger.info(f"   >>> Flushing {len(updates)} updates to Sheet...")
                            _update_row_status_and_id(gs, transform_url, tab_tr, updates)
                            updates = [] 

                    if updates:
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