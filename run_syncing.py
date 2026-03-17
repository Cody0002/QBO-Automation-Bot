from __future__ import annotations
import argparse
import os
import time
from contextlib import nullcontext
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
from src.utils.run_lock import single_instance_lock

logger = setup_logger("syncing_runner")

def _env_int(name: str, default: int) -> int:
    try:
        return max(1, int(os.getenv(name, str(default))))
    except Exception:
        return default

def _env_float(name: str, default: float) -> float:
    try:
        return max(0.0, float(os.getenv(name, str(default))))
    except Exception:
        return default

# Sync pacing controls:
# - patch size for status updates to sheet
# - delay after each QBO call
# - delay after each sheet patch flush
SYNC_PATCH_SIZE = _env_int("QBO_SYNC_PATCH_SIZE", 10)
QBO_SYNC_CALL_DELAY_SEC = _env_float("QBO_SYNC_CALL_DELAY_SEC", 0.35)
QBO_SYNC_PATCH_DELAY_SEC = _env_float("QBO_SYNC_PATCH_DELAY_SEC", 0.8)

def _throttle_qbo_call():
    if QBO_SYNC_CALL_DELAY_SEC > 0:
        time.sleep(QBO_SYNC_CALL_DELAY_SEC)

def _flush_updates(gs, spreadsheet_url, tab_name, updates: list):
    if not updates:
        return []
    logger.info(f"   >>> Flushing {len(updates)} updates to Sheet...")
    _update_row_status_and_id(gs, spreadsheet_url, tab_name, updates)
    if QBO_SYNC_PATCH_DELAY_SEC > 0:
        time.sleep(QBO_SYNC_PATCH_DELAY_SEC)
    return []

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

def process_client_sync(
    gs: GSheetsClient,
    qbo_client: QBOClient,
    control_sheet_id: str,
    client_name: str,
    realm_id: str,
):
    BATCH_SIZE = SYNC_PATCH_SIZE
    logger.info(f"📂 [{client_name}] Processing Control Sheet...")
    try:
        ctrl_df = gs.read_as_df(control_sheet_id, settings.CONTROL_TAB_NAME)
    except Exception as e:
        logger.error(f"   ❌ [{client_name}] Failed to read Control Sheet: {e}")
        return

    if ctrl_df.empty:
        return

    status_series = ctrl_df.get(settings.CTRL_COL_QBO_SYNC, pd.Series("", index=ctrl_df.index))
    sync_now_count = int(status_series.astype(str).str.strip().eq("SYNC NOW").sum())
    if sync_now_count == 0:
        logger.info(f"   ⏭️ [{client_name}] No SYNC NOW rows. Skipping QBO auth/mappings.")
        return

    try:
        logger.info(f"🔐 [{client_name}] Authenticating with Realm ID: {realm_id}")
        qbo_client.set_company(realm_id)
        logger.info(f"✅ [{client_name}] Authenticated. Ready to sync.")
    except Exception as e:
        logger.error(f"❌ Auth/Sync failed for {client_name}: {e}")
        return

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
                            already_synced_msg = f"Skipper (Already synced in QBO at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})"
                            for idx in group.index:
                                updates.append({"row_idx": idx, "status": already_synced_msg, "qbo_id": "", "qbo_link": ""})
                            if len(updates) >= BATCH_SIZE:
                                updates = _flush_updates(gs, transform_url, tab_jv, updates)
                            continue

                        try:
                            resp = sync_engine.push_journal(jv_no, group)
                            _throttle_qbo_call()
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
                            has_error = True
                            section_fail = True
                            _throttle_qbo_call()
                            for idx in group.index:
                                updates.append({"row_idx": idx, "status": msg, "qbo_id": "", "qbo_link": ""})

                        if len(updates) >= BATCH_SIZE:
                            updates = _flush_updates(gs, transform_url, tab_jv, updates)

                    if updates:
                        updates = _flush_updates(gs, transform_url, tab_jv, updates)
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
                            already_synced_msg = f"Skipper (Already synced in QBO at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})"
                            updates.append({"row_idx": idx, "status": already_synced_msg, "qbo_id": "", "qbo_link": ""})
                        
                        # --- Logic: Push to QBO ---
                        else:
                            try:
                                resp = sync_engine.push_expense(ref_no, row_data)
                                _throttle_qbo_call()
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
                                _throttle_qbo_call()
                        
                        # --- NEW: BATCH UPDATE ---
                        # If we hit the batch size, write to Sheet immediately and clear memory
                        if len(updates) >= BATCH_SIZE:
                            updates = _flush_updates(gs, transform_url, tab_exp, updates)

                    # Flush any remaining updates after the loop finishes
                    if updates:
                        updates = _flush_updates(gs, transform_url, tab_exp, updates)

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
                            already_synced_msg = f"Skipper (Already synced in QBO at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})"
                            updates.append({"row_idx": idx, "status": already_synced_msg, "qbo_id": "", "qbo_link": ""})
                        else:
                            try:
                                resp = sync_engine.push_transfer(row_data)
                                _throttle_qbo_call()
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
                                _throttle_qbo_call()

                        # --- NEW: BATCH UPDATE ---
                        if len(updates) >= BATCH_SIZE:
                            updates = _flush_updates(gs, transform_url, tab_tr, updates)

                    if updates:
                        updates = _flush_updates(gs, transform_url, tab_tr, updates)

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

def _is_target_client(row: pd.Series, target_client: str | None) -> bool:
    if not target_client:
        return True

    target = str(target_client).strip()
    if not target:
        return True
    target_norm = settings.normalize_workspace_name(target)
    if target_norm in {"all", "*", "all clients"}:
        return True

    row_client = str(row.get(settings.MST_COL_CLIENT, "")).strip()
    row_realm = str(row.get(settings.MST_COL_REALM_ID, "")).strip()
    row_sheet_id = str(row.get(settings.MST_COL_SHEET_ID, "")).strip()

    # Allow targeting by realm ID or by normalized client name.
    if target == row_realm:
        return True
    if target == row_sheet_id:
        return True
    return target_norm == settings.normalize_workspace_name(row_client)

def _target_is_all(target_client: str | None) -> bool:
    if not target_client:
        return True
    t = settings.normalize_workspace_name(target_client)
    return t in {"", "all", "*", "all clients"}

def main(target_client: str | None = None):
    target_is_all = _target_is_all(target_client)
    dispatch_ctx = single_instance_lock("run_syncing_all_dispatch") if target_is_all else nullcontext(True)
    with dispatch_ctx as acquired:
        if target_is_all and not acquired:
            logger.warning("Another ALL syncing dispatch is already in progress. Skipping this run.")
            return

        gs = GSheetsClient()
        qbo_client = QBOClient(gs_client=gs)
        try:
            master_df = gs.read_as_df(settings.MASTER_SHEET_ID, settings.MASTER_TAB_NAME)
        except Exception as e:
            logger.error(f"❌ Critical: {e}")
            return

        # Normalize headers to avoid silent misses from extra spaces/newlines in sheet columns.
        master_df.columns = [" ".join(str(c).replace("\n", " ").split()) for c in master_df.columns]

        matched_clients = 0
        for _, row in master_df.iterrows():
            if str(row.get(settings.MST_COL_STATUS, "")).strip().lower() != "active": continue
            if not _is_target_client(row, target_client):
                continue
            matched_clients += 1
            client_name = row.get(settings.MST_COL_CLIENT)
            if not settings.is_allowed_workspace(client_name):
                logger.warning(
                    f"⚠️ Skipping {client_name}: workspace not allowed for QBO API. "
                    f"Allowed: {', '.join(settings.ALLOWED_QBO_WORKSPACES)}"
                )
                continue
            sheet_id = row.get(settings.MST_COL_SHEET_ID)
            realm_id = str(row.get(settings.MST_COL_REALM_ID)).strip()
            if not sheet_id or not realm_id: continue

            logger.info(f"🏢 STARTING SYNC FOR: {client_name} ({realm_id})")
            client_lock_name = f"run_syncing_client_{realm_id}"
            with single_instance_lock(client_lock_name) as client_acquired:
                if not client_acquired:
                    logger.warning(
                        f"⏭️ Skipping {client_name}: another syncing run is already processing Realm {realm_id}."
                    )
                    continue
                try:
                    process_client_sync(gs, qbo_client, sheet_id, client_name, realm_id)
                except Exception as e:
                    logger.error(f"❌ Sync failed for {client_name}: {e}")

        if target_client and matched_clients == 0:
            logger.warning(f"No client matched target '{target_client}'.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run QBO syncing pipeline.")
    parser.add_argument("--client", dest="client", default="", help="Target client name or Realm ID.")
    args = parser.parse_args()
    main(target_client=args.client)
