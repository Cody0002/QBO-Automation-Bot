from __future__ import annotations
import argparse
from contextlib import nullcontext

# --- FIX: USE WINDOWS SYSTEM CERTIFICATES ---
try:
    import pip_system_certs.wrappers
    pip_system_certs.wrappers.wrap_requests()
except ImportError:
    pass
# --------------------------------------------

from dotenv import load_dotenv
load_dotenv("config/secrets.env")

import calendar
import re
from datetime import datetime
from typing import Tuple, List, Dict
import pandas as pd
from config import settings
from src.connectors.gsheets_client import GSheetsClient
from src.connectors.qbo_client import QBOClient
from src.logic.syncing import QBOSync
from src.logic.transformer import transform_raw
from src.utils.logger import setup_logger
from src.logic.raw_adapter import standardize_raw_df
from src.utils.run_lock import single_instance_lock

logger = setup_logger("ingestion")

def parse_mixed_date(series: pd.Series) -> pd.Series:
    """Parse Excel serial dates and regular date strings safely."""
    numeric = pd.to_numeric(series, errors="coerce")
    excel_mask = numeric.between(-60000, 120000)

    parsed = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
    if excel_mask.any():
        parsed.loc[excel_mask] = pd.to_datetime(
            numeric.loc[excel_mask],
            origin="1899-12-30",
            unit="D",
            errors="coerce",
        )
    if (~excel_mask).any():
        parsed.loc[~excel_mask] = pd.to_datetime(series.loc[~excel_mask], errors="coerce")
    return parsed

# ==========================================
# 1. HELPER FUNCTIONS
# ==========================================

def get_month_date_range(month_str: str, last_month_date_val=None) -> Tuple[datetime, datetime]:
    """Builds [start, end] date range for a month.
    Start is always first day of month; end uses 'Last Month Date' when provided.
    """
    try:
        dt = pd.to_datetime(month_str)
        start_date = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        _, last_day = calendar.monthrange(start_date.year, start_date.month)
        month_end = start_date.replace(day=last_day, hour=23, minute=59, second=59, microsecond=999999)

        if pd.isna(last_month_date_val) or str(last_month_date_val).strip() == "":
            return start_date, month_end

        numeric = pd.to_numeric(pd.Series([last_month_date_val]), errors="coerce").iloc[0]
        if pd.notna(numeric) and -60000 <= numeric <= 120000:
            custom_end = pd.to_datetime(numeric, origin="1899-12-30", unit="D", errors="coerce")
        else:
            custom_end = pd.to_datetime(last_month_date_val, errors="coerce")

        if pd.isna(custom_end):
            return start_date, month_end

        end_date = custom_end.replace(hour=23, minute=59, second=59, microsecond=999999)
        if end_date < start_date:
            return start_date, end_date
        return start_date, min(end_date, month_end)
    except Exception:
        return None, None

def _now_iso_local() -> str:
    """Returns current timestamp string."""
    now = datetime.now().astimezone()
    return now.strftime(f"%Y-%m-%d %H:%M:%S")

def _batch_update_control(gs, sheet_id, tab_name, row_num, columns, updates_dict):
    """Updates specific columns for a row in the Control Sheet."""
    headers = list(columns)
    batch_data = []
    for col_name, val in updates_dict.items():
        if col_name in headers:
            col_idx = headers.index(col_name) + 1
            batch_data.append({'row': row_num, 'col': col_idx, 'val': str(val)})
    if batch_data:
        gs.batch_update_cells(sheet_id, tab_name, batch_data)

def format_month_name(date_str: str) -> str:
    if not date_str: return ""
    try:
        return pd.to_datetime(date_str).strftime("%b %y")
    except:
        return date_str

def _parse_no_set(raw_val) -> set[int]:
    if pd.isna(raw_val) or str(raw_val).strip() == "":
        return set()
    out: set[int] = set()
    for tok in re.split(r"[,\s;|]+", str(raw_val).strip()):
        if not tok:
            continue
        try:
            n = int(float(tok))
            if n > 0:
                out.add(n)
        except Exception:
            continue
    return out

def _serialize_no_set(vals: set[int]) -> str:
    if not vals:
        return ""
    return ";".join(str(x) for x in sorted(vals))

def _cap_pending_nos(vals: set[int], max_processed_no: int) -> set[int]:
    if max_processed_no <= 0:
        return set()
    return {x for x in vals if 0 < x <= max_processed_no}

def _get_successfully_processed_nos(gs: GSheetsClient, spreadsheet_url: str, tabs: list[str]) -> set[int]:
    """
    Returns set of raw 'No' values that already exist in any output tab
    with a non-error Remarks (used to avoid reprocessing fully-completed rows).
    """
    processed: set[int] = set()
    for tab in tabs:
        try:
            df_out = gs.read_as_df_sync(spreadsheet_url, tab)
        except Exception:
            df_out = pd.DataFrame()

        if df_out.empty or "No" not in df_out.columns:
            continue

        df_tmp = df_out.copy()
        if "Remarks" in df_tmp.columns:
            err_mask = df_tmp["Remarks"].astype(str).str.contains("ERROR|Unbalance", case=False, na=False)
            df_tmp = df_tmp[~err_mask]

        if df_tmp.empty:
            continue

        nos = pd.to_numeric(df_tmp["No"], errors="coerce").dropna().astype(int).tolist()
        processed.update(nos)

    return processed

def get_retry_context(gs: GSheetsClient, spreadsheet_url: str, tab_name: str, id_col_name: str) -> Tuple[List[int], Dict[int, str]]:
    """Identifies rows marked as 'ERROR' in the Transform file to re-process them."""
    try:
        # Use read_as_df to keep row positions aligned with sheet rows.
        df = gs.read_as_df(spreadsheet_url, tab_name)
        if df.empty or "Remarks" not in df.columns or id_col_name not in df.columns:
            return [], {}

        work_df = df.copy()
        work_df["_sheet_row"] = work_df.index + 2  # +2 for header + 0-indexed DataFrame
        work_df["_remarks"] = work_df["Remarks"].astype(str)
        work_df["_doc_id"] = work_df[id_col_name].astype(str).str.strip()
        work_df["_no"] = pd.to_numeric(work_df.get("No"), errors="coerce")

        # Any row flagged as error/unbalanced should trigger full cleanup for its document/no.
        error_mask = work_df["_remarks"].str.contains("ERROR|Unbalance", case=False, na=False)
        bad_rows = work_df[error_mask]
        if bad_rows.empty:
            return [], {}

        bad_ids = set(bad_rows["_doc_id"].dropna().tolist())
        bad_ids.discard("")
        bad_nos = set(
            bad_rows["_no"]
            .dropna()
            .astype(int)
            .tolist()
        )

        target_mask = pd.Series(False, index=work_df.index)
        if bad_ids:
            target_mask = target_mask | work_df["_doc_id"].isin(bad_ids)
        if bad_nos:
            target_mask = target_mask | work_df["_no"].fillna(-1).astype(int).isin(bad_nos)

        target_df = work_df[target_mask].copy()
        if target_df.empty:
            return [], {}

        rows_to_delete = sorted(target_df["_sheet_row"].astype(int).unique().tolist(), reverse=True)
        existing_id_map = {}

        for _, row in target_df.iterrows():
            try:
                s_no = int(float(str(row.get("No", ""))))
                doc_id = str(row.get(id_col_name, "")).strip()
                if s_no > 0 and doc_id:
                    existing_id_map[s_no] = doc_id
            except Exception:
                pass

        return rows_to_delete, existing_id_map
    except Exception as e:
        logger.exception(f"get_retry_context crashed on tab '{tab_name}': {e}")
        raise

# ==========================================
# 2. CORE LOGIC (PER CLIENT)
# ==========================================

def process_client_control_sheet(
    gs: GSheetsClient,
    qbo_client: QBOClient,
    control_sheet_id: str,
    client_name: str,
    realm_id: str,
):
    """
    Reads the specific Client's Control Sheet and processes all 'READY' jobs.
    """
    logger.info(f"📂 [{client_name}] Opening Control Sheet (ID: {control_sheet_id})...")

    # --- A. Read the Control Sheet ---
    try:
        ctrl_df = gs.read_as_df(control_sheet_id, settings.CONTROL_TAB_NAME)
    except Exception as e:
        logger.error(f"   ❌ [{client_name}] Failed to read Control Tab: {e}")
        return

    if ctrl_df.empty: 
        logger.warning(f"   ⚠️ [{client_name}] Control Sheet is empty.")
        return

    # Avoid expensive QBO auth/mapping calls when this client has nothing to run.
    status_series = ctrl_df.get(settings.CTRL_COL_ACTIVE, pd.Series("", index=ctrl_df.index))
    ready_count = int(status_series.astype(str).str.strip().eq("READY").sum())
    if ready_count == 0:
        logger.info(f"   ⏭️ [{client_name}] No READY rows in control sheet. Skipping QBO auth/mappings.")
        return

    # --- B. Authenticate/Switch QBO context ---
    try:
        logger.info(f"🔐 [{client_name}] Authenticating with Realm ID: {realm_id}")
        qbo_client.set_company(realm_id)
        logger.info(f"✅ [{client_name}] Successfully authenticated. Ready to fetch QBO mappings.")
    except Exception as e:
        logger.error(f"❌ Critical Auth Failure for {client_name}: {e}")
        return

    # --- C. Fetch QBO Mappings (Specific to this Client/Realm) ---
    try:
        temp_sync = QBOSync(qbo_client)
        qbo_mappings = temp_sync.mappings
        num_accounts = len(qbo_mappings.get('accounts', {}))
        num_locations = len(qbo_mappings.get('locations', {}))
        logger.info(f"   ✅ [{client_name}] QBO Mappings fetched: {num_accounts} accounts, {num_locations} locations.")
        if num_accounts == 0:
            logger.warning(f"   ⚠️ [{client_name}] WARNING: No accounts found! Check Realm ID is correct.")
    except Exception as e:
        logger.error(f"   ❌ [{client_name}] Failed to fetch mappings. Check Realm ID/Token. Error: {e}")
        return

    # --- Constants for this Client ---
    COL_LAST_JV = "Last Journal No"
    COL_LAST_EXP = "Last Expense No"
    COL_LAST_TR = "Last Transfer No"
    COL_QBO_JV = "QBO Journal"
    COL_QBO_EXP = "QBO Expense"
    COL_QBO_TR = "QBO Transfer"
    COL_PENDING_AMOUNT_NOS = "Pending Amount Nos"
    def safe_int(val):
        try: return int(float(val))
        except: return 0

    # Get the max journal number currently recorded in the sheet
    global_last_jv = ctrl_df[COL_LAST_JV].apply(safe_int).max()

    # --- D. Iterate Control Sheet Rows ---
    for i, row in ctrl_df.iterrows():
        # 1. Check Trigger
        status_val = str(row.get(settings.CTRL_COL_ACTIVE, "")).strip()
        if status_val != 'READY': continue

        row_num = i + 2
        logger.info(f"🚀 [{client_name}] Processing Row {row_num}...")
        _batch_update_control(gs, control_sheet_id, settings.CONTROL_TAB_NAME, row_num, ctrl_df.columns, {settings.CTRL_COL_ACTIVE: "PROCESSING"})

        try:
            # 2. Extract Job Details
            country = str(row.get(settings.CTRL_COL_COUNTRY, "")).strip()
            source_url = str(row.get(settings.CTRL_COL_SOURCE_URL, "")).strip()
            transform_url = str(row.get(settings.CTRL_COL_TRANSFORM_URL, "")).strip()
            raw_tab_name = str(row.get(settings.CTRL_COL_TAB_NAME, "")).strip()
            raw_month = str(row.get(settings.CTRL_COL_MONTH, "")).strip()
            last_month_date = row.get(settings.CTRL_COL_LAST_MONTH_DATE, "")
            month = format_month_name(raw_month)

            # 3. Create/Link Transform File
            created_new_transform = False
            if not transform_url or len(transform_url) < 10:
                new_title = f"{client_name} - {country} QBO - {month}"
                logger.info(f"   ⚠️ No Transform File. Creating: '{new_title}'...")
                try:
                    transform_url = gs.create_spreadsheet(new_title)
                    new_file_id = transform_url.split("/d/")[1].split("/")[0]
                    # Copy permissions from the Client's Control Sheet to the new Transform File
                    gs.copy_permissions(source_id=control_sheet_id, target_id=new_file_id)
                    
                    _batch_update_control(gs, control_sheet_id, settings.CONTROL_TAB_NAME, row_num, ctrl_df.columns, {settings.CTRL_COL_TRANSFORM_URL: transform_url})
                    created_new_transform = True
                except Exception as e:
                    logger.error(f"   ❌ Failed to create spreadsheet: {e}")
                    raise e
            
            # 4. Prepare ID Counters
            last_processed = safe_int(row.get(settings.CTRL_COL_LAST_PROCESSED_ROW, 0))
            
            # Fetch latest QBO Journal No to prevent overlap.
            client_lower = client_name.lower()
            if "kzp" in client_lower:
                journal_prefix = "KZP-JV"
            elif "kzdw" in client_lower:
                journal_prefix = "KZDW-JV"
            else:
                journal_prefix = "KZO-JV"
            qbo_last_jv = qbo_client.get_max_journal_number(journal_prefix)
            final_start_jv = max(global_last_jv, qbo_last_jv)
            
            last_exp = safe_int(row.get(COL_LAST_EXP, 0))
            last_tr = safe_int(row.get(COL_LAST_TR, 0))
            previous_pending_nos = _cap_pending_nos(
                _parse_no_set(row.get(COL_PENDING_AMOUNT_NOS, "")),
                last_processed
            )

            # If this run created a brand-new transform file, treat it as a fresh row state.
            # This avoids accidental skipping when a duplicated control row carries old counters.
            if created_new_transform:
                if last_processed > 0 or previous_pending_nos:
                    logger.info(
                        f"   [{client_name}] New transform detected; resetting carried row state "
                        f"(Last Processed Row {last_processed} -> 0, Pending Amount Nos cleared)."
                    )
                last_processed = 0
                previous_pending_nos = set()

            tab_prefix = f"{country} {month}"
            tab_jv, tab_exp, tab_tr = f"{tab_prefix} - Journals", f"{tab_prefix} - Expenses", f"{tab_prefix} - Transfers"
        
            # 5. Handle Retries (Find 'ERROR' rows in Output)
            preserved_ids = {'journals': {}, 'expenses': {}, 'transfers': {}}
            deletions: Dict[str, List[int]] = {}

            # Check Journals tab
            d_jv, ids_jv = get_retry_context(gs, transform_url, tab_jv, "Journal No")
            if d_jv: deletions[tab_jv] = d_jv; preserved_ids['journals'] = ids_jv

            # Check Expenses tab
            d_exp, ids_exp = get_retry_context(gs, transform_url, tab_exp, "Exp Ref. No")
            if d_exp: deletions[tab_exp] = d_exp; preserved_ids['expenses'] = ids_exp

            # Check Transfers tab
            d_tr, ids_tr = get_retry_context(gs, transform_url, tab_tr, "Ref No")
            if d_tr: deletions[tab_tr] = d_tr; preserved_ids['transfers'] = ids_tr

            retry_nos = list(set([k for sub in preserved_ids.values() for k in sub.keys()]))
            tabs_out = [tab_jv, tab_exp, tab_tr]
            processed_ok_nos = _get_successfully_processed_nos(gs, transform_url, tabs_out)

            # 6. Read & Clean Source Data
            source_header_row = 5 if "kzdw" in client_name.lower() else 1
            raw_df = gs.read_as_df(
                source_url,
                raw_tab_name,
                header_row=source_header_row,
                value_render_option='UNFORMATTED_VALUE'
            )
            raw_df = standardize_raw_df(raw_df, client_name=client_name, raw_month=raw_month)

            # --- LOGGING START ---
            initial_count = len(raw_df)
            logger.info(f"   📊 [{client_name}] Step 6: Raw Rows Read: {initial_count}")
            # ---------------------

            if raw_df.empty:
                logger.info(f"   [{client_name}] Raw tab empty.")
                _batch_update_control(gs, control_sheet_id, settings.CONTROL_TAB_NAME, row_num, ctrl_df.columns, {settings.CTRL_COL_ACTIVE: "DONE (Empty)"})
                continue
            
            raw_df = raw_df.iloc[:, :25]  # Keep first 25 columns
            
            # Apply Standard Columns
            raw_df.columns = [
                "CO", "COY", "Date", "Category", "Type", "Item Description", 
                "TrxHarsh", "Account Fr", "Account To", "Currency", "Amount Fr", 
                "Currency To", "Amount To", "Budget", "USD - Raw", "USD - Actual", 
                "USD - Loss", "USD - QBO", "Reclass", "QBO Method", 
                "If Journal/Expense Method", "QBO Transfer Fr", "QBO Transfer To", 
                "Check (Internal use)", "No"
            ]

            raw_df["CO"] = raw_df["CO"].astype(str).str.replace("GRP", "GROUP").str.strip()

            # 7. Date Filtering (Strict Month Match)
            target_start, target_end = get_month_date_range(raw_month, last_month_date)
            future_pending_nos: set[int] = set()
            if target_start and target_end:
                # Robust Parse
                raw_df["_TempDate"] = parse_mixed_date(raw_df["Date"])

                # Track all future-dated rows so they can be retried later
                # even though date filter removes them from this run.
                no_numeric = pd.to_numeric(raw_df["No"], errors="coerce").fillna(0)
                future_late_mask = (raw_df["_TempDate"] > target_end) & (no_numeric > 0)
                future_pending_nos = set(int(x) for x in no_numeric[future_late_mask].astype(int).tolist())
                
                # Filter
                month_mask = (raw_df["_TempDate"] >= target_start) & (raw_df["_TempDate"] <= target_end)
                raw_df = raw_df[month_mask].copy()
                raw_df.drop(columns=["_TempDate"], inplace=True)
                
                # --- LOGGING DATE FILTER ---
                after_date_count = len(raw_df)
                dropped_date = initial_count - after_date_count
                logger.info(
                    f"   🗓️ [{client_name}] Step 7: Date Filter "
                    f"({target_start.date()} -> {target_end.date()}) -> "
                    f"Kept: {after_date_count} | Dropped: {dropped_date}"
                )
                if future_pending_nos:
                    logger.info(f"   [{client_name}] Step 7a: Future-date rows saved to pending: {len(future_pending_nos)}")
                # ---------------------------

                if raw_df.empty:
                    logger.warning(f"   [{client_name}] ⚠️ No rows found for {month} in Source.")
                    _batch_update_control(gs, control_sheet_id, settings.CONTROL_TAB_NAME, row_num, ctrl_df.columns, {settings.CTRL_COL_ACTIVE: "DONE (No Data)"})
                    continue

            # 8. Numeric Cleanup (Do this first so we can check for 0 amounts)
            for col in ["No", "USD - QBO", "Amount Fr", "Amount To"]:
                if col in raw_df.columns:
                    raw_df[col] = pd.to_numeric(raw_df[col], errors="coerce").fillna(0)

            # 9. Exclude Rows
            before_exclude = len(raw_df)
            raw_df = raw_df[~raw_df["Check (Internal use)"].astype(str).str.contains("exclude", na=False, case=False)].copy()

            after_exclude = len(raw_df)
            dropped_exclude = before_exclude - after_exclude
            if dropped_exclude > 0:
                logger.info(f"   🚫 [{client_name}] Step 9: 'Exclude' Filter -> Kept: {after_exclude} | Dropped: {dropped_exclude}")

            # 10. Track Pending Rows & Select Rows to Process
            method_col = "QBO Method"
            amount_col = "USD - QBO" # We use USD - QBO as the standard amount column

            method_non_blank = raw_df[method_col].notna() & (raw_df[method_col].str.strip() != "")
            amt_numeric = raw_df[amount_col] # Already converted to numeric in Step 8

            # ---> A. Identify Pending Rows (Method exists, but amount is 0)
            pending_amount_mask = method_non_blank & (amt_numeric == 0)
            current_pending_nos = set(
                int(x) for x in raw_df.loc[pending_amount_mask, "No"].astype(int).tolist() if int(x) > 0
            )
            current_pending_nos.update(previous_pending_nos)
            current_pending_nos.update(future_pending_nos)

            # ---> B. Identify Ready Rows (Method exists, and amount is NOT 0)
            ready_mask = method_non_blank & (amt_numeric != 0)
            ready_df = raw_df[ready_mask].copy()

            # 10a. Strictly new rows (No > last_processed)
            new_df = ready_df[ready_df["No"] > last_processed].copy()

            # 10b. Late-filled rows: No <= last_processed, not yet successfully processed
            late_filled_df = ready_df[
                (ready_df["No"] <= last_processed) &
                (ready_df["No"].isin(previous_pending_nos))
            ].copy()

            # 10c. Explicit retries from ERROR outputs
            retry_df = ready_df[ready_df["No"].isin(retry_nos)].copy()

            processing_df = (
                pd.concat([new_df, late_filled_df, retry_df])
                  .drop_duplicates(subset=["No"])
            )

            # --- LOGGING SELECTION ---
            no_numeric = pd.to_numeric(raw_df["No"], errors="coerce").fillna(0).astype(int)
            no_method_count = int((~method_non_blank).sum())
            zero_amount_count = int((method_non_blank & (amt_numeric == 0)).sum())
            positive_amt_count = int((method_non_blank & (amt_numeric != 0)).sum())
            eligible_old_done_count = int(
                ((no_numeric <= last_processed) &
                 (~no_numeric.isin(previous_pending_nos)) &
                 (~no_numeric.isin(retry_nos)) &
                 (method_non_blank & (amt_numeric != 0))).sum()
            )
            logger.info(
                f"   🔢 [{client_name}] Step 10: Selection -> New: {len(new_df)}, "
                f"Late-filled: {len(late_filled_df)}, Retry: {len(retry_df)} | "
                f"Total: {len(processing_df)}"
            )
            logger.info(
                f"   🔍 [{client_name}] Step 10 Detail -> No Method: {no_method_count}, "
                f"Zero Amount(Pending): {zero_amount_count}, Ready Rows: {positive_amt_count}, "
                f"Eligible Old & done: {eligible_old_done_count}, Last Processed Row: {last_processed}"
            )
            # -------------------------

            if processing_df.empty:
                logger.info(f"   [{client_name}] No new rows to process.")
                pending_to_write = _cap_pending_nos(current_pending_nos, last_processed)
                _batch_update_control(gs, control_sheet_id, settings.CONTROL_TAB_NAME, row_num, ctrl_df.columns, {
                    settings.CTRL_COL_LAST_RUN_AT: _now_iso_local(), 
                    COL_PENDING_AMOUNT_NOS: _serialize_no_set(pending_to_write), # <-- ADDED
                    settings.CTRL_COL_ACTIVE: "DONE"
                })
                continue
            
            # 11. Execute Deletions (Clean up bad rows before appending new ones)
            for tab, rows in deletions.items(): gs.delete_rows(transform_url, tab, rows)

            logger.info(f"   [{client_name}] Transforming {len(processing_df)} rows...")

            # 12. RUN TRANSFORMER
            result = transform_raw(
                raw_df=processing_df, 
                last_jv=final_start_jv, 
                last_exp=last_exp, 
                last_tr=last_tr, 
                country=country,  # <--- NEW ARGUMENT
                qbo_mappings=qbo_mappings, 
                existing_ids=preserved_ids,
                client_name=client_name
            )
            # 13. Write Output
            # Note: We use 'control_sheet_id' as the template source. 
            # Assumes the Client's Control Sheet has the "Sample - Journals" etc. hidden tabs.
            
            def write_tab(df_out, tab_out, templ_name):
                if not df_out.empty:
                    # Fix dates for JSON serialization
                    for col in df_out.select_dtypes(include=['datetime64', 'datetimetz']).columns:
                        df_out[col] = df_out[col].dt.strftime('%Y-%m-%d')
                    
                    gs.append_or_create_df(
                        transform_url, 
                        tab_out, 
                        df_out, 
                        template_tab_name=templ_name, 
                        template_spreadsheet_id=control_sheet_id
                    )

            write_tab(result.journals, tab_jv, "Sample - Journals")
            write_tab(result.expenses, tab_exp, "Sample - Expenses")
            write_tab(result.withdraw, tab_tr, "Sample - Transfers")

            gs.cleanup_default_sheet(transform_url)

            # 14. Check Status of Output (Any errors generated by Transformer?)
            def check_status(df):
                if df.empty: return ""
                if "Remarks" in df.columns and df["Remarks"].astype(str).str.contains("ERROR", case=False, na=False).any(): return "ERROR"
                return "READY TO SYNC"

            status_jv = check_status(result.journals)
            status_exp = check_status(result.expenses)
            status_tr = check_status(result.withdraw)

            # 15. Final Updates to Control Sheet
            final_last_row = max(last_processed, result.max_row_processed) if result.max_row_processed else last_processed
            pending_to_write = _cap_pending_nos(current_pending_nos, final_last_row)

            updates = {
                settings.CTRL_COL_LAST_PROCESSED_ROW: final_last_row,
                COL_LAST_JV: result.last_journal_no,
                COL_LAST_EXP: result.last_expense_no,
                COL_LAST_TR: result.last_withdraw_no,
                COL_PENDING_AMOUNT_NOS: _serialize_no_set(pending_to_write), # <-- ADDED
                settings.CTRL_COL_LAST_RUN_AT: _now_iso_local(),
                settings.CTRL_COL_ACTIVE: "DONE"
            }
            if COL_QBO_JV in ctrl_df.columns: updates[COL_QBO_JV] = status_jv
            if COL_QBO_EXP in ctrl_df.columns: updates[COL_QBO_EXP] = status_exp
            if COL_QBO_TR in ctrl_df.columns: updates[COL_QBO_TR] = status_tr
            
            _batch_update_control(gs, control_sheet_id, settings.CONTROL_TAB_NAME, row_num, ctrl_df.columns, updates)
            logger.info(f"   ✅ [{client_name}] Row {row_num} Complete.")

        except Exception as e:
            logger.error(f"❌ [{client_name}] Error processing row {row_num}: {e}")
            _batch_update_control(gs, control_sheet_id, settings.CONTROL_TAB_NAME, row_num, ctrl_df.columns, {settings.CTRL_COL_ACTIVE: "ERROR"})
            continue

# ==========================================
# 3. MAIN ENTRY POINT
# ==========================================
def _is_target_client(client_row: pd.Series, target_client: str | None) -> bool:
    if not target_client:
        return True

    target = str(target_client).strip()
    if not target:
        return True
    target_norm = settings.normalize_workspace_name(target)
    if target_norm in {"all", "*", "all clients"}:
        return True

    row_client = str(client_row.get(settings.MST_COL_CLIENT, "")).strip()
    row_realm = str(client_row.get(settings.MST_COL_REALM_ID, "")).strip()
    row_sheet_id = str(client_row.get(settings.MST_COL_SHEET_ID, "")).strip()
    row_folder_id = str(client_row.get(settings.MST_COL_OUTPUT, "")).strip()

    if target == row_realm:
        return True
    if target == row_sheet_id:
        return True
    if target == row_folder_id:
        return True
    return target_norm == settings.normalize_workspace_name(row_client)

def _target_is_all(target_client: str | None) -> bool:
    if not target_client:
        return True
    t = settings.normalize_workspace_name(target_client)
    return t in {"", "all", "*", "all clients"}

def main(target_client: str | None = None):
    target_is_all = _target_is_all(target_client)
    dispatch_ctx = single_instance_lock("run_ingestion_all_dispatch") if target_is_all else nullcontext(True)
    with dispatch_ctx as acquired:
        if target_is_all and not acquired:
            logger.warning("Another ALL ingestion dispatch is already in progress. Skipping this run.")
            return

        gs = GSheetsClient()
        
        # Initialize QBO Client with GSheets (to allow it to read/write tokens)
        qbo_client = QBOClient(gs_client=gs)

        logger.info("🌍 Reading MASTER SHEET to find active clients...")
        
        try:
            master_df = gs.read_as_df(settings.MASTER_SHEET_ID, settings.MASTER_TAB_NAME)
        except Exception as e:
            logger.error(f"❌ Critical: Could not read Master Sheet: {e}")
            return

        # Normalize headers to avoid silent misses from extra spaces/newlines in sheet columns.
        master_df.columns = [" ".join(str(c).replace("\n", " ").split()) for c in master_df.columns]

        if master_df.empty:
            logger.warning("Master sheet is empty.")
            return

        # Loop through Clients
        matched_clients = 0
        for i, client_row in master_df.iterrows():
            if not _is_target_client(client_row, target_client):
                continue
            matched_clients += 1

            client_name = str(client_row.get(settings.MST_COL_CLIENT, "Unknown"))
            status = str(client_row.get(settings.MST_COL_STATUS, "")).strip()
            
            # Filter Active Clients
            if status.lower() != "active":
                continue

            if not settings.is_allowed_workspace(client_name):
                logger.warning(
                    f"⚠️ Skipping {client_name}: workspace not allowed for QBO API. "
                    f"Allowed: {', '.join(settings.ALLOWED_QBO_WORKSPACES)}"
                )
                continue

            sheet_id = str(client_row.get(settings.MST_COL_SHEET_ID, "")).strip()
            realm_id = str(client_row.get(settings.MST_COL_REALM_ID, "")).strip()

            if not sheet_id or not realm_id:
                logger.warning(f"⚠️ Skipping {client_name}: Missing Sheet ID or Realm ID.")
                continue

            print(f"🏢 STARTING CLIENT: {client_name}")
            print(f"   Realm ID: {realm_id} | Sheet: {sheet_id}")

            client_lock_name = f"run_ingestion_client_{realm_id}"
            with single_instance_lock(client_lock_name) as client_acquired:
                if not client_acquired:
                    logger.warning(
                        f"⏭️ Skipping {client_name}: another ingestion run is already processing Realm {realm_id}."
                    )
                    continue
                # Run Ingestion for this Client
                try:
                    process_client_control_sheet(gs, qbo_client, sheet_id, client_name, realm_id)
                except Exception as e:
                    logger.error(f"❌ Critical Logic Failure for {client_name}: {e}")

        if target_client and matched_clients == 0:
            logger.warning(f"No client matched target '{target_client}'.")

        logger.info("🏁 All Clients Processed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run QBO ingestion/transform pipeline.")
    parser.add_argument(
        "--client",
        dest="client",
        default="",
        help="Target client name, Realm ID, Spreadsheet ID, or Output Folder ID.",
    )
    args = parser.parse_args()
    main(target_client=args.client)
