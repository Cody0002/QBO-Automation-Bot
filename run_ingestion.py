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

import calendar
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

def get_month_date_range(month_str: str) -> Tuple[datetime, datetime]:
    """Converts 'Oct 2025' into Start and End datetime objects."""
    try:
        dt = pd.to_datetime(month_str)
        start_date = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        _, last_day = calendar.monthrange(start_date.year, start_date.month)
        end_date = start_date.replace(day=last_day, hour=23, minute=59, second=59, microsecond=999999)
        return start_date, end_date
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

def get_retry_context(gs: GSheetsClient, spreadsheet_url: str, tab_name: str, id_col_name: str) -> Tuple[List[int], Dict[int, str]]:
    """Identifies rows marked as 'ERROR' in the Transform file to re-process them."""
    print(tab_name, id_col_name)
    try:
        df = gs.read_as_df_sync(spreadsheet_url, tab_name)
        if df.empty or "Remarks" not in df.columns or id_col_name not in df.columns:
            return [], {}
        print(df.head())
        # Filter for Error rows
        error_mask = df["Remarks"].astype(str).str.contains("ERROR|Unbalance", case=False, na=False)
        bad_rows = df[error_mask]
        # print(bad_rows.head())
        if bad_rows.empty: return [], {}

        # Identify IDs to delete and map existing sequential IDs
        bad_ids = bad_rows[id_col_name].unique()
        target_df = df[df[id_col_name].isin(bad_ids)].copy()
        
        rows_to_delete = []
        existing_id_map = {}
        
        for idx, row in target_df.iterrows():
            rows_to_delete.append(idx + 2) # +2 for header and 0-index
            if "No" in row:
                try: 
                    s_no = int(float(str(row["No"])))
                    existing_id_map[s_no] = str(row[id_col_name])
                except: pass
                
        return rows_to_delete, existing_id_map
    except Exception as e:
        print(f"🔥 get_retry_context crashed on tab '{tab_name}': {e}")
        raise

# ==========================================
# 2. CORE LOGIC (PER CLIENT)
# ==========================================

def process_client_control_sheet(gs: GSheetsClient, qbo_client: QBOClient, control_sheet_id: str, client_name: str):
    """
    Reads the specific Client's Control Sheet and processes all 'READY' jobs.
    """
    logger.info(f"📂 [{client_name}] Opening Control Sheet (ID: {control_sheet_id})...")

    # --- A. Fetch QBO Mappings (Specific to this Client/Realm) ---
    try:
        temp_sync = QBOSync(qbo_client)
        qbo_mappings = temp_sync.mappings
        logger.info(f"   ✅ [{client_name}] QBO Mappings fetched successfully.")
    except Exception as e:
        logger.error(f"   ❌ [{client_name}] Failed to fetch mappings. Check Realm ID/Token. Error: {e}")
        return

    # --- B. Read the Control Sheet ---
    try:
        ctrl_df = gs.read_as_df(control_sheet_id, settings.CONTROL_TAB_NAME)
    except Exception as e:
        logger.error(f"   ❌ [{client_name}] Failed to read Control Tab: {e}")
        return

    if ctrl_df.empty: 
        logger.warning(f"   ⚠️ [{client_name}] Control Sheet is empty.")
        return

    # --- Constants for this Client ---
    COL_LAST_JV = "Last Journal No"
    COL_LAST_EXP = "Last Expense No"
    COL_LAST_TR = "Last Transfer No"
    COL_QBO_JV = "QBO Journal"
    COL_QBO_EXP = "QBO Expense"
    COL_QBO_TR = "QBO Transfer"
    def safe_int(val):
        try: return int(float(val))
        except: return 0

    # Get the max journal number currently recorded in the sheet
    global_last_jv = ctrl_df[COL_LAST_JV].apply(safe_int).max()

    # --- C. Iterate Control Sheet Rows ---
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
            month = format_month_name(raw_month)

            # 3. Create/Link Transform File
            if not transform_url or len(transform_url) < 10:
                new_title = f"{client_name} - {country} QBO - {month}"
                logger.info(f"   ⚠️ No Transform File. Creating: '{new_title}'...")
                try:
                    transform_url = gs.create_spreadsheet(new_title)
                    new_file_id = transform_url.split("/d/")[1].split("/")[0]
                    # Copy permissions from the Client's Control Sheet to the new Transform File
                    gs.copy_permissions(source_id=control_sheet_id, target_id=new_file_id)
                    
                    _batch_update_control(gs, control_sheet_id, settings.CONTROL_TAB_NAME, row_num, ctrl_df.columns, {settings.CTRL_COL_TRANSFORM_URL: transform_url})
                except Exception as e:
                    logger.error(f"   ❌ Failed to create spreadsheet: {e}")
                    raise e
            
            # 4. Prepare ID Counters
            last_processed = safe_int(row.get(settings.CTRL_COL_LAST_PROCESSED_ROW, 0))
            
            # Fetch latest QBO Journal No to prevent overlap
            qbo_last_jv = qbo_client.get_max_journal_number("KZO-JV")
            final_start_jv = max(global_last_jv, qbo_last_jv)
            
            last_exp = safe_int(row.get(COL_LAST_EXP, 0))
            last_tr = safe_int(row.get(COL_LAST_TR, 0))

            tab_prefix = f"{country} {month}"
            tab_jv, tab_exp, tab_tr = f"{tab_prefix} - Journals", f"{tab_prefix} - Expenses", f"{tab_prefix} - Transfers"
        
            # 5. Handle Retries (Find 'ERROR' rows in Output)
            preserved_ids = {'journals': {}, 'expenses': {}, 'transfers': {}}
            deletions = {}

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

            # 6. Read & Clean Source Data
            raw_df = gs.read_as_df(source_url, raw_tab_name, header_row=1, value_render_option='UNFORMATTED_VALUE')
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
            target_start, target_end = get_month_date_range(raw_month)
            if target_start and target_end:
                # Robust Parse
                raw_df["_TempDate"] = parse_mixed_date(raw_df["Date"])
                
                # Filter
                month_mask = (raw_df["_TempDate"] >= target_start) & (raw_df["_TempDate"] <= target_end)
                raw_df = raw_df[month_mask].copy()
                raw_df.drop(columns=["_TempDate"], inplace=True)
                
                # --- LOGGING DATE FILTER ---
                after_date_count = len(raw_df)
                dropped_date = initial_count - after_date_count
                logger.info(f"   🗓️ [{client_name}] Step 7: Date Filter ({raw_month}) -> Kept: {after_date_count} | Dropped: {dropped_date}")
                # ---------------------------

                if raw_df.empty:
                    logger.warning(f"   [{client_name}] ⚠️ No rows found for {month} in Source.")
                    _batch_update_control(gs, control_sheet_id, settings.CONTROL_TAB_NAME, row_num, ctrl_df.columns, {settings.CTRL_COL_ACTIVE: "DONE (No Data)"})
                    continue

            # 8. Numeric Cleanup
            for col in ["No", "USD - QBO", "Amount Fr", "Amount To"]:
                if col in raw_df.columns:
                    raw_df[col] = pd.to_numeric(raw_df[col], errors="coerce").fillna(0)

            # 9. Exclude Rows
            before_exclude = len(raw_df)
            raw_df = raw_df[~raw_df["Check (Internal use)"].astype(str).str.contains("exclude", na=False, case=False)].copy()

            # --- LOGGING EXCLUDE FILTER ---
            after_exclude = len(raw_df)
            dropped_exclude = before_exclude - after_exclude
            if dropped_exclude > 0:
                logger.info(f"   🚫 [{client_name}] Step 9: 'Exclude' Filter -> Kept: {after_exclude} | Dropped: {dropped_exclude}")
            # ------------------------------

            # 10. Select Rows to Process (New + Retry)
            new_df = raw_df[raw_df["No"] > last_processed].copy()
            retry_df = raw_df[raw_df["No"].isin(retry_nos)].copy()
            processing_df = pd.concat([new_df, retry_df]).drop_duplicates(subset=["No"])

            # --- LOGGING SELECTION ---
            dropped_processed = after_exclude - len(processing_df)
            logger.info(f"   🔢 [{client_name}] Step 10: Selection -> New: {len(new_df)}, Retry: {len(retry_df)} | Total: {len(processing_df)} | Skipped (Old): {dropped_processed}")
            # -------------------------

            if processing_df.empty:
                logger.info(f"   [{client_name}] No new rows to process.")
                _batch_update_control(gs, control_sheet_id, settings.CONTROL_TAB_NAME, row_num, ctrl_df.columns, {settings.CTRL_COL_LAST_RUN_AT: _now_iso_local(), settings.CTRL_COL_ACTIVE: "DONE"})
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
                existing_ids=preserved_ids
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
            
            _batch_update_control(gs, control_sheet_id, settings.CONTROL_TAB_NAME, row_num, ctrl_df.columns, updates)
            logger.info(f"   ✅ [{client_name}] Row {row_num} Complete.")

        except Exception as e:
            logger.error(f"❌ [{client_name}] Error processing row {row_num}: {e}")
            _batch_update_control(gs, control_sheet_id, settings.CONTROL_TAB_NAME, row_num, ctrl_df.columns, {settings.CTRL_COL_ACTIVE: "ERROR"})
            continue

# ==========================================
# 3. MAIN ENTRY POINT
# ==========================================
def main():
    gs = GSheetsClient()
    
    # Initialize QBO Client with GSheets (to allow it to read/write tokens)
    qbo_client = QBOClient(gs_client=gs)

    logger.info("🌍 Reading MASTER SHEET to find active clients...")
    
    try:
        master_df = gs.read_as_df(settings.MASTER_SHEET_ID, settings.MASTER_TAB_NAME)
    except Exception as e:
        logger.error(f"❌ Critical: Could not read Master Sheet: {e}")
        return

    if master_df.empty:
        logger.warning("Master sheet is empty.")
        return

    # Loop through Clients
    for i, client_row in master_df.iterrows():
        client_name = str(client_row.get(settings.MST_COL_CLIENT, "Unknown"))
        status = str(client_row.get(settings.MST_COL_STATUS, "")).strip()
        
        # Filter Active Clients
        if status.lower() != "active":
            continue

        sheet_id = str(client_row.get(settings.MST_COL_SHEET_ID, "")).strip()
        realm_id = str(client_row.get(settings.MST_COL_REALM_ID, "")).strip()

        if not sheet_id or not realm_id:
            logger.warning(f"⚠️ Skipping {client_name}: Missing Sheet ID or Realm ID.")
            continue

        print(f"🏢 STARTING CLIENT: {client_name}")
        print(f"   Realm ID: {realm_id} | Sheet: {sheet_id}")

        # 1. Authenticate / Switch Context
        try:
            qbo_client.set_company(realm_id)
        except Exception as e:
            logger.error(f"❌ Critical Auth Failure for {client_name}: {e}")
            continue

        # 2. Run Ingestion for this Client
        try:
            process_client_control_sheet(gs, qbo_client, sheet_id, client_name)
        except Exception as e:
            logger.error(f"❌ Critical Logic Failure for {client_name}: {e}")

    logger.info("🏁 All Clients Processed.")

if __name__ == "__main__":
    main()
