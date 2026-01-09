from __future__ import annotations
from dataclasses import dataclass
from typing import Tuple, Dict, Any
import pandas as pd
import numpy as np
from config import settings

# --- HELPER FUNCTIONS ---
def find_id_in_map(mapping_dict: dict, search_name: str) -> str | None:
    if not search_name or pd.isna(search_name) or str(search_name).strip() == "":
        return None
    search_name = str(search_name).strip().lower()
    for qbo_name, qbo_id in mapping_dict.items():
        if qbo_name.lower() == search_name: return qbo_id
    for qbo_name, qbo_id in mapping_dict.items():
        if search_name in qbo_name.lower(): return qbo_id
    return None

def _fix_grp_location(df: pd.DataFrame, col_name: str = "Location"):
    if col_name in df.columns:
        df[col_name] = df[col_name].apply(
            lambda x: "GROUP" if str(x).strip().upper() == "GRP" else x
        )

@dataclass
class TransformResult:
    journals: pd.DataFrame
    expenses: pd.DataFrame
    withdraw: pd.DataFrame
    last_journal_no: int
    last_expense_no: int
    last_withdraw_no: int
    max_row_processed: int | None

# ==========================================
# 1. PROCESS JOURNALS
# ==========================================
def process_journals(df: pd.DataFrame, start_no: int, qbo_mappings: Dict[str, dict], existing_ids: Dict[int, str] = None) -> tuple[pd.DataFrame, int]:
    if df.empty:
        return pd.DataFrame(), start_no

    if existing_ids is None: existing_ids = {}

    PREFIX = "KZO-JV"
    COL_METHOD = "QBO Import Method \n (Journal/Expenses/Transfer)"
    COL_USD = "USD"
    COL_DATE = "Date"
    
    # 1. Clean USD
    df[COL_USD] = pd.to_numeric(df[COL_USD], errors='coerce').fillna(0.0)
    
    # 2. [MODIFIED] We keep ALL rows initially to ensure we can debug missing IDs
    # (We will filter zeros at the very end)
    # df = df[df[COL_USD].abs() > 1e-9].copy()  <-- COMMENTED OUT FOR DEBUGGING

    if df.empty:
        return pd.DataFrame(), start_no

    if COL_METHOD not in df.columns:
        return pd.DataFrame(), start_no

    mask_std = df[COL_METHOD] == "Journal"
    mask_reclass = df[COL_METHOD] == "Reclass"
    
    df_std = df[mask_std].copy()
    df_reclass = df[mask_reclass].copy()
    
    processed_std = pd.DataFrame()
    processed_reclass = pd.DataFrame()
    current_max = start_no

    # --- A. STANDARD JOURNALS ---
    if not df_std.empty:
        if "CO" in df_std.columns: _fix_grp_location(df_std, "CO")

        generated_ids = []
        for _, row in df_std.iterrows():
            try:
                s_no = int(float(str(row.get("No", 0))))
            except:
                s_no = 0
                
            if s_no in existing_ids:
                generated_ids.append(existing_ids[s_no])
            else:
                current_max += 1
                generated_ids.append(f"{PREFIX}{str(current_max).zfill(4)}")

        df_std["Journal No"] = generated_ids
        df_std["Currency Code"] = "USD"
        df_std["Class"] = ""
        
        # Debit
        deb = df_std.copy()
        deb["Amount"] = deb[COL_USD].astype(float) * -1 
        deb["Name"] = deb["Item Description"]
        deb = deb.rename(columns={"Item Description": "Memo", "Type": "Account", "CO": "Location"})
        deb = deb[["No", "Journal No", "Date", "Memo", "Account", "Amount", "Name", "Location", "Currency Code", "Class"]]

        # Credit
        cred = df_std.copy()
        cred["Amount"] = cred[COL_USD].astype(float)
        cred["Name"] = cred["Item Description"]
        cred = cred.rename(columns={"Item Description": "Memo", "If Journal/Expense method:\n Another records": "Account", "CO": "Location"})
        cred = cred[["No", "Journal No", "Date", "Memo", "Account", "Amount", "Name", "Location", "Currency Code", "Class"]]

        processed_std = pd.concat([deb, cred], ignore_index=True)

    # --- B. RECLASS JOURNALS ---
    if not df_reclass.empty:
        if "CO" in df_reclass.columns: _fix_grp_location(df_reclass, "CO")
        
        # [FIX] Normalize Date to remove Time (Hours/Mins) so grouping works
        df_reclass["_GroupDate"] = df_reclass[COL_DATE].dt.normalize()
        
        unique_dates = df_reclass["_GroupDate"].dropna().unique()
        unique_dates = sorted(unique_dates)
        print(unique_dates)
        
        date_map = {}
        for dt in unique_dates:
            current_max += 1
            date_map[dt] = f"{PREFIX}{str(current_max).zfill(4)}"
        
        df_reclass["Journal No"] = df_reclass["_GroupDate"].map(date_map)
        print(df_reclass[["_GroupDate", "Journal No"]].drop_duplicates())
        df_reclass.drop(columns=["_GroupDate"], inplace=True, errors='ignore')

        df_reclass["Amount"] = df_reclass[COL_USD].fillna(0.0).astype(float)
        df_reclass["Currency Code"] = "USD"
        df_reclass["Class"] = ""
        df_reclass["Name"] = df_reclass["Item Description"]
        df_reclass = df_reclass.rename(columns={"Item Description": "Memo", "Type": "Account", "CO": "Location"})
        
        processed_reclass = df_reclass[["No", "Journal No", "Date", "Memo", "Account", "Amount", "Name", "Location", "Currency Code", "Class"]]

        # Balancing logic
        diffs = processed_reclass.groupby("Journal No")["Amount"].sum()
        for journal_id, diff in diffs.items():
            if not np.isclose(diff, 0, atol=1e-9):
                mask = processed_reclass["Journal No"] == journal_id
                if not mask.any(): continue
                subset_indices = processed_reclass[mask].index
                max_row_idx = processed_reclass.loc[subset_indices, "Amount"].abs().idxmax()
                processed_reclass.loc[max_row_idx, "Amount"] -= diff

    # --- COMBINE ---
    total_journals = pd.concat([processed_std, processed_reclass], ignore_index=True)

    if total_journals.empty:
        return pd.DataFrame(), start_no

    for col in total_journals.select_dtypes(include=['datetime64', 'datetimetz']).columns:
        total_journals[col] = total_journals[col].astype(str)
    
    # [RESTORE FILTER] Now we remove zero amounts for the final output
    total_journals = total_journals[total_journals["Amount"].abs() > 1e-9].copy()
    
    total_journals["Account"] = total_journals["Account"].fillna("").astype(str).str.strip()

    # --- VALIDATION ---
    balance_map = total_journals.groupby("Journal No")["Amount"].sum()
    unbalanced_ids = balance_map[abs(balance_map) > 0.01].index.tolist()

    map_acc = qbo_mappings.get('accounts', {})
    map_loc = qbo_mappings.get('locations', {})

    def validate_journal_row(row):
        if row["Journal No"] in unbalanced_ids:
            diff = round(balance_map[row["Journal No"]], 2)
            return f"ERROR | Unbalanced ({diff})"
        
        acc_name = row["Account"]
        if not acc_name: return "ERROR | Missing Account Name"
        if not find_id_in_map(map_acc, acc_name): return f"ERROR | Account not found in QBO: '{acc_name}'"
        
        loc_name = row.get("Location")
        if loc_name and not find_id_in_map(map_loc, loc_name):
             return f"ERROR | Location not found in QBO: '{loc_name}'"
        
        return "Ready to sync"

    total_journals["Remarks"] = total_journals.apply(validate_journal_row, axis=1)

    cols_order = ["No", "Journal No", "Date", "Memo", "Account", "Amount", "Name", "Location", "Currency Code", "Class", "Remarks"]
    for c in cols_order:
        if c not in total_journals.columns: total_journals[c] = ""
    
    total_journals["Amount"] = total_journals["Amount"].astype(float).round(2)
    return total_journals[cols_order], current_max

# ==========================================
# 2. PROCESS EXPENSES
# ==========================================
def process_expenses(df: pd.DataFrame, start_no: int, qbo_mappings: Dict[str, dict], existing_ids: Dict[int, str] = None) -> Tuple[pd.DataFrame, int]:
    if df is None or df.empty: return pd.DataFrame(), start_no
    if existing_ids is None: existing_ids = {}

    df = df[df["USD"].apply(lambda x: round(x, 2)) != 0].copy()
    d = df.copy()
    d = d[[c for c in d.columns if "currency" not in c.lower()]]

    method_col = "QBO Import Method \n (Journal/Expenses/Transfer)"
    if method_col in d.columns: d = d[d[method_col].astype(str) == "Expense"]
    if "In/Out" in d.columns: d = d[pd.to_numeric(d["In/Out"], errors="coerce") < 0]

    if d.empty: return pd.DataFrame(), start_no

    d["Payee (Dummy)"] = "Dummy"
    d["Payment Method"] = "Cash"
    d["Currency Code"] = "USD"
    if "Bank" not in d.columns: d["Bank"] = ""
    d["Bank"] = d["Bank"].fillna("").astype(str)
    d.loc[d["Bank"].str.strip() == "", "Bank"] = "Payment Gateway - PH"
    if "USD" not in d.columns: d["USD"] = 0.0
    d["USD"] = pd.to_numeric(d["USD"], errors="coerce").fillna(0.0) * -1
    if "Date" not in d.columns: d["Date"] = pd.NaT
    d["Date"] = pd.to_datetime(d["Date"], errors="coerce")

    # ID GENERATION / PRESERVATION
    ref_nos = []
    for i, row in d.iterrows():
        s_no = int(row.get("No", 0))
        if s_no in existing_ids:
            ref_nos.append(existing_ids[s_no])
        else:
            mm_yy = row["Date"].strftime("%m%y") if pd.notna(row["Date"]) else "0000"
            seq = int(start_no) + 1
            start_no += 1 
            ref_nos.append(f"KZOPH{mm_yy}E{str(seq).zfill(4)}")
    
    d["Exp Ref. No"] = ref_nos

    rename_map = {
        "Item Description": "Memo",
        "CO": "Location",
        "Type": "Expense Account (Dr)",
        "Bank": "Account (Cr)",
        "Date": "Payment Date",
        "USD": "Expense Line Amount",
        "Currency Code": "Currency"
    }
    for k in rename_map.keys():
        if k not in d.columns: d[k] = ""
    d = d.rename(columns=rename_map)
    d["Expense Description"] = d["Memo"]

    _fix_grp_location(d, "Location")

    d["Account (Cr)"] = d["Account (Cr)"].fillna("").astype(str).str.strip()
    d["Expense Account (Dr)"] = d["Expense Account (Dr)"].fillna("").astype(str).str.strip()
    d["Expense Line Amount"] = pd.to_numeric(d["Expense Line Amount"]).fillna(0.0)

    # --- [UPDATE] FILTER ZERO AMOUNTS ---
    d = d[d["Expense Line Amount"].abs() > 1e-9].copy()

    if d.empty: return pd.DataFrame(), start_no

    # Validation
    map_acc = qbo_mappings.get('accounts', {})
    map_loc = qbo_mappings.get('locations', {})

    def validate_expense_row(row):
        if not row["Account (Cr)"]: return "ERROR | Missing Source Account"
        if not row["Expense Account (Dr)"]: return "ERROR | Missing Expense Account"
        # Zero check removed as rows are deleted
        if pd.isna(row["Payment Date"]): return "ERROR | Missing Date"
        if not find_id_in_map(map_acc, row["Account (Cr)"]): return f"ERROR | Source Account not in QBO: '{row['Account (Cr)']}'"
        if not find_id_in_map(map_acc, row["Expense Account (Dr)"]): return f"ERROR | Expense Account not in QBO: '{row['Expense Account (Dr)']}'"
        loc_name = row.get("Location")
        if loc_name and not find_id_in_map(map_loc, loc_name): return f"ERROR | Location not in QBO: '{loc_name}'"
        return "Ready to sync"

    d["Remarks"] = d.apply(validate_expense_row, axis=1)

    cols_order = ["No", "Exp Ref. No", "Account (Cr)", "Payee (Dummy)", "Memo", "Payment Date", "Payment Method", "Expense Account (Dr)", "Expense Description", "Expense Line Amount", "Currency", "Location", "Remarks"]
    for c in cols_order:
        if c not in d.columns: d[c] = ""
    
    d = d[cols_order].copy()
    for col in d.select_dtypes(include=["datetime64", "datetimetz"]).columns:
        d[col] = d[col].astype(str)
    if "No" in d.columns:
        d["No"] = pd.to_numeric(d["No"], errors='coerce').fillna(0).astype(int)

    return d, start_no

# ==========================================
# 3. PROCESS TRANSFERS
# ==========================================
def process_transfers(df: pd.DataFrame, start_no: int, qbo_mappings: Dict[str, dict], existing_ids: Dict[int, str] = None) -> tuple[pd.DataFrame, int]:
    if df.empty: return pd.DataFrame(), start_no
    if existing_ids is None: existing_ids = {}

    df = df[df["USD"].apply(lambda x: round(x, 2)) != 0].copy()
    
    transfers = df.copy()
    transfers = transfers[[c for c in transfers.columns if "currency" not in c.lower()]]

    col_method = "QBO Import Method \n (Journal/Expenses/Transfer)"
    if col_method in transfers.columns:
        transfers = transfers[transfers[col_method] == "Transfer"].copy()

    if transfers.empty: return pd.DataFrame(), start_no

    if "USD" in transfers.columns:
        transfers["USD"] = pd.to_numeric(transfers["USD"], errors="coerce").fillna(0.0).abs()
    else:
        transfers["USD"] = 0.0
    transfers["Currency Code"] = "USD"
    
    # ID GENERATION / PRESERVATION
    ref_nos = []
    for i, row in transfers.iterrows():
        s_no = int(row.get("No", 0))
        if s_no in existing_ids:
            ref_nos.append(existing_ids[s_no])
        else:
            try: date_str = row["Date"].strftime('%m%y')
            except: date_str = "0000"
            seq = int(start_no) + 1
            start_no += 1
            ref_nos.append(f"KZOPH{date_str}T{str(seq).zfill(4)}")

    transfers["Ref No"] = ref_nos

    # Rename and Clean
    raw_col_from = "If Transfer method: \n Fund Transfer From "
    raw_col_to = "Transfer to  ( (Can copy from column H )" 
    if raw_col_from not in transfers.columns: transfers[raw_col_from] = ""
    if raw_col_to not in transfers.columns:
        possible = [c for c in transfers.columns if "Transfer to" in str(c)]
        if possible: transfers.rename(columns={possible[0]: raw_col_to}, inplace=True)
        else: transfers[raw_col_to] = ""

    rename_map = {
        raw_col_from: "Transfer Funds From",
        raw_col_to: "Transfer Funds To",
        "USD": "Transfer Amount",
        "Item Description": "Memo",
        "CO": "Location",
        "Currency Code": "Currency"
    }
    for k in rename_map.keys():
        if k not in transfers.columns: transfers[k] = ""

    transfers = transfers.rename(columns=rename_map)
    transfers["Memo"] = transfers["Ref No"] + " - " + transfers["Memo"].astype(str)
    _fix_grp_location(transfers, "Location")
    transfers["Transfer Funds From"] = transfers["Transfer Funds From"].fillna("").astype(str).str.strip()
    transfers["Transfer Funds To"] = transfers["Transfer Funds To"].fillna("").astype(str).str.strip()
    transfers["Transfer Amount"] = pd.to_numeric(transfers["Transfer Amount"]).fillna(0.0)

    # --- [UPDATE] FILTER ZERO AMOUNTS ---
    transfers = transfers[transfers["Transfer Amount"].abs() > 1e-9].copy()

    if transfers.empty: return pd.DataFrame(), start_no

    # Validation
    map_acc = qbo_mappings.get('accounts', {})
    map_loc = qbo_mappings.get('locations', {})
    
    def validate_transfer_row(row):
        if not row["Transfer Funds From"]: return "ERROR | Missing From Account"
        if not row["Transfer Funds To"]: return "ERROR | Missing To Account"
        # Zero check removed
        if not find_id_in_map(map_acc, row["Transfer Funds From"]): return f"ERROR | 'From' Account not in QBO: '{row['Transfer Funds From']}'"
        if not find_id_in_map(map_acc, row["Transfer Funds To"]): return f"ERROR | 'To' Account not in QBO: '{row['Transfer Funds To']}'"
        if row["Transfer Funds From"] == row["Transfer Funds To"]: return "ERROR | 'From' and 'To' Accounts cannot be the same"
        loc_name = row.get("Location")
        if loc_name and not find_id_in_map(map_loc, loc_name): return f"ERROR | Location not in QBO: '{loc_name}'"
        return "Ready to sync"

    transfers["Remarks"] = transfers.apply(validate_transfer_row, axis=1)

    cols_order = ["No", "Ref No", "Transfer Funds From", "Transfer Funds To", "Transfer Amount", "Memo", "Date", "Location", "Currency", "Type", "Remarks"]
    for c in cols_order:
        if c not in transfers.columns: transfers[c] = ""
    transfers = transfers[cols_order]

    for col in transfers.select_dtypes(include=['datetime64', 'datetimetz']).columns:
        transfers[col] = transfers[col].astype(str)
    if "No" in transfers.columns:
        transfers["No"] = pd.to_numeric(transfers["No"], errors='coerce').fillna(0).astype(int)

    return transfers, start_no

# ==========================================
# 4. MAIN TRANSFORM ENTRY POINT
# ==========================================
def transform_raw(raw_df: pd.DataFrame, last_jv: int, last_exp: int, last_tr: int, qbo_mappings: Dict[str, dict] = None, existing_ids: Dict[str, dict] = None) -> TransformResult:
    if qbo_mappings is None: qbo_mappings = {'accounts': {}, 'locations': {}, 'classes': {}}
    if existing_ids is None: existing_ids = {'journals': {}, 'expenses': {}, 'transfers': {}}

    if raw_df is None or raw_df.empty:
        return TransformResult(pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), last_jv, last_exp, last_tr, None)

    df = raw_df.copy()

    # Shared Cleaning
    if settings.RAW_COL_NO in df.columns:
        df[settings.RAW_COL_NO] = pd.to_numeric(df[settings.RAW_COL_NO], errors="coerce")
    if "Category" in df.columns: df = df[df["Category"] != ""]

    # --- [CRITICAL FIX] ROBUST DATE PARSING ---
    if "Date" in df.columns:
        # 1. Try converting assuming they are Excel Numbers (e.g. 45002)
        # errors='coerce' turns text like "2023-01-01" into NaN temporarily
        numeric_dates = pd.to_numeric(df["Date"], errors="coerce")
        
        # 2. Convert the numbers to Datetimes
        date_results = pd.to_datetime(numeric_dates, origin="1899-12-30", unit="D", errors="coerce")
        
        # 3. For any rows that failed (NaN), try reading them as standard Text Dates
        mask_nan = date_results.isna()
        if mask_nan.any():
            # This catches "2023-01-01" or "1/1/2023" that the numeric parser missed
            date_results[mask_nan] = pd.to_datetime(df.loc[mask_nan, "Date"], errors="coerce")
            
        df["Date"] = date_results

    if "In/Out" in df.columns: df["In/Out"] = pd.to_numeric(df["In/Out"], errors="coerce")
    if "USD" in df.columns:
        df["USD"] = pd.to_numeric(df["USD"], errors="coerce")
        df = df[~df["USD"].isna()]

    # PASS MAPS DOWN
    final_jv, new_jv_no = process_journals(df, last_jv, qbo_mappings, existing_ids.get('journals'))
    final_exp, new_exp_no = process_expenses(df, last_exp, qbo_mappings, existing_ids.get('expenses'))
    final_tr, new_tr_no = process_transfers(df, last_tr, qbo_mappings, existing_ids.get('transfers'))

    max_row = int(df[settings.RAW_COL_NO].max()) if settings.RAW_COL_NO in df.columns and not df[settings.RAW_COL_NO].isna().all() else None

    return TransformResult(
        journals=final_jv,
        expenses=final_exp,
        withdraw=final_tr,
        last_journal_no=new_jv_no,
        last_expense_no=new_exp_no,
        last_withdraw_no=new_tr_no,
        max_row_processed=max_row
    )