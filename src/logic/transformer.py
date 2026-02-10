from __future__ import annotations
from dataclasses import dataclass
from typing import Tuple, Dict, Any
import pandas as pd
import numpy as np
import difflib  # <--- NEW IMPORT FOR FUZZY MATCHING
from config import settings
import re

# --- CONSTANTS ---
PREFIX = "KZO-JV" 

COL_NO = "No"
COL_DATE = "Date"
COL_USD = "USD - QBO"             
COL_METHOD = "QBO Method"         
COL_ACC_CR = "If Journal/Expense Method" 
COL_TR_FROM = "QBO Transfer Fr"
COL_TR_TO = "QBO Transfer To"
COL_TYPE = "Type"
COL_ITEM_DESC = "Item Description"
COL_CO = "CO"
COL_IN_OUT = "In/Out"
COL_BANK = "Account Fr"

def safe_to_float(series: pd.Series, decimals: int = 4) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")

    # remove infinite values
    s = s.replace([np.inf, -np.inf], np.nan)

    # cap extreme values (VERY important)
    MAX_ALLOWED = 1e9
    s = s.where(s.abs() < MAX_ALLOWED)

    # fill nulls
    s = s.fillna(0.0)

    # finally round
    return s.round(decimals)

def _normalize_df_headers(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans newlines and maps varying column names to standard constants."""
    df.columns = [str(c).replace('\n', ' ').strip() for c in df.columns]
    
    mapping = {
        "QBO Import Method (Journal/Expenses/Transfer)": COL_METHOD,
        "If Journal/Expense method: Another records": COL_ACC_CR,
        "If Transfer method: Fund Transfer From": COL_TR_FROM,
        "Transfer to ((Can copy from column H )": COL_TR_TO,
        "Transfer to": COL_TR_TO,
        "USD": COL_USD  
    }
    return df.rename(columns=mapping)

# --- UPDATED HELPER FUNCTION ---
def find_id_in_map(mapping_dict: dict, search_name: str) -> str | None:
    if not search_name or pd.isna(search_name) or str(search_name).strip() == "":
        return None
    
    # 1. Clean: Remove double spaces & trim
    clean_name = re.sub(r'\s+', ' ', str(search_name)).strip()

    # 2. Explicit Replacements (Hardcoded fixes)
    # Dictionary mapping: { "Text to Find": "Target Account Name in QBO" }
    replacements = {
        "CBD Z Card":   "KZO CBD Z",  # Catch variations
        "Leading Card MKT - 1238": "Leading Card - 1238"
    }

    # Check if we need to swap the name
    for bad_text, target_text in replacements.items():
        if bad_text.lower() in clean_name.lower():
            clean_name = target_text
            break

    search_lower = clean_name.lower()

    # 3. Exact Match (Case-insensitive)
    for qbo_name, qbo_id in mapping_dict.items():
        if qbo_name.lower() == search_lower: 
            return qbo_id
    
    # 4. Substring Match (Existing logic)
    # Be careful: "Bank" matches "Bank of America"
    for qbo_name, qbo_id in mapping_dict.items():
        if search_lower in qbo_name.lower(): 
            return qbo_id

    # 5. Fuzzy Match (New: ~85-90% Similarity)
    # 'difflib' finds the best match from the list of QBO names
    # cutoff=0.85 allows for small typos or extra characters (like "MKT")
    qbo_names = list(mapping_dict.keys())
    matches = difflib.get_close_matches(clean_name, qbo_names, n=1, cutoff=0.85)
    
    if matches:
        best_match = matches[0]
        # Optional: Print debug info to see what matched what
        print(matches)
        # print(f"   ✨ Fuzzy Matched: '{search_name}' -> '{best_match}'")
        return mapping_dict[best_match]

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
    print(f"\n--- DEBUG: Processing JOURNALS (Input Rows: {len(df)}) ---")
    SPECIAL_CASE = 'Check (Internal use)'
    print("JOURNALS:", df.columns)
    if df.empty:
        return pd.DataFrame(), start_no
        
    if COL_METHOD not in df.columns:
        return pd.DataFrame(), start_no
    
    # Dont change values if it is monthend
    df.loc[df[SPECIAL_CASE] == 'Monthend', COL_USD] *= -1

    mask_std = df[COL_METHOD].astype(str).str.contains("Journal", case=False, na=False)
    mask_reclass = df[COL_METHOD].astype(str).str.contains("Reclass", case=False, na=False)
    
    df_std = df[mask_std].copy()
    df_reclass = df[mask_reclass].copy()
    
    current_max = start_no
    processed_std = pd.DataFrame()
    processed_reclass = pd.DataFrame()

    # --- A. STANDARD JOURNALS ---
    if not df_std.empty:
        generated_ids = []
        for _, row in df_std.iterrows():
            s_no = int(float(str(row.get(COL_NO, 0))))
            if existing_ids and s_no in existing_ids:
                generated_ids.append(existing_ids[s_no])
            else:
                current_max += 1
                generated_ids.append(f"{PREFIX}{str(current_max).zfill(4)}")

        df_std["Journal No"] = generated_ids
        df_std["Currency Code"] = "USD"
        df_std["Name"] = df_std[COL_ITEM_DESC]
        
        # Debit
        deb = df_std.copy()
        deb["Amount"] =  safe_to_float(deb[COL_USD]) * -1

        # Change values when it is Monthend
        deb = deb.rename(columns={COL_ITEM_DESC: "Memo", COL_TYPE: "Account", COL_CO: "Location"})
        
        # Credit
        cred = df_std.copy()
        cred["Amount"] = pd.to_numeric(cred[COL_USD], errors='coerce').fillna(0.0)

        cred = cred.rename(columns={COL_ITEM_DESC: "Memo", COL_ACC_CR: "Account", COL_CO: "Location"})
        
        processed_std = pd.concat([deb, cred], ignore_index=True)

    # --- B. RECLASS JOURNALS ---
    if not df_reclass.empty:
        if COL_CO in df_reclass.columns: _fix_grp_location(df_reclass, COL_CO)
        
        df_reclass["_GroupDate"] = pd.to_datetime(df_reclass[COL_DATE]).dt.normalize()
        unique_dates = sorted(df_reclass["_GroupDate"].dropna().unique())
        
        date_map = {}
        for dt in unique_dates:
            current_max += 1
            date_map[dt] = f"{PREFIX}{str(current_max).zfill(4)}"
        
        df_reclass["Journal No"] = df_reclass["_GroupDate"].map(date_map)
        df_reclass.drop(columns=["_GroupDate"], inplace=True, errors='ignore')

        df_reclass["Amount"] = pd.to_numeric(df_reclass[COL_USD], errors='coerce').fillna(0.0)
        df_reclass["Currency Code"] = "USD"
        df_reclass["Class"] = ""
        df_reclass["Name"] = df_reclass[COL_ITEM_DESC]
        df_reclass = df_reclass.rename(columns={COL_ITEM_DESC: "Memo", COL_TYPE: "Account", COL_CO: "Location"})

        processed_reclass = df_reclass[["No", "Journal No", "Date", "Memo", "Account", "Amount", "Name", "Location", "Currency Code", "Class"]]


    # --- 3. Safe Combination ---
    total_journals = pd.concat([processed_std, processed_reclass], ignore_index=True)

    if total_journals.empty:
        return pd.DataFrame(), start_no

    total_journals = total_journals[total_journals["Amount"].abs() > 1e-9].copy()
    
    for col in total_journals.select_dtypes(include=['datetime64', 'datetimetz']).columns:
        total_journals[col] = total_journals[col].astype(str)
    
    # 1. Round everything to 2 decimals FIRST
    total_journals["Amount"] = total_journals["Amount"].astype(float).round(2)

    # 2. Recalculate diffs based on the rounded numbers
    diffs = total_journals.groupby("Journal No")["Amount"].sum()

    for journal_id, diff in diffs.items():
        if not np.isclose(diff, 0, atol=1e-3): # If sum is not zero
            if abs(diff) <= 0.50:
                mask = total_journals["Journal No"] == journal_id
                if not mask.any(): continue
                
                # 3. Find the last index of that journal to apply the fix
                target_idx = total_journals[mask].index[-1]
                # Subtract the diff (e.g., if sum is 0.01, subtract 0.01 from the last row)
                total_journals.loc[target_idx, "Amount"] -= diff

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
        
        # --- NEW FIND LOGIC ---
        if not find_id_in_map(map_acc, acc_name): 
            return f"ERROR | Account not found: '{acc_name}'"
            
        loc_name = row.get("Location")
        if loc_name and not find_id_in_map(map_loc, loc_name):
             return f"ERROR | Location not found: '{loc_name}'"
        
        
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
def process_expenses(df: pd.DataFrame, country: str,
                     start_no: int, qbo_mappings: Dict[str, dict], existing_ids: Dict[int, str] = None) -> Tuple[pd.DataFrame, int]:
    print(f"\n--- DEBUG: Processing EXPENSES (Input Rows: {len(df)}) ---")
    if df is None or df.empty: return pd.DataFrame(), start_no
    if existing_ids is None: existing_ids = {}

    # print(df[["No","CO", "COY", "Date", "Category", "USD - QBO"]])
    df[COL_USD] = pd.to_numeric(df[COL_USD], errors='coerce').fillna(0.0)
    d = df[df[COL_USD].round(2) != 0].copy()
    # --- LOGGING FILTER ---
    if len(df) != len(d):
        print(f"      ⚠️ Dropped {len(df) - len(d)} rows due to 0.00 amount.")
    # ----------------------
    if d.empty: return pd.DataFrame(), start_no

    d = d[[c for c in d.columns if "currency" not in c.lower()]]

    if COL_METHOD in d.columns: 
        d = d[d[COL_METHOD].astype(str).str.contains("Expense", case=False, na=False)]
    
    if COL_IN_OUT in d.columns:
        numeric_vals = pd.to_numeric(d[COL_IN_OUT], errors="coerce").fillna(0)
        d = d[numeric_vals < 0]

    if d.empty: 
        return pd.DataFrame(), start_no

    d["Payee (Dummy)"] = "Dummy"
    d["Payment Method"] = "Cash"
    d["Currency Code"] = "USD"
    d["Account (Cr)"] = d[COL_ACC_CR]
    
    d["Expense Line Amount"] = safe_to_float(d[COL_USD]) * -1
    d["Payment Date"] = pd.to_datetime(d[COL_DATE], errors="coerce")

    # ID GENERATION
    ref_nos = []
    for i, row in d.iterrows():
        s_no = int(row.get(COL_NO, 0))
        if existing_ids and s_no in existing_ids:
            ref_nos.append(existing_ids[s_no])
        else:
            mm_yy = row["Payment Date"].strftime("%m%y") if pd.notna(row["Payment Date"]) else "0000"
            start_no += 1 
            # FIX: Use dynamic country code instead of hardcoded 'PH'
            ref_nos.append(f"KZO{country}{mm_yy}E{str(start_no).zfill(4)}")
    
    d["Exp Ref. No"] = ref_nos

    rename_map = {
        COL_ITEM_DESC: "Memo",
        COL_CO: "Location",
        COL_TYPE: "Expense Account (Dr)",
        "Currency Code": "Currency"
    }
    d = d.rename(columns=rename_map)
    d["Expense Description"] = d["Memo"]

    _fix_grp_location(d, "Location")

    # Validation
    map_acc = qbo_mappings.get('accounts', {})
    map_loc = qbo_mappings.get('locations', {})

    def validate_expense_row(row):
        if not row["Account (Cr)"]: return "ERROR | Missing Source Account"
        if not row["Expense Account (Dr)"]: return "ERROR | Missing Expense Account"
        if pd.isna(row["Payment Date"]): return "ERROR | Missing Date"
        
        # --- NEW FIND LOGIC ---
        if not find_id_in_map(map_acc, row["Account (Cr)"]): 
            return f"ERROR | Source Account not in QBO: '{row['Account (Cr)']}'"
            
        if not find_id_in_map(map_acc, row["Expense Account (Dr)"]): 
            return f"ERROR | Expense Account not in QBO: '{row['Expense Account (Dr)']}'"
            
        loc_name = row.get("Location")
        if loc_name and not find_id_in_map(map_loc, loc_name): 
            return f"ERROR | Location not in QBO: '{loc_name}'"
        return "Ready to sync"

    d["Remarks"] = d.apply(validate_expense_row, axis=1)

    cols_order = ["No", "Exp Ref. No", "Account (Cr)", "Payee (Dummy)", "Memo", "Payment Date", "Payment Method", "Expense Account (Dr)", "Expense Description", "Expense Line Amount", "Currency", "Location", "Remarks"]
    for c in cols_order:
        if c not in d.columns: d[c] = ""
    
    d = d[cols_order].copy()
    for col in d.select_dtypes(include=["datetime64", "datetimetz"]).columns:
        d[col] = d[col].astype(str)
    
    return d, start_no

# ==========================================
# 3. PROCESS TRANSFERS
# ==========================================
def process_transfers(df: pd.DataFrame, country: str,
                      start_no: int, qbo_mappings: Dict[str, dict], existing_ids: Dict[int, str] = None) -> tuple[pd.DataFrame, int]:
    print(f"\n--- DEBUG: Processing TRANSFERS (Input Rows: {len(df)}) ---")
    if df.empty: return pd.DataFrame(), start_no
    if existing_ids is None: existing_ids = {}

    df[COL_USD] = pd.to_numeric(df[COL_USD], errors='coerce').fillna(0.0)
    transfers = df[df[COL_USD].round(2) != 0].copy()

    if COL_METHOD in transfers.columns:
        transfers = transfers[transfers[COL_METHOD].astype(str).str.contains("Transfer", case=False, na=False)]
    
    if transfers.empty: 
        return pd.DataFrame(), start_no

    transfers["Transfer Amount"] = transfers[COL_USD].abs()
    transfers["Currency"] = "USD"
    
    # ID GENERATION
    ref_nos = []
    for i, row in transfers.iterrows():
        s_no = int(row.get(COL_NO, 0))
        if existing_ids and s_no in existing_ids:
            ref_nos.append(existing_ids[s_no])
        else:
            dt = pd.to_datetime(row[COL_DATE], errors='coerce')
            date_str = dt.strftime('%m%y') if pd.notna(dt) else "0000"
            start_no += 1
            ref_nos.append(f"KZO{country}{date_str}T{str(start_no).zfill(4)}")

    transfers["Ref No"] = ref_nos

    rename_map = {
        COL_TR_FROM: "Transfer Funds From",
        COL_TR_TO: "Transfer Funds To",
        COL_ITEM_DESC: "Memo",
        COL_CO: "Location",
    }
    transfers = transfers.rename(columns=rename_map)
    transfers["Memo"] = transfers["Ref No"] + " - " + transfers["Memo"].astype(str)
    
    _fix_grp_location(transfers, "Location")

    # Validation
    map_acc = qbo_mappings.get('accounts', {})
    map_loc = qbo_mappings.get('locations', {})
    
    def validate_transfer_row(row):
        if not row["Transfer Funds From"]: return "ERROR | Missing From Account"
        if not row["Transfer Funds To"]: return "ERROR | Missing To Account"
        
        # --- NEW FIND LOGIC ---
        if not find_id_in_map(map_acc, row["Transfer Funds From"]): 
            return f"ERROR | 'From' Account not in QBO: '{row['Transfer Funds From']}'"
            
        if not find_id_in_map(map_acc, row["Transfer Funds To"]): 
            return f"ERROR | 'To' Account not in QBO: '{row['Transfer Funds To']}'"
            
        if row["Transfer Funds From"] == row["Transfer Funds To"]: 
            return "ERROR | 'From' and 'To' Accounts cannot be the same"
            
        loc_name = row.get("Location")
        if loc_name and not find_id_in_map(map_loc, loc_name): 
            return f"ERROR | Location not in QBO: '{loc_name}'"
        return "Ready to sync"

    transfers["Remarks"] = transfers.apply(validate_transfer_row, axis=1)

    cols_order = ["No", "Ref No", "Transfer Funds From", "Transfer Funds To", "Transfer Amount", "Memo", COL_DATE, "Location", "Currency", COL_TYPE, "Remarks"]
    for c in cols_order:
        if c not in transfers.columns: transfers[c] = ""
    
    return transfers[cols_order], start_no

# ==========================================
# 4. MAIN TRANSFORM ENTRY POINT
# ==========================================
def transform_raw(raw_df: pd.DataFrame, country: str,
                  last_jv: int, last_exp: int, last_tr: int, qbo_mappings: Dict[str, dict] = None, existing_ids: Dict[str, dict] = None) -> TransformResult:
    if raw_df is None or raw_df.empty:
        return TransformResult(pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), last_jv, last_exp, last_tr, None)

    df = _normalize_df_headers(raw_df.copy())

    # Shared Cleaning
    if COL_NO in df.columns:
        df[COL_NO] = pd.to_numeric(df[COL_NO], errors="coerce").fillna(0).astype(int)

    if "Category" in df.columns: 
        df = df[df["Category"].fillna("").astype(str).str.strip() != ""]

    if COL_DATE in df.columns:
        numeric_dates = pd.to_numeric(df[COL_DATE], errors="coerce")
        date_results = pd.to_datetime(numeric_dates, origin="1899-12-30", unit="D", errors="coerce")
        mask_nan = date_results.isna()
        if mask_nan.any():
            date_results[mask_nan] = pd.to_datetime(df.loc[mask_nan, COL_DATE], errors="coerce")
        df[COL_DATE] = date_results

    if COL_IN_OUT in df.columns: 
        df[COL_IN_OUT] = pd.to_numeric(df[COL_IN_OUT], errors="coerce").fillna(0)

    if COL_USD in df.columns:
        df[COL_USD] = pd.to_numeric(df[COL_USD], errors="coerce").fillna(0.0)
        df = df[~df[COL_USD].isna()]

    final_jv, new_jv_no = process_journals(df, last_jv, qbo_mappings, existing_ids.get('journals') if existing_ids else None)
    final_exp, new_exp_no = process_expenses(df, country, last_exp, qbo_mappings, existing_ids.get('expenses') if existing_ids else None)
    final_tr, new_tr_no = process_transfers(df, country, last_tr, qbo_mappings, existing_ids.get('transfers') if existing_ids else None)

    max_row = int(df[COL_NO].max()) if not df.empty else None

    return TransformResult(
        journals=final_jv,
        expenses=final_exp,
        withdraw=final_tr,
        last_journal_no=new_jv_no,
        last_expense_no=new_exp_no,
        last_withdraw_no=new_tr_no,
        max_row_processed=max_row
    )