from __future__ import annotations
from dataclasses import dataclass
from typing import Tuple, Dict, Any
import pandas as pd
import numpy as np
import difflib
from config import settings
import re

# --- CONSTANTS ---
DEFAULT_JV_PREFIX = "KZO-JV"
DEFAULT_DOC_PREFIX = "KZO"
KZP_JV_PREFIX = "KZP-JV"
KZP_DOC_PREFIX = "KZP"
KZDW_JV_PREFIX = "KZDW-JV"
KZDW_DOC_PREFIX = "KZDW"

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

def safe_to_float(series: pd.Series, decimals: int = 4) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    s = s.replace([np.inf, -np.inf], np.nan)
    MAX_ALLOWED = 1e9
    s = s.where(s.abs() < MAX_ALLOWED)
    s = s.fillna(0.0)
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
    if pd.isna(search_name) or str(search_name).strip() == "":
        return None
    
    # 1. Clean: Remove double spaces & trim
    clean_name = re.sub(r'\s+', ' ', str(search_name)).strip()
    search_lower = clean_name.lower()

    # 2. Explicit Replacements (Hardcoded fixes)
    replacements = {
        "CBD Z Card":   "KZO CBD Z",
        "Leading Card MKT - 1238": "Leading Card - 1238"
    }

    for bad_text, target_text in replacements.items():
        # Check if the bad text exists (Case Insensitive)
        if bad_text.lower() in clean_name.lower():
            # regex sub: Replace ONLY the bad_text part with target_text
            # flags=re.IGNORECASE ensures "cbd z card" matches "CBD Z Card"
            clean_name = re.sub(re.escape(bad_text), target_text, clean_name, flags=re.IGNORECASE)
            
            # Update the search variable for the next steps
            search_lower = clean_name.lower()
            break

    # --- STRATEGY 1: EXACT FULL MATCH ---
    # Checks "Fixed Assets:Equipment" == "Fixed Assets:Equipment"
    for qbo_name, qbo_id in mapping_dict.items():
        if qbo_name.lower() == search_lower: 
            print(f"   ✅ [Mapping] EXACT: '{search_name}' -> '{qbo_name}'")
            return qbo_id
    
    # --- STRATEGY 2: LEAF NODE MATCH (Split by :) ---
    # Checks "Equipment" == "Fixed Assets:Equipment" (Splits QBO name)
    # This solves the "Equipment" vs "Accumulated..." issue
    for qbo_name, qbo_id in mapping_dict.items():
        # Get the part after the last colon (the actual account name)
        # e.g. "Fixed Assets:Equipment" -> "Equipment"
        if ":" in qbo_name:
            leaf_name = qbo_name.split(":")[-1].strip()
            if leaf_name.lower() == search_lower:
                # print(f"   ✅ [Mapping] LEAF MATCH: '{search_name}' -> '{qbo_name}'")
                return qbo_id

    # --- STRATEGY 3: STRICT FUZZY MATCH (90%) ---
    # Only allows very close matches (typos), rejects partial substrings
    qbo_names = list(mapping_dict.keys())
    # cutoff=0.9 ensures "Equipment" does NOT match "Accumulated..."
    matches = difflib.get_close_matches(clean_name, qbo_names, n=1, cutoff=0.80)
    
    if matches:
        best_match = matches[0]
        print(f"   ✨ [Mapping] FUZZY (80%): '{search_name}' -> '{best_match}'")
        return mapping_dict[best_match]

    print(f"   ❌ [Mapping] FAILED: Could not find '{search_name}'")
    return None

def _fix_grp_location(df: pd.DataFrame, col_name: str = "Location"):
    if col_name in df.columns:
        df[col_name] = df[col_name].apply(
            lambda x: "GROUP" if str(x).strip().upper() == "GRP" else x
        )


def _is_blank(value: Any) -> bool:
    if pd.isna(value):
        return True
    return str(value).strip() == ""


def _is_kzp_case(client_name: str = "") -> bool:
    return "kzp" in str(client_name).lower()


def _is_kzo_case(client_name: str = "") -> bool:
    return "kzo" in str(client_name).lower()


def _is_kzdw_case(client_name: str = "") -> bool:
    return "kzdw" in str(client_name).lower()

def _should_check_currency_transfer_only(client_name: str = "") -> bool:
    """
    Workspaces where currency validation should run only for Transfer,
    not for Journal/Expense.
    """
    return _is_kzdw_case(client_name)

def _should_check_transfer_currency(client_name: str = "") -> bool:
    return _is_kzdw_case(client_name)


def _normalize_currency(value: Any) -> str:
    text = str(value).strip() if not pd.isna(value) else ""
    if text == "":
        return "USD"
    return "USD" if "USD" in text.upper() else text

def _normalize_currency_code(value: Any) -> str:
    text = _normalize_currency(value).strip().upper()
    if not text:
        return "USD"
    return text[:3]

def _build_currency_exchange_series(
    df: pd.DataFrame,
    currency_col: str,
    rate_col: str = "Currency Rate",
) -> pd.Series:
    out = pd.Series(np.nan, index=df.index, dtype="float64")
    if rate_col in df.columns:
        raw_vals = df[rate_col].astype(str).str.replace(",", "", regex=False).str.strip()
        parsed = pd.to_numeric(raw_vals, errors="coerce")
        out = parsed.where(parsed > 0)

    if currency_col in df.columns:
        usd_mask = df[currency_col].apply(_normalize_currency_code) == "USD"
        out.loc[usd_mask & out.isna()] = 1.0
    return out

def _account_currency_from_id(qbo_mappings: Dict[str, dict], account_id: str | None) -> str | None:
    if not account_id:
        return None
    acc_meta = (qbo_mappings or {}).get("accounts_meta", {})
    raw_currency = (acc_meta.get(account_id) or {}).get("currency")
    if not raw_currency:
        return None
    return _normalize_currency_code(raw_currency)

def _currency_mismatch_error(
    row_no: Any,
    file_currency: Any,
    account_checks: list[tuple[str, str, str | None]],
) -> str | None:
    """
    account_checks: [(label, account_name, qbo_account_currency_or_none), ...]
    """
    file_ccy = _normalize_currency_code(file_currency)
    mismatches = []
    for label, acc_name, qbo_ccy in account_checks:
        if not qbo_ccy:
            continue
        if qbo_ccy != file_ccy:
            mismatches.append(f"{label} '{acc_name}'={qbo_ccy}")

    if not mismatches:
        return None
    return (
        f"ERROR | Currency mismatch: File={file_ccy} | "
        f"{'; '.join(mismatches)} | Row No: {row_no}"
    )


def _build_id_prefixes(client_name: str = "") -> tuple[str, str]:
    if _is_kzp_case(client_name):
        return KZP_JV_PREFIX, KZP_DOC_PREFIX
    if _is_kzdw_case(client_name):
        return KZDW_JV_PREFIX, KZDW_DOC_PREFIX
    return DEFAULT_JV_PREFIX, DEFAULT_DOC_PREFIX

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
def process_journals(df: pd.DataFrame, start_no: int, qbo_mappings: Dict[str, dict], existing_ids: Dict[int, str] = None, client_name: str = "") -> tuple[pd.DataFrame, int]:
    print(f"\n--- DEBUG: Processing JOURNALS (Input Rows: {len(df)}) ---")
    SPECIAL_CASE = 'Reclass'
    
    if df.empty: return pd.DataFrame(), start_no
    if COL_METHOD not in df.columns: return pd.DataFrame(), start_no
    
    # KZP special rule: Type == Reimbursements keeps Journal debit/credit behavior,
    # but Journal No is grouped by date (handled in ID generation below).
    mask_kzp_reimbursements = pd.Series(False, index=df.index)
    type_series = df[COL_TYPE] if COL_TYPE in df.columns else pd.Series("", index=df.index)
    if _is_kzp_case(client_name) and COL_TYPE in df.columns:
        mask_kzp_reimbursements = (
            type_series.astype(str).str.strip().str.lower() == "reimbursements"
        )

    # Keep true Reclass behavior for non-reimbursements only.
    df.loc[(df[SPECIAL_CASE] == 'Reclass') & (~mask_kzp_reimbursements), COL_USD] *= -1

    mask_method_journal = df[COL_METHOD].astype(str).str.contains("Journal", case=False, na=False)
    mask_method_reclass = df[COL_METHOD].astype(str).str.contains("Reclass", case=False, na=False)

    # Reimbursements should use Journal debit/credit account logic.
    mask_std = mask_method_journal | mask_kzp_reimbursements
    # Reimbursements are excluded from Reclass single-line processing.
    mask_reclass = mask_method_reclass & (~mask_kzp_reimbursements)
    
    df_std = df[mask_std].copy()
    df_reclass = df[mask_reclass].copy()
    
    current_max = start_no
    processed_std = pd.DataFrame()
    processed_reclass = pd.DataFrame()

    # --- A. STANDARD JOURNALS ---
    if not df_std.empty:
        df_std = df_std.copy()
        # Keep source row order so debit/credit lines can be written as grouped pairs.
        df_std["_LineGroupOrder"] = range(len(df_std))
        jv_prefix, _ = _build_id_prefixes(client_name)
        is_date_group_workspace = (
            (_is_kzo_case(client_name) and not _is_kzp_case(client_name))
            or _is_kzdw_case(client_name)
        )
        date_group_map = {}
        if is_date_group_workspace and COL_DATE in df_std.columns:
            grouped_dates = pd.to_datetime(df_std[COL_DATE], errors="coerce").dt.normalize()
        else:
            grouped_dates = pd.Series(pd.NaT, index=df_std.index)

        if _is_kzdw_case(client_name):
            currency_codes = df_std["Currency"].apply(_normalize_currency_code) if "Currency" in df_std.columns else pd.Series("USD", index=df_std.index)
        else:
            currency_codes = pd.Series("USD", index=df_std.index)

        reimbursements_in_std = pd.Series(False, index=df_std.index)
        reimburse_date_map = {}
        if _is_kzp_case(client_name) and COL_TYPE in df_std.columns:
            reimbursements_in_std = (
                df_std[COL_TYPE].astype(str).str.strip().str.lower() == "reimbursements"
            )
            if reimbursements_in_std.any() and COL_DATE in df_std.columns:
                reimburse_dates = pd.to_datetime(df_std[COL_DATE], errors="coerce").dt.normalize()
            else:
                reimburse_dates = pd.Series(pd.NaT, index=df_std.index)
        else:
            reimburse_dates = pd.Series(pd.NaT, index=df_std.index)

        generated_ids = []
        for idx, row in df_std.iterrows():
            s_no = int(float(str(row.get(COL_NO, 0))))
            if existing_ids and s_no in existing_ids:
                existing_id = existing_ids[s_no]
                generated_ids.append(existing_id)
                if is_date_group_workspace:
                    dt = grouped_dates.loc[idx]
                    if pd.notna(dt):
                        key = (dt, currency_codes.loc[idx]) if _is_kzdw_case(client_name) else dt
                        date_group_map[key] = existing_id
            elif is_date_group_workspace:
                dt = grouped_dates.loc[idx]
                if pd.notna(dt):
                    key = (dt, currency_codes.loc[idx]) if _is_kzdw_case(client_name) else dt
                    if key not in date_group_map:
                        current_max += 1
                        date_group_map[key] = f"{jv_prefix}{str(current_max).zfill(4)}"
                    generated_ids.append(date_group_map[key])
                else:
                    current_max += 1
                    generated_ids.append(f"{jv_prefix}{str(current_max).zfill(4)}")
            elif reimbursements_in_std.loc[idx]:
                dt = reimburse_dates.loc[idx]
                if pd.notna(dt):
                    if dt not in reimburse_date_map:
                        current_max += 1
                        reimburse_date_map[dt] = f"{jv_prefix}{str(current_max).zfill(4)}"
                    generated_ids.append(reimburse_date_map[dt])
                else:
                    current_max += 1
                    generated_ids.append(f"{jv_prefix}{str(current_max).zfill(4)}")
            else:
                current_max += 1
                generated_ids.append(f"{jv_prefix}{str(current_max).zfill(4)}")

        df_std["Journal No"] = generated_ids
        if _is_kzdw_case(client_name):
            df_std["Currency Code"] = currency_codes
            df_std["Currency Exchange"] = _build_currency_exchange_series(df_std, "Currency Code")
        else:
            df_std["Currency Code"] = "USD"
            df_std["Currency Exchange"] = ""
        df_std["Name"] = df_std[COL_ITEM_DESC]
        
        # Standard mapping:
        # - KZP: Debit uses Type, Credit uses Bank/Account Fr
        # - Others: Debit uses Bank/Account Fr, Credit uses If Journal/Expense Method
        deb = df_std.copy()
        deb["Amount"] =  safe_to_float(deb[COL_USD]) * -1
        if _is_kzp_case(client_name):
            deb = deb.rename(columns={COL_ITEM_DESC: "Memo", COL_CO: "Location"})
            if COL_TYPE in deb.columns:
                deb["Account"] = deb[COL_TYPE]
            else:
                deb["Account"] = ""
            if COL_BANK in deb.columns:
                deb["Account"] = deb["Account"].where(
                    deb["Account"].astype(str).str.strip() != "",
                    deb[COL_BANK],
                )
                deb["Account"] = deb["Account"].fillna(deb[COL_BANK])
        else:
            deb = deb.rename(columns={COL_ITEM_DESC: "Memo", COL_BANK: "Account", COL_CO: "Location"})
            # Fallback for old layouts where debit account sits in Type.
            if COL_TYPE in deb.columns:
                deb["Account"] = deb["Account"].where(
                    deb["Account"].astype(str).str.strip() != "",
                    deb[COL_TYPE],
                )
                deb["Account"] = deb["Account"].fillna(deb[COL_TYPE])
        # Fill NA locations with raw CO value
        deb["Location"] = deb["Location"].fillna(df_std[COL_CO])
        deb["_LineRole"] = 0
        
        # Credit
        cred = df_std.copy()
        cred["Amount"] = pd.to_numeric(cred[COL_USD], errors='coerce').fillna(0.0)
        if _is_kzp_case(client_name):
            cred = cred.rename(columns={COL_ITEM_DESC: "Memo", COL_CO: "Location"})
            if COL_BANK in cred.columns:
                cred["Account"] = cred[COL_BANK]
            elif COL_ACC_CR in cred.columns:
                cred["Account"] = cred[COL_ACC_CR]
            else:
                cred["Account"] = ""
            if COL_ACC_CR in cred.columns:
                cred["Account"] = cred["Account"].where(
                    cred["Account"].astype(str).str.strip() != "",
                    cred[COL_ACC_CR],
                )
                cred["Account"] = cred["Account"].fillna(cred[COL_ACC_CR])
        else:
            cred = cred.rename(columns={COL_ITEM_DESC: "Memo", COL_ACC_CR: "Account", COL_CO: "Location"})
        # Fill NA locations with raw CO value
        cred["Location"] = cred["Location"].fillna(df_std[COL_CO])
        cred["_LineRole"] = 1
        
        processed_std = pd.concat([deb, cred], ignore_index=True)
        processed_std = processed_std.sort_values(
            by=["Journal No", "_LineGroupOrder", "_LineRole"],
            kind="stable",
        ).reset_index(drop=True)

    # --- B. RECLASS JOURNALS ---
    if not df_reclass.empty:
        jv_prefix, _ = _build_id_prefixes(client_name)
        if COL_CO in df_reclass.columns: _fix_grp_location(df_reclass, COL_CO)
        
        df_reclass["_GroupDate"] = pd.to_datetime(df_reclass[COL_DATE]).dt.normalize()
        if _is_kzdw_case(client_name):
            df_reclass["_CurrencyCode"] = df_reclass["Currency"].apply(_normalize_currency_code) if "Currency" in df_reclass.columns else "USD"
            unique_groups = sorted(df_reclass[["_GroupDate", "_CurrencyCode"]].dropna().drop_duplicates().to_records(index=False))
        else:
            unique_groups = sorted(df_reclass["_GroupDate"].dropna().unique())

        date_map = {}
        for grp in unique_groups:
            current_max += 1
            if _is_kzdw_case(client_name):
                date_map[grp] = f"{jv_prefix}{str(current_max).zfill(4)}"
            else:
                date_map[grp] = f"{jv_prefix}{str(current_max).zfill(4)}"

        if _is_kzdw_case(client_name):
            df_reclass["Journal No"] = df_reclass.apply(
                lambda r: date_map.get((r["_GroupDate"], r["_CurrencyCode"]), "") if pd.notna(r["_GroupDate"]) else "",
                axis=1,
            )
        else:
            df_reclass["Journal No"] = df_reclass["_GroupDate"].map(date_map)

        df_reclass.drop(columns=["_GroupDate", "_CurrencyCode"], inplace=True, errors='ignore')

        df_reclass["Amount"] = pd.to_numeric(df_reclass[COL_USD], errors='coerce').fillna(0.0)
        if _is_kzdw_case(client_name):
            df_reclass["Currency Code"] = df_reclass["Currency"].apply(_normalize_currency_code) if "Currency" in df_reclass.columns else "USD"
            df_reclass["Currency Exchange"] = _build_currency_exchange_series(df_reclass, "Currency Code")
        else:
            df_reclass["Currency Code"] = "USD"
            df_reclass["Currency Exchange"] = ""
        df_reclass["Class"] = ""
        df_reclass["Name"] = df_reclass[COL_ITEM_DESC]
        df_reclass = df_reclass.rename(columns={COL_ITEM_DESC: "Memo", COL_BANK: "Account", COL_CO: "Location"})
        # Reclass follows the same fallback as standard journals.
        if COL_TYPE in df_reclass.columns:
            df_reclass["Account"] = df_reclass["Account"].where(
                df_reclass["Account"].astype(str).str.strip() != "",
                df_reclass[COL_TYPE],
            )
            df_reclass["Account"] = df_reclass["Account"].fillna(df_reclass[COL_TYPE])
        # Fill NA locations with raw CO value
        df_reclass["Location"] = df_reclass["Location"].fillna(df_reclass[COL_CO] if COL_CO in df_reclass.columns else "")
        df_reclass["_LineGroupOrder"] = range(len(df_reclass))
        df_reclass["_LineRole"] = 0

        processed_reclass = df_reclass[["No", "Journal No", "Date", "Memo", "Account", "Amount", "Name", "Location", "Currency Code", "Currency Exchange", "Class", "_LineGroupOrder", "_LineRole"]]

    # --- 3. Safe Combination ---
    total_journals = pd.concat([processed_std, processed_reclass], ignore_index=True)

    if total_journals.empty:
        return pd.DataFrame(), start_no

    total_journals = total_journals[total_journals["Amount"].abs() > 1e-9].copy()
    if "_LineGroupOrder" not in total_journals.columns:
        total_journals["_LineGroupOrder"] = 0
    if "_LineRole" not in total_journals.columns:
        total_journals["_LineRole"] = 0
     
    for col in total_journals.select_dtypes(include=['datetime64', 'datetimetz']).columns:
        total_journals[col] = total_journals[col].astype(str)
    
    total_journals["Amount"] = total_journals["Amount"].astype(float).round(2)
    diffs = total_journals.groupby("Journal No")["Amount"].sum()

    for journal_id, diff in diffs.items():
        if not np.isclose(diff, 0, atol=1e-3):
            if abs(diff) <= 0.50:
                mask = total_journals["Journal No"] == journal_id
                if not mask.any(): continue
                target_idx = total_journals[mask].index[-1]
                total_journals.loc[target_idx, "Amount"] -= diff

    # Keep each Journal No contiguous in output (debit/credit grouped together).
    total_journals = total_journals.sort_values(
        by=["Journal No", "_LineGroupOrder", "_LineRole"],
        kind="stable",
    ).reset_index(drop=True)

    # --- VALIDATION ---
    balance_map = total_journals.groupby("Journal No")["Amount"].sum()
    unbalanced_ids = balance_map[abs(balance_map) > 0.01].index.tolist()

    map_acc = qbo_mappings.get('accounts', {})
    map_loc = qbo_mappings.get('locations', {})

    def validate_journal_row(row):
        row_no = row.get("No", "")
        if row["Journal No"] in unbalanced_ids:
            diff = round(balance_map[row["Journal No"]], 2)
            return f"ERROR | Unbalanced ({diff}) | Row No: {row_no}"
        acc_name = row["Account"]
        if _is_blank(acc_name):
            return f"ERROR | Missing Account Name | Row No: {row_no}"
        
        acc_id = find_id_in_map(map_acc, acc_name)
        if not acc_id:
            return f"ERROR | Account not found: '{acc_name}' | Row No: {row_no}"

        if _is_kzdw_case(client_name) and not _should_check_currency_transfer_only(client_name):
            mismatch_error = _currency_mismatch_error(
                row_no=row_no,
                file_currency=row.get("Currency Code", "USD"),
                account_checks=[("Account", acc_name, _account_currency_from_id(qbo_mappings, acc_id))],
            )
            if mismatch_error:
                return mismatch_error
             
        loc_name = row.get("Location")
        if (not _is_blank(loc_name)) and not find_id_in_map(map_loc, loc_name):
             return f"ERROR | Location not found: '{loc_name}' | Row No: {row_no}"
        
        return "Ready to sync"

    total_journals["Remarks"] = total_journals.apply(validate_journal_row, axis=1)
    total_journals["Class"] = total_journals.get("Location", "").fillna("")

    if _is_kzdw_case(client_name):
        cols_order = ["No", "Journal No", "Date", "Memo", "Account", "Amount", "Name", "Location", "Currency Code", "Currency Exchange", "Class", "Remarks"]
    else:
        cols_order = ["No", "Journal No", "Date", "Memo", "Account", "Amount", "Name", "Location", "Currency Code", "Class", "Remarks"]
    for c in cols_order:
        if c not in total_journals.columns: total_journals[c] = ""
    
    total_journals["Amount"] = total_journals["Amount"].astype(float).round(2)
    return total_journals[cols_order], current_max

# ==========================================
# 2. PROCESS EXPENSES
# ==========================================
def process_expenses(df: pd.DataFrame, country: str,
                     start_no: int, qbo_mappings: Dict[str, dict], existing_ids: Dict[int, str] = None, client_name: str = "") -> Tuple[pd.DataFrame, int]:
    print(f"\n--- DEBUG: Processing EXPENSES (Input Rows: {len(df)}) ---")
    if df is None or df.empty: return pd.DataFrame(), start_no
    if existing_ids is None: existing_ids = {}

    df[COL_USD] = pd.to_numeric(df[COL_USD], errors='coerce').fillna(0.0)
    d = df[df[COL_USD].round(2) != 0].copy()
    
    if len(df) != len(d):
        print(f"      ⚠️ Dropped {len(df) - len(d)} rows due to 0.00 amount.")
        
    if d.empty: return pd.DataFrame(), start_no

    if _is_kzdw_case(client_name):
        # Keep "Currency" for KZDW while dropping transfer-side currency columns.
        d = d[[c for c in d.columns if c.lower() not in {"currency to", "currency from"}]]
    else:
        d = d[[c for c in d.columns if "currency" not in c.lower()]]

    if COL_METHOD in d.columns: 
        d = d[d[COL_METHOD].astype(str).str.contains("Expense", case=False, na=False)]

    # KZP reimbursements are journal-only; do not duplicate into Expense tab.
    if _is_kzp_case(client_name) and COL_TYPE in d.columns:
        d = d[d[COL_TYPE].astype(str).str.strip().str.lower() != "reimbursements"]
    
    if COL_IN_OUT in d.columns:
        numeric_vals = pd.to_numeric(d[COL_IN_OUT], errors="coerce").fillna(0)
        d = d[numeric_vals < 0]

    if d.empty: 
        return pd.DataFrame(), start_no

    d["Payee (Dummy)"] = "Dummy"
    d["Payment Method"] = "Cash"
    if _is_kzdw_case(client_name) and "Currency" in d.columns:
        d["Currency Code"] = d["Currency"].apply(_normalize_currency)
        d["Currency Exchange"] = _build_currency_exchange_series(d, "Currency Code")
    else:
        d["Currency Code"] = "USD"
        d["Currency Exchange"] = ""
    # Expense source account:
    # Prefer Transfer From (W) and fall back to If Journal/Expense Method (V).
    if COL_TR_FROM in d.columns:
        transfer_from_vals = d[COL_TR_FROM]
        fallback_vals = d[COL_ACC_CR] if COL_ACC_CR in d.columns else ""
        d["Account (Cr)"] = transfer_from_vals.where(
            transfer_from_vals.astype(str).str.strip() != "",
            fallback_vals,
        )
        d["Account (Cr)"] = d["Account (Cr)"].fillna(fallback_vals)
    else:
        d["Account (Cr)"] = d[COL_ACC_CR]
    
    d["Expense Line Amount"] = safe_to_float(d[COL_USD]) * -1
    d["Payment Date"] = pd.to_datetime(d[COL_DATE], errors="coerce")

    # ID GENERATION
    _, doc_prefix = _build_id_prefixes(client_name)
    ref_nos = []
    for i, row in d.iterrows():
        s_no = int(row.get(COL_NO, 0))
        if existing_ids and s_no in existing_ids:
            ref_nos.append(existing_ids[s_no])
        else:
            mm_yy = row["Payment Date"].strftime("%m%y") if pd.notna(row["Payment Date"]) else "0000"
            start_no += 1 
            if _is_kzp_case(client_name):
                ref_nos.append(f"{doc_prefix}{mm_yy}E{str(start_no).zfill(4)}")
            elif _is_kzdw_case(client_name):
                ref_nos.append(f"{doc_prefix}{mm_yy}E{str(start_no).zfill(4)}")
            else:
                ref_nos.append(f"{doc_prefix}{country}{mm_yy}E{str(start_no).zfill(4)}")
    
    d["Exp Ref. No"] = ref_nos

    rename_map = {
        COL_ITEM_DESC: "Memo",
        COL_CO: "Location",
        COL_TYPE: "Expense Account (Dr)",
        "Currency Code": "Currency"
    }
    # Avoid duplicate "Currency" headers when raw currency is still present (e.g. KZDW).
    if "Currency" in d.columns and "Currency Code" in d.columns:
        d = d.drop(columns=["Currency"])
    d = d.rename(columns=rename_map)
    d["Expense Description"] = d["Memo"]

    _fix_grp_location(d, "Location")
    # Fill NA locations with raw CO value
    d["Location"] = d["Location"].fillna(d.get(COL_CO, ""))
    d["Class"] = d["Location"].fillna("")

    # Validation
    map_acc = qbo_mappings.get('accounts', {})
    map_loc = qbo_mappings.get('locations', {})
    
    # DEBUG: Log account mappings for troubleshooting
    if not map_acc:
        print(f"   ⚠️ WARNING: No accounts in QBO mappings for country={country}. Check realm ID.")

    def validate_expense_row(row):
        row_no = row.get("No", "")
        if _is_blank(row["Account (Cr)"]):
            return f"ERROR | Missing Source Account | Row No: {row_no}"
        if _is_blank(row["Expense Account (Dr)"]):
            return f"ERROR | Missing Expense Account | Row No: {row_no}"
        if pd.isna(row["Payment Date"]):
            return f"ERROR | Missing Date | Row No: {row_no}"
        
        # --- MAPPING CHECKS ---
        src_acc = row["Account (Cr)"]
        exp_acc = row["Expense Account (Dr)"]
        
        src_acc_id = find_id_in_map(map_acc, src_acc)
        if not src_acc_id:
            available = ', '.join(list(map_acc.keys())[:3]) if map_acc else 'NONE'
            return f"ERROR | Source Account not in QBO: '{src_acc}' | Available: {available}... | Row No: {row_no}"
             
        exp_acc_id = find_id_in_map(map_acc, exp_acc)
        if not exp_acc_id:
            available = ', '.join(list(map_acc.keys())[:3]) if map_acc else 'NONE'
            return f"ERROR | Expense Account not in QBO: '{exp_acc}' | Available: {available}... | Row No: {row_no}"

        if _is_kzdw_case(client_name) and not _should_check_currency_transfer_only(client_name):
            mismatch_error = _currency_mismatch_error(
                row_no=row_no,
                file_currency=row.get("Currency", "USD"),
                account_checks=[
                    ("Source", src_acc, _account_currency_from_id(qbo_mappings, src_acc_id)),
                    ("Expense", exp_acc, _account_currency_from_id(qbo_mappings, exp_acc_id)),
                ],
            )
            if mismatch_error:
                return mismatch_error
             
        loc_name = row.get("Location")
        if (not _is_blank(loc_name)) and not find_id_in_map(map_loc, loc_name): 
            return f"ERROR | Location not in QBO: '{loc_name}' | Row No: {row_no}"
        return "Ready to sync"

    d["Remarks"] = d.apply(validate_expense_row, axis=1)

    if _is_kzdw_case(client_name):
        cols_order = ["No", "Exp Ref. No", "Account (Cr)", "Payee (Dummy)", "Memo", "Payment Date", "Payment Method", "Expense Account (Dr)", "Expense Description", "Expense Line Amount", "Currency", "Currency Exchange", "Location", "Class", "Remarks"]
    else:
        cols_order = ["No", "Exp Ref. No", "Account (Cr)", "Payee (Dummy)", "Memo", "Payment Date", "Payment Method", "Expense Account (Dr)", "Expense Description", "Expense Line Amount", "Currency", "Location", "Class", "Remarks"]
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
                      start_no: int, qbo_mappings: Dict[str, dict], existing_ids: Dict[int, str] = None, client_name: str = "") -> tuple[pd.DataFrame, int]:
    print(f"\n--- DEBUG: Processing TRANSFERS (Input Rows: {len(df)}) ---")
    if df.empty: return pd.DataFrame(), start_no
    if existing_ids is None: existing_ids = {}

    df[COL_USD] = pd.to_numeric(df[COL_USD], errors='coerce').fillna(0.0)
    transfers = df[df[COL_USD].round(2) != 0].copy()

    if COL_METHOD in transfers.columns:
        transfers = transfers[transfers[COL_METHOD].astype(str).str.contains("Transfer", case=False, na=False)]

    # KZP reimbursements are journal-only; do not duplicate into Transfer tab.
    if _is_kzp_case(client_name) and COL_TYPE in transfers.columns:
        transfers = transfers[transfers[COL_TYPE].astype(str).str.strip().str.lower() != "reimbursements"]
    
    if transfers.empty: 
        return pd.DataFrame(), start_no

    # ID GENERATION
    _, doc_prefix = _build_id_prefixes(client_name)
    ref_nos = []
    for i, row in transfers.iterrows():
        s_no = int(row.get(COL_NO, 0))
        if existing_ids and s_no in existing_ids:
            ref_nos.append(existing_ids[s_no])
        else:
            dt = pd.to_datetime(row[COL_DATE], errors='coerce')
            date_str = dt.strftime('%m%y') if pd.notna(dt) else "0000"
            start_no += 1
            if _is_kzp_case(client_name):
                ref_nos.append(f"{doc_prefix}{date_str}T{str(start_no).zfill(4)}")
            elif _is_kzdw_case(client_name):
                ref_nos.append(f"{doc_prefix}{date_str}T{str(start_no).zfill(4)}")
            else:
                ref_nos.append(f"{doc_prefix}{country}{date_str}T{str(start_no).zfill(4)}")

    transfers["Ref No"] = ref_nos

    rename_map = {
        COL_TR_FROM: "Transfer Funds From",
        COL_TR_TO: "Transfer Funds To",
        COL_ITEM_DESC: "Memo",
        COL_CO: "Location",
    }
    transfers = transfers.rename(columns=rename_map)

    # QBO Transfer requires positive amount.
    # For negative source amounts, flip direction to preserve movement semantics.
    negative_mask = pd.to_numeric(transfers[COL_USD], errors="coerce").fillna(0.0) < 0
    if negative_mask.any():
        from_vals = transfers.loc[negative_mask, "Transfer Funds From"].copy()
        transfers.loc[negative_mask, "Transfer Funds From"] = transfers.loc[negative_mask, "Transfer Funds To"]
        transfers.loc[negative_mask, "Transfer Funds To"] = from_vals

    transfers["Transfer Amount"] = pd.to_numeric(transfers[COL_USD], errors="coerce").fillna(0.0).abs()
    if _is_kzdw_case(client_name) and "Currency" in transfers.columns:
        transfers["Currency"] = transfers["Currency"].apply(_normalize_currency)
        transfers["Currency Exchange"] = _build_currency_exchange_series(transfers, "Currency")
    else:
        transfers["Currency"] = "USD"
        transfers["Currency Exchange"] = ""
    transfers["Memo"] = transfers["Ref No"] + " - " + transfers["Memo"].astype(str)
    
    _fix_grp_location(transfers, "Location")
    # Fill NA locations with raw CO value
    transfers["Location"] = transfers["Location"].fillna(transfers.get(COL_CO, ""))
    transfers["Class"] = transfers["Location"].fillna("")

    # Validation
    map_acc = qbo_mappings.get('accounts', {})
    map_loc = qbo_mappings.get('locations', {})
    
    # DEBUG: Log account mappings for troubleshooting
    if not map_acc:
        print(f"   ⚠️ WARNING: No accounts in QBO mappings for country={country}. Check realm ID.")
    
    def validate_transfer_row(row):
        row_no = row.get("No", "")
        if _is_blank(row["Transfer Funds From"]):
            return f"ERROR | Missing From Account | Row No: {row_no}"
        if _is_blank(row["Transfer Funds To"]):
            return f"ERROR | Missing To Account | Row No: {row_no}"
        
        from_acc = row["Transfer Funds From"]
        to_acc = row["Transfer Funds To"]
        
        from_acc_id = find_id_in_map(map_acc, from_acc)
        if not from_acc_id:
            available = ', '.join(list(map_acc.keys())[:3]) if map_acc else 'NONE'
            return f"ERROR | 'From' Account not in QBO: '{from_acc}' | Available: {available}... | Row No: {row_no}"
             
        to_acc_id = find_id_in_map(map_acc, to_acc)
        if not to_acc_id:
            available = ', '.join(list(map_acc.keys())[:3]) if map_acc else 'NONE'
            return f"ERROR | 'To' Account not in QBO: '{to_acc}' | Available: {available}... | Row No: {row_no}"

        row_currency_code = _normalize_currency_code(row.get("Currency", "USD"))
        if _should_check_transfer_currency(client_name) and row_currency_code == "USD":
            mismatch_error = _currency_mismatch_error(
                row_no=row_no,
                file_currency=row_currency_code,
                account_checks=[
                    ("From", from_acc, _account_currency_from_id(qbo_mappings, from_acc_id)),
                    ("To", to_acc, _account_currency_from_id(qbo_mappings, to_acc_id)),
                ],
            )
            if mismatch_error:
                return mismatch_error
             
        if row["Transfer Funds From"] == row["Transfer Funds To"]: 
            return f"ERROR | 'From' and 'To' Accounts cannot be the same | Row No: {row_no}"
            
        loc_name = row.get("Location")
        if (not _is_blank(loc_name)) and not find_id_in_map(map_loc, loc_name): 
            return f"ERROR | Location not in QBO: '{loc_name}' | Row No: {row_no}"
        return "Ready to sync"

    transfers["Remarks"] = transfers.apply(validate_transfer_row, axis=1)

    if _is_kzdw_case(client_name):
        cols_order = ["No", "Ref No", "Transfer Funds From", "Transfer Funds To", "Transfer Amount", "Memo", COL_DATE, "Location", "Class", "Currency", "Currency Exchange", COL_TYPE, "Remarks"]
    else:
        cols_order = ["No", "Ref No", "Transfer Funds From", "Transfer Funds To", "Transfer Amount", "Memo", COL_DATE, "Location", "Class", "Currency", COL_TYPE, "Remarks"]
    for c in cols_order:
        if c not in transfers.columns: transfers[c] = ""
    
    return transfers[cols_order], start_no

# ==========================================
# 4. MAIN TRANSFORM ENTRY POINT
# ==========================================
def transform_raw(raw_df: pd.DataFrame, country: str,
                  last_jv: int, last_exp: int, last_tr: int, qbo_mappings: Dict[str, dict] = None, existing_ids: Dict[str, dict] = None, client_name: str = "") -> TransformResult:
    if raw_df is None or raw_df.empty:
        return TransformResult(pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), last_jv, last_exp, last_tr, None)

    df = _normalize_df_headers(raw_df.copy())

    # Shared Cleaning
    if COL_NO in df.columns:
        df[COL_NO] = pd.to_numeric(df[COL_NO], errors="coerce").fillna(0).astype(int)

    if "Category" in df.columns: 
        df = df[df["Category"].fillna("").astype(str).str.strip() != ""]

    if COL_DATE in df.columns:
        df[COL_DATE] = parse_mixed_date(df[COL_DATE])

    if COL_IN_OUT in df.columns: 
        df[COL_IN_OUT] = pd.to_numeric(df[COL_IN_OUT], errors="coerce").fillna(0)

    if COL_USD in df.columns:
        df[COL_USD] = pd.to_numeric(df[COL_USD], errors="coerce").fillna(0.0)
        df = df[~df[COL_USD].isna()]

    final_jv, new_jv_no = process_journals(df, last_jv, qbo_mappings, existing_ids.get('journals') if existing_ids else None, client_name=client_name)
    final_exp, new_exp_no = process_expenses(df, country, last_exp, qbo_mappings, existing_ids.get('expenses') if existing_ids else None, client_name=client_name)
    final_tr, new_tr_no = process_transfers(df, country, last_tr, qbo_mappings, existing_ids.get('transfers') if existing_ids else None, client_name=client_name)

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
