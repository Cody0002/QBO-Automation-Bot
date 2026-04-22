from __future__ import annotations

import re
from typing import Iterable

import pandas as pd

RAW_STANDARD_COLUMNS = [
    "CO",
    "COY",
    "Date",
    "Category",
    "Type",
    "Item Description",
    "TrxHarsh",
    "Account Fr",
    "Account To",
    "Currency",
    "Amount Fr",
    "Currency To",
    "Amount To",
    "Budget",
    "USD - Raw",
    "USD - Actual",
    "USD - Loss",
    "USD - QBO",
    "Reclass",
    "QBO Method",
    "If Journal/Expense Method",
    "QBO Transfer Fr",
    "QBO Transfer To",
    "Check (Internal use)",
    "No",
    "Currency Rate",
]


def _norm_name(name: str) -> str:
    return re.sub(r"\s+", " ", str(name).replace("\n", " ").strip()).lower()


def _find_col(df: pd.DataFrame, aliases: Iterable[str]) -> str | None:
    by_norm = {_norm_name(c): c for c in df.columns}
    for alias in aliases:
        real = by_norm.get(_norm_name(alias))
        if real:
            return real
    return None


def _value_series(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip()


def _normalize_currency_series(series: pd.Series) -> pd.Series:
    vals = _value_series(series)
    has_usd = vals.str.contains("USD", case=False, na=False)
    out = vals.where(~has_usd, "USD")
    return out.where(out != "", "USD")


def _normalize_qbo_method_series(series: pd.Series) -> pd.Series:
    vals = _value_series(series)
    low = vals.str.lower()
    out = vals.copy()
    out = out.where(~low.str.contains(r"\b(?:jv|journal)\b", regex=True, na=False), "Journal")
    out = out.where(~low.str.contains("expense", case=False, na=False), "Expenses")
    out = out.where(~low.str.contains("transfer", case=False, na=False), "Transfer")
    out = out.where(~low.str.contains("reclass", case=False, na=False), "Reclass")
    return out


def _clean_headers(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [re.sub(r"\s+", " ", str(c).replace("\n", " ").strip()) for c in out.columns]
    # Deduplicate column names: keep first occurrence, rename subsequent ones with suffix
    seen: dict[str, int] = {}
    new_cols = []
    for col in out.columns:
        if col in seen:
            seen[col] += 1
            new_cols.append(f"{col}._dup{seen[col]}")
        else:
            seen[col] = 0
            new_cols.append(col)
    out.columns = new_cols
    return out


def _parse_amount_series(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce").fillna(0.0)

    s = series.astype(str).str.strip()
    s = s.str.replace(",", "", regex=False)
    s = s.str.replace("(", "-", regex=False).str.replace(")", "", regex=False)
    s = s.str.replace(r"[^0-9\.\-]", "", regex=True)
    return pd.to_numeric(s, errors="coerce").fillna(0.0)


def _coerce_standard_numeric_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ["USD - QBO", "Amount Fr", "Amount To", "Currency Rate"]:
        if col in out.columns:
            out[col] = _parse_amount_series(out[col])
    if "No" in out.columns:
        out["No"] = pd.to_numeric(out["No"], errors="coerce").fillna(0)
    return out


def _normalize_kzp_date(date_series: pd.Series, raw_month: str) -> pd.Series:
    out = date_series.astype(str).str.strip()
    is_blank = out.eq("") | out.str.lower().isin(["nan", "nat", "none"])
    has_year = out.str.contains(r"\b\d{4}\b", regex=True, na=False)
    is_numeric = pd.to_numeric(date_series, errors="coerce").notna()

    target_year = None
    dt = pd.to_datetime(raw_month, errors="coerce")
    if pd.notna(dt):
        target_year = int(dt.year)

    if target_year is not None:
        needs_year = (~is_blank) & (~has_year) & (~is_numeric)
        out = out.where(~needs_year, out + f" {target_year}")
    return out


def _standardize_legacy(df: pd.DataFrame) -> pd.DataFrame:
    out = df.iloc[:, : len(RAW_STANDARD_COLUMNS)].copy()
    while out.shape[1] < len(RAW_STANDARD_COLUMNS):
        out[f"_pad_{out.shape[1]}"] = ""
    out.columns = RAW_STANDARD_COLUMNS
    return _coerce_standard_numeric_cols(out)


def _standardize_kzp(df: pd.DataFrame, raw_month: str) -> pd.DataFrame:
    idx = df.index

    month_col = _find_col(df, ["Month"])
    co_col = _find_col(df, ["CO"])
    coy_col = _find_col(df, ["COY"])
    date_col = _find_col(df, ["Date"])
    category_col = _find_col(df, ["Category"])
    type_col = _find_col(df, ["Type", "Sub Category"])
    desc_col = _find_col(df, ["Item Description"])
    trx_hash_col = _find_col(df, ["Trx Hash", "TrxHarsh"])
    currency_col = _find_col(df, ["Currency", "Currency From"])
    transfer_from_col = _find_col(
        df,
        ["Fund Transfer From", "Transfer from", "Transfer From", "If Transfer method: Fund Transfer From", "From Account"],
    )
    transfer_to_col = _find_col(df, ["Transfer to", "Transfer To", "Transfer to ((Can copy from column H )", "To Account"])
    bank_col = _find_col(df, ["From Account", "Bank", "Bank ( To )", "Bank (To)", "Bank To"])
    account_to_col = _find_col(df, ["To Account", "If Journal/Expense method: Another records", "If Journal/Expense Method"])
    amount_col = _find_col(df, ["USD - QBO", "In/Out (USD)", "In/Out", "Amount From", "USD"])
    check_col = _find_col(df, ["Check", "Checking ( For our use only )"])
    method_col = _find_col(df, ["QBO Import", "QBO Import Method (Journal/Expenses/Transfer)", "QBO Method"])
    no_col = _find_col(df, ["No"])

    def col_or_empty(col_name: str | None) -> pd.Series:
        if col_name and col_name in df.columns:
            return df[col_name]
        return pd.Series([""] * len(df), index=idx)

    amount = _parse_amount_series(col_or_empty(amount_col))
    date_series = _normalize_kzp_date(col_or_empty(date_col), raw_month)
    account_from = _value_series(col_or_empty(bank_col))
    account_to = _value_series(col_or_empty(account_to_col))
    account_to_filled = account_to.where(account_to != "", account_from)
    transfer_from = _value_series(col_or_empty(transfer_from_col))
    transfer_to = _value_series(col_or_empty(transfer_to_col))
    transfer_from_filled = transfer_from.where(transfer_from != "", account_from)
    transfer_to_filled = transfer_to.where(transfer_to != "", account_to_filled)
    type_vals = col_or_empty(type_col)
    type_vals = type_vals.where(_value_series(type_vals) != "", account_to_filled)

    no_series = pd.to_numeric(col_or_empty(no_col), errors="coerce")
    if no_series.fillna(0).eq(0).all():
        no_series = pd.Series(range(1, len(df) + 1), index=idx, dtype="float64")

    out = pd.DataFrame(index=idx)
    # KZP Location/Class should come from CO (not COY).
    out["CO"] = col_or_empty(co_col)
    out["COY"] = col_or_empty(coy_col)
    out["Date"] = date_series
    out["Category"] = col_or_empty(category_col)
    out["Type"] = type_vals
    out["Item Description"] = col_or_empty(desc_col)
    out["TrxHarsh"] = col_or_empty(trx_hash_col)
    # KZP journals/expenses should source account from the "from" side.
    out["Account Fr"] = account_from
    out["Account To"] = account_to_filled
    out["Currency"] = col_or_empty(currency_col).replace("", "USD")
    out["Amount Fr"] = amount
    out["Currency To"] = ""
    out["Amount To"] = 0.0
    out["Budget"] = 0.0
    out["USD - Raw"] = amount
    out["USD - Actual"] = amount
    out["USD - Loss"] = 0.0
    out["USD - QBO"] = amount
    out["Reclass"] = ""
    out["QBO Method"] = col_or_empty(method_col)
    out["If Journal/Expense Method"] = account_from.where(account_from != "", account_to_filled)
    out["QBO Transfer Fr"] = transfer_from_filled
    out["QBO Transfer To"] = transfer_to_filled
    out["Check (Internal use)"] = col_or_empty(check_col)
    out["No"] = no_series
    out["Currency Rate"] = 0.0

    # Remove month/header separator rows from exports (if present), keep real data rows.
    if month_col:
        category_clean = out["Category"].astype(str).str.strip()
        out = out[category_clean != ""].copy()

    return _coerce_standard_numeric_cols(out)


def _standardize_kzdw(df: pd.DataFrame) -> pd.DataFrame:
    idx = df.index

    co_col = _find_col(df, ["CO"])
    coy_col = _find_col(df, ["COY"])
    date_col = _find_col(df, ["Date"])
    category_col = _find_col(df, ["Category"])
    type_col = _find_col(df, ["Type"])
    desc_col = _find_col(df, ["Item Description"])
    trx_hash_col = _find_col(df, ["Trx Hash", "TrxHarsh"])
    amount_col = _find_col(df, ["Final Amount to be take (different currency)", "USD - QBO", "USD"])
    currency_col = _find_col(df, ["Currency"])
    currency_rate_col = _find_col(df, ["Currency Rate", "Currency Exchange", "Exchange Rate"])
    method_col = _find_col(df, ["QBO Import", "QBO Method", "QBO Import Method (Journal/Expenses/Transfer)"])
    acc_debit_col = _find_col(df, ["If Journal/Expense method: Another records", "If Journal/Expense Method"])
    transfer_from_col = _find_col(df, ["Transfer from", "Transfer From", "If Transfer method: Fund Transfer From", "Fund Transfer From"])
    transfer_to_col = _find_col(df, ["Transfer to", "Transfer To", "Transfer to ((Can copy from column H )"])
    check_col = _find_col(df, ["Checking ( For our use only )", "Check (Internal use)", "Check"])
    no_col = _find_col(df, ["No"])

    def col_or_empty(col_name: str | None) -> pd.Series:
        if col_name and col_name in df.columns:
            return df[col_name]
        return pd.Series([""] * len(df), index=idx)

    amount = _parse_amount_series(col_or_empty(amount_col))
    currency_rate = _parse_amount_series(col_or_empty(currency_rate_col))
    currency = _normalize_currency_series(col_or_empty(currency_col))
    acc_v = _value_series(col_or_empty(acc_debit_col))
    transfer_to = _value_series(col_or_empty(transfer_to_col))
    transfer_from = _value_series(col_or_empty(transfer_from_col))
    # KZDW mapping rules from source sheet:
    # 1) Prefill Transfer From/To with V ("If Journal/Expense ... Another records")
    # 2) Reverse Journal account direction (Debit/Credit) for KZDW.
    transfer_from_filled = transfer_from.where(transfer_from != "", acc_v)
    transfer_to_filled = transfer_to.where(transfer_to != "", acc_v)
    debit_account = transfer_from_filled
    credit_account = transfer_to_filled
    type_vals = transfer_to_filled

    no_series = pd.to_numeric(col_or_empty(no_col), errors="coerce")
    if no_series.fillna(0).eq(0).all():
        no_series = pd.Series(range(1, len(df) + 1), index=idx, dtype="float64")

    out = pd.DataFrame(index=idx)
    out["CO"] = col_or_empty(co_col)
    out["COY"] = col_or_empty(coy_col)
    out["Date"] = col_or_empty(date_col)
    out["Category"] = col_or_empty(category_col)
    out["Type"] = type_vals
    out["Item Description"] = col_or_empty(desc_col)
    out["TrxHarsh"] = col_or_empty(trx_hash_col)
    # Debit account for Journal/Expense.
    out["Account Fr"] = debit_account
    out["Account To"] = transfer_to_filled
    out["Currency"] = currency
    out["Amount Fr"] = amount
    out["Currency To"] = ""
    out["Amount To"] = 0.0
    out["Budget"] = 0.0
    out["USD - Raw"] = amount
    out["USD - Actual"] = amount
    out["USD - Loss"] = 0.0
    out["USD - QBO"] = amount
    out["Reclass"] = ""
    out["QBO Method"] = col_or_empty(method_col)
    # Credit/source account for Journal/Expense: X -> V (reversed for KZDW).
    out["If Journal/Expense Method"] = credit_account
    out["QBO Transfer Fr"] = transfer_from_filled
    out["QBO Transfer To"] = transfer_to_filled
    out["Check (Internal use)"] = col_or_empty(check_col)
    out["No"] = no_series
    out["Currency Rate"] = currency_rate

    sub_category_col = "Type"
    
    if sub_category_col:
        out["Category"] = out["Category"].where(
            _value_series(out["Category"]) != "",
            col_or_empty(sub_category_col),
        )

    return _coerce_standard_numeric_cols(out)


def _standardize_s5(df: pd.DataFrame) -> pd.DataFrame:
    idx = df.index

    co_col = _find_col(df, ["CO"])
    coy_col = _find_col(df, ["COY"])
    date_col = _find_col(df, ["Date"])
    category_col = _find_col(df, ["Category"])
    type_col = _find_col(df, ["Type", "Sub Category"])
    desc_col = _find_col(df, ["Item Description"])
    trx_hash_col = _find_col(df, ["Trx Hash", "TrxHarsh"])
    currency_col = _find_col(df, ["Currency", "Currency From"])
    bank_col = _find_col(df, ["Bank", "From Account", "Bank ( To )", "Bank (To)", "Bank To"])
    amount_final_col = _find_col(df, ["Final Amount to be take (different currency)", "USD - QBO"])
    amount_inout_col = _find_col(df, ["In/Out (USD)", "In/Out", "Amount From", "USD"])
    transfer_from_col = _find_col(
        df,
        ["Transfer from", "Transfer From", "If Transfer method: Fund Transfer From", "Fund Transfer From"],
    )
    transfer_to_col = _find_col(df, ["Transfer to", "Transfer To", "Transfer to ((Can copy from column H )", "To Account"])
    acc_cr_col = _find_col(df, ["If Journal/Expense method: Another records", "If Journal/Expense Method"])
    method_col = _find_col(df, ["QBO Import", "QBO Method", "QBO Import Method (Journal/Expenses/Transfer)"])
    method_fallback_col = _find_col(df, ["QBO Way"])
    check_col = _find_col(df, ["QBO Post", "Check", "Checking ( For our use only )"])
    no_col = _find_col(df, ["No"])

    def col_or_empty(col_name: str | None) -> pd.Series:
        if col_name and col_name in df.columns:
            return df[col_name]
        return pd.Series([""] * len(df), index=idx)

    amount_final_raw = col_or_empty(amount_final_col)
    amount_inout_raw = col_or_empty(amount_inout_col)
    amount_final = _parse_amount_series(amount_final_raw)
    amount_inout = _parse_amount_series(amount_inout_raw)
    amount = amount_final.where(_value_series(amount_final_raw) != "", amount_inout)

    method_primary = _value_series(col_or_empty(method_col))
    method_fallback = _value_series(col_or_empty(method_fallback_col))
    method = method_primary.where(method_primary != "", method_fallback)
    method = _normalize_qbo_method_series(method)

    bank_vals = _value_series(col_or_empty(bank_col))
    acc_cr_vals = _value_series(col_or_empty(acc_cr_col))
    transfer_from_vals = _value_series(col_or_empty(transfer_from_col))
    transfer_to_vals = _value_series(col_or_empty(transfer_to_col))
    type_vals = _value_series(col_or_empty(type_col))

    transfer_from_filled = transfer_from_vals.where(transfer_from_vals != "", bank_vals)
    transfer_from_filled = transfer_from_filled.where(transfer_from_filled != "", acc_cr_vals)
    transfer_to_filled = transfer_to_vals.where(transfer_to_vals != "", type_vals)
    transfer_to_filled = transfer_to_filled.where(transfer_to_filled != "", bank_vals)

    no_series = pd.to_numeric(col_or_empty(no_col), errors="coerce")
    if no_series.fillna(0).eq(0).all():
        no_series = pd.Series(range(1, len(df) + 1), index=idx, dtype="float64")

    co_vals = _value_series(col_or_empty(co_col))
    coy_vals = _value_series(col_or_empty(coy_col))
    coy_vals = coy_vals.where(coy_vals != "", co_vals)

    out = pd.DataFrame(index=idx)
    out["CO"] = co_vals
    out["COY"] = coy_vals
    out["Date"] = col_or_empty(date_col)
    out["Category"] = col_or_empty(category_col)
    out["Type"] = col_or_empty(type_col)
    out["Item Description"] = col_or_empty(desc_col)
    out["TrxHarsh"] = col_or_empty(trx_hash_col)
    out["Account Fr"] = bank_vals.where(bank_vals != "", transfer_from_filled)
    out["Account To"] = transfer_to_filled
    out["Currency"] = "USD"  # S5 always uses USD in QBO
    out["Amount Fr"] = amount
    out["Currency To"] = ""
    out["Amount To"] = 0.0
    out["Budget"] = 0.0
    out["USD - Raw"] = amount
    out["USD - Actual"] = amount
    out["USD - Loss"] = 0.0
    out["USD - QBO"] = amount
    out["Reclass"] = ""
    out["QBO Method"] = method
    out["If Journal/Expense Method"] = transfer_from_filled
    out["QBO Transfer Fr"] = transfer_from_filled
    out["QBO Transfer To"] = transfer_to_filled
    out["Check (Internal use)"] = col_or_empty(check_col)
    out["No"] = no_series
    out["Currency Rate"] = 0.0

    category_clean = _value_series(out["Category"])
    date_clean = _value_series(out["Date"])
    out = out[(category_clean != "") & (date_clean != "")].copy()

    return _coerce_standard_numeric_cols(out)


def standardize_raw_df(raw_df: pd.DataFrame, client_name: str, raw_month: str) -> pd.DataFrame:
    """
    Convert incoming raw data into the canonical schema expected by
    transform/reconcile logic. Handles legacy KZO layout and custom KZP/S5 layouts.
    """
    if raw_df is None or raw_df.empty:
        return pd.DataFrame(columns=RAW_STANDARD_COLUMNS)

    cleaned = _clean_headers(raw_df)
    client_lower = str(client_name).lower()
    is_kzp_client = "kzp" in client_lower
    is_s5_client = "s5" in client_lower
    is_kzdw_client = "kzdw" in client_lower
    has_kzp_shape = (
        (
            _find_col(cleaned, ["In/Out (USD)"]) is not None
            and _find_col(cleaned, ["Bank"]) is not None
            and _find_col(cleaned, ["USD - QBO"]) is None
        )
        or (
            _find_col(cleaned, ["USD - QBO"]) is not None
            and _find_col(cleaned, ["QBO Import"]) is not None
            and _find_col(cleaned, ["From Account"]) is not None
        )
    )
    has_kzdw_shape = (
        _find_col(cleaned, ["Final Amount to be take (different currency)"]) is not None
        and _find_col(cleaned, ["Transfer from"]) is not None
        and _find_col(cleaned, ["Transfer to"]) is not None
    )
    has_s5_shape = (
        _find_col(cleaned, ["CO"]) is not None
        and _find_col(cleaned, ["QBO Way"]) is not None
        and _find_col(cleaned, ["Bank"]) is not None
        and _find_col(cleaned, ["Final Amount to be take (different currency)"]) is not None
    )

    if is_kzdw_client:
        return _standardize_kzdw(cleaned)
    if is_s5_client or has_s5_shape:
        return _standardize_s5(cleaned)
    if is_kzp_client or has_kzp_shape:
        return _standardize_kzp(cleaned, raw_month)
    if has_kzdw_shape:
        return _standardize_kzdw(cleaned)
    return _standardize_legacy(cleaned)