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


def _clean_headers(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [re.sub(r"\s+", " ", str(c).replace("\n", " ").strip()) for c in out.columns]
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
    for col in ["USD - QBO", "Amount Fr", "Amount To"]:
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
    type_col = _find_col(df, ["Type"])
    desc_col = _find_col(df, ["Item Description"])
    currency_col = _find_col(df, ["Currency"])
    transfer_from_col = _find_col(df, ["Fund Transfer From"])
    bank_col = _find_col(df, ["Bank"])
    amount_col = _find_col(df, ["In/Out (USD)", "In/Out"])
    check_col = _find_col(df, ["Check", "Checking ( For our use only )"])
    method_col = _find_col(df, ["QBO Import Method (Journal/Expenses/Transfer)", "QBO Method"])
    no_col = _find_col(df, ["No"])

    def col_or_empty(col_name: str | None) -> pd.Series:
        if col_name and col_name in df.columns:
            return df[col_name]
        return pd.Series([""] * len(df), index=idx)

    amount = _parse_amount_series(col_or_empty(amount_col))
    date_series = _normalize_kzp_date(col_or_empty(date_col), raw_month)

    # For KZP raw, COY usually carries market code (TH/PH/BD...), while CO is always KZP.
    # Keep only compact country-like codes to avoid forcing invalid location mappings.
    location_src = col_or_empty(coy_col)
    location = location_src.astype(str).str.strip().str.upper()
    location = location.where(location.str.fullmatch(r"[A-Z]{2}", na=False), "")

    no_series = pd.to_numeric(col_or_empty(no_col), errors="coerce")
    if no_series.fillna(0).eq(0).all():
        no_series = pd.Series(range(1, len(df) + 1), index=idx, dtype="float64")

    out = pd.DataFrame(index=idx)
    out["CO"] = location
    out["COY"] = col_or_empty(coy_col)
    out["Date"] = date_series
    out["Category"] = col_or_empty(category_col)
    out["Type"] = col_or_empty(type_col)
    out["Item Description"] = col_or_empty(desc_col)
    out["TrxHarsh"] = ""
    out["Account Fr"] = col_or_empty(transfer_from_col)
    out["Account To"] = col_or_empty(bank_col)
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
    out["If Journal/Expense Method"] = col_or_empty(bank_col)
    out["QBO Transfer Fr"] = col_or_empty(transfer_from_col)
    out["QBO Transfer To"] = col_or_empty(bank_col)
    out["Check (Internal use)"] = col_or_empty(check_col)
    out["No"] = no_series

    # Remove month/header separator rows from exports (if present), keep real data rows.
    if month_col:
        category_clean = out["Category"].astype(str).str.strip()
        out = out[category_clean != ""].copy()

    return _coerce_standard_numeric_cols(out)


def standardize_raw_df(raw_df: pd.DataFrame, client_name: str, raw_month: str) -> pd.DataFrame:
    """
    Convert incoming raw data into the canonical 25-column schema expected by
    transform/reconcile logic. Handles legacy KZO layout and simplified KZP layout.
    """
    if raw_df is None or raw_df.empty:
        return pd.DataFrame(columns=RAW_STANDARD_COLUMNS)

    cleaned = _clean_headers(raw_df)
    is_kzp_client = "kzp" in str(client_name).lower()
    has_kzp_shape = (
        _find_col(cleaned, ["In/Out (USD)"]) is not None
        and _find_col(cleaned, ["Bank"]) is not None
        and _find_col(cleaned, ["USD - QBO"]) is None
    )

    if is_kzp_client or has_kzp_shape:
        return _standardize_kzp(cleaned, raw_month)
    return _standardize_legacy(cleaned)
