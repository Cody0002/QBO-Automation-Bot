from __future__ import annotations
import pandas as pd
import numpy as np
import calendar
from datetime import datetime
from src.connectors.qbo_client import QBOClient
from src.utils.logger import setup_logger

logger = setup_logger("reconciler")

class Reconciler:
    def __init__(self, qbo_client: QBOClient):
        self.client = qbo_client

    def _get_month_range(self, date_str: str) -> tuple[str, str]:
        """
        Input: "2025-10-10"
        Output: ("2025-10-01", "2025-10-31")
        """
        try:
            dt = pd.to_datetime(date_str)
            year, month = dt.year, dt.month
            
            # Get last day of month
            _, last_day = calendar.monthrange(year, month)
            
            start_date = f"{year}-{month:02d}-01"
            end_date = f"{year}-{month:02d}-{last_day}"
            return start_date, end_date
        except Exception as e:
            logger.error(f"Error parsing date '{date_str}': {e}")
            return None, None

    def _fetch_qbo_batch_by_date(self, entity: str, start_date: str, end_date: str) -> dict:
        """
        Fetches records by TxnDate range.
        """
        logger.info(f"   🔍 Querying QBO for {entity} from {start_date} to {end_date}...")
        
        # Query by Date Range
        query = f"SELECT * FROM {entity} WHERE TxnDate >= '{start_date}' AND TxnDate <= '{end_date}' MAXRESULTS 1000"
        
        results = self.client.query(query)
        
        lookup = {}
        for item in results:
            doc_num = item.get("DocNumber")
            if doc_num:
                lookup[doc_num] = item
        
        logger.info(f"      ✅ Found {len(lookup)} records in QBO.")
        return lookup
    
    def _fetch_transfer_batch_by_date(self, entity: str, start_date: str, end_date: str) -> dict:
        """
        Fetches Transfers by TxnDate range.
        """
        logger.info(f"   🔍 Querying QBO for {entity} from {start_date} to {end_date}...")
        
        # Query by Date Range
        query = f"SELECT * FROM {entity} WHERE TxnDate >= '{start_date}' AND TxnDate <= '{end_date}' MAXRESULTS 1000"
        
        results = self.client.query(query)
        
        # We still KEY the dictionary by DocNumber (if available) or rely on other matching if needed.
        # Assuming Transfers have DocNumber or we match differently.
        lookup = {}
        for item in results:
            doc_num = item.get("DocNumber")
            if doc_num:
                lookup[doc_num] = item
        
        logger.info(f"      ✅ Found {len(lookup)} records in QBO.")
        return lookup
    
    def _compare_values(self, sheet_val, qbo_val, tolerance=0.01) -> bool:
        """Helper to compare floats with tolerance."""
        try:
            v1 = float(sheet_val or 0)
            v2 = float(qbo_val or 0)
            return abs(v1 - v2) < tolerance
        except:
            return False

    def _check_mismatch(self, errors: list, field_name: str, sheet_val: any, qbo_val: any, is_float=False):
        if is_float:
            if not self._compare_values(sheet_val, qbo_val):
                try: s_fmt = f"{float(sheet_val):.2f}"
                except: s_fmt = str(sheet_val)
                try: q_fmt = f"{float(qbo_val):.2f}"
                except: q_fmt = str(qbo_val)
                errors.append(f"{field_name} Mismatch (Sheet: {s_fmt} != QBO: {q_fmt})")
        else:
            s_str = str(sheet_val or "").strip()
            q_str = str(qbo_val or "").strip()
            
            if not s_str and not q_str:
                return

            if s_str.lower() != q_str.lower():
                errors.append(f"{field_name} Mismatch (Sheet: '{s_str}' != QBO: '{q_str}')")

    def reconcile_journals(self, df: pd.DataFrame, month_str: str) -> list[dict]:
        """Compares Sheet Journals vs QBO JournalEntries (Filtered by Month)."""
        if df.empty or "Journal No" not in df.columns: return []

        # Calculate Dates
        start_date, end_date = self._get_month_range(month_str)
        if not start_date: return []

        # Fetch Data for that Month
        qbo_map = self._fetch_qbo_batch_by_date("JournalEntry", start_date, end_date)
        
        updates = []
        grouped = df.groupby("Journal No")
        
        for jv_no, group in grouped:
            qbo_record = qbo_map.get(str(jv_no)) # Ensure string key
            errors = []
            
            if not qbo_record:
                status_msg = "❌ Missing in QBO"
            else:
                first_row = group.iloc[0]
                sheet_date = pd.to_datetime(first_row.get("Date")).strftime("%Y-%m-%d")
                sheet_memo = first_row.get("Memo", "")
                sheet_curr = first_row.get("Currency Code", "USD")
                sheet_debits = group[group["Amount"] > 0]["Amount"].sum()

                qbo_date = qbo_record.get("TxnDate", "")
                qbo_memo = qbo_record.get("PrivateNote", "")
                qbo_amt = qbo_record.get("TotalAmt", 0)
                qbo_curr = qbo_record.get("CurrencyRef", {}).get("value", "USD")

                self._check_mismatch(errors, "Date", sheet_date, qbo_date)
                self._check_mismatch(errors, "Amount", sheet_debits, qbo_amt, is_float=True)
                self._check_mismatch(errors, "Memo", sheet_memo, qbo_memo)
                self._check_mismatch(errors, "Currency", sheet_curr, qbo_curr)

                if not errors:
                    status_msg = "✅ Matched"
                else:
                    status_msg = "⚠️ " + "; ".join(errors)

            for idx in group.index:
                updates.append({"row_idx": idx, "status": status_msg})
                
        return updates

    def reconcile_expenses(self, df: pd.DataFrame, month_str: str) -> list[dict]:
        """Compares Sheet Expenses vs QBO Purchases (Filtered by Month)."""
        if df.empty or "Exp Ref. No" not in df.columns: return []

        start_date, end_date = self._get_month_range(month_str)
        if not start_date: return []

        qbo_map = self._fetch_qbo_batch_by_date("Purchase", start_date, end_date)
        
        updates = []
        for idx, row in df.iterrows():
            ref_no = str(row["Exp Ref. No"])
            qbo_record = qbo_map.get(ref_no)
            errors = []
            
            if not qbo_record:
                status_msg = "❌ Missing in QBO"
            else:
                sheet_date = pd.to_datetime(row.get("Payment Date")).strftime("%Y-%m-%d")
                sheet_amt = abs(float(row.get("Expense Line Amount", 0)))
                sheet_memo = row.get("Memo", "")
                sheet_curr = row.get("Currency", "USD")
                sheet_acct_cr = row.get("Account (Cr)", "")
                sheet_acct_dr = row.get("Expense Account (Dr)", "")

                qbo_date = qbo_record.get("TxnDate", "")
                qbo_amt = float(qbo_record.get("TotalAmt", 0))
                qbo_memo = qbo_record.get("PrivateNote", "")
                qbo_curr = qbo_record.get("CurrencyRef", {}).get("value", "USD")
                qbo_acct_cr = qbo_record.get("AccountRef", {}).get("name", "")
                
                qbo_lines = qbo_record.get("Line", [])
                qbo_acct_dr = ""
                if qbo_lines:
                    detail = qbo_lines[0].get("AccountBasedExpenseLineDetail", {})
                    qbo_acct_dr = detail.get("AccountRef", {}).get("name", "")

                self._check_mismatch(errors, "Date", sheet_date, qbo_date)
                self._check_mismatch(errors, "Amount", sheet_amt, qbo_amt, is_float=True)
                self._check_mismatch(errors, "Memo", sheet_memo, qbo_memo)
                self._check_mismatch(errors, "Currency", sheet_curr, qbo_curr)
                self._check_mismatch(errors, "Pay Acct", sheet_acct_cr, qbo_acct_cr)
                self._check_mismatch(errors, "Exp Acct", sheet_acct_dr, qbo_acct_dr)

                if not errors:
                    status_msg = "✅ Matched"
                else:
                    status_msg = "⚠️ " + "; ".join(errors)

            updates.append({"row_idx": idx, "status": status_msg})
            
        return updates

    def reconcile_transfers(self, df: pd.DataFrame, month_str: str) -> list[dict]:
        """Compares Sheet Transfers vs QBO Transfers (Filtered by Month)."""
        if df.empty or "Ref No" not in df.columns: return []

        start_date, end_date = self._get_month_range(month_str)
        if not start_date: return []

        qbo_map = self._fetch_transfer_batch_by_date("Transfer", start_date, end_date)
        
        updates = []
        for idx, row in df.iterrows():
            ref_no = str(row["Ref No"])
            qbo_record = qbo_map.get(ref_no)
            errors = []
            
            if not qbo_record:
                status_msg = "❌ Missing in QBO"
            else:
                sheet_date = pd.to_datetime(row.get("Date")).strftime("%Y-%m-%d")
                sheet_amt = abs(float(row.get("Transfer Amount", 0)))
                sheet_memo_full = str(row.get("Memo", ""))
                
                sheet_curr = row.get("Currency", "USD")
                sheet_from = row.get("Transfer Funds From", "")
                sheet_to = row.get("Transfer Funds To", "")

                qbo_date = qbo_record.get("TxnDate", "")
                qbo_amt = float(qbo_record.get("Amount", 0))
                qbo_memo = qbo_record.get("PrivateNote", "")
                qbo_curr = qbo_record.get("CurrencyRef", {}).get("value", "USD")
                qbo_from = qbo_record.get("FromAccountRef", {}).get("name", "")
                qbo_to = qbo_record.get("ToAccountRef", {}).get("name", "")

                self._check_mismatch(errors, "Date", sheet_date, qbo_date)
                self._check_mismatch(errors, "Amount", sheet_amt, qbo_amt, is_float=True)
                self._check_mismatch(errors, "Memo", sheet_memo_full, qbo_memo)
                self._check_mismatch(errors, "Currency", sheet_curr, qbo_curr)
                self._check_mismatch(errors, "From Acct", sheet_from, qbo_from)
                self._check_mismatch(errors, "To Acct", sheet_to, qbo_to)

                if not errors:
                    status_msg = "✅ Matched"
                else:
                    status_msg = "⚠️ " + "; ".join(errors)

            updates.append({"row_idx": idx, "status": status_msg})
            
        return updates