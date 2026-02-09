# src/logic/reconciler.py
from __future__ import annotations
import pandas as pd
import calendar
from src.connectors.qbo_client import QBOClient
from src.utils.logger import setup_logger

logger = setup_logger("reconciler")

class Reconciler:
    def __init__(self, qbo_client: QBOClient):
        self.client = qbo_client

    def _get_month_range(self, date_str: str) -> tuple[str, str]:
        try:
            dt = pd.to_datetime(date_str)
            _, last_day = calendar.monthrange(dt.year, dt.month)
            return f"{dt.year}-{dt.month:02d}-01", f"{dt.year}-{dt.month:02d}-{last_day}"
        except:
            return None, None

    def _fetch_qbo_data(self, entity: str, start_date: str, end_date: str) -> tuple[dict, dict]:
        """
        Fetches records and maps them in TWO ways:
        1. By ID (primary)
        2. By DocNumber (fallback)
        """
        logger.info(f"   🔍 Querying QBO {entity} [{start_date} to {end_date}]...")
        query = f"SELECT * FROM {entity} WHERE TxnDate >= '{start_date}' AND TxnDate <= '{end_date}' MAXRESULTS 1000"
        results = self.client.query(query)
        
        map_id = {}
        map_doc = {}
        
        for item in results:
            # Map by ID
            if "Id" in item:
                map_id[str(item["Id"])] = item
            
            # Map by DocNumber (if exists)
            doc_num = item.get("DocNumber")
            if doc_num:
                map_doc[str(doc_num)] = item
                
        return map_id, map_doc

    def _fetch_transfers_list(self, start_date: str, end_date: str) -> list:
        logger.info(f"   🔍 Querying QBO Transfers [{start_date} to {end_date}]...")
        query = f"SELECT * FROM Transfer WHERE TxnDate >= '{start_date}' AND TxnDate <= '{end_date}' MAXRESULTS 1000"
        return self.client.query(query)

    def _check_mismatch(self, errors: list, field: str, sheet_val, qbo_val, is_float=False):
        try:
            if is_float:
                v1 = float(sheet_val or 0)
                v2 = float(qbo_val or 0)
                if abs(v1 - v2) > 0.01:
                    errors.append(f"{field}: {v1:,.2f} != {v2:,.2f}")
            else:
                s_str = str(sheet_val or "").strip().lower()
                q_str = str(qbo_val or "").strip().lower()
                if s_str == q_str: return
                if q_str.endswith(":" + s_str): return
                if not s_str and not q_str: return
                errors.append(f"{field}: '{sheet_val}' != '{qbo_val}'")
        except Exception as e:
            errors.append(f"{field} Err: {e}")

    # --- 1. JOURNALS ---
    def reconcile_journals(self, df: pd.DataFrame, month_str: str) -> list[dict]:
        if df.empty or "Journal No" not in df.columns: return []
        start, end = self._get_month_range(month_str)
        if not start: return []

        map_id, map_doc = self._fetch_qbo_data("JournalEntry", start, end)
        updates = []

        # Group by Journal No
        for jv_no, group in df.groupby("Journal No"):
            row = group.iloc[0]
            
            # A. Try ID Match (Best)
            qbo_record = None
            if "QBO ID" in row and pd.notna(row["QBO ID"]) and str(row["QBO ID"]).strip():
                qbo_record = map_id.get(str(row["QBO ID"]).strip())
            
            # B. Fallback to DocNumber
            if not qbo_record:
                qbo_record = map_doc.get(str(jv_no).strip())

            errors = []
            if not qbo_record:
                status = "❌ Not found in QBO"
            else:
                sheet_amt = group.loc[group["Amount"] > 0, "Amount"].sum()
                self._check_mismatch(errors, "Date", pd.to_datetime(row["Date"]).strftime("%Y-%m-%d"), qbo_record.get("TxnDate"))
                self._check_mismatch(errors, "Total", sheet_amt, qbo_record.get("TotalAmt"), is_float=True)
                self._check_mismatch(errors, "Memo", row.get("Memo"), qbo_record.get("PrivateNote"))
                status = "✅ Matched" if not errors else "⚠️ " + "; ".join(errors)

            for idx in group.index:
                updates.append({"row_idx": idx, "status": status})
        return updates

    # --- 2. EXPENSES ---
    def reconcile_expenses(self, df: pd.DataFrame, month_str: str) -> list[dict]:
        if df.empty or "Exp Ref. No" not in df.columns: return []
        start, end = self._get_month_range(month_str)
        if not start: return []

        map_id, map_doc = self._fetch_qbo_data("Purchase", start, end)
        updates = []

        for idx, row in df.iterrows():
            # A. Try ID Match
            qbo_record = None
            if "QBO ID" in row and pd.notna(row["QBO ID"]) and str(row["QBO ID"]).strip():
                qbo_record = map_id.get(str(row["QBO ID"]).strip())

            # B. Fallback to DocNumber
            if not qbo_record:
                ref_no = str(row.get("Exp Ref. No", "")).strip()
                qbo_record = map_doc.get(ref_no)

            errors = []
            if not qbo_record:
                status = "❌ Not found in QBO"
            else:
                sheet_amt = abs(float(row.get("Expense Line Amount", 0)))
                self._check_mismatch(errors, "Date", pd.to_datetime(row["Payment Date"]).strftime("%Y-%m-%d"), qbo_record.get("TxnDate"))
                self._check_mismatch(errors, "Amount", sheet_amt, qbo_record.get("TotalAmt"), is_float=True)
                status = "✅ Matched" if not errors else "⚠️ " + "; ".join(errors)
            
            updates.append({"row_idx": idx, "status": status})
        return updates

    # --- 3. TRANSFERS ---
    def reconcile_transfers(self, df: pd.DataFrame, month_str: str) -> list[dict]:
        if df.empty or "Ref No" not in df.columns: return []
        start, end = self._get_month_range(month_str)
        if not start: return []

        # Transfers usually don't have DocNumber, so we fetch list
        qbo_list = self._fetch_transfers_list(start, end)
        
        # Create an ID Map for Transfers too
        map_id = {str(item["Id"]): item for item in qbo_list if "Id" in item}

        updates = []

        for idx, row in df.iterrows():
            qbo_record = None
            
            # A. Try ID Match
            if "QBO ID" in row and pd.notna(row["QBO ID"]) and str(row["QBO ID"]).strip():
                qbo_record = map_id.get(str(row["QBO ID"]).strip())
            
            # B. Fallback to PrivateNote Logic
            if not qbo_record:
                ref_no = str(row.get("Ref No", "")).strip()
                qbo_record = next((item for item in qbo_list if ref_no in item.get("PrivateNote", "")), None)

            errors = []
            if not qbo_record:
                status = "❌ Not found in QBO"
            else:
                sheet_amt = abs(float(row.get("Transfer Amount", 0)))
                self._check_mismatch(errors, "Date", pd.to_datetime(row["Date"]).strftime("%Y-%m-%d"), qbo_record.get("TxnDate"))
                self._check_mismatch(errors, "Amount", sheet_amt, qbo_record.get("Amount"), is_float=True)
                status = "✅ Matched" if not errors else "⚠️ " + "; ".join(errors)

            updates.append({"row_idx": idx, "status": status})
        return updates