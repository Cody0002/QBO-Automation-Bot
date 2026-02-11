from __future__ import annotations
import pandas as pd
import calendar
import re
import difflib
from src.connectors.qbo_client import QBOClient
from src.utils.logger import setup_logger

logger = setup_logger("reconciler")

class Reconciler:
    def __init__(self, qbo_client: QBOClient):
        self.client = qbo_client

    def _normalize_account(self, name: str) -> str:
        """
        Simple normalization for display/logging.
        Example: 'Fixed Assets:Equipment' -> 'equipment'
        """
        if not name: return ""
        return name.split(":")[-1].strip().lower()

    def _get_month_range(self, date_str: str) -> tuple[str, str]:
        try:
            dt = pd.to_datetime(date_str)
            _, last_day = calendar.monthrange(dt.year, dt.month)
            return f"{dt.year}-{dt.month:02d}-01", f"{dt.year}-{dt.month:02d}-{last_day}"
        except:
            return None, None

    def _fetch_qbo_data(self, entity: str, start_date: str, end_date: str) -> tuple[dict, dict]:
        logger.info(f"   🔍 Querying QBO {entity} [{start_date} to {end_date}]...")
        query = f"SELECT * FROM {entity} WHERE TxnDate >= '{start_date}' AND TxnDate <= '{end_date}' MAXRESULTS 1000"
        results = self.client.query(query)
        
        map_id = {}
        map_doc = {}
        
        for item in results:
            if "Id" in item: map_id[str(item["Id"])] = item
            doc_num = item.get("DocNumber")
            if doc_num: map_doc[str(doc_num)] = item
                
        return map_id, map_doc

    def _fetch_transfers_list(self, start_date: str, end_date: str) -> list:
        logger.info(f"   🔍 Querying QBO Transfers [{start_date} to {end_date}]...")
        query = f"SELECT * FROM Transfer WHERE TxnDate >= '{start_date}' AND TxnDate <= '{end_date}' MAXRESULTS 1000"
        return self.client.query(query)

    # --- NEW: ROBUST MATCHING LOGIC (COPIED FROM SYNCING) ---
    def _is_account_match(self, sheet_acc: str, qbo_acc: str) -> bool:
        """
        Returns True if sheet_acc matches qbo_acc using:
        1. Explicit Replacements
        2. Exact Match
        3. Leaf Match
        4. Fuzzy Match (>80%)
        """
        if not sheet_acc or not qbo_acc: return False
        
        # 0. Basic Clean
        sheet_clean = re.sub(r'\s+', ' ', str(sheet_acc)).strip()
        qbo_clean = re.sub(r'\s+', ' ', str(qbo_acc)).strip()
        
        # 1. Replacements (Hardcoded fixes)
        replacements = {
            "CBD Z Card":   "KZO CBD Z",
            "Leading Card MKT - 1238": "Leading Card - 1238"
        }
        for bad_text, target_text in replacements.items():
            if bad_text.lower() in sheet_clean.lower():
                sheet_clean = re.sub(re.escape(bad_text), target_text, sheet_clean, flags=re.IGNORECASE)
                break

        s_lower = sheet_clean.lower()
        q_lower = qbo_clean.lower()

        # 2. EXACT MATCH
        if s_lower == q_lower: return True

        # 3. LEAF MATCH (e.g. Sheet="Equipment" == QBO="Fixed Assets:Equipment")
        if ":" in q_lower:
            q_leaf = q_lower.split(":")[-1].strip()
            if s_lower == q_leaf: return True
        
        # 4. FUZZY MATCH (80%)
        # Check against full name
        if difflib.SequenceMatcher(None, s_lower, q_lower).ratio() >= 0.80:
            return True
        
        # Check against leaf name (often needed for fuzzy matches on sub-accounts)
        if ":" in q_lower:
            q_leaf = q_lower.split(":")[-1].strip()
            if difflib.SequenceMatcher(None, s_lower, q_leaf).ratio() >= 0.80:
                return True
                
        return False

    # --- 1. JOURNALS (UPGRADED) ---
    def reconcile_journals(self, df: pd.DataFrame, month_str: str) -> list[dict]:
        """
        Performs Line-by-Line reconciliation with Fuzzy Matching.
        """
        if df.empty or "Journal No" not in df.columns: return []

        start, end = self._get_month_range(month_str)
        if not start: return []

        map_id, map_doc = self._fetch_qbo_data("JournalEntry", start, end)
        updates = []

        # Process each Journal Group (e.g., JV-001)
        for jv_no, group in df.groupby("Journal No"):
            first_row = group.iloc[0]
            
            # 1. FIND QBO RECORD
            qbo_record = None
            if "QBO ID" in first_row and pd.notna(first_row["QBO ID"]):
                qbo_record = map_id.get(str(first_row["QBO ID"]).strip())
            if not qbo_record:
                qbo_record = map_doc.get(str(jv_no).strip())

            # 2. IF MISSING ENTIRELY
            if not qbo_record:
                for idx in group.index:
                    updates.append({"row_idx": idx, "status": "❌ Journal Not Found in QBO"})
                continue

            # 3. CHECK HEADER (Date & Memo)
            header_errors = []
            
            # Date Check
            sheet_date = pd.to_datetime(first_row["Date"]).strftime("%Y-%m-%d")
            qbo_date = qbo_record.get("TxnDate")
            if sheet_date != qbo_date:
                header_errors.append(f"Date Mismatch ({sheet_date} vs {qbo_date})")

            # Memo Check (Loose Match)
            sheet_memo = str(first_row.get("Memo", "")).strip().lower()
            qbo_memo = str(qbo_record.get("PrivateNote", "")).strip().lower()
            if sheet_memo and sheet_memo not in qbo_memo:
                 header_errors.append(f"Memo Mismatch")

            header_status_prefix = "⚠️ " + "; ".join(header_errors) + " | " if header_errors else ""

            # 4. PREPARE QBO LINES POOL (For Matching)
            qbo_lines_pool = []
            for line in qbo_record.get("Line", []):
                detail = line.get("JournalEntryLineDetail", {})
                
                # STORE FULL NAME FOR FUZZY MATCHING
                full_acc_name = detail.get("AccountRef", {}).get("name", "")
                
                # Determine amount (Debit positive, Credit negative for matching logic)
                amt = float(line.get("Amount", 0))
                if detail.get("PostingType") == "Credit":
                    amt = -amt
                
                qbo_lines_pool.append({
                    "full_name": full_acc_name, # Stored for matching
                    "display_name": self._normalize_account(full_acc_name), # Stored for error msg
                    "amount": amt,
                    "matched": False
                })

            # 5. MATCH SHEET ROWS TO QBO LINES
            for idx, row in group.iterrows():
                row_status = "✅ Matched"
                if header_errors:
                    row_status = f"⚠️ Header: {'; '.join(header_errors)}"

                sheet_acc_raw = str(row["Account"])
                try:
                    sheet_amt = float(row["Amount"])
                except:
                    sheet_amt = 0.0

                found_match = False
                
                # Pass 1: FUZZY Account Match AND Strict Amount Match
                for q_line in qbo_lines_pool:
                    if not q_line["matched"]:
                        # Use new fuzzy helper
                        is_acc_match = self._is_account_match(sheet_acc_raw, q_line["full_name"])
                        is_amt_match = abs(q_line["amount"] - sheet_amt) < 0.01

                        if is_acc_match and is_amt_match:
                            q_line["matched"] = True
                            found_match = True
                            break
                
                if found_match:
                    updates.append({"row_idx": idx, "status": row_status})
                    continue

                # Pass 2: Amount Match ONLY (Wrong Account)
                for q_line in qbo_lines_pool:
                    if not q_line["matched"]:
                        if abs(q_line["amount"] - sheet_amt) < 0.01:
                            updates.append({
                                "row_idx": idx, 
                                "status": f"{header_status_prefix}❌ Account Mismatch (Sheet: '{sheet_acc_raw}' vs QBO: '{q_line['display_name']}')"
                            })
                            q_line["matched"] = True
                            found_match = True
                            break
                
                if found_match: continue

                # Pass 3: FUZZY Account Match ONLY (Wrong Amount)
                for q_line in qbo_lines_pool:
                    if not q_line["matched"]:
                        if self._is_account_match(sheet_acc_raw, q_line["full_name"]):
                            updates.append({
                                "row_idx": idx, 
                                "status": f"{header_status_prefix}❌ Amount Mismatch (Sheet: {sheet_amt} vs QBO: {q_line['amount']})"
                            })
                            q_line["matched"] = True
                            found_match = True
                            break
                
                if found_match: continue

                # Pass 4: No match found
                updates.append({
                    "row_idx": idx, 
                    "status": f"{header_status_prefix}❌ No matching line in QBO for '{sheet_acc_raw}' : {sheet_amt}"
                })

        return updates

    # --- 2. EXPENSES (UPDATED: ONLY CHECK PAYMENT ACCOUNT) ---
    def reconcile_expenses(self, df: pd.DataFrame, month_str: str) -> list[dict]:
        """
        Validates Date, Amount, and PAYMENT ACCOUNT (Account Cr).
        Ignores Expense Category Lines.
        """
        if df.empty or "Exp Ref. No" not in df.columns: return []
        start, end = self._get_month_range(month_str)
        if not start: return []
        map_id, map_doc = self._fetch_qbo_data("Purchase", start, end)
        updates = []

        for idx, row in df.iterrows():
            qbo_record = None
            if "QBO ID" in row and pd.notna(row["QBO ID"]):
                qbo_record = map_id.get(str(row["QBO ID"]).strip())
            if not qbo_record:
                qbo_record = map_doc.get(str(row.get("Exp Ref. No", "")).strip())

            if not qbo_record:
                updates.append({"row_idx": idx, "status": "❌ Not found in QBO"})
                continue

            errors = []
            
            # 1. Date Check
            s_date = pd.to_datetime(row["Payment Date"]).strftime("%Y-%m-%d")
            q_date = qbo_record.get("TxnDate")
            if s_date != q_date: errors.append(f"Date: {s_date} != {q_date}")

            # 2. Amount Check
            s_amt = abs(float(row.get("Expense Line Amount", 0)))
            q_amt = float(qbo_record.get("TotalAmt", 0))
            if abs(s_amt - q_amt) > 0.01:
                errors.append(f"Amount: {s_amt:,.2f} != {q_amt:,.2f}")

            # 3. PAYMENT ACCOUNT CHECK (Account Cr)
            # This checks the main 'AccountRef' on the Purchase object (where money came FROM)
            sheet_pay_acc = str(row.get("Account (Cr)", ""))
            qbo_pay_acc = qbo_record.get("AccountRef", {}).get("name", "")
            
            # Use robust match
            if not self._is_account_match(sheet_pay_acc, qbo_pay_acc):
                 errors.append(f"Payment Account Mismatch: '{sheet_pay_acc}' != '{self._normalize_account(qbo_pay_acc)}'")

            status = "✅ Matched" if not errors else "⚠️ " + "; ".join(errors)
            updates.append({"row_idx": idx, "status": status})

        return updates

    # --- 3. TRANSFERS ---
    def reconcile_transfers(self, df: pd.DataFrame, month_str: str) -> list[dict]:
        if df.empty or "Ref No" not in df.columns: return []
        start, end = self._get_month_range(month_str)
        if not start: return []

        qbo_list = self._fetch_transfers_list(start, end)
        map_id = {str(item["Id"]): item for item in qbo_list if "Id" in item}
        updates = []

        for idx, row in df.iterrows():
            qbo_record = None
            if "QBO ID" in row and pd.notna(row["QBO ID"]) and str(row["QBO ID"]).strip():
                qbo_record = map_id.get(str(row["QBO ID"]).strip())
            
            if not qbo_record:
                ref_no = str(row.get("Ref No", "")).strip()
                qbo_record = next((item for item in qbo_list if ref_no in item.get("PrivateNote", "")), None)

            if not qbo_record:
                updates.append({"row_idx": idx, "status": "❌ Not found in QBO"})
                continue

            errors = []
            s_amt = abs(float(row.get("Transfer Amount", 0)))
            q_amt = float(qbo_record.get("Amount", 0))
            
            if abs(s_amt - q_amt) > 0.01:
                errors.append(f"Amount: {s_amt:,.2f} != {q_amt:,.2f}")
            
            s_date = pd.to_datetime(row["Date"]).strftime("%Y-%m-%d")
            q_date = qbo_record.get("TxnDate")
            if s_date != q_date:
                errors.append(f"Date: {s_date} != {q_date}")

            status = "✅ Matched" if not errors else "⚠️ " + "; ".join(errors)
            updates.append({"row_idx": idx, "status": status})

        return updates

    # --- 4. RAW vs TRANSFORM RECONCILE (FIXED) ---
    def reconcile_raw_vs_transform(self, raw_df: pd.DataFrame, transform_df: pd.DataFrame, entity_type: str) -> list[dict]:
        updates = []
        if raw_df.empty or "No" not in raw_df.columns: 
            logger.warning("⚠️ Raw Comparison Skipped: Empty DataFrame or missing 'No' column")
            return []
        
        # 1. Clean & Index Raw
        raw_clean = raw_df.copy()
        raw_clean["_Key"] = pd.to_numeric(raw_clean["No"], errors="coerce").fillna(0).astype(int)
        raw_clean = raw_clean[raw_clean["_Key"] > 0].set_index("_Key")

        # 2. Group Transform (Aggregate Max Abs Value)
        transform_df["_Key"] = pd.to_numeric(transform_df["No"], errors="coerce").fillna(0).astype(int)
        
        # --- FIX: Restore Dynamic Column Selection for Transform DF ---
        # The Transform DataFrame (output of transformer.py) does NOT have "USD - QBO".
        # It has "Amount", "Expense Line Amount", or "Transfer Amount".
        amt_col = "Amount"
        if entity_type == "Purchase": amt_col = "Expense Line Amount"
        elif entity_type == "Transfer": amt_col = "Transfer Amount"
        
        transform_agg = transform_df.groupby("_Key")[amt_col].apply(lambda x: x.apply(self._safe_float).abs().max())

        # 3. Compare & Assign Status
        status_map = {}
        mismatch_count = 0
        
        for no_val, sheet_amt in transform_agg.items():
            if no_val not in raw_clean.index:
                status_map[no_val] = "Unmatched: Missing in Raw"
                continue

            raw_row = raw_clean.loc[no_val]
            
            # --- FIX: STRICTLY use "USD - QBO" from Raw ---
            final_raw_val = raw_row.get("USD - QBO", 0)
            
            raw_abs = abs(self._safe_float(final_raw_val))
            sheet_abs = abs(self._safe_float(sheet_amt))

            if abs(raw_abs - sheet_abs) > 0.05:
                # logger.info(f"   ❌ MISMATCH [No {no_val}]: Raw={raw_abs:,.2f} vs Sheet={sheet_abs:,.2f}")
                status_map[no_val] = f"Unmatched: Amt Diff ({raw_abs:,.2f} vs {sheet_abs:,.2f})"
                mismatch_count += 1
            else:
                status_map[no_val] = "Matched"

        # if mismatch_count > 0:
        #     logger.info(f"   ⚠️ Found {mismatch_count} value mismatches in {entity_type}")

        # 4. Broadcast
        for idx, row in transform_df.iterrows():
            no_val = int(row.get("_Key", 0))
            if no_val == 0: continue
            status = status_map.get(no_val, "Skipped")
            updates.append({"row_idx": idx, "status": status})

        return updates