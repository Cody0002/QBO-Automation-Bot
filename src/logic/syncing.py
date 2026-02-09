from __future__ import annotations
import pandas as pd
import re
import difflib
from datetime import datetime
from src.utils.logger import setup_logger
from src.connectors.qbo_client import QBOClient

from dotenv import load_dotenv
load_dotenv("config/secrets.env")

logger = setup_logger("syncing_logic")

def _parse_date_yyyy_mm_dd(val) -> str:
    if pd.isna(val) or val == "":
        return datetime.today().strftime("%Y-%m-%d")
    try:
        dt = pd.to_datetime(val)
        return dt.strftime("%Y-%m-%d")
    except:
        return datetime.today().strftime("%Y-%m-%d")

def _parse_amount(val) -> float:
    try:
        return float(pd.to_numeric(val))
    except:
        return 0.0

def _parse_date_yyyy_mm_dd(val) -> str:
    if pd.isna(val) or val == "":
        return datetime.today().strftime("%Y-%m-%d")
    try:
        dt = pd.to_datetime(val)
        return dt.strftime("%Y-%m-%d")
    except:
        return datetime.today().strftime("%Y-%m-%d")

def _parse_amount(val) -> float:
    try:
        return float(pd.to_numeric(val))
    except:
        return 0.0

class QBOSync:
    def __init__(self, client: QBOClient):
        self.client = client
        self.mappings = self._get_qbo_mappings()

    def _get_qbo_mappings(self) -> dict:
        """Fetches Accounts, Locations, Classes, and Vendors for lookups."""
        logger.info(f"🔍 Fetching QBO Mappings for Realm: {self.client.realm_id}...")
        mappings = {"accounts": {}, "locations": {}, "classes": {}, "vendors": {}}
        
        entities = [
            ("Account", "accounts", "Name, FullyQualifiedName, Id"),
            ("Department", "locations", "Name, FullyQualifiedName, Id"),
            ("Class", "classes", "Name, FullyQualifiedName, Id"),
            ("Vendor", "vendors", "DisplayName, Id") 
        ]

        for table, key, fields in entities:
            try:
                data = self.client.query(f"SELECT {fields} FROM {table} MAXRESULTS 1000")
                for item in data:
                    name = item.get("FullyQualifiedName", item.get("Name", item.get("DisplayName")))
                    mappings[key][name] = item["Id"]
            except Exception as e:
                logger.error(f"❌ Failed to fetch {table}: {e}")

        return mappings

    def find_id(self, mapping_key: str, search_name: str) -> str | None:
        if not search_name or pd.isna(search_name) or str(search_name).strip() == "": return None
        mapping_dict = self.mappings.get(mapping_key, {})
        clean_name = re.sub(r'\s+', ' ', str(search_name)).strip()
        
        # Replacements
        replacements = { "CBD Z Card": "KZO CBD Z", "Leading Card MKT - 1238": "Leading Card - 1238" }
        for k, v in replacements.items():
            if k.lower() in clean_name.lower(): clean_name = v; break

        search_lower = clean_name.lower()
        # Exact & Substring
        for name, qbo_id in mapping_dict.items():
            if name.lower() == search_lower: return qbo_id
        for name, qbo_id in mapping_dict.items():
            if search_lower in name.lower(): return qbo_id
        # Fuzzy
        matches = difflib.get_close_matches(clean_name, list(mapping_dict.keys()), n=1, cutoff=0.85)
        if matches: return mapping_dict[matches[0]]
        return None

    # --- UPDATED: DUPLICATE CHECKER ---
    def get_existing_duplicates(self, entity_type: str, doc_nums: list) -> set:
        """
        Queries QBO to see which IDs already exist.
        - For Journal/Expense: Checks 'DocNumber'.
        - For Transfer: Checks if 'PrivateNote' CONTAINS the Ref No.
        """
        if not doc_nums: return set()
        existing = set()
        clean_docs = list(set([str(d).strip() for d in doc_nums if str(d).strip()]))
        
        # ---------------------------------------------------------
        # CASE A: Journals & Expenses (Check DocNumber)
        # ---------------------------------------------------------
        if entity_type in ["JournalEntry", "Purchase"]:
            chunk_size = 50 
            for i in range(0, len(clean_docs), chunk_size):
                chunk = clean_docs[i:i+chunk_size]
                safe_chunk = [d.replace("'", "\\'") for d in chunk]
                formatted_list = "', '".join(safe_chunk)
                
                query = f"SELECT DocNumber FROM {entity_type} WHERE DocNumber IN ('{formatted_list}')"
                try:
                    results = self.client.query(query)
                    for item in results:
                        existing.add(item.get("DocNumber"))
                except Exception as e:
                    logger.error(f"⚠️ Failed duplicate check {entity_type}: {e}")

        # ---------------------------------------------------------
        # CASE B: Transfers (Check PrivateNote)
        # ---------------------------------------------------------
        elif entity_type == "Transfer":
            # QBO doesn't support "PrivateNote IN (...)" efficiently.
            # We must fetch recent transfers and check Python-side.
            # Fetching last 1000 transfers (or filter by date if you pass date range)
            try:
                # Optimized: We only need PrivateNote
                query = "SELECT PrivateNote FROM Transfer ORDERBY TxnDate DESC MAXRESULTS 500"
                results = self.client.query(query)
                
                # Check if any of our clean_docs exist inside the PrivateNotes
                qbo_notes = [str(item.get("PrivateNote", "")) for item in results]
                
                for doc_ref in clean_docs:
                    # If the Doc Ref is found inside ANY existing PrivateNote
                    if any(doc_ref in note for note in qbo_notes):
                        existing.add(doc_ref)
                        
            except Exception as e:
                logger.error(f"⚠️ Failed duplicate check Transfer: {e}")

        return existing

    def push_journal(self, journal_no: str, group: pd.DataFrame):
        first_row = group.iloc[0]
        line_items = []
        for _, row in group.iterrows():
            amt = _parse_amount(row['Amount'])
            acc_id = self.find_id('accounts', row['Account'])
            if not acc_id: raise ValueError(f"Account '{row['Account']}' not found.")
            
            entity_ref = None
            if row.get('Name'):
                ven_id = self.find_id('vendors', row['Name'])
                if ven_id: entity_ref = {"Type": "Vendor", "EntityRef": {"value": ven_id}}

            line_detail = {
                "PostingType": "Debit" if amt > 0 else "Credit",
                "AccountRef": {"value": acc_id},
                "DepartmentRef": {"value": self.find_id('locations', row.get('Location'))},
                "ClassRef": {"value": self.find_id('classes', row.get('Class'))}
            }
            if entity_ref: line_detail["Entity"] = entity_ref

            line_items.append({
                "Description": str(row.get('Memo') or ""),
                "Amount": abs(amt),
                "DetailType": "JournalEntryLineDetail",
                "JournalEntryLineDetail": line_detail
            })

        payload = {
            "Line": line_items,
            "DocNumber": str(journal_no),
            "TxnDate": _parse_date_yyyy_mm_dd(first_row.get('Date')),
            "PrivateNote": str(first_row.get('Memo', '')),
            "CurrencyRef": {"value": str(first_row.get('Currency Code', 'USD'))}
        }
        # return self.client.post(f"/v3/company/{self.client.realm_id}/journalentry", payload)

    def push_expense(self, exp_ref_no: str, row: pd.Series):
        pay_acc_id = self.find_id("accounts", row.get("Account (Cr)"))
        exp_acc_id = self.find_id("accounts", row.get("Expense Account (Dr)"))
        
        if not pay_acc_id: raise ValueError(f"Payment Account '{row.get('Account (Cr)')}' missing.")
        if not exp_acc_id: raise ValueError(f"Expense Account '{row.get('Expense Account (Dr)')}' missing.")
        
        payee = str(row.get("Payee (Dummy)") or "Dummy")
        vendor_id = self.find_id("vendors", payee)
        entity_ref = {'value': vendor_id, 'name': payee, 'type': 'Vendor'} if vendor_id else {}

        payload = {
            "AccountRef": {"value": pay_acc_id},
            "PaymentType": "Cash",
            "EntityRef": entity_ref,
            "DocNumber": str(exp_ref_no),
            "TxnDate": _parse_date_yyyy_mm_dd(row.get("Payment Date")),
            "CurrencyRef": {"value": str(row.get("Currency", "USD"))},
            "DepartmentRef": {"value": self.find_id('locations', row.get('Location'))},
            "Line": [{
                "DetailType": "AccountBasedExpenseLineDetail",
                "Amount": abs(_parse_amount(row.get("Expense Line Amount"))),
                "AccountBasedExpenseLineDetail": {
                    "AccountRef": {"value": exp_acc_id},
                    "ClassRef": {"value": self.find_id('classes', row.get('Class'))}
                },
                "Description": str(row.get("Memo") or "")
            }]
        }
        # return self.client.post(f"/v3/company/{self.client.realm_id}/purchase", payload)

    def push_transfer(self, row: pd.Series):
        from_id = self.find_id("accounts", row.get("Transfer Funds From"))
        to_id = self.find_id("accounts", row.get("Transfer Funds To"))
        
        if not from_id or not to_id: raise ValueError("Source or Destination Account missing.")
        
        # Note: We put Ref No in PrivateNote because QBO Transfer doesn't support DocNumber well
        ref_no = str(row.get("Ref No", ""))
        memo = str(row.get("Memo", ""))
        full_memo = f"{ref_no} - {memo}"

        payload = {
            "TxnDate": _parse_date_yyyy_mm_dd(row.get("Date")),
            "Amount": abs(_parse_amount(row.get("Transfer Amount"))),
            "FromAccountRef": {"value": from_id},
            "ToAccountRef": {"value": to_id},
            "PrivateNote": full_memo 
        }
        # return self.client.post(f"/v3/company/{self.client.realm_id}/transfer", payload)