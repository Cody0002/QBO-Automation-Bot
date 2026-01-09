from __future__ import annotations
import pandas as pd
from datetime import datetime
from src.utils.logger import setup_logger
from src.connectors.qbo_client import QBOClient

logger = setup_logger("syncing_logic")

# --- MISSING HELPER FUNCTIONS ADDED HERE ---
def _parse_date_yyyy_mm_dd(val) -> str:
    """Safely converts input (str or datetime) to 'YYYY-MM-DD' string for QBO."""
    if pd.isna(val) or val == "":
        return datetime.today().strftime("%Y-%m-%d")
    
    try:
        # Attempt to convert to datetime then format
        dt = pd.to_datetime(val, errors='raise')
        return dt.strftime("%Y-%m-%d")
    except:
        # Fallback if parsing fails
        return datetime.today().strftime("%Y-%m-%d")

def _parse_amount(val) -> float:
    """Safely converts input to float."""
    try:
        return float(pd.to_numeric(val))
    except:
        return 0.0

class QBOSync:
    def __init__(self, client: QBOClient):
        self.client = client
        self.mappings = self._get_qbo_mappings()

    def _get_qbo_mappings(self) -> dict:
        """Fetches Mapping IDs using QBOClient's built-in pagination."""
        logger.info("🔍 Fetching QBO Mappings...")
        
        mappings = {"accounts": {}, "locations": {}, "classes": {}}
        queries = {
            "Account": "accounts", 
            "Department": "locations", 
            "Class": "classes"
        }

        for entity, key in queries.items():
            select_stmt = f"SELECT FullyQualifiedName, Id FROM {entity}"
            try:
                all_items = self.client.query(select_stmt)
                mappings[key] = {
                    item.get("FullyQualifiedName", item.get("Name")): item["Id"] 
                    for item in all_items
                }
            except Exception as e:
                logger.error(f"❌ Failed to fetch {entity}: {e}")
                
        return mappings

    def find_id(self, mapping_key: str, search_name: str) -> str | None:
        """Partial match logic for QBO names."""
        if not search_name: return None
        search_name = str(search_name).strip().lower()
        mapping_dict = self.mappings.get(mapping_key, {})
        
        # 1. Exact match
        for qbo_name, qbo_id in mapping_dict.items():
            if qbo_name.lower() == search_name: return qbo_id
            
        # 2. Partial match
        for qbo_name, qbo_id in mapping_dict.items():
            if search_name in qbo_name.lower(): return qbo_id
            
        return None

    # ====================================================
    # 1. JOURNALS
    # ====================================================
    def push_journal(self, journal_no: str, group: pd.DataFrame):
        """Creates a Journal Entry in QBO."""
        
        # Determine Header Info from First Row
        first_row = group.iloc[0]
        currency_code = str(first_row.get('Currency Code', 'USD'))
        txn_date = _parse_date_yyyy_mm_dd(first_row.get('Date'))
        private_note = str(first_row.get('Memo', ''))

        line_items = []
        
        for _, row in group.iterrows():
            amt = _parse_amount(row['Amount'])
            
            # --- Map IDs ---
            acc_id = self.find_id('accounts', row['Account'])
            loc_id = self.find_id('locations', row.get('Location'))
            class_id = self.find_id('classes', row.get('Class'))

            if not acc_id: 
                raise ValueError(f"Account '{row['Account']}' not found in QBO Mappings.")

            # --- Construct Line Item ---
            line_item = {
                "Description": str(row.get('Memo')),
                "Amount": abs(amt),
                "DetailType": "JournalEntryLineDetail",
                "JournalEntryLineDetail": {
                    "PostingType": "Debit" if amt > 0 else "Credit",
                    "AccountRef": {"value": acc_id},
                }
            }
            
            if loc_id:
                line_item["JournalEntryLineDetail"]["DepartmentRef"] = {"value": loc_id}
            if class_id:
                line_item["JournalEntryLineDetail"]["ClassRef"] = {"value": class_id}

            line_items.append(line_item)

        # --- Construct Payload ---
        payload = {
            "Line": line_items,
            "DocNumber": str(journal_no),
            "TxnDate": txn_date,
            "PrivateNote": private_note,
            "CurrencyRef": {"value": currency_code}
        }
        
        # EXECUTE POST
        # print(payload)
        # return
        # return self.client.post("/v3/company/" + self.client.cfg.realm_id + "/journalentry", payload)
# ====================================================
    # 2. EXPENSES (API: Purchase)
    # ====================================================
    def push_expense(self, exp_ref_no: str, row: pd.Series):
        """Create an Expense (Purchase) in QBO."""

        txn_date = _parse_date_yyyy_mm_dd(row.get("Payment Date"))
        amount = abs(_parse_amount(row.get("Expense Line Amount")))
        
        currency_code = str(row.get("Currency", "USD")).strip() or "USD"
        memo = str(row.get("Memo") or row.get("Expense Description") or "").strip()

        # --- 1. Map Accounts ---
        pay_account_name = row.get("Account (Cr)")
        pay_account_id = self.find_id("accounts", pay_account_name)
        if not pay_account_id:
            raise ValueError(f"Payment Account (Cr) not found: '{pay_account_name}'")

        exp_account_name = row.get("Expense Account (Dr)")
        exp_account_id = self.find_id("accounts", exp_account_name)
        if not exp_account_id:
            raise ValueError(f"Expense Account (Dr) not found: '{exp_account_name}'")
            
        if pay_account_id == exp_account_id:
            raise ValueError(f"Invalid Expense: Payment Account and Expense Category are identical (ID: {pay_account_id}).")

        loc_id = self.find_id("locations", row.get("Location"))
        
        # --- 2. HARDCODED PAYEE: "Dummy" ---
        # We explicitly look for "Dummy" in the vendors list.
        # --- 4. Build Payload ---
        # FIX: DepartmentRef is NOT allowed inside AccountBasedExpenseLineDetail for Purchases.
        # 1. Build the Detail Object (Where AccountRef BELONGS)
        line_detail = {
            "AccountRef": {"value": exp_account_id}  # <--- Correct location
        }
        
        # 2. Build the Line Item (Remove AccountRef from here)
        line = {
            "DetailType": "AccountBasedExpenseLineDetail",
            "Amount": amount,
            "AccountBasedExpenseLineDetail": line_detail,
            "Description": memo
            # REMOVED: "AccountRef" (Invalid at this level)
        }

        # 3. Build the Payload (Remove PaymentMethodRef)
        payload = {
            "AccountRef": {"value": pay_account_id},
            "PaymentMethodRef": {"value": "1"},  # Placeholder if needed
            "PaymentType": "Cash",
            "EntityRef": {'value': '1', 'name': 'Dummy', 'type': 'Vendor'},
            "DocNumber": str(exp_ref_no),
            "TxnDate": txn_date,
            "CurrencyRef": {"value": currency_code},
            "DepartmentRef": {"value": loc_id} if loc_id else None,
            "PrivateNote": memo,
            "Line": [line]
            # REMOVED: "PaymentMethodRef" (Invalid for Purchase objects)
        }

        # return self.client.post(f"/v3/company/{self.client.cfg.realm_id}/purchase", payload)
    
    # ====================================================
    # 3. TRANSFERS
    # ====================================================
    def push_transfer(self, row: pd.Series):
        """Create a Transfer in QBO."""
        
        ref_no = str(row.get("Ref No", "")).strip()
        txn_date = _parse_date_yyyy_mm_dd(row.get("Date"))
        currency_code = str(row.get("Currency", "USD")).strip()
        memo = str(row.get("Memo", "")).strip()
        amt = _parse_amount(row.get("Transfer Amount"))

        from_name = row.get("Transfer Funds From")
        to_name = row.get("Transfer Funds To")

        from_id = self.find_id("accounts", from_name)
        to_id = self.find_id("accounts", to_name)

        if not from_id:
            raise ValueError(f"From account not found: '{from_name}'")
        if not to_id:
            raise ValueError(f"To account not found: '{to_name}'")

        # --- NEW CHECK: Prevent Self-Transfer ---
        # This is what caused your 400 Bad Request error
        if from_id == to_id:
            raise ValueError(f"Invalid Transfer: 'From' and 'To' accounts are identical (ID: {from_id}). QBO does not allow self-transfers.")

        payload = {
            "TxnDate": txn_date,
            "Amount": abs(amt),
            "FromAccountRef": {"value": from_id},
            "ToAccountRef": {"value": to_id},
            "PrivateNote": memo,
            "CurrencyRef": {"value": currency_code},
            "DocNumber": ref_no, 
        }

        # return self.client.post(f"/v3/company/{self.client.cfg.realm_id}/transfer", payload)