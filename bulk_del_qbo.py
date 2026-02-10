from __future__ import annotations
import os
import time
import pandas as pd
from dotenv import load_dotenv

# --- 1. INITIALIZE ENVIRONMENT ---
load_dotenv("config/secrets.env")

try:
    import pip_system_certs.wrappers
    pip_system_certs.wrappers.wrap_requests()
except ImportError:
    pass

from src.connectors.gsheets_client import GSheetsClient
from src.connectors.qbo_client import QBOClient
from src.utils.logger import setup_logger

logger = setup_logger("master_bulk_delete")

class QBOMasterDeleter:
    def __init__(self, client: QBOClient):
        self.client = client
        self.request_delay = 0.5  # Pacing to avoid connection drops

    def delete_by_ids(self, id_list: list[str], entity_type: str) -> pd.DataFrame:
        """
        Deletes items by QBO ID.
        1. Fetches the required 'SyncToken' for each ID.
        2. Sends a batch delete request.
        """
        clean_ids = [str(x).strip() for x in id_list if str(x).strip()]
        if not clean_ids:
            logger.warning("⚠️ No IDs provided.")
            return pd.DataFrame()

        logger.info(f"🔍 Fetching SyncTokens for {len(clean_ids)} {entity_type}(s)...")
        
        # --- Step 1: Get SyncTokens ---
        valid_items = []
        chunk_size = 40  # QBO Query limit is usually safe around 40-50 for simple selects
        
        for i in range(0, len(clean_ids), chunk_size):
            chunk = clean_ids[i : i + chunk_size]
            formatted_ids = ", ".join([f"'{x}'" for x in chunk])
            
            # Query QBO to confirm ID exists and get current SyncToken
            query = f"SELECT Id, SyncToken FROM {entity_type} WHERE Id IN ({formatted_ids})"
            
            try:
                data = self.client.query(query)
                valid_items.extend(data)
            except Exception as e:
                logger.error(f"❌ Failed to fetch metadata for chunk {i}: {e}")

        if not valid_items:
            logger.error("❌ No matching records found in QBO. Check your IDs and Entity Type.")
            return pd.DataFrame()

        logger.info(f"✅ Found {len(valid_items)} valid items. Starting Deletion...")

        # --- Step 2: Execute Batch Delete ---
        results = []
        
        # Batch size for deletion (max 25-30 per request recommended)
        batch_size = 25 
        
        for i in range(0, len(valid_items), batch_size):
            batch = valid_items[i : i + batch_size]
            batch_req = { "BatchItemRequest": [] }
            
            for idx, item in enumerate(batch):
                batch_req["BatchItemRequest"].append({
                    "bId": f"del_{item['Id']}",
                    "operation": "delete",
                    entity_type: { 
                        "Id": item['Id'], 
                        "SyncToken": item['SyncToken'] 
                    }
                })
            
            try:
                endpoint = f"/v3/company/{self.client.realm_id}/batch"
                self.client.post(endpoint, batch_req)
                
                # If no exception, assume success for this batch
                for item in batch:
                    logger.info(f"   🗑️ Deleted ID: {item['Id']}")
                    results.append({"Id": item['Id'], "Type": entity_type, "Status": "Deleted"})
                    
            except Exception as e:
                logger.error(f"   ❌ Batch failed: {e}")
                for item in batch:
                    results.append({"Id": item['Id'], "Type": entity_type, "Status": f"Error: {e}"})
            
            time.sleep(self.request_delay)
            
        return pd.DataFrame(results)

# ==========================================
# CONFIGURATION & RUN
# ==========================================
if __name__ == "__main__":
    gs = GSheetsClient()
    qbo = QBOClient(gs_client=gs)

    # -------------------------------------------
    # 1. SETTINGS
    # -------------------------------------------
    # Enter your Company Realm ID here
    TARGET_REALM_ID = "9341455236392142"  
    
    # PASTE YOUR IDs HERE (as strings or numbers)
    ids_to_delete = [616, 617, 618, 619, 620, 621, 622]

    # SELECT THE TYPE (Must match the IDs above)
    # Options: "JournalEntry", "Purchase" (Expense), "Transfer", "Deposit"
    ENTITY_TYPE = "JournalEntry" 
    
    # -------------------------------------------
    # 2. EXECUTION
    # -------------------------------------------
    qbo.set_company(TARGET_REALM_ID)
    deleter = QBOMasterDeleter(qbo)

    print(f"🚀 Starting deletion for {len(ids_to_delete)} {ENTITY_TYPE}(s)...")
    
    df_result = deleter.delete_by_ids(ids_to_delete, ENTITY_TYPE)
    
    print("\n--- SUMMARY ---")
    print(df_result)
    
    if not df_result.empty:
        df_result.to_csv("deletion_log.csv", index=False)
        print("📁 Log saved to deletion_log.csv")