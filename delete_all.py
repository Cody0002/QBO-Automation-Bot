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

logger = setup_logger("master_delete_all")

class QBOMasterDeleter:
    def __init__(self, client: QBOClient):
        self.client = client
        self.request_delay = 0.5  # Pacing to avoid connection drops

    def delete_all(self, entity_types: list[str]) -> pd.DataFrame:
        """
        Deletes ALL transactions for multiple entity types in the workspace.
        Queries items in batches of 500 and deletes them in batches of 25.
        """
        all_results = []

        for entity_type in entity_types:
            logger.info(f"🔍 Fetching ALL {entity_type}(s) to delete...")

            while True:
                # 1. Query up to 500 items at a time and fetch SyncToken immediately!
                # This is highly optimized to avoid the extra fetching step.
                query = f"SELECT Id, SyncToken FROM {entity_type} MAXRESULTS 500"
                
                try:
                    data = self.client.query(query)
                except Exception as e:
                    logger.error(f"❌ Query failed for {entity_type}: {e}")
                    break

                if not data:
                    logger.info(f"✅ No more {entity_type}s found in the workspace.")
                    break

                logger.info(f"✅ Found {len(data)} {entity_type}(s) in this batch. Executing deletion...")

                # 2. Execute Batch Delete (max 25-30 per request)
                batch_size = 25 
                for i in range(0, len(data), batch_size):
                    batch = data[i : i + batch_size]
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
                        
                        # If no exception, record success
                        for item in batch:
                            logger.info(f"   🗑️ Deleted {entity_type} ID: {item['Id']}")
                            all_results.append({"Id": item['Id'], "Type": entity_type, "Status": "Deleted"})
                            
                    except Exception as e:
                        logger.error(f"   ❌ Batch failed: {e}")
                        for item in batch:
                            all_results.append({"Id": item['Id'], "Type": entity_type, "Status": f"Error: {e}"})
                    
                    # Sleep to respect QBO API rate limits
                    time.sleep(self.request_delay)

        if all_results:
            return pd.DataFrame(all_results)
        else:
            return pd.DataFrame()

# ==========================================
# CONFIGURATION & RUN
# ==========================================
if __name__ == "__main__":
    gs = GSheetsClient()
    qbo = QBOClient(gs_client=gs)

    # -------------------------------------------
    # 1. SETTINGS
    # -------------------------------------------
    # Workspace/Realm ID you provided
    TARGET_REALM_ID = "9341455236392142"  
    
    # List of all entity types you want to completely wipe.
    # Note: Adjust this list depending on exactly what you want to wipe. 
    # Deleting some parent entities (like Invoices) might automatically delete related payments depending on QBO settings.
    ENTITY_TYPES_TO_WIPE = [
        "JournalEntry",
        "Purchase",    # Expenses
        "Transfer",
        "Deposit",
        "Invoice",
        "Payment",
        "Bill",
        "BillPayment",
        "SalesReceipt",
        "RefundReceipt"
    ]
    
    # -------------------------------------------
    # 2. EXECUTION
    # -------------------------------------------
    qbo.set_company(TARGET_REALM_ID)
    deleter = QBOMasterDeleter(qbo)

    print(f"🚀 Starting MASS DELETION for Workspace: {TARGET_REALM_ID}...")
    
    df_result = deleter.delete_all(ENTITY_TYPES_TO_WIPE)
    
    print("\n--- MASS DELETION SUMMARY ---")
    if not df_result.empty:
        # Print a clean summary of what happened
        summary = df_result.groupby(['Type', 'Status']).size().reset_index(name='Count')
        print(summary.to_string(index=False))
        
        # Save exact log
        df_result.to_csv("mass_deletion_log.csv", index=False)
        print("\n📁 Detailed log saved to mass_deletion_log.csv")
    else:
        print("No items were deleted (the workspace might already be empty for these specific entities).")