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
    
    def delete_by_date(self, target_date: str, entity_types: list[str]) -> pd.DataFrame:
        """
        Deletes transactions by date for multiple entity types.
        target_date format: 'YYYY-MM-DD' (e.g., '2026-03-31')
        entity_types: ["JournalEntry", "Purchase", "Transfer"]
        """
        all_results = []

        for entity_type in entity_types:
            logger.info(f"🔍 Fetching {entity_type} for date {target_date}...")

            query = f"""
            SELECT Id, SyncToken 
            FROM {entity_type} 
            WHERE TxnDate = '{target_date}'
            """

            try:
                data = self.client.query(query)
            except Exception as e:
                logger.error(f"❌ Query failed for {entity_type}: {e}")
                continue

            if not data:
                logger.warning(f"⚠️ No {entity_type} found for {target_date}")
                continue

            logger.info(f"✅ Found {len(data)} {entity_type}(s), deleting...")

            # reuse your existing logic
            ids = [item["Id"] for item in data]
            df = self.delete_by_ids(ids, entity_type)

            all_results.append(df)

        if all_results:
            return pd.concat(all_results, ignore_index=True)
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
    # Enter your Company Realm ID here
    # TARGET_REALM_ID = "9341455236413167"  
    
    # PASTE YOUR IDs HERE (as strings or numbers)
    ids_to_delete = [
    #     4553,4554,4555,4556,4557,4558,4559,4560,4561,4562,4563,4564,4565,4566,4567,4568,4569,4570,4571,4572,4573,4574,4575,4576,4577,4578,4579,4580,4581,4582,4583,4584,4585,4586,4587,4588,4589,4590,4591,4592,4593,4594,4595,4596,4597,4598,4599,4600,4601,4602,4603,4604,4605,4606,4607,4608,4609,4610,4611,4612,4613,4614,4615,4616,4617,4618,4619,4620,4621,4622,4623,4624,4625,4626,4627,4628,4629,4630,4631,4632,4633,4634,4635,4636,4637,4638,4639,4640,4641,4642,4643,4644,4645,4646,4647,4648,4649,4650,4651,4652,4653,4654,4655,4656,4657,4658,4659,4660,4661,4662,4663,4664,4665,4666,4667,4668,4669,4670,4671,4672,4673,4674,4675,4676,4677,4678,4679,4680,4681,4682,4683,4684,4685,4686,4687,4688,4689,4690,4691,4692,4693,4694,4695,4696,4697,4698,4699,4700,4701,4702,4703,4704,4705,4706,4707,4708,4709,4710,4711,4712,4713,4714,4715,4716,4717,4718,4719,4720,4721,4722,4723,4724,4725,4726,4727,4728,4729,4730,4731,4732,4733,4734,4735,4736,4737,4738,4739,4740,4741,4742,4743,4744,4745,4746,4747,4748,4749,4750,4751,4752,4753,4754,4755,4756,4757,4758,4759,4760,4761,4762,4763,4764,4765,4766,4767,4768,4769,4770,
    ]
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

    # TARGET_DATE = "2026-04-31"

    # ENTITY_TYPES = [
    #     "JournalEntry",
    #     "Purchase",   # Expense
    #     "Transfer"
    # ]

    # # 👉 THIS LINE bạn đang thiếu
    # deleter = QBOMasterDeleter(qbo)

    # df_result = deleter.delete_by_date(TARGET_DATE, ENTITY_TYPES)