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
    TARGET_REALM_ID = "9341455236416310"  
    
    # PASTE YOUR IDs HERE (as strings or numbers)
    ids_to_delete = [
       5060,5061,5062,5063,5064,5065,5164,5165,5166,5167,5168,5169,5170,5171,5172,5173,5174,5066,5067,5068,5069,5070,5071,5072,5073,5074,5075,5076,5077,5078,5079,5080,5081,5082,5083,5084,5085,5086,5087,5088,5089,5090,5091,5092,5093,5094,5095,5096,5097,5098,5099,5100,5101,5102,5103,5104,5105,5106,5107,5108,5109,5110,5111,5112,5113,5175,5176,5177,5178,5179,5180,5181,5182,5183,5184,5185,5186,5187,5188,5189,5190,5191,5192,5193,5194,5195,5196,5197,5198,5199,5200,5201,5202,5203,5204,5205,5206,5207,5208,5209,5210,5211,5212,5213,5214,5215,5216,5217,5218,5219,5220,5221,5222,5223,5224,5225,5226,5227,5228,5229,5230,5231,5232,5233,5234,5235,5236,5237,5238,5239,5240,5241,5242,5243,5244,5245,5246,5247,5115,5116,5117,5118,5119,5120,5121,5122,5123,5124,5125,5126,5127,5128,5129,5130,5131,5132,5133,5134,5135,5136,5137,5138,5139,5140,5141,5142,5143,5144,5145,5146,5147,5148,5149,5150,5151,5152,5153,5154,5155,5156,5157,5158,5159,5160,5161,5162,5248,5249,5250,5251,5252,5253,5254,5255,5256,5257,5258,5259,5260,5261,5262,5263,5264,5265,5266,5267,5268,5269,5270,5271,5272,5273,5274,5275,5276,5277,5278,5279,5280,5281,5282,5283,5284,5285,5286,5287,5288,5289,5290,5291,5292,5293,5294,5295,5296,5297,5298,5299,5300,5301,5302,5303,5304,5305,5306,5307,5308,5309,5310,5311,5312,5313,5314,5315,5316,5317,5318,5319,5320,5321,5322,5323,5324,
    ]
    # Options: "JournalEntry", "Purchase" (Expense), "Transfer", "Deposit"
    ENTITY_TYPE = "Transfer" 
    
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