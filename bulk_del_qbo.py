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
    TARGET_REALM_ID = "9341455236401293"  
    
    # PASTE YOUR IDs HERE (as strings or numbers)
    # ids_to_delete = [
    #     1607,1608,1609,1610,1611,1612,1613,1614,1615,1616,1617,1618,1619,1620,1621,1622,1623,1624,1625,1626,1627,1628,1629,1630,1631,1632,1633,1634,1635,1636,1637,1638,1639,1640,1641,1642,1643,1644,1645,1646,1647,1648,1649,1650,1651,1652,1653,1654,1655,1656,1657,1658,1659,1660,1661,1662,1663,1664,1665,1666,1667,1668,1669,1670,1671,1672,1673,1674,1675,1676,1677,1678,1679,1680,1681,1682,1683,1684,1685,1686,1687,1688,1689,1690,1691,1692,1693,1694,1695,1696,1697,1698,1699,1700,1701,1702,1703,1704,1705,1706,1707,1708,1709,1710,1711,1712,1713,1714,1715,1716,1717,1718,1719,1720,1721,1722,1723,1724,1725,1726,1727,1728,1729,1730,1731,1732,1733,1734,1735,1736,1737,1738,1739,1740,1741,1742,1743,1744,1745,1746,1747,1748,1749,1750,1751,1597,1598,1599,1600,1601,1602,1603,1604,1605,1606,
    # ]
    # Options: "JournalEntry", "Purchase" (Expense), "Transfer", "Deposit"
    # ENTITY_TYPE = "Transfer" 
    
    # -------------------------------------------
    # 2. EXECUTION
    # -------------------------------------------
    qbo.set_company(TARGET_REALM_ID)
    # deleter = QBOMasterDeleter(qbo)

    # print(f"🚀 Starting deletion for {len(ids_to_delete)} {ENTITY_TYPE}(s)...")
    
    # df_result = deleter.delete_by_ids(ids_to_delete, ENTITY_TYPE)
    
    # print("\n--- SUMMARY ---")
    # print(df_result)
    
    # if not df_result.empty:
    #     df_result.to_csv("deletion_log.csv", index=False)
    #     print("📁 Log saved to deletion_log.csv")

    # TARGET_DATE = "2026-04-31"

    # ENTITY_TYPES = [
    #     "JournalEntry",
    #     "Purchase",   # Expense
    #     "Transfer"
    # ]

    # # 👉 THIS LINE bạn đang thiếu
    # deleter = QBOMasterDeleter(qbo)

    # df_result = deleter.delete_by_date(TARGET_DATE, ENTITY_TYPES)

    TARGET_DATE = "2026-07-31"

    ENTITY_TYPES = [
        "JournalEntry",
        "Purchase",   # Expense
        "Transfer"
    ]

    # 👉 THIS LINE bạn đang thiếu
    deleter = QBOMasterDeleter(qbo)

    df_result = deleter.delete_by_date(TARGET_DATE, ENTITY_TYPES)