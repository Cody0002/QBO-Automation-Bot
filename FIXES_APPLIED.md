# Fixes Applied for KZP Data Transformation Issues

## Changes Made

### 1. **Journal Account Source (transformer.py)**
**Issue:** Journal accounts were using the "Type" column instead of the "Bank" column  
**Fix:** Changed both debit and reclass journals to use `COL_BANK` ("Account Fr") instead of `COL_TYPE` ("Type")

**Lines Modified:**
- Standard Journals (Line ~198): Debit account now uses `COL_BANK` instead of `COL_TYPE`
- Reclass Journals (Line ~223): Account now uses `COL_BANK` instead of `COL_TYPE`

### 2. **Blank Countries/Locations (transformer.py)**
**Issue:** Empty Location values weren't being filled with raw CO values  
**Fix:** Added `.fillna()` to fill empty Location columns with raw CO values in:
- Expenses processing (Line ~346)
- Transfers processing (Line ~446)
- Standard Journals (Line ~206)
- Reclass Journals (Line ~228)

### 3. **QBO Account Mapping Diagnostics (transformer.py)**
**Issue:** Difficult to diagnose why accounts aren't found in QBO  
**Fix:** Enhanced error messages to show available account samples:
- Expenses validation (Line ~354-356): Now shows first 3 available accounts
- Transfers validation (Line ~462-464): Now shows first 3 available accounts
- Added warning when no accounts in QBO mappings (Line ~451)

### 4. **Realm ID Verification (run_ingestion.py)**
**Issue:** Unable to verify if correct realm ID was being used  
**Fix:** Added enhanced logging:
- Line 128-135: Now logs number of accounts/locations fetched from QBO
- Line 133: Warns if 0 accounts found (indication of wrong realm ID or invalid token)
- Line 200-202: Added auth logging showing realm ID being authenticated

## Troubleshooting Guide for KZP Account Issues

### If you see "Account not in QBO" errors:

1. **Verify Realm ID (CRITICAL)**
   - Check Master Sheet for KZP's Realm ID
   - Verify the Realm ID is correct by comparing with QBO Admin settings
   - Run ingestion and check logs for: `QBO Mappings fetched: X accounts`
   - If it shows `0 accounts`, realm ID or token is wrong

2. **Check Account Names**
   - The source data in "Account Fr", "Account To", etc. must match exactly what's in QBO
   - Account names in QBO might have hierarchies (e.g., "Banking:Checking Account")
   - The mapping function attempts: exact match → leaf node match → fuzzy match (80%)

3. **Verify Token**
   - Check if Refresh Token in Master Sheet is still valid
   - Tokens expire --- if old, need to re-authorize the QBO connection

### Reference IDs from your request:
- **KZP-JV0159**: Journal entry - now uses Bank/Account Fr column
- **KZP0126T0001**: Transfer (Jan 26) - checking From/To accounts against QBO mappings
- **KZP0126E0003**: Expense (Jan 26) - checking Expense and Source accounts against QBO mappings

## How to Validate Fixes

1. **Run ingestion for KZP**
2. **Check logs for:**
   ```
   🔐 [KZP] Authenticating with Realm ID: <REALM_ID>
   ✅ [KZP] Successfully authenticated
   ✅ [KZP] QBO Mappings fetched: <NUM> accounts, <NUM> locations
   ```

3. **If accounts still not found:**
   - Error messages now show sample available accounts
   - Compare your source account names with the available ones
   - Check if they need to be cleaned/normalized

4. **Verify Location Fills:**
   - Check output for empty Location values
   - Should now be filled with CO value from source

## Reference: Column Mapping

| Raw Column | Renamed To | Used For | Journal | Expense | Transfer |
|---|---|---|---|---|---|
| Account Fr | Bank | Account from Bank column | ✅ Debit | ✅ Source Account | ✅ From Account |
| Account To | - | - | - | - | - |
| If Journal/Expense Method | - | Account for credit/source | ✅ Credit | - | - |
| QBO Transfer Fr | Transfer Funds From | Transfer source | - | - | ✅ Used as-is |
| QBO Transfer To | Transfer Funds To | Transfer destination | - | - | ✅ Used as-is |
| Type | - | (Previously used for Journal debit - NOW REMOVED) | ❌ | | |
| CO | Location | Company/Location | ✅ | ✅ | ✅ |
