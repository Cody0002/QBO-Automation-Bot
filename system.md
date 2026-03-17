# QBO Automation Bot - System Guide

## 1. Purpose
This system automates a multi-workspace accounting pipeline:

1. Ingestion/transform from raw Google Sheets data to structured Journals, Expenses, Transfers.
2. Sync transformed rows to QuickBooks Online (QBO).
3. Reconcile Transform vs QBO (and Raw vs Transform).

Primary scripts:

- `run_ingestion.py`
- `run_syncing.py`
- `run_reconciliation.py`
- `server.py` (webhook trigger entrypoint)

## 2. Core Data Model

### 2.1 Master Sheet (global client registry)
Configured in `config/settings.py`:

- `MASTER_SHEET_ID`, `MASTER_TAB_NAME`
- Key columns:
  - `Client Name` (`MST_COL_CLIENT`)
  - `Spreadsheet ID` (`MST_COL_SHEET_ID`) -> per-client control sheet
  - `Realm ID` (`MST_COL_REALM_ID`)
  - `Status` (`MST_COL_STATUS`)
  - `Refresh Token` (`MST_COL_REFRESH_TOKEN`)

Only rows with `Status=active` and allowed workspace are processed.

### 2.2 Per-client Control Sheet (`JOBS CONTROL`)
Configured by `CONTROL_TAB_NAME` and `CTRL_COL_*` constants:

- `Country`
- `Source File`
- `Transform File`
- `Tab Name`
- `Month`
- `Transform`
- `QBO Sync`
- `QBO Reconcile`
- tracking columns: last processed, last run, counters, statuses

## 3. Pipeline Architecture

### 3.1 Ingestion (`run_ingestion.py`)
Flow per client:

1. Read control sheet.
2. Fast gate: if no `Transform == READY`, skip auth/mappings.
3. Authenticate QBO realm.
4. Fetch mappings.
5. Process each `READY` control row:
   - read/standardize raw
   - filter/select rows
   - transform with `transform_raw`
   - write output tabs
   - update control row statuses/counters

Notes:

- Supports targeting via `--client` (client name, realm id, spreadsheet id, or `all`).
- Journal outputs are now sorted/grouped so debit/credit lines are contiguous per `Journal No`.

### 3.2 Sync (`run_syncing.py`)
Flow per client:

1. Read control sheet.
2. Fast gate: if no `QBO Sync == SYNC NOW`, skip auth/mappings.
3. Authenticate QBO realm.
4. Initialize `QBOSync` mappings.
5. Process tabs and push rows with `Remarks` containing `Ready to sync`.

Current sync pacing behavior:

- Patch/flush size default: `10` (`QBO_SYNC_PATCH_SIZE`)
- Delay per QBO API call: `0.35s` (`QBO_SYNC_CALL_DELAY_SEC`)
- Delay after sheet patch flush: `0.8s` (`QBO_SYNC_PATCH_DELAY_SEC`)

### 3.3 Reconciliation (`run_reconciliation.py`)
Flow per client:

1. Read control sheet.
2. Fast gate: if no `QBO Reconcile == RECONCILE NOW`, skip auth.
3. Authenticate QBO realm.
4. Initialize `Reconciler`.
5. Reconcile Journals/Expenses/Transfers and write statuses.

## 4. Business Rules and Workspace-specific Behavior

### 4.1 Workspace targeting and access control

- Target matching: client name, realm id, or spreadsheet id.
- Allowed workspaces are enforced by `ALLOWED_QBO_WORKSPACES`.
- Names are normalized via `normalize_workspace_name`.

### 4.2 KZDW raw mapping behavior
`src/logic/raw_adapter.py` standardizes KZDW source shape.
Important mapping intent:

- Journal debit: `Transfer to (X)` fallback `If Journal/Expense Another records (V)`
- Journal/Expense source account: `Transfer from (W)` fallback `V`
- Expense destination account derives from `Transfer to (X)`

### 4.3 Journal grouping behavior
`src/logic/transformer.py` ensures rows are grouped by `Journal No` with stable line ordering. This keeps debit/credit pairs together in output tabs.

### 4.4 Vendor and class in sync
`vendors` and `classes` are optional during sync mapping:

- Missing vendor/class does not produce blocking warnings.
- Payload includes `EntityRef` / `ClassRef` only when mapping exists.

### 4.5 FX / currency behavior
`src/logic/syncing.py`:

- Transfer already had FX logic.
- Journal and Expense now also apply explicit `ExchangeRate` for non-USD via `_attach_exchange_rate_if_needed`.
- If FX is missing, sync fails intentionally to avoid accidental 1:1 postings.

## 5. QBO ID Semantics and Reconcile Safety

Current policy:

- Journal sync writes plain QBO transaction ID (no `.1`, `.2` suffix) for all lines in same journal group.

Backward compatibility:

- Reconciler normalizes legacy IDs like `2888.1` -> `2888` before lookup.
- This prevents false "not found" for old historical data.

## 6. Critical Invariants

1. Never sync rows unless `Remarks` indicates ready state.
2. Every API operation must run under the correct realm context (`set_company`).
3. Workspace allow-list is mandatory.
4. Sheet headers can drift; code normalizes many headers before matching.
5. Missing FX for foreign currency should block posting (safety over silent wrong postings).

## 7. Operational Risks to Watch

1. Header drift in source/control/master sheets.
2. Realm mismatch (wrong refresh token/realm pair).
3. Currency/account mismatch in multi-currency workspaces (especially KZDW).
4. Concurrent runs for same control row can cause race conditions.
5. Manual edits to Transform tabs can break expected schema.

## 8. Recommended Change Discipline

1. Keep logic changes localized to one pipeline stage when possible.
2. Preserve backward compatibility for existing sheet data.
3. Run syntax checks after changes:
   - `python -m py_compile run_ingestion.py run_syncing.py run_reconciliation.py src/logic/*.py`
4. Prefer adding non-destructive compatibility helpers (as done for legacy `QBO ID` suffixes).

## 9. Key Source Files

- `config/settings.py` - all constants/column names/base behavior.
- `src/connectors/qbo_client.py` - auth/query/post, pagination, exchange rate API.
- `src/logic/raw_adapter.py` - raw sheet standardization.
- `src/logic/transformer.py` - transform rules and validation.
- `src/logic/syncing.py` - QBO payload building and pushing.
- `src/logic/reconciler.py` - reconciliation logic.
- `run_ingestion.py`, `run_syncing.py`, `run_reconciliation.py` - stage runners.
- `server.py` - webhook trigger API.

