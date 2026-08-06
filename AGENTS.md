# AI Agent Instructions

## Codebase Graph

A Graphify knowledge graph exists in `graphify-out/`.

Before architecture work, refactors, coding optimization, or client-specific logic changes:

1. Read `graphify-out/GRAPH_REPORT.md`.
2. Use `graphify-out/graph.json` or run `graphify query "<question>"` to inspect relevant code paths.
3. For client-specific logic, compare the KZO / KZP / KZDW / S5 / UMBER branches before editing shared logic.

Key areas for client-specific behavior:

- `run_ingestion.py`: client detection, source header rows, journal prefixes.
- `src/logic/raw_adapter.py`: raw standardization per client family.
- `src/logic/transformer.py`: transform rules and client case helpers.
- `src/logic/syncing.py`: QBO payload and KZDW transfer/FX behavior.
- `src/logic/reconciler.py`: reconciliation behavior.

When optimizing, preserve behavior differences between KZO, KZP, KZDW, S5, and UMBER unless the requested change explicitly unifies them.
