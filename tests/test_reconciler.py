import unittest
from unittest.mock import MagicMock

import pandas as pd

from src.logic.reconciler import Reconciler


class RawVsTransformEstNoTests(unittest.TestCase):
    def setUp(self):
        self.reconciler = Reconciler(MagicMock())

    def _run(self, raw_rows, transform_rows, entity_type="JournalEntry", client_name="KZO"):
        raw_df = pd.DataFrame(raw_rows)
        transform_df = pd.DataFrame(transform_rows)
        return self.reconciler.reconcile_raw_vs_transform(raw_df, transform_df, entity_type, client_name)

    def test_shifted_row_gets_a_single_est_no(self):
        # Transform captured No=100 for a $50 tx on 2026-07-31. A row was inserted above it
        # in raw, so that same tx now lives at No=101; No=100 in raw is a different, unrelated tx.
        raw_rows = [
            {"No": 100, "Date": "2026-07-31", "USD - QBO": 999.00},
            {"No": 101, "Date": "2026-07-31", "USD - QBO": 50.00},
        ]
        transform_rows = [
            {"No": 100, "Date": "2026-07-31", "Amount": 50.00, "Account": "Bank"},
        ]

        updates = self._run(raw_rows, transform_rows)

        self.assertEqual(len(updates), 1)
        self.assertIn("Unmatched: Amt Diff", updates[0]["status"])
        self.assertIn("est. No: 101", updates[0]["status"])

    def test_no_candidate_reports_not_found(self):
        raw_rows = [
            {"No": 100, "Date": "2026-07-31", "USD - QBO": 999.00},
        ]
        transform_rows = [
            {"No": 100, "Date": "2026-07-31", "Amount": 50.00, "Account": "Bank"},
        ]

        updates = self._run(raw_rows, transform_rows)

        self.assertIn("est. No: not found", updates[0]["status"])

    def test_ambiguous_candidates_are_listed(self):
        raw_rows = [
            {"No": 100, "Date": "2026-07-31", "USD - QBO": 999.00},
            {"No": 101, "Date": "2026-07-31", "USD - QBO": 50.00},
            {"No": 102, "Date": "2026-07-31", "USD - QBO": 50.00},
        ]
        transform_rows = [
            {"No": 100, "Date": "2026-07-31", "Amount": 50.00, "Account": "Bank"},
        ]

        updates = self._run(raw_rows, transform_rows)

        self.assertIn("est. No: ambiguous (101, 102)", updates[0]["status"])

    def test_matched_raw_row_is_not_offered_as_a_candidate(self):
        # Raw No=101 is already the correct (Matched) destination for transform No=101.
        # Transform No=100 is unrelated and Unmatched (raw No=100 holds a different amount);
        # without excluding already-claimed raw rows, No=101 would be wrongly re-offered.
        raw_rows = [
            {"No": 100, "Date": "2026-07-31", "USD - QBO": 999.00},
            {"No": 101, "Date": "2026-07-31", "USD - QBO": 50.00},
        ]
        transform_rows = [
            {"No": 100, "Date": "2026-07-31", "Amount": 50.00, "Account": "Bank"},
            {"No": 101, "Date": "2026-07-31", "Amount": 50.00, "Account": "Bank"},
        ]

        updates = self._run(raw_rows, transform_rows)

        statuses = {u["row_idx"]: u["status"] for u in updates}
        self.assertIn("est. No: not found", statuses[0])
        self.assertEqual(statuses[1], "Matched")

    def test_non_kzo_client_is_unaffected(self):
        raw_rows = [
            {"No": 100, "Date": "2026-07-31", "USD - QBO": 999.00},
            {"No": 101, "Date": "2026-07-31", "USD - QBO": 50.00},
        ]
        transform_rows = [
            {"No": 100, "Date": "2026-07-31", "Amount": 50.00, "Account": "Bank"},
        ]

        updates = self._run(raw_rows, transform_rows, client_name="KZP")

        self.assertEqual(updates[0]["status"], "Unmatched: Amt Diff (999.00 vs 50.00)")

    def test_missing_client_name_is_unaffected(self):
        raw_rows = [
            {"No": 100, "Date": "2026-07-31", "USD - QBO": 999.00},
            {"No": 101, "Date": "2026-07-31", "USD - QBO": 50.00},
        ]
        transform_rows = [
            {"No": 100, "Date": "2026-07-31", "Amount": 50.00, "Account": "Bank"},
        ]

        updates = self._run(raw_rows, transform_rows, client_name="")

        self.assertEqual(updates[0]["status"], "Unmatched: Amt Diff (999.00 vs 50.00)")


if __name__ == "__main__":
    unittest.main()
