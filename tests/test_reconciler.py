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

    def test_excel_serial_raw_dates_are_parsed(self):
        # Raw is read with value_render_option='UNFORMATTED_VALUE', so real raw Date cells
        # arrive as bare Excel serial numbers, not date strings. Naive pd.to_datetime()
        # silently mis-parses a bare number as Unix-epoch nanoseconds instead of erroring,
        # so this must be handled explicitly or every date+amount match silently fails.
        serial = (pd.Timestamp("2026-07-31") - pd.Timestamp("1899-12-30")).days
        raw_rows = [
            {"No": 100, "Date": float(serial), "USD - QBO": 999.00},
            {"No": 101, "Date": float(serial), "USD - QBO": 50.00},
        ]
        transform_rows = [
            {"No": 100, "Date": "2026-07-31", "Amount": 50.00, "Account": "Bank"},
        ]

        updates = self._run(raw_rows, transform_rows)

        self.assertIn("est. No: 101", updates[0]["status"])
        self.assertNotIn("nearby match", updates[0]["status"])

    def test_nearby_no_fallback_when_date_unparseable(self):
        # Real source data isn't always clean; when the raw/transform Date can't be
        # normalized, fall back to scanning Nos near the old No for an amount-only match
        # -- the same manual check an analyst does by hand.
        raw_rows = [
            {"No": 7310129, "Date": "not-a-date", "USD - QBO": 999.00},
            {"No": 7310130, "Date": "not-a-date", "USD - QBO": 111.00},
            {"No": 7310132, "Date": "not-a-date", "USD - QBO": 14398.50},
        ]
        transform_rows = [
            {"No": 7310131, "Date": "not-a-date", "Amount": 14398.50, "Account": "Bank"},
        ]

        updates = self._run(raw_rows, transform_rows)

        self.assertIn("est. No: 7310132", updates[0]["status"])
        self.assertIn("nearby match", updates[0]["status"])

    def test_nearby_no_fallback_respects_window(self):
        # A candidate far outside the neighborhood window should not be offered even if
        # the amount happens to match -- too likely to be an unrelated coincidence.
        raw_rows = [
            {"No": 7310129, "Date": "not-a-date", "USD - QBO": 999.00},
            {"No": 7320500, "Date": "not-a-date", "USD - QBO": 14398.50},
        ]
        transform_rows = [
            {"No": 7310131, "Date": "not-a-date", "Amount": 14398.50, "Account": "Bank"},
        ]

        updates = self._run(raw_rows, transform_rows)

        self.assertIn("est. No: not found", updates[0]["status"])

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
