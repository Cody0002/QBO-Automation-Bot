import unittest

import run_reconciliation


class SummarizeOrphanRowsTests(unittest.TestCase):
    def test_single_row_is_not_grouped(self):
        orphan_rows = [
            {"no": 50, "entity_type": "Journal", "date": "2026-07-10", "amount": 1250.00},
        ]

        note, log_lines = run_reconciliation._summarize_orphan_rows(orphan_rows, last_processed_no=100)

        self.assertEqual(note, "50 (Journal, 2026-07-10, $1,250.00)")
        self.assertEqual(len(log_lines), 1)

    def test_identical_rows_are_grouped_into_one_line(self):
        # This is the exact production scenario that crashed reconciliation: many
        # consecutive Transfer rows with the same date+amount, one line per No.
        orphan_rows = [
            {"no": n, "entity_type": "Transfer", "date": "2026-07-31", "amount": 3038.82}
            for n in range(7310039, 7310046)
        ]

        note, log_lines = run_reconciliation._summarize_orphan_rows(orphan_rows, last_processed_no=7310301)

        self.assertEqual(note, "7310039-7310045 x7 (Transfer, 2026-07-31, $3,038.82)")
        self.assertEqual(len(log_lines), 1)

    def test_distinct_groups_stay_separate(self):
        orphan_rows = [
            {"no": 50, "entity_type": "Journal", "date": "2026-07-10", "amount": 1250.00},
            {"no": 51, "entity_type": "Journal", "date": "2026-07-10", "amount": 999.00},
            {"no": 52, "entity_type": "Purchase", "date": "2026-07-10", "amount": 1250.00},
        ]

        note, log_lines = run_reconciliation._summarize_orphan_rows(orphan_rows, last_processed_no=100)

        self.assertEqual(len(log_lines), 3)
        self.assertIn("50 (Journal, 2026-07-10, $1,250.00)", note)
        self.assertIn("51 (Journal, 2026-07-10, $999.00)", note)
        self.assertIn("52 (Purchase, 2026-07-10, $1,250.00)", note)

    def test_huge_batch_is_truncated_below_sheets_cell_limit(self):
        # Even a pathological number of distinct groups (so grouping alone can't collapse
        # them) must never produce a note anywhere near Sheets' 50,000-char/cell limit.
        orphan_rows = [
            {"no": n, "entity_type": "Transfer", "date": "2026-07-31", "amount": float(n)}
            for n in range(7310000, 7315000)
        ]

        note, log_lines = run_reconciliation._summarize_orphan_rows(orphan_rows, last_processed_no=7320000)

        self.assertLessEqual(len(note), 2000)
        self.assertIn("truncated", note)
        self.assertEqual(len(log_lines), 5000)


if __name__ == "__main__":
    unittest.main()
