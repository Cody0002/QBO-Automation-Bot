import unittest
from unittest.mock import patch

import pandas as pd

import run_ingestion


class KzdwForcedPendingTests(unittest.TestCase):
    def test_kzdw_td_rows_are_matched_case_insensitively(self):
        raw_df = pd.DataFrame({"COY": ["TD", " td ", "Td", "TDD", "", None]})

        mask = run_ingestion._get_kzdw_forced_pending_mask(raw_df, "KZDW")

        self.assertEqual(mask.tolist(), [True, True, True, False, False, False])

    def test_td_rows_are_not_forced_pending_for_other_clients(self):
        raw_df = pd.DataFrame({"COY": ["TD", "td"]}, index=[4, 9])

        mask = run_ingestion._get_kzdw_forced_pending_mask(raw_df, "KZO")

        self.assertEqual(mask.to_dict(), {4: False, 9: False})

    def test_clearing_hold_values_releases_td_rows(self):
        raw_df = pd.DataFrame({"COY": ["TD"]})

        with patch.object(run_ingestion, "KZDW_FORCED_PENDING_COY_VALUES", set()):
            mask = run_ingestion._get_kzdw_forced_pending_mask(raw_df, "KZDW")

        self.assertEqual(mask.tolist(), [False])

    def test_forced_nos_are_kept_even_above_processed_checkpoint(self):
        pending_nos = run_ingestion._pending_nos_for_control(
            current_pending_nos={2, 6, 9},
            max_processed_no=6,
            forced_pending_nos={9, 12, -1},
        )

        self.assertEqual(pending_nos, {2, 6, 9, 12})


if __name__ == "__main__":
    unittest.main()
