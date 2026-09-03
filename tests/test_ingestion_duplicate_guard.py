import unittest
from unittest.mock import MagicMock

import pandas as pd

from run_ingestion import (
    _already_transformed_nos,
    _heal_last_processed,
    get_transform_tab_state,
)


def _gs_with_tab(rows, columns=None):
    """A GSheetsClient stub whose single tab holds `rows`."""
    gs = MagicMock()
    df = pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame()
    gs.read_as_df.return_value = df
    return gs


class TransformTabStateTests(unittest.TestCase):
    """The Transform file is the record of what was actually produced."""

    def test_present_nos_reported_when_no_error_rows(self):
        # The common case: a clean tab. Older code returned early here and told the caller
        # nothing, which is why a stale checkpoint could re-transform these rows.
        gs = _gs_with_tab([
            {"No": 8190012, "Exp Ref. No": "KZOPH0826E0027", "Remarks": "Ready to sync"},
            {"No": 8230025, "Exp Ref. No": "KZOPH0826E0034", "Remarks": "Ready to sync"},
        ])

        rows, ids, present = get_transform_tab_state(
            gs, "url", "PH Aug 26 - Expenses", "Exp Ref. No", include_doc_id_match=False
        )

        self.assertEqual(rows, [])
        self.assertEqual(ids, {})
        self.assertEqual(present, {8190012, 8230025})

    def test_error_rows_still_yield_retry_context_plus_present_nos(self):
        gs = _gs_with_tab([
            {"No": 8190012, "Exp Ref. No": "KZOPH0826E0027", "Remarks": "Ready to sync"},
            {"No": 8230025, "Exp Ref. No": "KZOPH0826E0034", "Remarks": "ERROR | Account not found"},
        ])

        rows, ids, present = get_transform_tab_state(
            gs, "url", "PH Aug 26 - Expenses", "Exp Ref. No", include_doc_id_match=False
        )

        self.assertEqual(rows, [3])                              # sheet row of the ERROR row
        self.assertEqual(ids, {8230025: "KZOPH0826E0034"})       # Ref No is reused on rebuild
        self.assertEqual(present, {8190012, 8230025})

    def test_blank_and_zero_nos_are_ignored(self):
        gs = _gs_with_tab([
            {"No": 0, "Ref No": "", "Remarks": ""},
            {"No": "", "Ref No": "", "Remarks": ""},
            {"No": 8230025, "Ref No": "KZOPH0826T0100", "Remarks": "Ready to sync"},
        ])

        _, _, present = get_transform_tab_state(gs, "url", "tab", "Ref No")

        self.assertEqual(present, {8230025})

    def test_missing_tab_reports_nothing_present(self):
        _, _, present = get_transform_tab_state(_gs_with_tab([]), "url", "missing", "Ref No")
        self.assertEqual(present, set())

    def test_tab_without_a_no_column_reports_nothing_present(self):
        gs = _gs_with_tab([{"Ref No": "X", "Remarks": "Ready to sync"}])
        _, _, present = get_transform_tab_state(gs, "url", "tab", "Ref No")
        self.assertEqual(present, set())


class HealLastProcessedTests(unittest.TestCase):
    """Reproduces the KZO PH Aug 2026 incident: checkpoint behind the Transform file."""

    def test_checkpoint_is_raised_to_the_transform_files_max_no(self):
        # The Aug 26 run transformed up to No 8230025 and synced it; the checkpoint cell was
        # later back at 8160052, so 115 already-synced rows looked new again.
        self.assertEqual(_heal_last_processed(8160052, {8170001, 8190012, 8230025}), 8230025)

    def test_checkpoint_never_moves_backwards(self):
        # A month whose later rows were removed from the Transform file must not rewind.
        self.assertEqual(_heal_last_processed(8310090, {8170001, 8230025}), 8310090)

    def test_empty_transform_file_leaves_the_checkpoint_alone(self):
        self.assertEqual(_heal_last_processed(8160052, set()), 8160052)

    def test_fresh_row_state_stays_at_zero_when_nothing_transformed(self):
        self.assertEqual(_heal_last_processed(0, set()), 0)


class AlreadyTransformedNosTests(unittest.TestCase):
    def test_rows_present_in_the_transform_file_are_skipped(self):
        selected = {8170001, 8190012, 8240001}
        transformed = {8170001, 8190012}

        self.assertEqual(
            _already_transformed_nos(selected, transformed, retry_nos=set()),
            {8170001, 8190012},
        )

    def test_retry_nos_are_not_skipped(self):
        # Their Transform rows are deleted before the append, so rebuilding them is intended.
        selected = {8170001, 8190012}
        transformed = {8170001, 8190012}

        self.assertEqual(
            _already_transformed_nos(selected, transformed, retry_nos={8190012}),
            {8170001},
        )

    def test_nothing_to_skip_on_a_clean_run(self):
        self.assertEqual(
            _already_transformed_nos({8240001, 8240002}, {8170001}, retry_nos=set()),
            set(),
        )


if __name__ == "__main__":
    unittest.main()
