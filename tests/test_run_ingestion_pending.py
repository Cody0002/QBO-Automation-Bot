import unittest
from unittest.mock import Mock, call, patch

import pandas as pd

import run_ingestion


class KzpSourceHeaderTests(unittest.TestCase):
    REQUIRED_COLUMNS = ["Date", "From Account", "USD - QBO", "QBO Import", "No"]

    def test_reads_new_kzp_layout_from_row_five(self):
        expected = pd.DataFrame(columns=self.REQUIRED_COLUMNS)
        gs = Mock()
        gs.read_as_df.return_value = expected

        result = run_ingestion._read_source_raw_df(gs, "source", "Aug 26", "KZP")

        self.assertIs(result, expected)
        gs.read_as_df.assert_called_once_with(
            "source",
            "Aug 26",
            header_row=5,
            value_render_option="UNFORMATTED_VALUE",
        )

    def test_falls_back_to_legacy_kzp_header_row_four(self):
        invalid_row_five = pd.DataFrame(columns=["", "Unnamed"])
        expected = pd.DataFrame(columns=self.REQUIRED_COLUMNS[:-1])
        gs = Mock()
        gs.read_as_df.side_effect = [invalid_row_five, expected]

        result = run_ingestion._read_source_raw_df(gs, "source", "Jul 26", "KZP")

        self.assertIs(result, expected)
        self.assertEqual(
            gs.read_as_df.call_args_list,
            [
                call(
                    "source",
                    "Jul 26",
                    header_row=5,
                    value_render_option="UNFORMATTED_VALUE",
                ),
                call(
                    "source",
                    "Jul 26",
                    header_row=4,
                    value_render_option="UNFORMATTED_VALUE",
                ),
            ],
        )

    def test_rejects_unrecognized_kzp_headers(self):
        gs = Mock()
        gs.read_as_df.return_value = pd.DataFrame(columns=["", "Unexpected"])

        with self.assertRaisesRegex(ValueError, "KZP raw header not found"):
            run_ingestion._read_source_raw_df(gs, "source", "Aug 26", "KZP")

        self.assertEqual(gs.read_as_df.call_count, 2)


class KzdwForcedPendingTests(unittest.TestCase):
    def test_no_coy_values_are_held_by_default(self):
        raw_df = pd.DataFrame({"COY": ["TD", " td ", "Td"]})

        mask = run_ingestion._get_kzdw_forced_pending_mask(raw_df, "KZDW")

        self.assertEqual(run_ingestion.KZDW_FORCED_PENDING_COY_VALUES, set())
        self.assertEqual(mask.tolist(), [False, False, False])

    def test_held_coy_values_are_matched_case_insensitively(self):
        raw_df = pd.DataFrame({"COY": ["TD", " td ", "Td", "TDD", "", None]})

        with patch.object(run_ingestion, "KZDW_FORCED_PENDING_COY_VALUES", {"TD"}):
            mask = run_ingestion._get_kzdw_forced_pending_mask(raw_df, "KZDW")

        self.assertEqual(mask.tolist(), [True, True, True, False, False, False])

    def test_held_rows_are_not_forced_pending_for_other_clients(self):
        raw_df = pd.DataFrame({"COY": ["TD", "td"]}, index=[4, 9])

        with patch.object(run_ingestion, "KZDW_FORCED_PENDING_COY_VALUES", {"TD"}):
            mask = run_ingestion._get_kzdw_forced_pending_mask(raw_df, "KZO")

        self.assertEqual(mask.to_dict(), {4: False, 9: False})

    def test_forced_nos_are_kept_even_above_processed_checkpoint(self):
        pending_nos = run_ingestion._pending_nos_for_control(
            current_pending_nos={2, 6, 9},
            max_processed_no=6,
            forced_pending_nos={9, 12, -1},
        )

        self.assertEqual(pending_nos, {2, 6, 9, 12})


class DecodeKzoNoTests(unittest.TestCase):
    def test_single_digit_month_decodes_from_the_user_example(self):
        # No=7310132 -> date_str "731" (Jul 31) + run_cnt "0132" (132nd same-date row).
        self.assertEqual(run_ingestion._decode_kzo_no(7310132, expected_month=7), "7/31 seq #132")

    def test_two_digit_month_decodes(self):
        # No=1225045 -> date_str "1225" (Dec 25) + run_cnt "045" (45th same-date row).
        self.assertEqual(run_ingestion._decode_kzo_no(1225045, expected_month=12), "12/25 seq #45")

    def test_ambiguous_prefix_prefers_expected_month(self):
        # "1201234": 3-char reading = Jan 20 (run_cnt 1234); 4-char reading = Dec 1 (run_cnt 234).
        self.assertEqual(run_ingestion._decode_kzo_no(1201234, expected_month=12), "12/1 seq #234")
        self.assertEqual(run_ingestion._decode_kzo_no(1201234, expected_month=1), "1/20 seq #1234")

    def test_ambiguous_prefix_without_expected_month_uses_first_candidate(self):
        self.assertEqual(run_ingestion._decode_kzo_no(1201234, expected_month=None), "1/20 seq #1234")

    def test_wrong_length_is_undecodable(self):
        self.assertIsNone(run_ingestion._decode_kzo_no(123, expected_month=7))
        self.assertIsNone(run_ingestion._decode_kzo_no(12345678, expected_month=7))


if __name__ == "__main__":
    unittest.main()
