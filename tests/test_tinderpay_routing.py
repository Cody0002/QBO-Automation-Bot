import unittest
from unittest.mock import Mock, patch

import pandas as pd

import run_ingestion
from config import settings
from src.logic import raw_adapter, syncing, transformer


class KzdwFamilyTests(unittest.TestCase):
    """TINDERPAY runs on KZDW's layout and posting rules under its own QBO company."""

    def test_kzdw_and_tinderpay_are_the_kzdw_family(self):
        for name in ("KZDW", "kzdw", "KZDW Sports", "TINDERPAY", " tinderpay ", "TinderPay"):
            with self.subTest(name=name):
                self.assertTrue(settings.is_kzdw_family(name))

    def test_other_clients_are_not_the_kzdw_family(self):
        for name in ("KZO", "KZO Sports", "KZP", "S5", "UMBER", "", None, "TINDERPAY 2"):
            with self.subTest(name=name):
                self.assertFalse(settings.is_kzdw_family(name))

    def test_only_tinderpay_is_the_tinderpay_workspace(self):
        self.assertTrue(settings.is_tinderpay_workspace(" TinderPay "))
        self.assertFalse(settings.is_tinderpay_workspace("KZDW"))
        self.assertFalse(settings.is_tinderpay_workspace("KZDW Tinderpay"))


class TinderpayPrefixTests(unittest.TestCase):
    def test_tinderpay_mints_tdp_prefixes(self):
        self.assertEqual(transformer._build_id_prefixes("TINDERPAY"), ("TDP-JV", "TDP"))

    def test_existing_client_prefixes_are_unchanged(self):
        expected = {
            "KZDW": ("KZDW-JV", "KZDW"),
            "KZP": ("KZP-JV", "KZP"),
            "S5": ("S5-JV", "S5"),
            "UMBER": ("UMBER-", "UMBER"),
            "KZO": ("KZO-JV", "KZO"),
            "KZO Sports": ("KZO-JV", "KZO"),
        }
        for client_name, prefixes in expected.items():
            with self.subTest(client=client_name):
                self.assertEqual(transformer._build_id_prefixes(client_name), prefixes)


class TinderpayLayoutRoutingTests(unittest.TestCase):
    def test_source_header_is_read_from_kzdw_row_five(self):
        gs = Mock()
        gs.read_as_df.return_value = pd.DataFrame()

        run_ingestion._read_source_raw_df(gs, "source", "2026 transactions (new)", "TINDERPAY")

        gs.read_as_df.assert_called_once_with(
            "source",
            "2026 transactions (new)",
            header_row=5,
            value_render_option="UNFORMATTED_VALUE",
        )

    def test_raw_is_standardized_with_the_kzdw_adapter(self):
        raw_df = pd.DataFrame({"CO": ["TDTH"], "COY": ["TD"]})
        sentinel = pd.DataFrame({"marker": [1]})

        with patch.object(raw_adapter, "_standardize_kzdw", return_value=sentinel) as kzdw:
            result = raw_adapter.standardize_raw_df(raw_df, client_name="TINDERPAY", raw_month="2026-09-01")

        self.assertIs(result, sentinel)
        kzdw.assert_called_once()

    def test_transfers_use_kzdw_multicurrency_handling(self):
        self.assertTrue(syncing._is_kzdw_workspace("TINDERPAY"))
        self.assertTrue(transformer._is_kzdw_case("TINDERPAY"))
        self.assertFalse(syncing._is_kzdw_workspace("KZO"))


class TdHoldTests(unittest.TestCase):
    """COY=TD moved to TINDERPAY: KZDW holds it, TINDERPAY posts it."""

    def test_kzdw_holds_td_rows(self):
        raw_df = pd.DataFrame({"COY": ["TD", " td ", "DPP", "KZDW"]})

        mask = run_ingestion._get_kzdw_forced_pending_mask(raw_df, "KZDW")

        self.assertEqual(mask.tolist(), [True, True, False, False])

    def test_tinderpay_does_not_hold_its_own_td_rows(self):
        raw_df = pd.DataFrame({"COY": ["TD", "td"]})

        mask = run_ingestion._get_kzdw_forced_pending_mask(raw_df, "TINDERPAY")

        self.assertEqual(mask.tolist(), [False, False])


if __name__ == "__main__":
    unittest.main()
