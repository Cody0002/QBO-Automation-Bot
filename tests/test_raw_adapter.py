import unittest

import pandas as pd

from src.logic.raw_adapter import RAW_STANDARD_COLUMNS, standardize_raw_df


class KzoThailandRawAdapterTests(unittest.TestCase):
    def test_maps_no_from_aa_without_shifting_qbo_fields(self):
        source_columns = [
            "CO",
            "COY",
            "Date",
            "Category",
            "Sub Category",
            "Item Description",
            "Trx Hash",
            "From Account",
            "To Account",
            "Currency From",
            "Amount From",
            "Currency To",
            "Amount To",
            "Budget Rate",
            "USD",
            "Actual USD Transacted",
            "Realised Loss",
            "Transacted Amount Check",
            "Variance Check ",
            "USD - QBO",
            "Reclass/To check",
            "QBO Import Method \n (Journal/Expenses/Transfer)",
            "If Journal/Expense method:\n Another records",
            "Transfer from",
            "Transfer to",
            "Checking ( For our use only )",
            "No.",
        ]
        source_row = [
            "KZO",
            "TH",
            "2026-08-01",
            "Operations",
            "Office Supplies",
            "Printer paper",
            "0xabc",
            "Bank TH",
            "Clearing TH",
            "THB",
            3500,
            "USD",
            100,
            35,
            100,
            99,
            1,
            "OK",
            0,
            100,
            "",
            "Journal",
            "Accounts Payable",
            "Bank TH",
            "Clearing TH",
            "",
            812,
        ]
        raw_df = pd.DataFrame([source_row], columns=source_columns)

        result = standardize_raw_df(raw_df, client_name="KZO", raw_month="2026-08")

        self.assertEqual(result.columns.tolist(), RAW_STANDARD_COLUMNS)
        self.assertEqual(result.loc[0, "No"], 812)
        self.assertEqual(result.loc[0, "Type"], "Office Supplies")
        self.assertEqual(result.loc[0, "TrxHarsh"], "0xabc")
        self.assertEqual(result.loc[0, "USD - QBO"], 100)
        self.assertEqual(result.loc[0, "QBO Method"], "Journal")
        self.assertEqual(result.loc[0, "If Journal/Expense Method"], "Accounts Payable")
        self.assertEqual(result.loc[0, "QBO Transfer Fr"], "Bank TH")
        self.assertEqual(result.loc[0, "QBO Transfer To"], "Clearing TH")
        self.assertEqual(result.loc[0, "Check (Internal use)"], "")

    def test_keeps_legacy_kzo_positional_mapping(self):
        raw_df = pd.DataFrame(
            [[f"value-{i}" for i in range(len(RAW_STANDARD_COLUMNS))]],
            columns=[f"legacy-{i}" for i in range(len(RAW_STANDARD_COLUMNS))],
        )
        raw_df.iloc[0, RAW_STANDARD_COLUMNS.index("No")] = 44

        result = standardize_raw_df(raw_df, client_name="KZO", raw_month="2026-08")

        self.assertEqual(result.loc[0, "No"], 44)
        self.assertEqual(result.loc[0, "QBO Method"], "value-19")


if __name__ == "__main__":
    unittest.main()
