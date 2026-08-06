import unittest

import pandas as pd

from src.logic.raw_adapter import RAW_STANDARD_COLUMNS, standardize_raw_df


class KzoNamedNoHeaderRawAdapterTests(unittest.TestCase):
    """KZO tabs that do carry an explicit ``No.`` header, plus the positional fallback."""

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


class KzoCountryRawAdapterTests(unittest.TestCase):
    """The 2026 KZO country layout (BR/TH/...) as exported from Sheets.

    Columns A:Z are named, "No" sits at AA with an empty header cell, and everything
    from AB onward is dropdown-helper junk that also has empty headers.
    """

    HEADER = [
        "CO",
        "COY",
        "Date",
        "Category",
        "Sub Category",
        "Item Description",
        "TrxHash",
        "From Account",
        "To Account",
        "Currency From",
        "Amount From",
        "Currency To",
        "Amount To",
        "Budget\nRate",
        "USD",
        "Actual USD\nTransacted",
        "Realised\nLoss",
        "Transacted Amount Check",
        "Variance Check ",
        "USD - QBO",
        "Reclass/To check",
        "QBO Import Method \n (Journal/Expenses/Transfer)",
        "If Journal/Expense method:\n Another records",
        "Transfer from",
        "Transfer to",
        "Checking ( For our use only )",
        "",  # AA -> No
        "",  # AB.. -> dropdown helper values
        "",
        "",
    ]

    # BR,KZO,31-Dec,Revenue,Deposit,... ,Journal,Payment Gateway - BR,,,,2,,Deposit,Withdrawal
    JOURNAL_ROW = [
        "BR", "KZO", "31-Dec", "Revenue", "Deposit",
        "BR KZO Weekly Deposit 29th -31st Dec 25",
        "", "", "", "", "", "BRL", "17,776.72", "5.34", "3,326.73", "", "",
        "3,326.73", "0", "3,326.73", "",
        "Journal", "Payment Gateway - BR", "", "", "",
        "2", "", "Deposit", "Withdrawal",
    ]

    # A transfer row that carries "To exclude" in Z and its No in AA.
    TRANSFER_ROW = [
        "BR", "KZO", "29-Mar", "Transfer", "Settlement",
        "Settlement from BRLKZG1 CLICKPAY",
        "", "Payment Gateway - BR", "KZO BRL SETTLE TRC 2", "BRL", "-16,259.89",
        "USDT TRC", "2,978.00", "5.21", "-3,121.35", "2,978.00", "-143.35",
        "-3,121.35", "0", "2,978.00", "",
        "Transfer", "", "Payment Gateway - BR", "KZO BRL SETTLE TRC 2", "To exclude",
        "73", "", "B2B", "Fund In",
    ]

    def _standardize(self, rows):
        raw_df = pd.DataFrame(rows, columns=self.HEADER)
        return standardize_raw_df(raw_df, client_name="KZO", raw_month="2025-12")

    def test_maps_no_from_blank_aa_header(self):
        result = self._standardize([self.JOURNAL_ROW, self.TRANSFER_ROW])

        self.assertEqual(result.columns.tolist(), RAW_STANDARD_COLUMNS)
        self.assertEqual(result.loc[0, "No"], 2)
        self.assertEqual(result.loc[1, "No"], 73)

    def test_check_columns_do_not_shift_qbo_fields(self):
        result = self._standardize([self.JOURNAL_ROW, self.TRANSFER_ROW])

        self.assertEqual(result.loc[0, "Type"], "Deposit")
        self.assertEqual(result.loc[0, "USD - QBO"], 3326.73)
        self.assertEqual(result.loc[0, "QBO Method"], "Journal")
        self.assertEqual(result.loc[0, "If Journal/Expense Method"], "Payment Gateway - BR")
        self.assertEqual(result.loc[0, "Check (Internal use)"], "")

        self.assertEqual(result.loc[1, "USD - QBO"], 2978.00)
        self.assertEqual(result.loc[1, "QBO Method"], "Transfer")
        self.assertEqual(result.loc[1, "QBO Transfer Fr"], "Payment Gateway - BR")
        self.assertEqual(result.loc[1, "QBO Transfer To"], "KZO BRL SETTLE TRC 2")
        self.assertEqual(result.loc[1, "Check (Internal use)"], "To exclude")

    def test_maps_trxhash_without_space(self):
        rows = [list(self.TRANSFER_ROW)]
        rows[0][6] = "97e14be43735c4a1"

        result = self._standardize(rows)

        self.assertEqual(result.loc[0, "TrxHarsh"], "97e14be43735c4a1")

    def test_handles_layout_without_the_two_check_columns(self):
        """Pre-2026 KZO tabs: same headers, minus Transacted Amount/Variance Check."""
        drop = [self.HEADER.index("Transacted Amount Check"), self.HEADER.index("Variance Check ")]
        header = [c for i, c in enumerate(self.HEADER) if i not in drop]
        row = [v for i, v in enumerate(self.JOURNAL_ROW) if i not in drop]

        raw_df = pd.DataFrame([row], columns=header)
        result = standardize_raw_df(raw_df, client_name="KZO", raw_month="2025-12")

        self.assertEqual(result.loc[0, "No"], 2)
        self.assertEqual(result.loc[0, "USD - QBO"], 3326.73)
        self.assertEqual(result.loc[0, "QBO Method"], "Journal")

    def test_named_no_header_still_wins(self):
        header = list(self.HEADER)
        header[26] = "No."

        raw_df = pd.DataFrame([self.JOURNAL_ROW], columns=header)
        result = standardize_raw_df(raw_df, client_name="KZO", raw_month="2025-12")

        self.assertEqual(result.loc[0, "No"], 2)


if __name__ == "__main__":
    unittest.main()
