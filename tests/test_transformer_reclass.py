import unittest

import pandas as pd

from src.logic.transformer import process_journals

ACCOUNTS = {
    "Bank:KZO CBD Z": "100",
    "Investment:Investment in HR Company (ORZ)": "200",
    "Expense:Bank Charges": "300",
}
MAPPINGS = {"accounts": ACCOUNTS, "locations": {"TH": "1"}, "accounts_meta": {}}


def _raw_row(**overrides):
    row = {
        "No": 7310001,
        "Date": "2026-07-31",
        "Category": "Transfer",
        "Type": "Bank Charges",
        "Item Description": "Move funds",
        "CO": "TH",
        "QBO Method": "Reclass",
        "Reclass": "",
        "USD - QBO": 1000.0,
        "Account Fr": "KZO CBD Z",
        "Account To": "Investment in HR Company (ORZ)",
    }
    row.update(overrides)
    return row


def _run(rows, client_name="KZO"):
    df = pd.DataFrame(rows)
    out, _ = process_journals(df, start_no=0, qbo_mappings=MAPPINGS, client_name=client_name)
    return out


class ReclassCategoryTransferTests(unittest.TestCase):
    def test_positive_usd_credits_from_and_debits_to(self):
        out = _run([_raw_row(**{"USD - QBO": 1000.0})])

        self.assertEqual(len(out), 2)
        neg = out[out["Amount"] < 0].iloc[0]
        pos = out[out["Amount"] > 0].iloc[0]

        self.assertEqual(neg["Account"], "KZO CBD Z")                        # From
        self.assertEqual(pos["Account"], "Investment in HR Company (ORZ)")   # To
        self.assertAlmostEqual(neg["Amount"], -1000.0)
        self.assertAlmostEqual(pos["Amount"], 1000.0)

    def test_negative_usd_reverses_the_direction(self):
        out = _run([_raw_row(**{"USD - QBO": -1000.0})])

        self.assertEqual(len(out), 2)
        neg = out[out["Amount"] < 0].iloc[0]
        pos = out[out["Amount"] > 0].iloc[0]

        self.assertEqual(neg["Account"], "Investment in HR Company (ORZ)")   # To
        self.assertEqual(pos["Account"], "KZO CBD Z")                        # From
        self.assertAlmostEqual(neg["Amount"], -1000.0)
        self.assertAlmostEqual(pos["Amount"], 1000.0)

    def test_pair_shares_one_journal_and_nets_to_zero(self):
        out = _run([_raw_row()])

        self.assertEqual(out["Journal No"].nunique(), 1)
        self.assertAlmostEqual(out["Amount"].sum(), 0.0)
        self.assertTrue((out["Remarks"] == "Ready to sync").all(), out["Remarks"].tolist())

    def test_direction_uses_raw_sign_not_the_reclass_flip(self):
        # Reclass column == 'Reclass' triggers the USD *= -1 step. The direction must still
        # follow the sheet's own +1000, i.e. From is credited.
        out = _run([_raw_row(**{"USD - QBO": 1000.0, "Reclass": "Reclass"})])

        neg = out[out["Amount"] < 0].iloc[0]
        pos = out[out["Amount"] > 0].iloc[0]
        self.assertEqual(neg["Account"], "KZO CBD Z")
        self.assertEqual(pos["Account"], "Investment in HR Company (ORZ)")

    def test_non_transfer_category_still_emits_one_line(self):
        out = _run([_raw_row(Category="Operating", **{"Account Fr": "KZO CBD Z"})])

        self.assertEqual(len(out), 1)
        self.assertEqual(out.iloc[0]["Account"], "KZO CBD Z")

    def test_non_kzo_client_is_unchanged(self):
        # S5 rather than KZDW: the KZDW reclass branch has a separate, pre-existing crash
        # (unique_groups built via to_records() yields unhashable numpy void scalars used as
        # dict keys in transformer.py). Unrelated to this rule; see notes.
        out = _run([_raw_row()], client_name="S5")

        self.assertEqual(len(out), 1)
        self.assertEqual(out.iloc[0]["Account"], "KZO CBD Z")

    def test_blank_to_account_fails_validation_instead_of_falling_back_to_type(self):
        out = _run([_raw_row(**{"Account To": ""})])

        self.assertEqual(len(out), 2)
        remarks = " ".join(out["Remarks"].tolist())
        self.assertIn("ERROR", remarks)
        # Must NOT silently book to the Type/category account.
        self.assertNotIn("Bank Charges", out["Account"].tolist())

    def test_mixed_transfer_and_normal_reclass_rows(self):
        out = _run([
            _raw_row(No=7310001, Category="Transfer"),
            _raw_row(No=7310002, Category="Operating"),
        ])

        # 2 lines for the transfer row + 1 for the normal row.
        self.assertEqual(len(out), 3)
        self.assertEqual(len(out[out["No"] == 7310001]), 2)
        self.assertEqual(len(out[out["No"] == 7310002]), 1)


if __name__ == "__main__":
    unittest.main()
