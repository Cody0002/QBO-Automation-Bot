import unittest

from src.logic.transformer import find_id_in_map

# Real KZO fully-qualified account names (as stored in the QBO mappings dict), trimmed to the
# "Investment:" parent that exposes the sibling-collision problem.
KZO_INVESTMENT_ACCOUNTS = {
    "Investment:Investment in Anjouan": "1150040014",
    "Investment:Investment in Consultancy Company (ZA)": "1150040041",
    "Investment:Investment in HR Company": "1150040017",
    "Investment:Investment in HR Company (MP)": "1150040042",
    "Investment:Investment in HR Company (ORZ)": "1150040132",
    "Investment:Investment in HR Company (OSR)": "1150040039",
    "Investment:Investment in SA": "1150040074",
    "Investment:Investment in Umber": "1150040019",
}


class AccountMatchingTests(unittest.TestCase):
    def test_leaf_match_resolves_the_exact_sibling(self):
        acc_id = find_id_in_map(
            KZO_INVESTMENT_ACCOUNTS, "Investment in HR Company (ORZ)", allow_fuzzy=False
        )
        self.assertEqual(acc_id, "1150040132")

    def test_each_sibling_resolves_to_its_own_id(self):
        for fqn, expected_id in KZO_INVESTMENT_ACCOUNTS.items():
            leaf = fqn.split(":")[-1]
            with self.subTest(account=leaf):
                self.assertEqual(
                    find_id_in_map(KZO_INVESTMENT_ACCOUNTS, leaf, allow_fuzzy=False), expected_id
                )

    def test_exact_fully_qualified_name_resolves(self):
        acc_id = find_id_in_map(
            KZO_INVESTMENT_ACCOUNTS, "Investment:Investment in HR Company (MP)", allow_fuzzy=False
        )
        self.assertEqual(acc_id, "1150040042")

    def test_missing_account_returns_none_instead_of_a_sibling(self):
        # The regression: with the (ORZ) account absent, fuzzy at cutoff 0.80 scores
        # "Investment:Investment in HR Company (OSR)" at 0.817 and would silently book the
        # money to the wrong company. Accounts must fail instead of guessing.
        without_orz = {
            k: v for k, v in KZO_INVESTMENT_ACCOUNTS.items() if "(ORZ)" not in k
        }

        acc_id = find_id_in_map(without_orz, "Investment in HR Company (ORZ)", allow_fuzzy=False)

        self.assertIsNone(acc_id)

    def test_fuzzy_would_have_picked_the_wrong_sibling(self):
        # Documents exactly what the old behavior did, so the guard above can't be dropped
        # without this failing.
        without_orz = {
            k: v for k, v in KZO_INVESTMENT_ACCOUNTS.items() if "(ORZ)" not in k
        }

        fuzzy_id = find_id_in_map(without_orz, "Investment in HR Company (ORZ)", allow_fuzzy=True)

        self.assertEqual(fuzzy_id, "1150040039")  # (OSR) -- wrong account

    def test_case_and_whitespace_are_normalized(self):
        acc_id = find_id_in_map(
            KZO_INVESTMENT_ACCOUNTS, "  investment  in   hr company (orz) ", allow_fuzzy=False
        )
        self.assertEqual(acc_id, "1150040132")

    def test_hardcoded_replacements_still_apply_without_fuzzy(self):
        mapping = {"Bank:KZO CBD Z": "900", "Bank:Leading Card - 1238": "901"}

        self.assertEqual(find_id_in_map(mapping, "CBD Z Card", allow_fuzzy=False), "900")
        self.assertEqual(
            find_id_in_map(mapping, "Leading Card MKT - 1238", allow_fuzzy=False), "901"
        )

    def test_fuzzy_still_available_for_non_account_maps(self):
        # Vendors/classes/locations keep fuzzy -- names there genuinely vary and a wrong
        # guess does not misstate the books.
        vendors = {"Acme Trading Company": "5"}

        self.assertEqual(find_id_in_map(vendors, "Acme Trading Compny", allow_fuzzy=True), "5")

    def test_blank_input_returns_none(self):
        self.assertIsNone(find_id_in_map(KZO_INVESTMENT_ACCOUNTS, "", allow_fuzzy=False))
        self.assertIsNone(find_id_in_map(KZO_INVESTMENT_ACCOUNTS, "   ", allow_fuzzy=False))


if __name__ == "__main__":
    unittest.main()
