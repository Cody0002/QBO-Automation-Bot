import unittest
from unittest.mock import MagicMock, patch

import run_syncing
from src.logic.syncing import QBOSync


def _sync_with_query(query_recorder, results=None):
    """QBOSync whose client records every query string it receives."""
    client = MagicMock()

    def _query(q):
        query_recorder.append(q)
        return results if results is not None else []

    client.query.side_effect = _query
    with patch.object(QBOSync, "_get_qbo_mappings", return_value={}):
        return QBOSync(client)


class MonthBoundsTests(unittest.TestCase):
    def test_month_bounds(self):
        self.assertEqual(run_syncing._month_bounds("2026-07-01"), ("2026-07-01", "2026-07-31"))
        self.assertEqual(run_syncing._month_bounds("2026-02-15"), ("2026-02-01", "2026-02-28"))
        self.assertEqual(run_syncing._month_bounds("2028-02-03"), ("2028-02-01", "2028-02-29"))

    def test_unparseable_month_degrades_to_none(self):
        self.assertEqual(run_syncing._month_bounds(""), (None, None))
        self.assertEqual(run_syncing._month_bounds("garbage"), (None, None))


class TransferDuplicateScopeTests(unittest.TestCase):
    def test_transfer_query_is_scoped_by_date(self):
        # Without the TxnDate bounds this pulls the whole Transfer table (measured at
        # 14,064 rows / 320s for KZO).
        queries = []
        sync = _sync_with_query(queries)

        sync.get_existing_duplicates(
            "Transfer", ["KZOTH0726T0001"], date_start="2026-07-01", date_end="2026-07-31"
        )

        self.assertEqual(len(queries), 1)
        self.assertIn("FROM Transfer", queries[0])
        self.assertIn("TxnDate >= '2026-07-01'", queries[0])
        self.assertIn("TxnDate <= '2026-07-31'", queries[0])

    def test_no_dead_maxresults_clause(self):
        # query() appends its own STARTPOSITION/MAXRESULTS, so a MAXRESULTS in the statement
        # was silently ignored and must not reappear as false reassurance.
        queries = []
        sync = _sync_with_query(queries)

        sync.get_existing_duplicates(
            "Transfer", ["X"], date_start="2026-07-01", date_end="2026-07-31"
        )

        self.assertNotIn("MAXRESULTS", queries[0])

    def test_matching_ref_in_private_note_is_detected(self):
        notes = [
            {"PrivateNote": "KZOTH0726T0875 - moved funds"},
            {"PrivateNote": "KZOTH0726T0876 - something else"},
        ]
        sync = _sync_with_query([], results=notes)

        found = sync.get_existing_duplicates(
            "Transfer", ["KZOTH0726T0875", "KZOTH0726T9999"],
            date_start="2026-07-01", date_end="2026-07-31",
        )

        self.assertEqual(found, {"KZOTH0726T0875"})

    def test_unscoped_still_works_but_warns(self):
        queries = []
        sync = _sync_with_query(queries)

        with self.assertLogs("syncing_logic", level="WARNING") as logs:
            sync.get_existing_duplicates("Transfer", ["X"])

        self.assertNotIn("TxnDate", queries[0])
        self.assertTrue(any("unscoped" in m for m in logs.output))

    def test_journal_and_purchase_ignore_the_date_range(self):
        # These are already scoped by DocNumber; dates must not leak into their queries.
        for entity in ("JournalEntry", "Purchase"):
            with self.subTest(entity=entity):
                queries = []
                sync = _sync_with_query(queries)

                sync.get_existing_duplicates(
                    entity, ["KZO-JV0001"], date_start="2026-07-01", date_end="2026-07-31"
                )

                self.assertTrue(queries)
                self.assertIn("DocNumber IN", queries[0])
                self.assertNotIn("TxnDate", queries[0])


if __name__ == "__main__":
    unittest.main()
