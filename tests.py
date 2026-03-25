"""
Test suite for sqlitelite (main.c)

Covers:
  - Basic REPL output (prompt, Executed, errors)
  - insert / select correctness
  - Duplicate key rejection
  - Persistence across restarts
  - Sorted order on select
  - Boundary / edge inputs
  - B-tree leaf-node splits
  - B-tree internal-node splits (deep tree)
  - Unrecognized commands and meta-commands
"""

import os
import subprocess
import tempfile
import unittest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BINARY = os.path.join(os.path.dirname(__file__), "mini-db")


def run_db(commands: list[str], db_file: str) -> list[str]:
    """Feed *commands* line-by-line to the binary and return output lines."""
    stdin = "\n".join(commands) + "\n"
    result = subprocess.run(
        [BINARY, db_file],
        input=stdin,
        capture_output=True,
        text=True,
    )
    return result.stdout.splitlines()


def strip_prompts(lines: list[str]) -> list[str]:
    """Remove 'db > ' prompt prefix from every line."""
    return [line.removeprefix("db > ") for line in lines]


class DBTestCase(unittest.TestCase):
    """Base class: creates a fresh temp DB file for each test."""

    def setUp(self):
        self._tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        self._tmp.close()
        self.db = self._tmp.name

    def tearDown(self):
        if os.path.exists(self.db):
            os.unlink(self.db)

    def run_cmds(self, commands: list[str]) -> list[str]:
        return strip_prompts(run_db(commands, self.db))


# ---------------------------------------------------------------------------
# 1. Startup / meta commands
# ---------------------------------------------------------------------------


class TestMetaCommands(DBTestCase):

    def test_prompt_is_printed(self):
        raw = run_db([".exit"], self.db)
        self.assertTrue(any("db > " in line for line in raw))

    def test_exit_terminates_cleanly(self):
        lines = self.run_cmds([".exit"])
        # No error output expected
        self.assertNotIn("Error", "\n".join(lines))

    def test_unrecognized_meta_command(self):
        lines = self.run_cmds([".unknown", ".exit"])
        self.assertIn("Unrecognized command", lines)

    def test_no_db_file_argument(self):
        result = subprocess.run([BINARY], capture_output=True, text=True)
        self.assertIn("You must supply a db file", result.stdout)
        self.assertNotEqual(result.returncode, 0)


# ---------------------------------------------------------------------------
# 2. Insert – basic behaviour
# ---------------------------------------------------------------------------


class TestInsertBasic(DBTestCase):

    def test_insert_returns_executed(self):
        lines = self.run_cmds(["insert 1 alice alice@example.com", ".exit"])
        self.assertIn("Executed", lines)

    def test_insert_missing_email(self):
        lines = self.run_cmds(["insert 1 alice", ".exit"])
        self.assertIn("Incorrect arguments for insert", lines)

    def test_insert_missing_username_and_email(self):
        lines = self.run_cmds(["insert 1", ".exit"])
        self.assertIn("Incorrect arguments for insert", lines)

    def test_insert_bare_keyword(self):
        lines = self.run_cmds(["insert", ".exit"])
        self.assertIn("Incorrect arguments for insert", lines)

    def test_insert_non_numeric_id(self):
        # sscanf will fail to parse, so args_assigned < 3
        lines = self.run_cmds(["insert abc alice alice@example.com", ".exit"])
        self.assertIn("Incorrect arguments for insert", lines)

    def test_duplicate_key_rejected(self):
        lines = self.run_cmds([
            "insert 1 alice alice@example.com",
            "insert 1 bob bob@example.com",
            ".exit",
        ])
        self.assertIn("Error: Duplicate key.", lines)

    def test_unrecognized_sql_command(self):
        lines = self.run_cmds(["delete 1", ".exit"])
        # No 'Executed' and no crash
        self.assertNotIn("Executed", lines)


# ---------------------------------------------------------------------------
# 3. Select – basic behaviour
# ---------------------------------------------------------------------------


class TestSelectBasic(DBTestCase):

    def test_select_empty_table(self):
        lines = self.run_cmds(["select", ".exit"])
        # Should not crash; 'Executed' is printed for select too
        self.assertIn("Executed", lines)

    def test_insert_then_select(self):
        lines = self.run_cmds([
            "insert 1 alice alice@example.com",
            "select",
            ".exit",
        ])
        self.assertIn("(1, alice, alice@example.com)", lines)

    def test_select_multiple_rows(self):
        lines = self.run_cmds([
            "insert 3 charlie c@example.com",
            "insert 1 alice a@example.com",
            "insert 2 bob b@example.com",
            "select",
            ".exit",
        ])
        rows = [l for l in lines if l.startswith("(")]
        self.assertEqual(len(rows), 3)

    def test_select_returns_sorted_order(self):
        lines = self.run_cmds([
            "insert 30 charlie c@example.com",
            "insert 10 alice a@example.com",
            "insert 20 bob b@example.com",
            "select",
            ".exit",
        ])
        rows = [l for l in lines if l.startswith("(")]
        ids = [int(r.split(",")[0].strip("( ")) for r in rows]
        self.assertEqual(ids, sorted(ids))


# ---------------------------------------------------------------------------
# 4. Persistence
# ---------------------------------------------------------------------------


class TestPersistence(DBTestCase):

    def test_data_survives_restart(self):
        run_db(["insert 1 alice alice@example.com", ".exit"], self.db)
        lines = strip_prompts(run_db(["select", ".exit"], self.db))
        self.assertIn("(1, alice, alice@example.com)", lines)

    def test_multiple_records_survive_restart(self):
        cmds = [f"insert {i} user{i} u{i}@x.com" for i in range(1, 6)]
        run_db(cmds + [".exit"], self.db)
        lines = strip_prompts(run_db(["select", ".exit"], self.db))
        rows = [l for l in lines if l.startswith("(")]
        self.assertEqual(len(rows), 5)

    def test_incremental_writes_persist(self):
        run_db(["insert 1 alice a@x.com", ".exit"], self.db)
        run_db(["insert 2 bob b@x.com", ".exit"], self.db)
        lines = strip_prompts(run_db(["select", ".exit"], self.db))
        rows = [l for l in lines if l.startswith("(")]
        self.assertEqual(len(rows), 2)

    def test_duplicate_rejected_after_restart(self):
        run_db(["insert 1 alice a@x.com", ".exit"], self.db)
        lines = strip_prompts(
            run_db(["insert 1 alice2 a2@x.com", ".exit"], self.db)
        )
        self.assertIn("Error: Duplicate key.", lines)


# ---------------------------------------------------------------------------
# 5. Field-length boundaries
# ---------------------------------------------------------------------------


class TestFieldBoundaries(DBTestCase):

    def test_max_username_length(self):
        username = "a" * 31  # USERNAME_SIZE - 1 usable chars (null-terminated)
        lines = self.run_cmds([f"insert 1 {username} x@x.com", "select", ".exit"])
        self.assertIn(f"(1, {username}, x@x.com)", lines)

    def test_max_email_length(self):
        email = "a" * 254  # EMAIL_SIZE - 1 usable chars
        lines = self.run_cmds([f"insert 1 alice {email}", "select", ".exit"])
        self.assertIn(f"(1, alice, {email})", lines)

    def test_id_zero(self):
        lines = self.run_cmds(["insert 0 zero z@x.com", "select", ".exit"])
        rows = [l for l in lines if l.startswith("(")]
        self.assertEqual(len(rows), 1)
        self.assertIn("(0, zero, z@x.com)", lines)

    def test_large_id(self):
        lines = self.run_cmds(["insert 999999 bigid big@x.com", "select", ".exit"])
        self.assertIn("(999999, bigid, big@x.com)", lines)


# ---------------------------------------------------------------------------
# 6. B-tree leaf-node split
# ---------------------------------------------------------------------------


class TestLeafNodeSplit(DBTestCase):
    """Insert enough rows to force at least one leaf split."""

    # LEAF_NODE_MAX_CELLS = (4096 - header) / cell_size ≈ 13 cells.
    # Inserting 14+ rows guarantees a split.
    SPLIT_COUNT = 14

    def _insert_n(self, n):
        cmds = [f"insert {i} user{i} u{i}@x.com" for i in range(1, n + 1)]
        return self.run_cmds(cmds + ["select", ".exit"])

    def test_all_rows_returned_after_split(self):
        lines = self._insert_n(self.SPLIT_COUNT)
        rows = [l for l in lines if l.startswith("(")]
        self.assertEqual(len(rows), self.SPLIT_COUNT)

    def test_rows_in_sorted_order_after_split(self):
        lines = self._insert_n(self.SPLIT_COUNT)
        rows = [l for l in lines if l.startswith("(")]
        ids = [int(r.split(",")[0].strip("( ")) for r in rows]
        self.assertEqual(ids, sorted(ids))

    def test_no_data_loss_after_split_with_reverse_insert(self):
        cmds = [
            f"insert {i} user{i} u{i}@x.com"
            for i in range(self.SPLIT_COUNT, 0, -1)
        ]
        lines = self.run_cmds(cmds + ["select", ".exit"])
        rows = [l for l in lines if l.startswith("(")]
        self.assertEqual(len(rows), self.SPLIT_COUNT)

    def test_duplicate_detection_still_works_after_split(self):
        cmds = [f"insert {i} user{i} u{i}@x.com" for i in range(1, self.SPLIT_COUNT + 1)]
        cmds.append("insert 7 dup dup@x.com")  # duplicate mid-tree
        lines = self.run_cmds(cmds + [".exit"])
        self.assertIn("Error: Duplicate key.", lines)


# ---------------------------------------------------------------------------
# 7. B-tree internal-node split (deep tree)
# ---------------------------------------------------------------------------


class TestInternalNodeSplit(DBTestCase):
    """Insert enough rows to force internal-node splits.
    INTERNAL_NODE_MAX_KEYS = 3, so we need >4 leaf pages to trigger a split.
    With ~13 cells per leaf that means inserting ~55+ rows."""

    DEEP_COUNT = 60

    def _insert_sequential(self, n):
        cmds = [f"insert {i} user{i} u{i}@x.com" for i in range(1, n + 1)]
        return self.run_cmds(cmds + ["select", ".exit"])

    def test_all_rows_returned_after_internal_split(self):
        lines = self._insert_sequential(self.DEEP_COUNT)
        rows = [l for l in lines if l.startswith("(")]
        self.assertEqual(len(rows), self.DEEP_COUNT)

    def test_sorted_order_preserved_after_internal_split(self):
        lines = self._insert_sequential(self.DEEP_COUNT)
        rows = [l for l in lines if l.startswith("(")]
        ids = [int(r.split(",")[0].strip("( ")) for r in rows]
        self.assertEqual(ids, sorted(ids))

    def test_persistence_after_internal_split(self):
        cmds = [f"insert {i} user{i} u{i}@x.com" for i in range(1, self.DEEP_COUNT + 1)]
        run_db(cmds + [".exit"], self.db)
        lines = strip_prompts(run_db(["select", ".exit"], self.db))
        rows = [l for l in lines if l.startswith("(")]
        self.assertEqual(len(rows), self.DEEP_COUNT)

    def test_random_order_inserts_with_internal_split(self):
        # Insert in a non-sequential order to exercise different split paths
        ids = list(range(1, self.DEEP_COUNT + 1))
        # interleave: odd ids first, then even
        ordered = ids[::2] + ids[1::2]
        cmds = [f"insert {i} user{i} u{i}@x.com" for i in ordered]
        lines = self.run_cmds(cmds + ["select", ".exit"])
        rows = [l for l in lines if l.startswith("(")]
        self.assertEqual(len(rows), self.DEEP_COUNT)
        parsed_ids = [int(r.split(",")[0].strip("( ")) for r in rows]
        self.assertEqual(parsed_ids, sorted(parsed_ids))


# ---------------------------------------------------------------------------
# 8. End-to-end scenarios
# ---------------------------------------------------------------------------


class TestEndToEnd(DBTestCase):

    def test_insert_select_exit_full_flow(self):
        lines = self.run_cmds([
            "insert 10 alice alice@example.com",
            "insert 5 bob bob@example.com",
            "insert 20 charlie charlie@example.com",
            "select",
            ".exit",
        ])
        rows = [l for l in lines if l.startswith("(")]
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0], "(5, bob, bob@example.com)")
        self.assertEqual(rows[1], "(10, alice, alice@example.com)")
        self.assertEqual(rows[2], "(20, charlie, charlie@example.com)")

    def test_interleaved_inserts_and_selects(self):
        lines = self.run_cmds([
            "insert 1 alice a@x.com",
            "select",
            "insert 2 bob b@x.com",
            "select",
            ".exit",
        ])
        first_select = [l for l in lines if l.startswith("(")][0]
        self.assertIn("alice", first_select)

    def test_session_with_errors_continues(self):
        """Errors should not crash the REPL; subsequent commands still work."""
        lines = self.run_cmds([
            "insert 1 alice a@x.com",
            "insert 1 alice a@x.com",    # duplicate
            ".badcmd",                   # bad meta
            "garbage sql",               # unrecognized
            "insert 2 bob b@x.com",
            "select",
            ".exit",
        ])
        rows = [l for l in lines if l.startswith("(")]
        self.assertEqual(len(rows), 2)

    def test_large_dataset_end_to_end(self):
        n = 50
        cmds = [f"insert {i} user{i} u{i}@example.com" for i in range(1, n + 1)]
        lines = self.run_cmds(cmds + ["select", ".exit"])
        rows = [l for l in lines if l.startswith("(")]
        self.assertEqual(len(rows), n)
        ids = [int(r.split(",")[0].strip("( ")) for r in rows]
        self.assertEqual(ids, list(range(1, n + 1)))

    def test_data_integrity_after_multiple_restarts(self):
        for batch in range(1, 4):
            start = (batch - 1) * 5 + 1
            end = batch * 5 + 1
            cmds = [f"insert {i} user{i} u{i}@x.com" for i in range(start, end)]
            run_db(cmds + [".exit"], self.db)

        lines = strip_prompts(run_db(["select", ".exit"], self.db))
        rows = [l for l in lines if l.startswith("(")]
        self.assertEqual(len(rows), 15)


if __name__ == "__main__":
    unittest.main(verbosity=2)
