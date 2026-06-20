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
        lines = self.run_cmds(["notacommand 1", ".exit"])
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
# 3b. Column projection
# ---------------------------------------------------------------------------


class TestProjection(DBTestCase):

    def _one_row(self):
        return ["insert 1 alice alice@example.com"]

    def test_bare_select_returns_all_columns(self):
        lines = self.run_cmds(self._one_row() + ["select", ".exit"])
        self.assertIn("(1, alice, alice@example.com)", lines)

    def test_project_single_column(self):
        lines = self.run_cmds(self._one_row() + ["select id", ".exit"])
        self.assertIn("(1)", lines)

    def test_project_name_column(self):
        lines = self.run_cmds(self._one_row() + ["select name", ".exit"])
        self.assertIn("(alice)", lines)

    def test_project_multiple_columns_in_order(self):
        lines = self.run_cmds(self._one_row() + ["select id, email", ".exit"])
        self.assertIn("(1, alice@example.com)", lines)

    def test_projection_preserves_requested_order(self):
        # Reversed order must come out reversed, not in schema order.
        lines = self.run_cmds(self._one_row() + ["select email, id", ".exit"])
        self.assertIn("(alice@example.com, 1)", lines)

    def test_projection_without_spaces_around_comma(self):
        lines = self.run_cmds(self._one_row() + ["select id,email", ".exit"])
        self.assertIn("(1, alice@example.com)", lines)

    def test_repeated_column_allowed(self):
        lines = self.run_cmds(self._one_row() + ["select id, id", ".exit"])
        self.assertIn("(1, 1)", lines)

    def test_unknown_column_rejected(self):
        # 'bogus' resolves to no column, so the select fails and emits no row.
        lines = self.run_cmds(self._one_row() + ["select bogus", ".exit"])
        rows = [l for l in lines if l.startswith("(")]
        self.assertEqual(rows, [])

    def test_value_named_like_a_column_inserts(self):
        # Regression: column names must not be reserved words. Before generic
        # identifiers, 'id'/'name'/'email' as values broke the insert.
        lines = self.run_cmds([
            "insert 1 id id@example.com",
            "select",
            ".exit",
        ])
        self.assertIn("(1, id, id@example.com)", lines)


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


# ---------------------------------------------------------------------------
# 9. Delete – basic behaviour (no underflow)
# ---------------------------------------------------------------------------


class TestDeleteBasic(DBTestCase):
    """Delete operations that do NOT trigger underflow (root leaf only)."""

    def test_delete_single_key(self):
        lines = self.run_cmds([
            "insert 1 alice a@x.com",
            "insert 2 bob b@x.com",
            "insert 3 charlie c@x.com",
            "delete 2",
            "select",
            ".exit",
        ])
        rows = [l for l in lines if l.startswith("(")]
        ids = [int(r.split(",")[0].strip("( ")) for r in rows]
        self.assertEqual(ids, [1, 3])

    def test_delete_nonexistent_key(self):
        lines = self.run_cmds([
            "insert 1 alice a@x.com",
            "delete 99",
            ".exit",
        ])
        self.assertIn("Key 99 does not exist", lines)

    def test_delete_first_key(self):
        lines = self.run_cmds([
            "insert 1 alice a@x.com",
            "insert 2 bob b@x.com",
            "insert 3 charlie c@x.com",
            "delete 1",
            "select",
            ".exit",
        ])
        rows = [l for l in lines if l.startswith("(")]
        ids = [int(r.split(",")[0].strip("( ")) for r in rows]
        self.assertEqual(ids, [2, 3])

    def test_delete_last_key(self):
        lines = self.run_cmds([
            "insert 1 alice a@x.com",
            "insert 2 bob b@x.com",
            "insert 3 charlie c@x.com",
            "delete 3",
            "select",
            ".exit",
        ])
        rows = [l for l in lines if l.startswith("(")]
        ids = [int(r.split(",")[0].strip("( ")) for r in rows]
        self.assertEqual(ids, [1, 2])

    def test_delete_only_key_in_root_leaf(self):
        lines = self.run_cmds([
            "insert 1 alice a@x.com",
            "delete 1",
            "select",
            ".exit",
        ])
        rows = [l for l in lines if l.startswith("(")]
        self.assertEqual(len(rows), 0)

    def test_delete_then_reinsert(self):
        lines = self.run_cmds([
            "insert 1 alice a@x.com",
            "insert 2 bob b@x.com",
            "delete 1",
            "insert 1 alice2 a2@x.com",
            "select",
            ".exit",
        ])
        rows = [l for l in lines if l.startswith("(")]
        self.assertEqual(len(rows), 2)
        self.assertIn("(1, alice2, a2@x.com)", lines)

    def test_delete_persists_after_restart(self):
        run_db([
            "insert 1 alice a@x.com",
            "insert 2 bob b@x.com",
            "insert 3 charlie c@x.com",
            "delete 2",
            ".exit",
        ], self.db)
        lines = strip_prompts(run_db(["select", ".exit"], self.db))
        rows = [l for l in lines if l.startswith("(")]
        ids = [int(r.split(",")[0].strip("( ")) for r in rows]
        self.assertEqual(ids, [1, 3])


# ---------------------------------------------------------------------------
# 10. Delete – underflow / rebalancing
#     These tests WILL FAIL until handle_underflow() and its helpers
#     (leaf_node_redistribute, leaf_node_merge, internal_node_redistribute,
#     internal_node_merge) are implemented.
# ---------------------------------------------------------------------------


class TestDeleteUnderflow(DBTestCase):
    """Delete operations that trigger underflow and require rebalancing."""

    def _insert_range(self, start, end):
        return [f"insert {i} user{i} u{i}@x.com" for i in range(start, end)]

    def test_leaf_redistribute(self):
        """Insert 14 keys (split into two 7-cell leaves), then delete 2
        from one leaf to trigger redistribute from its sibling."""
        cmds = self._insert_range(1, 15)
        cmds += ["delete 1", "delete 2", "select", ".exit"]
        lines = self.run_cmds(cmds)
        rows = [l for l in lines if l.startswith("(")]
        ids = [int(r.split(",")[0].strip("( ")) for r in rows]
        self.assertEqual(ids, list(range(3, 15)))

    def test_leaf_merge(self):
        """Delete enough keys to force a leaf merge."""
        cmds = self._insert_range(1, 15)
        for i in range(1, 9):
            cmds.append(f"delete {i}")
        cmds += ["select", ".exit"]
        lines = self.run_cmds(cmds)
        rows = [l for l in lines if l.startswith("(")]
        ids = [int(r.split(",")[0].strip("( ")) for r in rows]
        self.assertEqual(ids, list(range(9, 15)))

    def test_delete_all_one_by_one(self):
        """Insert 30 keys, delete them all, verify empty table."""
        n = 30
        cmds = self._insert_range(1, n + 1)
        for i in range(1, n + 1):
            cmds.append(f"delete {i}")
        cmds += ["select", ".exit"]
        lines = self.run_cmds(cmds)
        rows = [l for l in lines if l.startswith("(")]
        self.assertEqual(len(rows), 0)

    def test_root_collapse(self):
        """Delete until the tree shrinks back to a single leaf root."""
        n = 20
        cmds = self._insert_range(1, n + 1)
        for i in range(1, n - 2):
            cmds.append(f"delete {i}")
        cmds += ["select", ".exit"]
        lines = self.run_cmds(cmds)
        rows = [l for l in lines if l.startswith("(")]
        ids = [int(r.split(",")[0].strip("( ")) for r in rows]
        self.assertEqual(ids, list(range(n - 2, n + 1)))

    def test_insert_after_rebalance(self):
        """After rebalancing, inserts should still work correctly."""
        cmds = self._insert_range(1, 15)
        for i in range(1, 8):
            cmds.append(f"delete {i}")
        cmds += [
            "insert 100 new1 new1@x.com",
            "insert 200 new2 new2@x.com",
            "select",
            ".exit",
        ]
        lines = self.run_cmds(cmds)
        rows = [l for l in lines if l.startswith("(")]
        ids = [int(r.split(",")[0].strip("( ")) for r in rows]
        self.assertEqual(ids, sorted(ids))
        self.assertIn(100, ids)
        self.assertIn(200, ids)

    def test_persistence_after_rebalance(self):
        """Data must survive restart after rebalancing."""
        cmds = self._insert_range(1, 15)
        for i in range(1, 8):
            cmds.append(f"delete {i}")
        run_db(cmds + [".exit"], self.db)
        lines = strip_prompts(run_db(["select", ".exit"], self.db))
        rows = [l for l in lines if l.startswith("(")]
        ids = [int(r.split(",")[0].strip("( ")) for r in rows]
        self.assertEqual(ids, list(range(8, 15)))

    def test_deep_tree_internal_merge(self):
        """60 keys → internal splits. Delete 45 to trigger internal merges."""
        n = 60
        cmds = self._insert_range(1, n + 1)
        for i in range(1, 46):
            cmds.append(f"delete {i}")
        cmds += ["select", ".exit"]
        lines = self.run_cmds(cmds)
        rows = [l for l in lines if l.startswith("(")]
        ids = [int(r.split(",")[0].strip("( ")) for r in rows]
        self.assertEqual(ids, list(range(46, n + 1)))

    def test_alternating_delete(self):
        """Delete every other key, verify sorted order is maintained."""
        n = 30
        cmds = self._insert_range(1, n + 1)
        for i in range(1, n + 1, 2):
            cmds.append(f"delete {i}")
        cmds += ["select", ".exit"]
        lines = self.run_cmds(cmds)
        rows = [l for l in lines if l.startswith("(")]
        ids = [int(r.split(",")[0].strip("( ")) for r in rows]
        self.assertEqual(ids, list(range(2, n + 1, 2)))

class TestWALRecovery(DBTestCase):
    """Write-Ahead Log recovery after a simulated crash.

    A 'crash' here means the process dies WITHOUT a clean `.exit`. The REPL
    only flushes pages to the .db file in close_db (on `.exit`); on EOF it
    calls exit(EXIT_FAILURE) and never flushes. So feeding commands without a
    trailing `.exit` leaves the .db untouched and only the fsync'd WAL on disk
    — meaning anything recovered on the next open came from the WAL alone.
    """

    def wal_path(self) -> str:
        return self.db + "-wal"

    def tearDown(self):
        super().tearDown()
        if os.path.exists(self.wal_path()):
            os.unlink(self.wal_path())

    # -- helpers -----------------------------------------------------------

    def crash(self, commands: list[str]) -> None:
        """Run commands with NO `.exit`. The binary processes (and fsyncs) every
        command, then dies on EOF before flushing pages to the .db file."""
        run_db(commands, self.db)

    def reopen_ids(self) -> list[int]:
        """Reopen cleanly, select everything, return the recovered key ids."""
        lines = strip_prompts(run_db(["select", ".exit"], self.db))
        rows = [l for l in lines if l.startswith("(")]
        return [int(r.split(",")[0].strip("( ")) for r in rows]

    @staticmethod
    def _inserts(lo: int, hi: int) -> list[str]:
        return [f"insert {i} user{i} u{i}@x.com" for i in range(lo, hi)]

    def clean_exit(self, commands: list[str]) -> None:
        """Run commands followed by a clean `.exit`, so close_db flushes every
        page to the .db file, fsyncs, and checkpoints (truncates) the WAL."""
        run_db(commands + [".exit"], self.db)

    # -- tests -------------------------------------------------------------

    def test_committed_inserts_survive_crash(self):
        self.crash(["insert 1 alice a@x.com", "insert 2 bob b@x.com"])
        # Nothing was flushed to .db: recovery must restore from the WAL alone.
        self.assertEqual(os.path.getsize(self.db), 0)
        self.assertEqual(self.reopen_ids(), [1, 2])

    def test_recovery_restores_all_rows_after_split(self):
        # 14 rows force a leaf split (multi-page transaction in the WAL).
        self.crash(self._inserts(1, 15))
        self.assertEqual(self.reopen_ids(), list(range(1, 15)))

    def test_committed_deletes_survive_crash(self):
        # Insert 14 (forces a split), delete 1..7, then crash before flush.
        self.crash(self._inserts(1, 15) + [f"delete {i}" for i in range(1, 8)])
        self.assertEqual(self.reopen_ids(), list(range(8, 15)))

    def test_recovery_is_idempotent_across_reopens(self):
        self.crash(["insert 1 a a@x.com", "insert 2 b b@x.com",
                    "insert 3 c c@x.com"])
        # Recovery runs on every open and the WAL is never truncated, so
        # replaying it repeatedly must keep producing the same state.
        self.assertEqual(self.reopen_ids(), [1, 2, 3])
        self.assertEqual(self.reopen_ids(), [1, 2, 3])

    def test_garbage_tail_in_wal_is_ignored(self):
        self.crash(["insert 1 a a@x.com", "insert 2 b b@x.com"])
        # Append bytes that cannot parse as a valid record (bad type/length).
        with open(self.wal_path(), "ab") as f:
            f.write(b"\xff" * 100)
        # Recovery stops at the unparseable tail; valid records still applied.
        self.assertEqual(self.reopen_ids(), [1, 2])

    def test_corrupted_payload_detected_by_checksum(self):
        self.crash(["insert 1 a a@x.com", "insert 2 b b@x.com"])
        size = os.path.getsize(self.wal_path())
        # Flip a byte deep inside the SECOND record's page payload (100 bytes
        # from EOF lands well inside it; the trailing commit record is ~25B).
        with open(self.wal_path(), "r+b") as f:
            f.seek(size - 100)
            original = f.read(1)
            f.seek(size - 100)
            f.write(bytes([original[0] ^ 0xFF]))
        # The 2nd record fails its checksum, so recovery stops there: only the
        # first transaction survives.
        self.assertEqual(self.reopen_ids(), [1])

    def test_torn_commit_record_discards_its_transaction(self):
        self.crash(["insert 1 a a@x.com", "insert 2 b b@x.com"])
        size = os.path.getsize(self.wal_path())
        # Chop the tail so the final COMMIT record is incomplete. Its
        # transaction's page-change is buffered but never committed -> dropped.
        with open(self.wal_path(), "r+b") as f:
            f.truncate(size - 10)
        self.assertEqual(self.reopen_ids(), [1])

    # -- checkpointing (M3) ------------------------------------------------

    def test_checkpoint_truncates_wal_on_clean_exit(self):
        # 14 rows force a split, so the WAL accumulates a multi-page commit.
        self.clean_exit(self._inserts(1, 15))
        # On clean exit close_db flushed every page to .db and fsync'd, then
        # checkpointed: the WAL must be truncated to empty (nothing to redo),
        # while the data now lives in the .db file.
        self.assertEqual(os.path.getsize(self.wal_path()), 0)
        self.assertGreater(os.path.getsize(self.db), 0)
        self.assertEqual(self.reopen_ids(), list(range(1, 15)))

    def test_clean_exit_makes_db_authoritative_over_stale_wal(self):
        self.clean_exit(self._inserts(1, 15))
        # The checkpoint emptied the WAL. Pour garbage into it: recovery must
        # not depend on the log, because the checkpoint already made .db the
        # authoritative copy. Data survives intact.
        with open(self.wal_path(), "ab") as f:
            f.write(b"\xff" * 200)
        self.assertEqual(self.reopen_ids(), list(range(1, 15)))


if __name__ == "__main__":
    unittest.main(verbosity=2)
