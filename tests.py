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

# The single fixture table every table-based test operates on. Its columns are
# id/name/email because resolve_column() still recognizes only those names.
USERS_SCHEMA = "create table users (id int primary key, name text, email text)"


def ins(id, name, email):
    """Build a qualified INSERT for the users table."""
    return f"insert into users {id} {name} {email}"


def sel(cols="*", where=None):
    """Build a qualified SELECT for the users table.

    sel()                     -> select * from users
    sel("id")                 -> select id from users
    sel("*", "id = 2")        -> select * from users where id = 2
    """
    query = f"select {cols} from users"
    if where is not None:
        query += f" where {where}"
    return query


def dele(id):
    """Build a qualified DELETE for the users table."""
    return f"delete from users {id}"


# --- generic, table-name-parameterized builders ----------------------------
# The helpers above are hardwired to `users`; these take an explicit table so
# the multi-table / arbitrary-schema suites can address any table.

def make_users_like(table):
    """A users-shaped schema under an arbitrary table name. Reuses the
    id/name/email columns so projection and WHERE (which only resolve those
    names) keep working against the table."""
    return f"create table {table} (id int primary key, name text, email text)"


def ins_into(table, *values):
    """Positional INSERT into any table: ins_into('kv', 1, 'hello')."""
    return f"insert into {table} " + " ".join(str(v) for v in values)


def sel_from(table, cols="*", where=None):
    query = f"select {cols} from {table}"
    if where is not None:
        query += f" where {where}"
    return query


def rows_of(lines):
    """Every emitted record line (those starting with '(')."""
    return [l for l in lines if l.startswith("(")]


def ids_of(lines):
    """The leading integer key of every emitted record line."""
    return [int(r.split(",")[0].strip("( )")) for r in rows_of(lines)]


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

    def run_cmds(self, commands: list[str], create: bool = True) -> list[str]:
        """Run a session. By default the users table is created first; pass
        create=False for sessions that must not touch a table (meta commands,
        reopen-only sessions, or tests asserting on raw create output)."""
        prefix = [USERS_SCHEMA] if create else []
        return strip_prompts(run_db(prefix + commands, self.db))


# ---------------------------------------------------------------------------
# 1. Startup / meta commands
# ---------------------------------------------------------------------------


class TestMetaCommands(DBTestCase):

    def test_prompt_is_printed(self):
        raw = run_db([".exit"], self.db)
        self.assertTrue(any("db > " in line for line in raw))

    def test_exit_terminates_cleanly(self):
        lines = self.run_cmds([".exit"], create=False)
        # No error output expected
        self.assertNotIn("Error", "\n".join(lines))

    def test_unrecognized_meta_command(self):
        lines = self.run_cmds([".unknown", ".exit"], create=False)
        self.assertIn("Unrecognized command", lines)

    def test_no_db_file_argument(self):
        result = subprocess.run([BINARY], capture_output=True, text=True)
        self.assertIn("You must supply a db file", result.stdout)
        self.assertNotEqual(result.returncode, 0)


# ---------------------------------------------------------------------------
# 1b. CREATE TABLE
# ---------------------------------------------------------------------------


class TestCreateTable(DBTestCase):

    def test_create_returns_executed(self):
        lines = self.run_cmds([".exit"])  # fixture already issues the CREATE
        self.assertIn("Executed", lines)

    def test_table_usable_immediately_after_create(self):
        lines = self.run_cmds([ins(1, "alice", "a@x.com"), sel(), ".exit"])
        self.assertIn("(1, alice, a@x.com)", lines)

    def test_table_persists_across_restart(self):
        # First session only creates the table (no rows), then reopen and use it.
        run_db([USERS_SCHEMA, ".exit"], self.db)
        lines = strip_prompts(
            run_db([ins(1, "alice", "a@x.com"), sel(), ".exit"], self.db)
        )
        self.assertIn("(1, alice, a@x.com)", lines)


# ---------------------------------------------------------------------------
# 2. Insert – basic behaviour
# ---------------------------------------------------------------------------


class TestInsertBasic(DBTestCase):

    def test_insert_returns_executed(self):
        lines = self.run_cmds([ins(1, "alice", "alice@example.com"), ".exit"])
        self.assertIn("Executed", lines)

    def test_duplicate_key_rejected(self):
        lines = self.run_cmds([
            ins(1, "alice", "alice@example.com"),
            ins(1, "bob", "bob@example.com"),
            ".exit",
        ])
        self.assertIn("Error: Duplicate key.", lines)

    def test_unrecognized_sql_command(self):
        # create=False so the fixture's own 'Executed' doesn't mask the check.
        lines = self.run_cmds(["notacommand 1", ".exit"], create=False)
        self.assertNotIn("Executed", lines)


# ---------------------------------------------------------------------------
# 2b. Insert – schema validation
#
#     Re-adds the guardrail the old suite tested as "Incorrect arguments for
#     insert". The engine must reject a row whose value count/types don't match
#     the table schema instead of storing a malformed record. These assert on
#     *behaviour* (the bad row is never stored -> select is empty), so they are
#     independent of the exact rejection message you choose.
#
#     EXPECTED TO FAIL until execute_insert validates record_to_insert against
#     table->schema (count + per-column type).
# ---------------------------------------------------------------------------


class TestInsertValidation(DBTestCase):
    """Each test issues a malformed insert, then a known-good row, and asserts
    that ONLY the good row survives. Requiring the good row to appear means a
    crash on the malformed insert fails the test (rather than passing silently
    because a dead process prints nothing); requiring the bad row to be absent
    means a stored-but-malformed record also fails."""

    GOOD = "(42, good, g@x.com)"

    def _rows(self, lines):
        return [l for l in lines if l.startswith("(")]

    def _run(self, bad_insert):
        return self._rows(self.run_cmds([
            bad_insert,
            ins(42, "good", "g@x.com"),
            sel(),
            ".exit",
        ]))

    def test_too_few_values_rejected(self):
        # Missing the email column.
        self.assertEqual(self._run("insert into users 1 alice"), [self.GOOD])

    def test_no_values_rejected(self):
        self.assertEqual(self._run("insert into users"), [self.GOOD])

    def test_too_many_values_rejected(self):
        self.assertEqual(
            self._run("insert into users 1 alice a@x.com extra"), [self.GOOD]
        )

    def test_wrong_type_in_pk_column_rejected(self):
        # 'abc' is text where the id column is INT. Currently SIGBUSes, which is
        # exactly why this test insists the good row still appears afterward.
        self.assertEqual(
            self._run("insert into users abc alice a@x.com"), [self.GOOD]
        )


# ---------------------------------------------------------------------------
# 3. Select – basic behaviour
# ---------------------------------------------------------------------------


class TestSelectBasic(DBTestCase):

    def test_select_empty_table(self):
        lines = self.run_cmds([sel(), ".exit"])
        # Should not crash; 'Executed' is printed for select too
        self.assertIn("Executed", lines)

    def test_insert_then_select(self):
        lines = self.run_cmds([
            ins(1, "alice", "alice@example.com"),
            sel(),
            ".exit",
        ])
        self.assertIn("(1, alice, alice@example.com)", lines)

    def test_select_multiple_rows(self):
        lines = self.run_cmds([
            ins(3, "charlie", "c@example.com"),
            ins(1, "alice", "a@example.com"),
            ins(2, "bob", "b@example.com"),
            sel(),
            ".exit",
        ])
        rows = [l for l in lines if l.startswith("(")]
        self.assertEqual(len(rows), 3)

    def test_select_returns_sorted_order(self):
        lines = self.run_cmds([
            ins(30, "charlie", "c@example.com"),
            ins(10, "alice", "a@example.com"),
            ins(20, "bob", "b@example.com"),
            sel(),
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
        return [ins(1, "alice", "alice@example.com")]

    def test_bare_select_returns_all_columns(self):
        lines = self.run_cmds(self._one_row() + [sel(), ".exit"])
        self.assertIn("(1, alice, alice@example.com)", lines)

    def test_project_single_column(self):
        lines = self.run_cmds(self._one_row() + [sel("id"), ".exit"])
        self.assertIn("(1)", lines)

    def test_project_name_column(self):
        lines = self.run_cmds(self._one_row() + [sel("name"), ".exit"])
        self.assertIn("(alice)", lines)

    def test_project_multiple_columns_in_order(self):
        lines = self.run_cmds(self._one_row() + [sel("id, email"), ".exit"])
        self.assertIn("(1, alice@example.com)", lines)

    def test_projection_preserves_requested_order(self):
        # Reversed order must come out reversed, not in schema order.
        lines = self.run_cmds(self._one_row() + [sel("email, id"), ".exit"])
        self.assertIn("(alice@example.com, 1)", lines)

    def test_projection_without_spaces_around_comma(self):
        lines = self.run_cmds(self._one_row() + [sel("id,email"), ".exit"])
        self.assertIn("(1, alice@example.com)", lines)

    def test_repeated_column_allowed(self):
        lines = self.run_cmds(self._one_row() + [sel("id, id"), ".exit"])
        self.assertIn("(1, 1)", lines)

    def test_unknown_column_rejected(self):
        # 'bogus' resolves to no column, so the select fails and emits no row.
        lines = self.run_cmds(self._one_row() + [sel("bogus"), ".exit"])
        rows = [l for l in lines if l.startswith("(")]
        self.assertEqual(rows, [])

    def test_value_named_like_a_column_inserts(self):
        # Regression: column names must not be reserved words. Before generic
        # identifiers, 'id'/'name'/'email' as values broke the insert.
        lines = self.run_cmds([
            ins(1, "id", "id@example.com"),
            sel(),
            ".exit",
        ])
        self.assertIn("(1, id, id@example.com)", lines)


class TestWhere(DBTestCase):
    """WHERE-clause filtering."""

    def _people(self):
        return [
            ins(1, "alice", "alice@example.com"),
            ins(2, "bob", "bob@example.com"),
            ins(3, "carol", "carol@example.com"),
        ]

    @staticmethod
    def _rows(lines):
        return [l for l in lines if l.startswith("(")]

    # --- single comparison ------------------------------------------------

    def test_where_int_selects_matching_row(self):
        lines = self.run_cmds(self._people() + [sel("*", "id = 2"), ".exit"])
        rows = self._rows(lines)
        self.assertEqual(rows, ["(2, bob, bob@example.com)"])

    def test_where_int_no_match_is_empty(self):
        lines = self.run_cmds(self._people() + [sel("*", "id = 99"), ".exit"])
        self.assertEqual(self._rows(lines), [])

    def test_where_matches_name_string(self):
        lines = self.run_cmds(self._people() + [sel("*", "name = bob"), ".exit"])
        self.assertEqual(self._rows(lines), ["(2, bob, bob@example.com)"])

    def test_where_matches_email_string(self):
        lines = self.run_cmds(
            self._people() + [sel("*", "email = carol@example.com"), ".exit"]
        )
        self.assertEqual(self._rows(lines), ["(3, carol, carol@example.com)"])

    def test_where_combines_with_projection(self):
        lines = self.run_cmds(self._people() + [sel("name", "id = 3"), ".exit"])
        self.assertEqual(self._rows(lines), ["(carol)"])

    def test_where_string_no_match_is_empty(self):
        lines = self.run_cmds(self._people() + [sel("*", "name = zoe"), ".exit"])
        self.assertEqual(self._rows(lines), [])

    def test_where_string_literal_on_int_column_rejected(self):
        # id is an int column; comparing it to a string must be rejected,
        # not silently return the wrong rows.
        lines = self.run_cmds(self._people() + [sel("*", "id = bob"), ".exit"])
        self.assertEqual(self._rows(lines), [])

    def test_where_int_literal_on_string_column_rejected(self):
        lines = self.run_cmds(self._people() + [sel("*", "name = 5"), ".exit"])
        self.assertEqual(self._rows(lines), [])

    # --- AND / OR ---------------------------------------------------------

    def test_where_and_both_true(self):
        lines = self.run_cmds(
            self._people() + [sel("*", "id = 1 and name = alice"), ".exit"]
        )
        self.assertEqual(self._rows(lines), ["(1, alice, alice@example.com)"])

    def test_where_and_one_false_is_empty(self):
        lines = self.run_cmds(
            self._people() + [sel("*", "id = 1 and name = bob"), ".exit"]
        )
        self.assertEqual(self._rows(lines), [])

    def test_where_or_selects_either(self):
        lines = self.run_cmds(
            self._people() + [sel("*", "id = 1 or id = 3"), ".exit"]
        )
        self.assertEqual(
            self._rows(lines),
            ["(1, alice, alice@example.com)", "(3, carol, carol@example.com)"],
        )

    def test_where_or_both_false_is_empty(self):
        lines = self.run_cmds(
            self._people() + [sel("*", "id = 8 or id = 9"), ".exit"]
        )
        self.assertEqual(self._rows(lines), [])

    def test_where_and_binds_tighter_than_or(self):
        # Parsed as (id=1 AND name=alice) OR id=3, so row 3 must appear.
        # Under wrong precedence -- id=1 AND (name=alice OR id=3) -- row 3
        # would be excluded. This test discriminates the grouping.
        lines = self.run_cmds(
            self._people()
            + [sel("*", "id = 1 and name = alice or id = 3"), ".exit"]
        )
        self.assertEqual(
            self._rows(lines),
            ["(1, alice, alice@example.com)", "(3, carol, carol@example.com)"],
        )

    def test_where_chained_or(self):
        lines = self.run_cmds(
            self._people() + [sel("*", "id = 1 or id = 2 or id = 3"), ".exit"]
        )
        self.assertEqual(
            self._rows(lines),
            [
                "(1, alice, alice@example.com)",
                "(2, bob, bob@example.com)",
                "(3, carol, carol@example.com)",
            ],
        )

    # --- comparison operators: <, >, <=, >= --------------------------------

    def test_where_int_greater_than(self):
        lines = self.run_cmds(self._people() + [sel("id", "id > 1"), ".exit"])
        self.assertEqual(self._rows(lines), ["(2)", "(3)"])

    def test_where_int_less_than(self):
        lines = self.run_cmds(self._people() + [sel("id", "id < 3"), ".exit"])
        self.assertEqual(self._rows(lines), ["(1)", "(2)"])

    def test_where_int_greater_or_equal_includes_boundary(self):
        # >= must include the equal row; > must not.
        geq = self._rows(
            self.run_cmds(self._people() + [sel("id", "id >= 2"), ".exit"])
        )
        gt = self._rows(
            self.run_cmds(self._people() + [sel("id", "id > 2"), ".exit"])
        )
        self.assertEqual(geq, ["(2)", "(3)"])
        self.assertEqual(gt, ["(3)"])

    def test_where_int_less_or_equal_includes_boundary(self):
        leq = self._rows(
            self.run_cmds(self._people() + [sel("id", "id <= 2"), ".exit"])
        )
        lt = self._rows(
            self.run_cmds(self._people() + [sel("id", "id < 2"), ".exit"])
        )
        self.assertEqual(leq, ["(1)", "(2)"])
        self.assertEqual(lt, ["(1)"])

    def test_where_int_range_with_and(self):
        # Half-open-ish range: 1 < id <= 3  ->  ids 2 and 3.
        lines = self.run_cmds(
            self._people() + [sel("id", "id > 1 and id <= 3"), ".exit"]
        )
        self.assertEqual(self._rows(lines), ["(2)", "(3)"])

    # --- lexicographic comparison on string columns ------------------------

    def test_where_string_less_than_is_lexicographic(self):
        # alice < bob alphabetically; carol is not.
        lines = self.run_cmds(self._people() + [sel("name", "name < bob"), ".exit"])
        self.assertEqual(self._rows(lines), ["(alice)"])

    def test_where_string_greater_than_is_lexicographic(self):
        lines = self.run_cmds(self._people() + [sel("name", "name > bob"), ".exit"])
        self.assertEqual(self._rows(lines), ["(carol)"])

    def test_where_string_greater_or_equal_includes_boundary(self):
        lines = self.run_cmds(
            self._people() + [sel("name", "name >= bob"), ".exit"]
        )
        self.assertEqual(self._rows(lines), ["(bob)", "(carol)"])

    def test_where_email_column_less_than_is_lexicographic(self):
        # Emails sort a < b < c, matching the insert order.
        lines = self.run_cmds(
            self._people() + [sel("id", "email < bob@example.com"), ".exit"]
        )
        self.assertEqual(self._rows(lines), ["(1)"])


# ---------------------------------------------------------------------------
# 4. Persistence
# ---------------------------------------------------------------------------


class TestPersistence(DBTestCase):

    def test_data_survives_restart(self):
        run_db([USERS_SCHEMA, ins(1, "alice", "alice@example.com"), ".exit"], self.db)
        lines = strip_prompts(run_db([sel(), ".exit"], self.db))
        self.assertIn("(1, alice, alice@example.com)", lines)

    def test_multiple_records_survive_restart(self):
        cmds = [ins(i, f"user{i}", f"u{i}@x.com") for i in range(1, 6)]
        run_db([USERS_SCHEMA] + cmds + [".exit"], self.db)
        lines = strip_prompts(run_db([sel(), ".exit"], self.db))
        rows = [l for l in lines if l.startswith("(")]
        self.assertEqual(len(rows), 5)

    def test_incremental_writes_persist(self):
        run_db([USERS_SCHEMA, ins(1, "alice", "a@x.com"), ".exit"], self.db)
        run_db([ins(2, "bob", "b@x.com"), ".exit"], self.db)
        lines = strip_prompts(run_db([sel(), ".exit"], self.db))
        rows = [l for l in lines if l.startswith("(")]
        self.assertEqual(len(rows), 2)

    def test_duplicate_rejected_after_restart(self):
        run_db([USERS_SCHEMA, ins(1, "alice", "a@x.com"), ".exit"], self.db)
        lines = strip_prompts(
            run_db([ins(1, "alice2", "a2@x.com"), ".exit"], self.db)
        )
        self.assertIn("Error: Duplicate key.", lines)


# ---------------------------------------------------------------------------
# 5. Field-length boundaries
#
#     TEXT is now variable-length (length-prefixed), so these are no longer
#     hard size limits -- they just exercise long values round-tripping.
# ---------------------------------------------------------------------------


class TestFieldBoundaries(DBTestCase):

    def test_long_username(self):
        username = "a" * 31
        lines = self.run_cmds([ins(1, username, "x@x.com"), sel(), ".exit"])
        self.assertIn(f"(1, {username}, x@x.com)", lines)

    def test_long_email(self):
        email = "a" * 254
        lines = self.run_cmds([ins(1, "alice", email), sel(), ".exit"])
        self.assertIn(f"(1, alice, {email})", lines)

    def test_id_zero(self):
        lines = self.run_cmds([ins(0, "zero", "z@x.com"), sel(), ".exit"])
        rows = [l for l in lines if l.startswith("(")]
        self.assertEqual(len(rows), 1)
        self.assertIn("(0, zero, z@x.com)", lines)

    def test_large_id(self):
        lines = self.run_cmds([ins(999999, "bigid", "big@x.com"), sel(), ".exit"])
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
        cmds = [ins(i, f"user{i}", f"u{i}@x.com") for i in range(1, n + 1)]
        return self.run_cmds(cmds + [sel(), ".exit"])

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
            ins(i, f"user{i}", f"u{i}@x.com")
            for i in range(self.SPLIT_COUNT, 0, -1)
        ]
        lines = self.run_cmds(cmds + [sel(), ".exit"])
        rows = [l for l in lines if l.startswith("(")]
        self.assertEqual(len(rows), self.SPLIT_COUNT)

    def test_duplicate_detection_still_works_after_split(self):
        cmds = [ins(i, f"user{i}", f"u{i}@x.com") for i in range(1, self.SPLIT_COUNT + 1)]
        cmds.append(ins(7, "dup", "dup@x.com"))  # duplicate mid-tree
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
        cmds = [ins(i, f"user{i}", f"u{i}@x.com") for i in range(1, n + 1)]
        return self.run_cmds(cmds + [sel(), ".exit"])

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
        cmds = [ins(i, f"user{i}", f"u{i}@x.com") for i in range(1, self.DEEP_COUNT + 1)]
        run_db([USERS_SCHEMA] + cmds + [".exit"], self.db)
        lines = strip_prompts(run_db([sel(), ".exit"], self.db))
        rows = [l for l in lines if l.startswith("(")]
        self.assertEqual(len(rows), self.DEEP_COUNT)

    def test_random_order_inserts_with_internal_split(self):
        # Insert in a non-sequential order to exercise different split paths
        ids = list(range(1, self.DEEP_COUNT + 1))
        # interleave: odd ids first, then even
        ordered = ids[::2] + ids[1::2]
        cmds = [ins(i, f"user{i}", f"u{i}@x.com") for i in ordered]
        lines = self.run_cmds(cmds + [sel(), ".exit"])
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
            ins(10, "alice", "alice@example.com"),
            ins(5, "bob", "bob@example.com"),
            ins(20, "charlie", "charlie@example.com"),
            sel(),
            ".exit",
        ])
        rows = [l for l in lines if l.startswith("(")]
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0], "(5, bob, bob@example.com)")
        self.assertEqual(rows[1], "(10, alice, alice@example.com)")
        self.assertEqual(rows[2], "(20, charlie, charlie@example.com)")

    def test_interleaved_inserts_and_selects(self):
        lines = self.run_cmds([
            ins(1, "alice", "a@x.com"),
            sel(),
            ins(2, "bob", "b@x.com"),
            sel(),
            ".exit",
        ])
        first_select = [l for l in lines if l.startswith("(")][0]
        self.assertIn("alice", first_select)

    def test_session_with_errors_continues(self):
        """Errors should not crash the REPL; subsequent commands still work."""
        lines = self.run_cmds([
            ins(1, "alice", "a@x.com"),
            ins(1, "alice", "a@x.com"),    # duplicate
            ".badcmd",                     # bad meta
            "garbage sql",                 # unrecognized
            ins(2, "bob", "b@x.com"),
            sel(),
            ".exit",
        ])
        rows = [l for l in lines if l.startswith("(")]
        self.assertEqual(len(rows), 2)

    def test_large_dataset_end_to_end(self):
        n = 50
        cmds = [ins(i, f"user{i}", f"u{i}@example.com") for i in range(1, n + 1)]
        lines = self.run_cmds(cmds + [sel(), ".exit"])
        rows = [l for l in lines if l.startswith("(")]
        self.assertEqual(len(rows), n)
        ids = [int(r.split(",")[0].strip("( ")) for r in rows]
        self.assertEqual(ids, list(range(1, n + 1)))

    def test_data_integrity_after_multiple_restarts(self):
        for batch in range(1, 4):
            start = (batch - 1) * 5 + 1
            end = batch * 5 + 1
            cmds = [ins(i, f"user{i}", f"u{i}@x.com") for i in range(start, end)]
            prefix = [USERS_SCHEMA] if batch == 1 else []
            run_db(prefix + cmds + [".exit"], self.db)

        lines = strip_prompts(run_db([sel(), ".exit"], self.db))
        rows = [l for l in lines if l.startswith("(")]
        self.assertEqual(len(rows), 15)


# ---------------------------------------------------------------------------
# 9. Delete – basic behaviour (no underflow)
# ---------------------------------------------------------------------------


class TestDeleteBasic(DBTestCase):
    """Delete operations that do NOT trigger underflow (root leaf only)."""

    def test_delete_single_key(self):
        lines = self.run_cmds([
            ins(1, "alice", "a@x.com"),
            ins(2, "bob", "b@x.com"),
            ins(3, "charlie", "c@x.com"),
            dele(2),
            sel(),
            ".exit",
        ])
        rows = [l for l in lines if l.startswith("(")]
        ids = [int(r.split(",")[0].strip("( ")) for r in rows]
        self.assertEqual(ids, [1, 3])

    def test_delete_nonexistent_key(self):
        lines = self.run_cmds([
            ins(1, "alice", "a@x.com"),
            dele(99),
            ".exit",
        ])
        self.assertIn("Key 99 does not exist", lines)

    def test_delete_first_key(self):
        lines = self.run_cmds([
            ins(1, "alice", "a@x.com"),
            ins(2, "bob", "b@x.com"),
            ins(3, "charlie", "c@x.com"),
            dele(1),
            sel(),
            ".exit",
        ])
        rows = [l for l in lines if l.startswith("(")]
        ids = [int(r.split(",")[0].strip("( ")) for r in rows]
        self.assertEqual(ids, [2, 3])

    def test_delete_last_key(self):
        lines = self.run_cmds([
            ins(1, "alice", "a@x.com"),
            ins(2, "bob", "b@x.com"),
            ins(3, "charlie", "c@x.com"),
            dele(3),
            sel(),
            ".exit",
        ])
        rows = [l for l in lines if l.startswith("(")]
        ids = [int(r.split(",")[0].strip("( ")) for r in rows]
        self.assertEqual(ids, [1, 2])

    def test_delete_only_key_in_root_leaf(self):
        lines = self.run_cmds([
            ins(1, "alice", "a@x.com"),
            dele(1),
            sel(),
            ".exit",
        ])
        rows = [l for l in lines if l.startswith("(")]
        self.assertEqual(len(rows), 0)

    def test_delete_then_reinsert(self):
        lines = self.run_cmds([
            ins(1, "alice", "a@x.com"),
            ins(2, "bob", "b@x.com"),
            dele(1),
            ins(1, "alice2", "a2@x.com"),
            sel(),
            ".exit",
        ])
        rows = [l for l in lines if l.startswith("(")]
        self.assertEqual(len(rows), 2)
        self.assertIn("(1, alice2, a2@x.com)", lines)

    def test_delete_persists_after_restart(self):
        run_db([
            USERS_SCHEMA,
            ins(1, "alice", "a@x.com"),
            ins(2, "bob", "b@x.com"),
            ins(3, "charlie", "c@x.com"),
            dele(2),
            ".exit",
        ], self.db)
        lines = strip_prompts(run_db([sel(), ".exit"], self.db))
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
        return [ins(i, f"user{i}", f"u{i}@x.com") for i in range(start, end)]

    def test_leaf_redistribute(self):
        """Insert 14 keys (split into two 7-cell leaves), then delete 2
        from one leaf to trigger redistribute from its sibling."""
        cmds = self._insert_range(1, 15)
        cmds += [dele(1), dele(2), sel(), ".exit"]
        lines = self.run_cmds(cmds)
        rows = [l for l in lines if l.startswith("(")]
        ids = [int(r.split(",")[0].strip("( ")) for r in rows]
        self.assertEqual(ids, list(range(3, 15)))

    def test_leaf_merge(self):
        """Delete enough keys to force a leaf merge."""
        cmds = self._insert_range(1, 15)
        for i in range(1, 9):
            cmds.append(dele(i))
        cmds += [sel(), ".exit"]
        lines = self.run_cmds(cmds)
        rows = [l for l in lines if l.startswith("(")]
        ids = [int(r.split(",")[0].strip("( ")) for r in rows]
        self.assertEqual(ids, list(range(9, 15)))

    def test_delete_all_one_by_one(self):
        """Insert 30 keys, delete them all, verify empty table."""
        n = 30
        cmds = self._insert_range(1, n + 1)
        for i in range(1, n + 1):
            cmds.append(dele(i))
        cmds += [sel(), ".exit"]
        lines = self.run_cmds(cmds)
        rows = [l for l in lines if l.startswith("(")]
        self.assertEqual(len(rows), 0)

    def test_root_collapse(self):
        """Delete until the tree shrinks back to a single leaf root."""
        n = 20
        cmds = self._insert_range(1, n + 1)
        for i in range(1, n - 2):
            cmds.append(dele(i))
        cmds += [sel(), ".exit"]
        lines = self.run_cmds(cmds)
        rows = [l for l in lines if l.startswith("(")]
        ids = [int(r.split(",")[0].strip("( ")) for r in rows]
        self.assertEqual(ids, list(range(n - 2, n + 1)))

    def test_insert_after_rebalance(self):
        """After rebalancing, inserts should still work correctly."""
        cmds = self._insert_range(1, 15)
        for i in range(1, 8):
            cmds.append(dele(i))
        cmds += [
            ins(100, "new1", "new1@x.com"),
            ins(200, "new2", "new2@x.com"),
            sel(),
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
            cmds.append(dele(i))
        run_db([USERS_SCHEMA] + cmds + [".exit"], self.db)
        lines = strip_prompts(run_db([sel(), ".exit"], self.db))
        rows = [l for l in lines if l.startswith("(")]
        ids = [int(r.split(",")[0].strip("( ")) for r in rows]
        self.assertEqual(ids, list(range(8, 15)))

    def test_deep_tree_internal_merge(self):
        """60 keys → internal splits. Delete 45 to trigger internal merges."""
        n = 60
        cmds = self._insert_range(1, n + 1)
        for i in range(1, 46):
            cmds.append(dele(i))
        cmds += [sel(), ".exit"]
        lines = self.run_cmds(cmds)
        rows = [l for l in lines if l.startswith("(")]
        ids = [int(r.split(",")[0].strip("( ")) for r in rows]
        self.assertEqual(ids, list(range(46, n + 1)))

    def test_alternating_delete(self):
        """Delete every other key, verify sorted order is maintained."""
        n = 30
        cmds = self._insert_range(1, n + 1)
        for i in range(1, n + 1, 2):
            cmds.append(dele(i))
        cmds += [sel(), ".exit"]
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

    Each crashed session issues the CREATE first, so recovery must rebuild both
    the catalog entry and the table's rows from the log.
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
        run_db([USERS_SCHEMA] + commands, self.db)

    def reopen_ids(self) -> list[int]:
        """Reopen cleanly, select everything, return the recovered key ids."""
        lines = strip_prompts(run_db([sel(), ".exit"], self.db))
        rows = [l for l in lines if l.startswith("(")]
        return [int(r.split(",")[0].strip("( ")) for r in rows]

    @staticmethod
    def _inserts(lo: int, hi: int) -> list[str]:
        return [ins(i, f"user{i}", f"u{i}@x.com") for i in range(lo, hi)]

    def clean_exit(self, commands: list[str]) -> None:
        """Run commands followed by a clean `.exit`, so close_db flushes every
        page to the .db file, fsyncs, and checkpoints (truncates) the WAL."""
        run_db([USERS_SCHEMA] + commands + [".exit"], self.db)

    # -- tests -------------------------------------------------------------

    def test_committed_inserts_survive_crash(self):
        self.crash([ins(1, "alice", "a@x.com"), ins(2, "bob", "b@x.com")])
        # Nothing was flushed to .db: recovery must restore from the WAL alone.
        self.assertEqual(os.path.getsize(self.db), 0)
        self.assertEqual(self.reopen_ids(), [1, 2])

    def test_recovery_restores_all_rows_after_split(self):
        # 14 rows force a leaf split (multi-page transaction in the WAL).
        self.crash(self._inserts(1, 15))
        self.assertEqual(self.reopen_ids(), list(range(1, 15)))

    def test_committed_deletes_survive_crash(self):
        # Insert 14 (forces a split), delete 1..7, then crash before flush.
        self.crash(self._inserts(1, 15) + [dele(i) for i in range(1, 8)])
        self.assertEqual(self.reopen_ids(), list(range(8, 15)))

    def test_recovery_is_idempotent_across_reopens(self):
        self.crash([ins(1, "a", "a@x.com"), ins(2, "b", "b@x.com"),
                    ins(3, "c", "c@x.com")])
        # Recovery runs on every open and the WAL is never truncated, so
        # replaying it repeatedly must keep producing the same state.
        self.assertEqual(self.reopen_ids(), [1, 2, 3])
        self.assertEqual(self.reopen_ids(), [1, 2, 3])

    def test_garbage_tail_in_wal_is_ignored(self):
        self.crash([ins(1, "a", "a@x.com"), ins(2, "b", "b@x.com")])
        # Append bytes that cannot parse as a valid record (bad type/length).
        with open(self.wal_path(), "ab") as f:
            f.write(b"\xff" * 100)
        # Recovery stops at the unparseable tail; valid records still applied.
        self.assertEqual(self.reopen_ids(), [1, 2])

    def test_corrupted_payload_detected_by_checksum(self):
        self.crash([ins(1, "a", "a@x.com"), ins(2, "b", "b@x.com")])
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
        self.crash([ins(1, "a", "a@x.com"), ins(2, "b", "b@x.com")])
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


# ---------------------------------------------------------------------------
# 11. Multiple tables
#
#     Every test above runs through a single `users` table, so none of the
#     db->tables lookup, the per-table root page, or the multi-entry catalog is
#     exercised. These create a SECOND table and assert the two behave as
#     independent B-trees -- both in one session and across a restart (which
#     forces open_db to rebuild every table by re-parsing its stored
#     create_statement).
# ---------------------------------------------------------------------------


class TestMultipleTables(DBTestCase):

    def test_two_tables_hold_independent_rows(self):
        # users and pets share a schema but must not share a tree.
        lines = self.run_cmds([
            make_users_like("pets"),
            ins(1, "alice", "a@x.com"),          # -> users
            ins(2, "bob", "b@x.com"),            # -> users
            ins_into("pets", 9, "rex", "r@x.com"),
            sel_from("users"),
            sel_from("pets"),
            ".exit",
        ])
        # Users select must not surface the pets row and vice versa.
        self.assertIn("(1, alice, a@x.com)", lines)
        self.assertIn("(2, bob, b@x.com)", lines)
        self.assertIn("(9, rex, r@x.com)", lines)

    def test_same_key_in_two_tables_is_not_a_duplicate(self):
        # id 1 exists in both tables; because each table is its own tree the
        # second insert must NOT be rejected as a duplicate key.
        lines = self.run_cmds([
            make_users_like("pets"),
            ins(1, "alice", "a@x.com"),
            ins_into("pets", 1, "rex", "r@x.com"),
            sel_from("pets"),
            ".exit",
        ])
        self.assertNotIn("Error: Duplicate key.", lines)
        self.assertIn("(1, rex, r@x.com)", lines)

    def test_second_table_survives_restart(self):
        run_db([
            USERS_SCHEMA,
            make_users_like("pets"),
            ins(1, "alice", "a@x.com"),
            ins_into("pets", 9, "rex", "r@x.com"),
            ".exit",
        ], self.db)
        lines = strip_prompts(run_db([sel_from("pets"), ".exit"], self.db))
        self.assertIn("(9, rex, r@x.com)", lines)

    def test_both_tables_intact_after_restart(self):
        run_db([
            USERS_SCHEMA,
            make_users_like("pets"),
            ins(1, "alice", "a@x.com"),
            ins_into("pets", 9, "rex", "r@x.com"),
            ".exit",
        ], self.db)
        u = strip_prompts(run_db([sel_from("users"), ".exit"], self.db))
        p = strip_prompts(run_db([sel_from("pets"), ".exit"], self.db))
        self.assertEqual(ids_of(u), [1])
        self.assertEqual(ids_of(p), [9])

    def test_split_in_one_table_leaves_the_other_untouched(self):
        # Fill users past a leaf split; pets keeps its single row. A bug that
        # crossed the trees' page bookkeeping would corrupt one of them.
        cmds = [make_users_like("pets"), ins_into("pets", 1, "rex", "r@x.com")]
        cmds += [ins(i, f"user{i}", f"u{i}@x.com") for i in range(1, 15)]
        cmds += [sel_from("pets"), sel_from("users"), ".exit"]
        lines = self.run_cmds(cmds)
        self.assertEqual(ids_of([l for l in lines if l == "(1, rex, r@x.com)"]), [1])
        self.assertIn("(1, rex, r@x.com)", lines)
        # users still has all 14 rows.
        user_rows = [l for l in rows_of(lines) if "user" in l]
        self.assertEqual(len(user_rows), 14)

    def test_four_tables_all_usable(self):
        # tables[0] is the catalog, so MAX_TABLES (5) leaves room for 4 user
        # tables. Create the ceiling and confirm each is an independent tree.
        names = ["t1", "t2", "t3", "t4"]
        cmds = [make_users_like(n) for n in names]
        cmds += [ins_into(n, i + 1, f"u{n}", f"{n}@x.com")
                 for i, n in enumerate(names)]
        cmds += [sel_from(n) for n in names] + [".exit"]
        lines = self.run_cmds(cmds, create=False)
        for i, n in enumerate(names):
            self.assertIn(f"({i + 1}, u{n}, {n}@x.com)", lines)


# ---------------------------------------------------------------------------
# 12. Statements against a table that does not exist
#
#     execute_statement() looks the table up by name but never checks whether
#     the lookup succeeded, so a missing table leaves `table` pointing at a
#     fresh uninitialized malloc -> the executor dereferences a garbage schema.
#     As with TestInsertValidation, each test pairs the bad statement with a
#     known-good one and requires the good result to appear, so a crash on the
#     bad statement (nothing printed afterward) fails the test.
#
#     EXPECTED TO FAIL until execute_statement rejects an unknown table name.
# ---------------------------------------------------------------------------


class TestUnknownTable(DBTestCase):

    def test_insert_into_unknown_table_does_not_corrupt_db(self):
        lines = self.run_cmds([
            ins_into("ghost", 1, "x", "x@x.com"),   # no such table
            ins(42, "good", "g@x.com"),             # real table, must survive
            sel(),
            ".exit",
        ])
        self.assertEqual(rows_of(lines), ["(42, good, g@x.com)"])

    def test_select_from_unknown_table_does_not_crash(self):
        lines = self.run_cmds([
            ins(42, "good", "g@x.com"),
            sel_from("ghost"),                       # no such table
            sel(),                                   # must still run
            ".exit",
        ])
        self.assertIn("(42, good, g@x.com)", lines)

    def test_delete_from_unknown_table_does_not_crash(self):
        lines = self.run_cmds([
            ins(42, "good", "g@x.com"),
            "delete from ghost 1",                   # no such table
            sel(),
            ".exit",
        ])
        self.assertIn("(42, good, g@x.com)", lines)


# ---------------------------------------------------------------------------
# 13. rowid keying: tables without an INTEGER PRIMARY KEY
#
#     execute_insert() keys a row on its INTEGER PRIMARY KEY when the schema has
#     one, and otherwise (no PK, or a TEXT PK) falls back to an auto-assigned
#     rowid from table->rowid_counter. Nothing above ever leaves the int-PK
#     path, so the entire rowid branch is untested. These build such tables and
#     use `select *` only, since projection/WHERE resolve id/name/email only.
# ---------------------------------------------------------------------------


class TestRowidKeying(DBTestCase):

    def test_no_pk_two_rows_both_stored(self):
        # No PK -> each insert gets its own rowid, so two rows with identical
        # content must NOT collide as a duplicate key.
        lines = self.run_cmds([
            "create table logs (msg text)",
            ins_into("logs", "hello"),
            ins_into("logs", "world"),
            sel_from("logs"),
            ".exit",
        ], create=False)
        self.assertNotIn("Error: Duplicate key.", lines)
        self.assertEqual(rows_of(lines), ["(hello)", "(world)"])

    def test_text_pk_does_not_enforce_uniqueness(self):
        # A TEXT primary key still keys on rowid (the alias only applies to
        # INTEGER PRIMARY KEY), so duplicate text values are both stored.
        lines = self.run_cmds([
            "create table logs (msg text primary key)",
            ins_into("logs", "dup"),
            ins_into("logs", "dup"),
            sel_from("logs"),
            ".exit",
        ], create=False)
        self.assertNotIn("Error: Duplicate key.", lines)
        self.assertEqual(rows_of(lines), ["(dup)", "(dup)"])

    def test_auto_rowid_does_not_collide_across_restart(self):
        # The crux: on reopen the counter must be restored above the highest
        # existing rowid (open_db uses get_rightmost_rowid + 1). If it reset to
        # 1 instead, the post-restart insert would reuse rowid 1 and clobber the
        # first row. Distinct payloads let us prove both survived.
        run_db(["create table logs (msg text)",
                ins_into("logs", "first"), ".exit"], self.db)
        run_db([ins_into("logs", "second"), ".exit"], self.db)
        lines = strip_prompts(run_db([sel_from("logs"), ".exit"], self.db))
        self.assertEqual(rows_of(lines), ["(first)", "(second)"])

    def test_no_pk_rows_survive_leaf_split(self):
        # Auto-rowid rows must page-split like keyed rows do.
        cmds = ["create table logs (msg text)"]
        cmds += [ins_into("logs", f"m{i}") for i in range(20)]
        cmds += [sel_from("logs"), ".exit"]
        lines = self.run_cmds(cmds, create=False)
        self.assertEqual(len(rows_of(lines)), 20)


# ---------------------------------------------------------------------------
# 14. Arbitrary schemas (shape other than id/name/email)
#
#     The catalog stores each table's create_statement and open_db rebuilds the
#     schema by re-parsing it. Every test above uses the one users shape, so a
#     bug in reconstructing a differently-shaped schema (column count/types)
#     would go unseen. Uses `select *` only.
# ---------------------------------------------------------------------------


class TestArbitrarySchema(DBTestCase):

    def test_two_column_table_roundtrips(self):
        lines = self.run_cmds([
            "create table kv (k int primary key, v text)",
            ins_into("kv", 1, "hello"),
            sel_from("kv"),
            ".exit",
        ], create=False)
        self.assertIn("(1, hello)", lines)

    def test_wide_table_roundtrips(self):
        # Five columns of mixed type in one record.
        lines = self.run_cmds([
            "create table wide (a int primary key, b text, c int, d text, e int)",
            ins_into("wide", 1, "two", 3, "four", 5),
            sel_from("wide"),
            ".exit",
        ], create=False)
        self.assertIn("(1, two, 3, four, 5)", lines)

    def test_arbitrary_schema_persists_across_restart(self):
        # Exercises open_db's re-parse of a NON-users create_statement.
        run_db([
            "create table kv (k int primary key, v text)",
            ins_into("kv", 1, "hello"),
            ins_into("kv", 2, "world"),
            ".exit",
        ], self.db)
        lines = strip_prompts(run_db([sel_from("kv"), ".exit"], self.db))
        self.assertEqual(rows_of(lines), ["(1, hello)", "(2, world)"])


# ---------------------------------------------------------------------------
# 15. CREATE TABLE edge cases
# ---------------------------------------------------------------------------


class TestCreateTableEdges(DBTestCase):

    def test_recreating_a_table_keeps_it_coherent(self):
        # Whether a second `create table users` is rejected or ignored is a
        # design choice; either way the table must remain ONE coherent tree.
        # Duplicate-key detection on it proves the rows share a single tree.
        lines = self.run_cmds([
            USERS_SCHEMA,               # users created a second time
            ins(1, "alice", "a@x.com"),
            ins(1, "bob", "b@x.com"),   # same key -> must be rejected
            ".exit",
        ])
        self.assertIn("Error: Duplicate key.", lines)

    def test_insert_before_any_create_does_not_crash(self):
        # No user table exists yet; the insert must be rejected, and the REPL
        # must stay alive to serve a following create + insert + select.
        lines = self.run_cmds([
            ins(1, "early", "e@x.com"),   # nothing to insert into
            USERS_SCHEMA,
            ins(2, "later", "l@x.com"),
            sel(),
            ".exit",
        ], create=False)
        self.assertIn("(2, later, l@x.com)", lines)
        self.assertNotIn("(1, early, e@x.com)", lines)


if __name__ == "__main__":
    unittest.main(verbosity=2)
