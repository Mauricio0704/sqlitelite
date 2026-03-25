# sqlitelite

A minimal SQLite-like database engine written in C, built from scratch to understand how databases work under the hood. It implements a persistent B+ tree over fixed-size pages, a simple REPL, and a binary file format compatible across restarts.

---

## Requirements

- GCC (any version supporting C99)
- Python 3.9+ (for the test suite)
- GNU Make

---

## Compile

```bash
make
```

This produces the `mini-db` binary. To remove it:

```bash
make clean
```

---

## Run

The binary takes a database file path as its only argument. The file is created if it does not exist.

```bash
./mini-db mydb.db
```

You will see an interactive REPL prompt:

```
db >
```

### Supported commands

| Command | Description |
|---|---|
| `insert <id> <username> <email>` | Insert a record. `id` must be a non-negative integer. |
| `select` | Print all records in ascending key order. |
| `.exit` | Flush all pages to disk and exit. |

### Example session

```
db > insert 3 charlie c@example.com
Executed
db > insert 1 alice a@example.com
Executed
db > insert 2 bob b@example.com
Executed
db > select
(1, alice, a@example.com)
(2, bob, b@example.com)
(3, charlie, c@example.com)
Executed
db > .exit
```

## Run the tests

The test suite (`tests.py`) drives the binary through subprocess calls and covers all major behaviors.

```bash
python3 -m pytest tests.py -v
```

Or with the built-in `unittest` runner:

```bash
python3 -m unittest tests -v
```

The tests create and destroy temporary database files automatically, so they are safe to run repeatedly.

### What the tests cover

| Test class | What it exercises |
|---|---|
| `TestMetaCommands` | Prompt output, `.exit`, unrecognized meta-commands, missing file argument |
| `TestInsertBasic` | `Executed` response, missing/invalid arguments, duplicate key rejection, unrecognized SQL |
| `TestSelectBasic` | Empty table, single row, multiple rows, sorted output |
| `TestPersistence` | Data survives process restart, incremental writes, duplicate detection after restart |
| `TestFieldBoundaries` | Maximum username (31 chars) and email (254 chars) lengths, `id = 0`, large ids |
| `TestLeafNodeSplit` | 14+ rows force a leaf split; all rows returned in order afterward |
| `TestInternalNodeSplit` | 60+ rows force internal node splits; sorted output and persistence verified |
| `TestEndToEnd` | Interleaved inserts and selects, errors do not crash the REPL, large datasets |

---

## Architecture

### Overview

```
REPL (main)
  └── prepare_statement()       parse input into a Statement
  └── execute_statement()       dispatch to insert or select
        ├── execute_insert()    B+ tree traversal + leaf_node_insert
        └── execute_select()    leftmost-path descent + leaf linked-list scan

Storage
  └── Pager                     page cache + read/write to disk
        └── get_page()          lazy load from file into pager->pages[]
        └── flush_page()        write one page back to its offset in the file
        └── close_db()          flush all pages and close the file descriptor
```

---

### Page layout

Every unit of storage is a **4096-byte page** (`PAGE_SIZE`). There are at most 100 pages per database file (`TABLE_MAX_PAGES`). Pages are loaded into memory on demand and written back to disk only on `.exit`.

---

### Common node header

Every page (whether a leaf or internal node) begins with the same 6-byte header:

```
Offset  Size  Field
0       1B    is_root       (uint8)
1       1B    node_type     (uint8: 0 = internal, 1 = leaf)
2       4B    parent_ptr    (uint32, page number of the parent)
```

---

### Leaf node

Leaf nodes store the actual records. They use a **slotted-page layout**, the same design used by PostgreSQL heap pages and SQLite leaf pages:

```
[ Header ] [ Slot directory → ] [ free space ] [ ← Cell payloads ]
```

**Header (18 bytes total)**

```
Offset  Size  Field
0       6B    Common node header
6       4B    next_pointer    (page number of the next leaf, 0 if last)
10      4B    num_cells       (number of records on this page)
14      2B    free_start      (first free byte in the slot directory region)
16      2B    free_end        (first free byte in the cell payload region, growing down)
```

**Slot directory**

Immediately after the header is an array of `uint16_t` slot entries, one per record. Each slot holds the **byte offset** within the page where that record's cell payload begins. Slots are ordered by key, so a binary search over slot indices gives O(log n) lookup without moving any cell data.

**Cell payload**

Each cell is stored at the high end of the page and contains:

```
| key (4B, uint32) | record payload (291B) |
```

Where the record payload is:

```
| id (4B) | username (32B) | email (255B) |
```

**Insertion into a leaf**

New cells are written at `free_end - CELL_SIZE` and grow upward. The slot entry pointing to the new cell is inserted into the slot directory in sorted order (shifting existing slot entries right). `free_start` advances by 2 bytes; `free_end` retreats by 295 bytes. When `free_end - free_start < CELL_SIZE + SLOT_SIZE`, the node is full and must split.

**Maximum capacity**

With a 4096-byte page and an 18-byte header, there are 4078 bytes for cells. Each cell occupies 297 bytes (4 key + 291 record + 2 slot), giving `LEAF_NODE_MAX_CELLS = 13`.

**Leaf linked list**

Each leaf's `next_pointer` points to the next leaf page in key order. `execute_select` uses this chain to scan all records without traversing the tree again after reaching the leftmost leaf.

---

### Internal node

Internal nodes act as the routing layer of the B+ tree. They do not store records.

**Layout**

```
Offset  Size  Field
0       6B    Common node header
6       4B    num_keys
10      4B    rightmost_pointer    (page number of the rightmost child)
14      ...   Array of (pointer: 4B, key: 4B) pairs
```

The body stores `num_keys` pairs of `(left_child_pointer, separator_key)` plus a single `rightmost_pointer`. To find the child for a given search key `k`:

- Walk the pairs left to right.
- If `k <= separator_key[i]`, descend into `pointer[i]`.
- If `k` is greater than all keys, descend into `rightmost_pointer`.

`INTERNAL_NODE_MAX_KEYS = 3` is intentionally small so that internal splits are exercised early during testing.

---

### B+ tree insertion

```
execute_insert
  1. Start at the root page.
  2. While the current node is internal:
       binary-search the separator keys to pick the next child page.
  3. On the leaf:
       binary-search the slot directory to find the insertion position.
       if duplicate key → return EXECUTE_DUPLICATE_KEY.
  4. leaf_node_insert:
       if enough free space → write cell + slot entry in place.
       else → leaf_node_split.
```

#### Leaf split

When a leaf is full:

1. All existing cells plus the new one are gathered into a temporary array, sorted by key.
2. The array is split at `(total_cells + 1) / 2`. Left half stays on the original page (or a new page if the leaf was the root); right half goes onto a new page.
3. The `next_pointer` chain is updated: `left → right → old_next`.
4. The separator key (the largest key in the left half) and a pointer to the right page are inserted into the parent.
5. If the parent internal node is also full, `internal_node_split` is called.

#### Internal node split

When a root internal node is full and needs a new entry:

1. All existing keys and child pointers, plus the new key and pointers, are assembled into temporary arrays.
2. The middle key is promoted. Keys/pointers to its left form a new left internal node; keys/pointers to its right form a new right internal node.
3. The original root page is reinitialized as a new root with one key (the promoted key) and two children.
4. Parent pointers of all children are updated to point to their new parent page.

---

### Persistence

When `.exit` is issued, `close_db` iterates over every cached page and calls `flush_page`, which seeks to `page_num * PAGE_SIZE` in the file and writes exactly 4096 bytes. On the next open, `get_page` reads each page back from the same offset.

---

## Resources used

- *https://cstack.github.io/db_tutorial/*
- *https://github.com/cstack/db_tutorial/blob/60b50c5b7be787a4aaa1e50ab8a90c6cabb75159/db.c*
- *https://github.com/davideuler/SQLite-2.5.0-for-code-reading/blob/95c98088ffd4fff76aea62a4f79b7d08bf9a3b7d/src/btree.c#L1890*
- *https://play.google.com/books/reader?id=9Z6IQQnX1JEC&pg=GBS.PA1&hl=en*
