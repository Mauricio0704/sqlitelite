#include <ctype.h>
#include <fcntl.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#define TABLE_MAX_PAGES 100
#define PAGE_SIZE 4096
#define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)

typedef struct {
  uint32_t id;
  char username[32];
  char email[255];
} Record;

const uint32_t ID_SIZE = size_of_attribute(Record, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Record, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Record, email);

const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;

const uint32_t RECORD_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
const uint32_t RECORDS_PER_PAGE = (PAGE_SIZE / RECORD_SIZE);

typedef struct {
  char *buffer;
  size_t input_length;
  ssize_t buffer_length;
} InputBuffer;

typedef enum {
  PREPARE_SUCCESS,
  PREPARE_FAILURE,
  PREPARE_UNRECOGNIZED_COMMAND
} PrepareStatus;

typedef enum { EXECUTE_SUCCESS, EXECUTE_DUPLICATE_KEY } ExecuteStatus;

typedef enum {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandStatus;

typedef enum {
  NODE_TYPE_INTERNAL,
  NODE_TYPE_LEAF,
  NODE_TYPE_INDEFINITE
} NodeType;

typedef enum {
  TOKEN_KW_SELECT,
  TOKEN_KW_INSERT,
  TOKEN_KW_DELETE,
  TOKEN_KW_WHERE,
  TOKEN_KW_AND,
  TOKEN_KW_OR,
  TOKEN_OP_ALL,
  TOKEN_OP_EQUAL,
  TOKEN_COMMA,
  TOKEN_IDENTIFIER,
  TOKEN_INT_LITERAL,
  TOKEN_EOF
} TokenType;

typedef enum {
  STATEMENT_INSERT,
  STATEMENT_SELECT,
  STATEMENT_DELETE
} StatementType;

typedef enum { COLUMN_ID, COLUMN_USERNAME, COLUMN_EMAIL } ColumnId;

/* Upper bound on columns in a SELECT projection list (allows repeats). */
#define MAX_SELECT_COLUMNS 8

typedef enum { COMPARISON, AND_EXPR, OR_EXPR } ExprKind;

typedef struct Expr {
  ColumnId col;
  TokenType operator;
  uint32_t intval;
  char *strval;
  ExprKind kind;
  struct Expr *left;  // Just for or/and
  struct Expr *right; // Just for or/and
} Expr;

typedef struct {
  StatementType statement_type;
  Record record_to_insert;
  uint32_t id_to_delete;
  /* Ordered projection list. A count of 0 means "all columns" (bare SELECT). */
  ColumnId projection[MAX_SELECT_COLUMNS];
  size_t projection_count;
  int has_where;
  Expr *where_expr;
} Statement;

typedef struct {
  TokenType type;
  char *start_lexeme;
  size_t len_lexeme;
} Token;

typedef enum { COMMIT, CHECKPOINT, PAGE_CHANGE } WALEntryType;

/* In-memory view of one WAL record's fixed header. Payload is handled
 * separately */
typedef struct {
  WALEntryType type;
  uint32_t checksum;
  off_t lsn;
  uint32_t txid;
  uint32_t page_num;
  uint32_t length;
} WALEntry;

// WAL on-disk header layout
const uint32_t WAL_CHECKSUM_SIZE = sizeof(uint32_t);
const uint32_t WAL_CHECKSUM_OFFSET = 0;
const uint8_t WAL_TYPE_SIZE = sizeof(uint8_t);
const uint32_t WAL_TYPE_OFFSET = WAL_CHECKSUM_OFFSET + WAL_CHECKSUM_SIZE;
const off_t WAL_LSN_SIZE = sizeof(off_t);
const uint32_t WAL_LSN_OFFSET = WAL_TYPE_OFFSET + WAL_TYPE_SIZE;
const uint32_t WAL_TXID_SIZE = sizeof(uint32_t);
const uint32_t WAL_TXID_OFFSET = WAL_LSN_OFFSET + WAL_LSN_SIZE;
const uint32_t WAL_PAGE_NUM_SIZE = sizeof(uint32_t);
const uint32_t WAL_PAGE_NUM_OFFSET = WAL_TXID_OFFSET + WAL_TXID_SIZE;
const uint32_t WAL_LENGTH_SIZE = sizeof(uint32_t);
const uint32_t WAL_LENGTH_OFFSET = WAL_PAGE_NUM_OFFSET + WAL_PAGE_NUM_SIZE;
enum { WAL_COMMON_HEADER_SIZE = WAL_LENGTH_OFFSET + WAL_LENGTH_SIZE };

typedef struct {
  off_t file_length;
  char *filename;
  int fd;
  uint32_t curr_txid;
} WAL;

typedef struct {
  void *pages[TABLE_MAX_PAGES];
  uint8_t dirty[TABLE_MAX_PAGES];
  off_t file_length;
  uint32_t num_pages;
  char *filename;
  int fd;
  WAL *wal;
} Pager;

typedef struct {
  Pager *pager;
  uint32_t root_page_num;
} Table;

typedef struct {
  uint32_t page_num;
  uint32_t slot_num;
  Table *table;
  int is_end_of_table;
} Cursor;

const uint8_t NODE_IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t NODE_IS_ROOT_OFFSET = 0;
const uint8_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = NODE_IS_ROOT_OFFSET + NODE_IS_ROOT_SIZE;
const uint32_t NODE_PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t NODE_PARENT_POINTER_OFFSET = NODE_TYPE_OFFSET + NODE_TYPE_SIZE;
const uint32_t COMMON_NODE_HEADER_SIZE =
    NODE_IS_ROOT_SIZE + NODE_TYPE_SIZE + NODE_PARENT_POINTER_SIZE;

// Internal node header
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_RIGHTMOST_POINTER_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_RIGHTMOST_POINTER_OFFSET =
    INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const uint32_t INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE +
                                           INTERNAL_NODE_NUM_KEYS_SIZE +
                                           INTERNAL_NODE_RIGHTMOST_POINTER_SIZE;
enum { INTERNAL_NODE_MAX_KEYS = 3 };

// Internal node body
const uint32_t INTERNAL_NODE_POINTER_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_POINTER_OFFSET = 0;
const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_KEY_OFFSET =
    INTERNAL_NODE_POINTER_OFFSET + INTERNAL_NODE_POINTER_SIZE;
const uint32_t INTERNAL_NODE_PAIR_SIZE =
    INTERNAL_NODE_KEY_SIZE + INTERNAL_NODE_POINTER_SIZE;

// Leaf node header
const uint32_t LEAF_NODE_NEXT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NEXT_POINTER_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET =
    LEAF_NODE_NEXT_POINTER_OFFSET + LEAF_NODE_NEXT_POINTER_SIZE;
const uint16_t LEAF_NODE_FREE_START_SIZE = sizeof(uint16_t);
const uint32_t LEAF_NODE_FREE_START_OFFSET =
    LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;
const uint16_t LEAF_NODE_FREE_END_SIZE = sizeof(uint16_t);
const uint32_t LEAF_NODE_FREE_END_OFFSET =
    LEAF_NODE_FREE_START_OFFSET + LEAF_NODE_FREE_START_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE =
    COMMON_NODE_HEADER_SIZE + LEAF_NODE_NEXT_POINTER_SIZE +
    LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_FREE_START_SIZE +
    LEAF_NODE_FREE_END_SIZE;

const uint32_t LEAF_NODE_SIZE = PAGE_SIZE;
// Leaf node body
const uint16_t LEAF_NODE_SLOT_SIZE = sizeof(uint16_t);
const uint32_t LEAF_NODE_CELL_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_CELL_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_CELL_VALUE_SIZE = RECORD_SIZE;
const uint32_t LEAF_NODE_CELL_VALUE_OFFSET =
    LEAF_NODE_CELL_KEY_OFFSET + LEAF_NODE_CELL_KEY_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS =
    LEAF_NODE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE =
    LEAF_NODE_CELL_KEY_SIZE + LEAF_NODE_CELL_VALUE_SIZE;
enum { LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE };
const uint32_t LEAF_NODE_MIN_CELLS = LEAF_NODE_MAX_CELLS / 2;
const uint32_t INTERNAL_NODE_MIN_KEYS = INTERNAL_NODE_MAX_KEYS / 2;

/* Returns a pointer to the node root flag field. */
uint8_t *node_is_root_value(void *node) { return node + NODE_IS_ROOT_OFFSET; }

/* Returns a pointer to the node type field. */
uint8_t *node_type_value(void *node) { return node + NODE_TYPE_OFFSET; }

/* Returns a pointer to the parent page number field. */
uint32_t *node_parent(void *node) { return node + NODE_PARENT_POINTER_OFFSET; }

/* Returns a pointer to the number-of-keys field of an internal node. */
uint32_t *internal_node_num_keys(void *node) {
  return node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}

/* Returns a pointer to the rightmost child pointer field of an internal node.
 */
uint32_t *internal_node_rightmost_pointer(void *node) {
  return node + INTERNAL_NODE_RIGHTMOST_POINTER_OFFSET;
}

/* Returns a pointer to the separator key at key_num in an internal node. */
uint32_t *internal_node_key(void *node, uint32_t key_num) {
  return node + INTERNAL_NODE_HEADER_SIZE + key_num * INTERNAL_NODE_PAIR_SIZE +
         INTERNAL_NODE_KEY_OFFSET;
}

/* Returns a pointer to the child pointer at key_num in an internal node. */
uint32_t *internal_node_pointer(void *node, uint32_t key_num) {
  return node + INTERNAL_NODE_HEADER_SIZE + key_num * INTERNAL_NODE_PAIR_SIZE +
         INTERNAL_NODE_POINTER_OFFSET;
}

/* Returns a pointer to the number-of-cells field of a leaf node. */
uint32_t *leaf_node_num_cells(void *node) {
  return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

/* Returns a pointer to the first free byte in the slot directory region. */
uint16_t *leaf_node_free_start(void *node) {
  return node + LEAF_NODE_FREE_START_OFFSET;
}

/* Returns a pointer to the first free byte in the cell payload region. */
uint16_t *leaf_node_free_end(void *node) {
  return node + LEAF_NODE_FREE_END_OFFSET;
}

/* Returns a pointer to the next leaf page number in the leaf linked list. */
uint32_t *leaf_node_next_pointer(void *node) {
  return node + LEAF_NODE_NEXT_POINTER_OFFSET;
}

/* Returns a pointer to the slot directory entry for slot_num. */
uint16_t *leaf_node_offset_value(void *node, uint16_t slot_num) {
  return node + LEAF_NODE_HEADER_SIZE + slot_num * LEAF_NODE_SLOT_SIZE;
};

/* Reads and returns the key stored at a given leaf slot. */
uint32_t leaf_node_key_at_slot(void *node, uint32_t slot_num) {
  uint16_t cell_offset = *leaf_node_offset_value(node, (uint16_t)slot_num);
  uint32_t key;
  memcpy(&key, (uint8_t *)node + cell_offset, sizeof(key));
  return key;
}

// Opens or creates the WAL file.
WAL *new_wal(char *db_filename) {
  size_t len = strlen(db_filename) + 4 + 1;
  char *wal_filename = malloc(len);
  if (!wal_filename)
    return NULL;

  snprintf(wal_filename, len, "%s-wal", db_filename);

  int fd = open(wal_filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
  if (fd == -1) {
    printf("Unable to open WAL file\n");
    exit(EXIT_FAILURE);
  }

  off_t file_length = lseek(fd, 0, SEEK_END);

  WAL *wal = (WAL *)malloc(sizeof(WAL));
  wal->fd = fd;
  wal->filename = wal_filename;
  wal->file_length = file_length;
  wal->curr_txid = 0;

  return wal;
}

void write_serialized_wal_entry(WALEntry *source, void *destination) {
  memcpy(destination + WAL_TYPE_OFFSET, &(source->type), WAL_TYPE_SIZE);
  memcpy(destination + WAL_LSN_OFFSET, &(source->lsn), WAL_LSN_SIZE);
  memcpy(destination + WAL_TXID_OFFSET, &(source->txid), WAL_TXID_SIZE);
  memcpy(destination + WAL_PAGE_NUM_OFFSET, &(source->page_num),
         WAL_PAGE_NUM_SIZE);
  memcpy(destination + WAL_LENGTH_OFFSET, &(source->length), WAL_LENGTH_SIZE);
  memcpy(destination + WAL_CHECKSUM_OFFSET, &(source->checksum),
         WAL_CHECKSUM_SIZE);
}

void read_deserialized_wal_entry(void *source, WALEntry *destination) {
  memcpy(&(destination->type), source + WAL_TYPE_OFFSET, WAL_TYPE_SIZE);
  memcpy(&(destination->lsn), source + WAL_LSN_OFFSET, WAL_LSN_SIZE);
  memcpy(&(destination->txid), source + WAL_TXID_OFFSET, WAL_TXID_SIZE);
  memcpy(&(destination->page_num), source + WAL_PAGE_NUM_OFFSET,
         WAL_PAGE_NUM_SIZE);
  memcpy(&(destination->length), source + WAL_LENGTH_OFFSET, WAL_LENGTH_SIZE);
  memcpy(&(destination->checksum), source + WAL_CHECKSUM_OFFSET,
         WAL_CHECKSUM_SIZE);
}

/* Additive byte-sum over length bytes, used to detect torn/corrupt records. */
uint32_t wal_checksum(void *data, uint32_t length) {
  uint32_t sum = 0;
  uint8_t *bytes = (uint8_t *)data;
  for (uint32_t i = 0; i < length; i++) {
    sum += bytes[i];
  }
  return sum;
}

/* Appends ONE page-change record to the WAL. Returns the LSN the
 record was written at. */
off_t wal_append_page(WAL *wal, uint32_t txid, uint32_t page_num,
                      void *page_image) {
  WALEntry entry;
  entry.lsn = wal->file_length;
  entry.txid = txid;
  entry.type = PAGE_CHANGE;
  entry.length = PAGE_SIZE;
  entry.page_num = page_num;

  uint8_t *wal_record = malloc(WAL_COMMON_HEADER_SIZE);
  write_serialized_wal_entry(&entry, wal_record);

  uint32_t checksum = wal_checksum(wal_record + WAL_CHECKSUM_SIZE,
                                   WAL_COMMON_HEADER_SIZE - WAL_CHECKSUM_SIZE) +
                      wal_checksum(page_image, PAGE_SIZE);
  memcpy(wal_record + WAL_CHECKSUM_OFFSET, &checksum, WAL_CHECKSUM_SIZE);

  pwrite(wal->fd, wal_record, WAL_COMMON_HEADER_SIZE, wal->file_length);
  pwrite(wal->fd, page_image, PAGE_SIZE,
         wal->file_length + WAL_COMMON_HEADER_SIZE);

  off_t lsn = wal->file_length;
  wal->file_length += WAL_COMMON_HEADER_SIZE + PAGE_SIZE;

  free(wal_record);

  return lsn;
}

/* Writes the COMMIT record for txid and issues the single fsync that makes
 the whole transaction durable. */
void wal_commit(WAL *wal, uint32_t txid) {
  WALEntry entry;
  entry.lsn = wal->file_length;
  entry.txid = txid;
  entry.type = COMMIT;
  entry.length = 0;
  entry.page_num = -1;

  uint8_t *wal_record = malloc(WAL_COMMON_HEADER_SIZE);

  write_serialized_wal_entry(&entry, wal_record);

  /* Header-only record: checksum covers the header minus the checksum field. */
  uint32_t checksum = wal_checksum(wal_record + WAL_CHECKSUM_SIZE,
                                   WAL_COMMON_HEADER_SIZE - WAL_CHECKSUM_SIZE);
  memcpy(wal_record + WAL_CHECKSUM_OFFSET, &checksum, WAL_CHECKSUM_SIZE);

  pwrite(wal->fd, wal_record, WAL_COMMON_HEADER_SIZE, wal->file_length);
  fsync(wal->fd);

  free(wal_record);
  wal->file_length += WAL_COMMON_HEADER_SIZE;
}

void wal_clean(WAL *wal) {
  ftruncate(wal->fd, 0);
  lseek(wal->fd, 0, SEEK_SET);
  wal->file_length = 0;
}

void wal_recover(Pager *pager) {
  uint8_t header[WAL_COMMON_HEADER_SIZE];
  uint8_t payload[PAGE_SIZE];
  off_t wal_offset = 0;

  typedef struct {
    uint32_t page_num;
    uint8_t image[PAGE_SIZE];
  } BufferedPage;

  struct PagesBuffer {
    ssize_t size;
    ssize_t capacity;
    BufferedPage *bufferedPages;
  } PageBuffer;

  PageBuffer.size = 0;
  PageBuffer.capacity = 2;
  PageBuffer.bufferedPages = malloc(2 * sizeof(BufferedPage));

  while (1) {
    ssize_t n =
        pread(pager->wal->fd, header, WAL_COMMON_HEADER_SIZE, wal_offset);
    if (n < (ssize_t)WAL_COMMON_HEADER_SIZE) {
      break;
    }

    WALEntry entry = {0};
    read_deserialized_wal_entry(header, &entry);

    // Length must match the record type
    uint32_t expected_length;
    if (entry.type == PAGE_CHANGE) {
      expected_length = PAGE_SIZE;
    } else if (entry.type == COMMIT) {
      expected_length = 0;
    } else {
      break;
    }
    if (entry.length != expected_length) {
      break;
    }

    if (entry.length > 0) {
      n = pread(pager->wal->fd, payload, entry.length,
                wal_offset + WAL_COMMON_HEADER_SIZE);
      if (n < (ssize_t)entry.length) {
        break;
      }
    }

    // Recompute the checksum exactly as it was written
    uint32_t computed = wal_checksum(
        header + WAL_CHECKSUM_SIZE, WAL_COMMON_HEADER_SIZE - WAL_CHECKSUM_SIZE);
    if (entry.length > 0) {
      computed += wal_checksum(payload, entry.length);
    }
    if (computed != entry.checksum) {
      break;
    }

    if (entry.type == COMMIT) {
      for (int i = 0; i < PageBuffer.size; i++) {
        uint32_t page_num = PageBuffer.bufferedPages[i].page_num;
        void *image = PageBuffer.bufferedPages[i].image;
        pwrite(pager->fd, image, PAGE_SIZE, PAGE_SIZE * page_num);
      }
      PageBuffer.size = 0;
    } else {
      if (PageBuffer.size >= PageBuffer.capacity) {
        ssize_t new_capacity = PageBuffer.capacity * 2;
        void *temp = realloc(PageBuffer.bufferedPages,
                             new_capacity * sizeof(BufferedPage));

        if (temp == NULL) {
          printf("Error realocating buffered pages");
          exit(EXIT_FAILURE);
        }

        PageBuffer.bufferedPages = temp;
        PageBuffer.capacity = new_capacity;
      }
      PageBuffer.bufferedPages[PageBuffer.size].page_num = entry.page_num;
      memcpy(PageBuffer.bufferedPages[PageBuffer.size].image, payload,
             PAGE_SIZE);
      PageBuffer.size++;
    }

    wal_offset += WAL_COMMON_HEADER_SIZE + entry.length;
  }
  pager->file_length = lseek(pager->fd, 0, SEEK_END);
  pager->num_pages = pager->file_length / PAGE_SIZE;

  fsync(pager->fd);
  free(PageBuffer.bufferedPages);
  wal_clean(pager->wal);
}

/* Frees WAL resources. */
void wal_close(WAL *wal) {
  close(wal->fd);
  free(wal->filename);
  free(wal);
}

/* Opens or creates the database file and initializes pager metadata. */
Pager *new_pager(char *filename) {
  int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);

  if (fd == -1) {
    printf("Unable to open file\n");
    exit(EXIT_FAILURE);
  }

  off_t file_length = lseek(fd, 0, SEEK_END);

  Pager *pager = (Pager *)malloc(sizeof(Pager));
  pager->wal = new_wal(filename);
  pager->fd = fd;
  pager->filename = filename;
  pager->file_length = file_length;
  pager->num_pages = file_length / LEAF_NODE_SIZE;

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    pager->pages[i] = NULL;
    pager->dirty[i] = 0;
  }

  return pager;
}

/* Marks page page_num dirty so it will be logged at the next commit. */
void pager_mark_dirty(Pager *pager, uint32_t page_num) {
  if (page_num >= TABLE_MAX_PAGES) {
    printf("Trying to mark as dirty a page out of bounds: %d", page_num);
    return;
  }
  pager->dirty[page_num] = 1;
}

/* Writes a single in-memory page back to disk at its page-aligned offset. */
void flush_page(Pager *pager, uint32_t num_page) {
  if (lseek(pager->fd, num_page * PAGE_SIZE, SEEK_SET) == -1) {
    printf("Error seeking while flushing page %d\n", num_page);
    exit(EXIT_FAILURE);
  }

  ssize_t bytes_written = write(pager->fd, pager->pages[num_page], PAGE_SIZE);
  if (bytes_written == -1) {
    printf("Error writing page %d\n", num_page);
    exit(EXIT_FAILURE);
  }
}

/* Flushes loaded pages, releases pager resources, and closes the table. */
void close_db(Table *table) {
  Pager *pager = table->pager;

  for (uint32_t i = 0; i < table->pager->num_pages; i++) {
    if (pager->pages[i] == NULL)
      continue;

    flush_page(pager, i);
    free(pager->pages[i]);
    pager->pages[i] = NULL;
  }
  fsync(pager->fd);

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    void *page = pager->pages[i];
    if (page) {
      free(page);
      pager->pages[i] = NULL;
    }
  }

  wal_clean(pager->wal);
  wal_close(pager->wal);
  close(pager->fd);
  free(pager);
  free(table);
}

/* Returns a page from cache or loads it from disk into the pager cache. */
void *get_page(Pager *pager, uint32_t page_num) {
  if (page_num >= TABLE_MAX_PAGES) {
    printf("Maximum number of pages reached");
    exit(EXIT_FAILURE);
  }

  if (pager->pages[page_num] == NULL) {
    // Cache miss.
    void *page = malloc(PAGE_SIZE);
    uint32_t num_pages = pager->file_length / PAGE_SIZE;

    if (pager->file_length % PAGE_SIZE) {
      num_pages += 1;
    }

    if (page_num < num_pages) {
      lseek(pager->fd, page_num * PAGE_SIZE, SEEK_SET);
      ssize_t bytes_read = read(pager->fd, page, PAGE_SIZE);

      if (bytes_read == -1) {
        printf("Error reading file\n");
        exit(EXIT_FAILURE);
      }
    }
    pager->pages[page_num] = page;
  }

  return pager->pages[page_num];
}

/* Returns the address of the serialized record payload for the cursor slot. */
void *get_record_start(Cursor *cursor) {
  void *node = get_page(cursor->table->pager, cursor->page_num);

  return node + *leaf_node_offset_value(node, cursor->slot_num) +
         LEAF_NODE_CELL_KEY_SIZE;
}

/* Creates a cursor positioned at slot 0 of the current root page. */
Cursor *new_cursor_start(Table *table) {
  Cursor *new_cursor = (Cursor *)malloc(sizeof(Cursor));
  new_cursor->page_num = table->root_page_num;
  new_cursor->slot_num = 0;
  new_cursor->table = table;
  void *node = get_page(table->pager, new_cursor->page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);
  new_cursor->is_end_of_table = (num_cells == 0);

  return new_cursor;
}

/* Advances the cursor to the next slot, crossing leaf pages as needed. */
void advance_cursor(Cursor *cursor) {
  void *node = get_page(cursor->table->pager, cursor->page_num);
  cursor->slot_num += 1;

  if (cursor->slot_num >= *leaf_node_num_cells(node)) {
    if (*leaf_node_next_pointer(node) == 0) {
      cursor->is_end_of_table = 1;
      return;
    }
    cursor->page_num = *leaf_node_next_pointer(node);
    cursor->slot_num = 0;
  }

  return;
}

/* Allocates and initializes a reusable input buffer structure. */
InputBuffer *new_input_buffer() {
  InputBuffer *new_buffer = malloc(sizeof(InputBuffer));
  new_buffer->buffer = NULL;
  new_buffer->buffer_length = 0;
  new_buffer->input_length = 0;

  return new_buffer;
}

/* Initializes fields common to all node types. */
void initialize_node(void *node, NodeType node_type) {
  *node_is_root_value(node) = 0;
  *node_type_value(node) = node_type;
  *node_parent(node) = 0;
}

/* Initializes an internal node header. */
void initialize_internal_node(void *node) {
  initialize_node(node, NODE_TYPE_INTERNAL);
  *internal_node_num_keys(node) = 0;
}

/* Initializes a leaf node header and free-space boundaries. */
void initialize_leaf_node(void *node) {
  initialize_node(node, NODE_TYPE_LEAF);
  *leaf_node_next_pointer(node) = 0;
  *leaf_node_num_cells(node) = 0;
  *leaf_node_free_start(node) =
      LEAF_NODE_HEADER_SIZE + *leaf_node_num_cells(node) * LEAF_NODE_SLOT_SIZE;
  *leaf_node_free_end(node) = LEAF_NODE_SIZE;
}

/* Binary-searches a leaf node for key and returns insertion/search cursor. */
Cursor *leaf_node_offset_find(Table *table, uint32_t page_num, uint32_t key) {
  void *node = get_page(table->pager, page_num);
  Cursor *cursor = (Cursor *)malloc(sizeof(Cursor));
  cursor->page_num = page_num;
  cursor->table = table;

  uint32_t left = 0;
  uint32_t right = *leaf_node_num_cells(node);

  while (left < right) {
    uint32_t middle = left + (right - left) / 2;
    uint32_t curr_key = leaf_node_key_at_slot(node, middle);

    if (key == curr_key) {
      cursor->slot_num = middle;
      return cursor;
    }
    if (key < curr_key) {
      right = middle;
    } else {
      left = middle + 1;
    }
  }

  cursor->slot_num = left;
  return cursor;
}

/* Opens the database file and ensures the root page is initialized. */
Table *open_db(char *filename) {
  Pager *pager = new_pager(filename);

  wal_recover(pager);

  Table *new_table = (Table *)malloc(sizeof(Table));
  new_table->pager = pager;
  new_table->root_page_num = 0;

  if (pager->num_pages == 0) {
    void *root_node = get_page(pager, 0);
    initialize_leaf_node(root_node);
    *node_is_root_value(root_node) = 1;
    new_table->pager->num_pages = 1;
  }

  return new_table;
}

/* Prints the interactive shell prompt. */
void print_prompt() { printf("db > "); }

/* Frees the dynamic input buffer and its backing string memory. */
void free_input_buffer(InputBuffer *input_buffer) {
  free(input_buffer->buffer);
  free(input_buffer);
}

/* Handles dot-prefixed meta commands such as .exit. */
MetaCommandStatus execute_meta_command(InputBuffer *input_buffer,
                                       Table *table) {
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
    free_input_buffer(input_buffer);
    close_db(table);
    exit(EXIT_SUCCESS);
  }

  return META_COMMAND_UNRECOGNIZED_COMMAND;
}

/* Reads one line of user input into the input buffer. */
void read_input(InputBuffer *input_buffer) {
  ssize_t bytes_read =
      getline(&(input_buffer->buffer), &(input_buffer->input_length), stdin);

  if (bytes_read <= 0) {
    printf("Error reading input\n");
    exit(EXIT_FAILURE);
  };

  input_buffer->input_length = bytes_read - 1;
  input_buffer->buffer[bytes_read - 1] = 0;
}

/* Serializes an in-memory Record into its fixed-width on-page layout. */
void write_serialized_record(Record *source, void *destination) {
  memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
  memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
  memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

/* Deserializes a fixed-width on-page record into an in-memory Record. */
void read_deserialized_record(void *source, Record *destination) {
  memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
  memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
  memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

/* Inserts a new key+child into a parent internal node that has room. */
void internal_node_insert_key(Pager *pager, uint32_t parent_page_num,
                              uint32_t promoted_key, uint32_t left_child_page,
                              uint32_t right_child_page) {
  void *parent = get_page(pager, parent_page_num);
  uint32_t parent_num_keys = *internal_node_num_keys(parent);

  int32_t parent_slot = -1;
  for (uint32_t i = 0; i < parent_num_keys; i++) {
    if (*internal_node_pointer(parent, i) == left_child_page) {
      parent_slot = (int32_t)i;
      break;
    }
  }

  if (parent_slot == -1) {
    /* left_child_page is the rightmost pointer. */
    *internal_node_num_keys(parent) += 1;
    *internal_node_key(parent, parent_num_keys) = promoted_key;
    *internal_node_pointer(parent, parent_num_keys) = left_child_page;
    *internal_node_rightmost_pointer(parent) = right_child_page;
  } else {
    uint32_t old_sep = *internal_node_key(parent, parent_slot);
    *internal_node_key(parent, parent_slot) = promoted_key;

    *internal_node_num_keys(parent) += 1;
    for (uint32_t i = parent_num_keys; i > (uint32_t)(parent_slot + 1); i--) {
      *internal_node_key(parent, i) = *internal_node_key(parent, i - 1);
      *internal_node_pointer(parent, i) = *internal_node_pointer(parent, i - 1);
    }
    *internal_node_key(parent, parent_slot + 1) = old_sep;
    *internal_node_pointer(parent, parent_slot + 1) = right_child_page;
  }
}

/* Splits a full internal node, promotes the middle key, and redistributes
   keys/children. Handles both root and non-root nodes correctly. */
void internal_node_split(Pager *pager, uint32_t node_page_num, uint32_t new_key,
                         uint32_t new_left_child, uint32_t new_right_child) {
  void *node = get_page(pager, node_page_num);
  uint32_t old_num_keys = *internal_node_num_keys(node);
  uint32_t total_keys = old_num_keys + 1;
  uint8_t was_root = *node_is_root_value(node);
  uint32_t old_parent_page = *node_parent(node);

  uint32_t old_children[INTERNAL_NODE_MAX_KEYS + 1];
  for (uint32_t i = 0; i < old_num_keys; i++) {
    old_children[i] = *internal_node_pointer(node, i);
  }
  old_children[old_num_keys] = *internal_node_rightmost_pointer(node);

  uint32_t insert_pos = old_num_keys;
  for (uint32_t i = 0; i < old_num_keys; i++) {
    if (new_key < *internal_node_key(node, i)) {
      insert_pos = i;
      break;
    }
  }

  uint32_t all_keys[INTERNAL_NODE_MAX_KEYS + 1];
  for (uint32_t i = 0, j = 0; i < total_keys; i++) {
    if (i == insert_pos) {
      all_keys[i] = new_key;
    } else {
      all_keys[i] = *internal_node_key(node, j);
      j++;
    }
  }

  uint32_t all_children[INTERNAL_NODE_MAX_KEYS + 2];
  for (uint32_t i = 0, j = 0; i <= total_keys; i++) {
    if (i == insert_pos + 1) {
      all_children[i] = new_right_child;
    } else {
      all_children[i] = old_children[j];
      j++;
    }
  }

  uint32_t promote_index = total_keys / 2;
  uint32_t promoted_key = all_keys[promote_index];
  uint32_t left_num_keys = promote_index;
  uint32_t right_num_keys = total_keys - promote_index - 1;

  if (was_root) {
    /* Root split: create two new child pages, reinitialize original as root. */
    uint32_t left_page_num = pager->num_pages;
    pager->num_pages += 1;
    uint32_t right_page_num = pager->num_pages;
    pager->num_pages += 1;

    void *left_node = get_page(pager, left_page_num);
    void *right_node = get_page(pager, right_page_num);

    pager_mark_dirty(pager, left_page_num);
    pager_mark_dirty(pager, right_page_num);

    initialize_internal_node(left_node);
    initialize_internal_node(right_node);

    *internal_node_num_keys(left_node) = left_num_keys;
    for (uint32_t i = 0; i < left_num_keys; i++) {
      *internal_node_key(left_node, i) = all_keys[i];
      *internal_node_pointer(left_node, i) = all_children[i];
    }
    *internal_node_rightmost_pointer(left_node) = all_children[promote_index];

    *internal_node_num_keys(right_node) = right_num_keys;
    for (uint32_t i = 0; i < right_num_keys; i++) {
      *internal_node_key(right_node, i) = all_keys[promote_index + 1 + i];
      *internal_node_pointer(right_node, i) =
          all_children[promote_index + 1 + i];
    }
    *internal_node_rightmost_pointer(right_node) = all_children[total_keys];

    *node_parent(left_node) = node_page_num;
    *node_parent(right_node) = node_page_num;
    pager_mark_dirty(pager, node_page_num);

    for (uint32_t i = 0; i <= left_num_keys; i++) {
      uint32_t child_page = (i < left_num_keys)
                                ? *internal_node_pointer(left_node, i)
                                : *internal_node_rightmost_pointer(left_node);
      *node_parent(get_page(pager, child_page)) = left_page_num;
    }
    for (uint32_t i = 0; i <= right_num_keys; i++) {
      uint32_t child_page = (i < right_num_keys)
                                ? *internal_node_pointer(right_node, i)
                                : *internal_node_rightmost_pointer(right_node);
      *node_parent(get_page(pager, child_page)) = right_page_num;
    }

    initialize_internal_node(node);
    *node_is_root_value(node) = 1;
    *internal_node_num_keys(node) = 1;
    *internal_node_key(node, 0) = promoted_key;
    *internal_node_pointer(node, 0) = left_page_num;
    *internal_node_rightmost_pointer(node) = right_page_num;
  } else {
    /* Non-root split: keep original page as left, create new right page,
       and propagate the promoted key up to the parent. */
    uint32_t right_page_num = pager->num_pages;
    pager->num_pages += 1;
    void *right_node = get_page(pager, right_page_num);
    pager_mark_dirty(pager, right_page_num);
    initialize_internal_node(right_node);

    /* Rebuild the original node as the left half. */
    initialize_internal_node(node);
    *node_parent(node) = old_parent_page;
    *internal_node_num_keys(node) = left_num_keys;
    for (uint32_t i = 0; i < left_num_keys; i++) {
      *internal_node_key(node, i) = all_keys[i];
      *internal_node_pointer(node, i) = all_children[i];
    }
    *internal_node_rightmost_pointer(node) = all_children[promote_index];

    /* Fill the right half. */
    *internal_node_num_keys(right_node) = right_num_keys;
    for (uint32_t i = 0; i < right_num_keys; i++) {
      *internal_node_key(right_node, i) = all_keys[promote_index + 1 + i];
      *internal_node_pointer(right_node, i) =
          all_children[promote_index + 1 + i];
    }
    *internal_node_rightmost_pointer(right_node) = all_children[total_keys];

    /* Update children's parent pointers. */
    for (uint32_t i = 0; i <= left_num_keys; i++) {
      uint32_t child_page = (i < left_num_keys)
                                ? *internal_node_pointer(node, i)
                                : *internal_node_rightmost_pointer(node);
      *node_parent(get_page(pager, child_page)) = node_page_num;
    }
    *node_parent(right_node) = old_parent_page;
    for (uint32_t i = 0; i <= right_num_keys; i++) {
      uint32_t child_page = (i < right_num_keys)
                                ? *internal_node_pointer(right_node, i)
                                : *internal_node_rightmost_pointer(right_node);
      *node_parent(get_page(pager, child_page)) = right_page_num;
    }

    /* Insert promoted key into parent. */
    void *parent = get_page(pager, old_parent_page);
    if (*internal_node_num_keys(parent) >= INTERNAL_NODE_MAX_KEYS) {
      internal_node_split(pager, old_parent_page, promoted_key, node_page_num,
                          right_page_num);
    } else {
      internal_node_insert_key(pager, old_parent_page, promoted_key,
                               node_page_num, right_page_num);
    }
  }
}

/* Inserts a key/value pair into a leaf node at the cursor position. */
void leaf_node_insert(Cursor *cursor, uint32_t key, Record *record);

/* Splits a full leaf node, redistributes cells, and updates parent links. */
void leaf_node_split(Cursor *cursor, void *node, uint32_t key, Record *record) {
  uint32_t old_num_cells = *leaf_node_num_cells(node);
  uint32_t total_cells = old_num_cells + 1;
  uint32_t insertion_slot = cursor->slot_num;
  const uint32_t SPLIT_POINT = (total_cells + 1) / 2;
  uint8_t was_root = *node_is_root_value(node);
  uint32_t old_parent_page_num = *node_parent(node);

  uint32_t all_keys[LEAF_NODE_MAX_CELLS + 1];
  Record all_records[LEAF_NODE_MAX_CELLS + 1];

  for (uint32_t i = 0, j = 0; i < total_cells; i++) {
    if (i == insertion_slot) {
      all_keys[i] = key;
      all_records[i] = *record;
      continue;
    }

    all_keys[i] = leaf_node_key_at_slot(node, j);
    uint16_t cell_offset = *leaf_node_offset_value(node, (uint16_t)j);
    read_deserialized_record((uint8_t *)node + cell_offset +
                                 LEAF_NODE_CELL_KEY_SIZE,
                             &all_records[i]);
    j++;
  }

  Pager *pager = cursor->table->pager;
  uint32_t left_page_num;
  uint32_t right_page_num;
  void *left_page;
  void *right_page;

  if (!was_root) {
    left_page_num = cursor->page_num;
    right_page_num = pager->num_pages;
    pager->num_pages += 1;

    left_page = node;
  } else {
    left_page_num = pager->num_pages;
    right_page_num = pager->num_pages + 1;
    pager->num_pages += 2;

    left_page = get_page(pager, left_page_num);
  }
  uint32_t old_next = *leaf_node_next_pointer(node);
  right_page = get_page(pager, right_page_num);
  initialize_leaf_node(left_page);
  initialize_leaf_node(right_page);

  Cursor left_cursor = {
      .page_num = left_page_num,
      .slot_num = 0,
      .table = cursor->table,
      .is_end_of_table = 0,
  };

  Cursor right_cursor = {
      .page_num = right_page_num,
      .slot_num = 0,
      .table = cursor->table,
      .is_end_of_table = 0,
  };

  for (uint32_t i = 0; i < total_cells; i++) {
    if (i < SPLIT_POINT) {
      left_cursor.slot_num = *leaf_node_num_cells(left_page);
      leaf_node_insert(&left_cursor, all_keys[i], &all_records[i]);
    } else {
      right_cursor.slot_num = *leaf_node_num_cells(right_page);
      leaf_node_insert(&right_cursor, all_keys[i], &all_records[i]);
    }
  }

  *leaf_node_next_pointer(left_page) = right_page_num;
  *leaf_node_next_pointer(right_page) = old_next;

  pager_mark_dirty(pager, left_page_num);
  pager_mark_dirty(pager, right_page_num);

  if (!was_root) {
    *node_parent(left_page) = old_parent_page_num;
    *node_parent(right_page) = old_parent_page_num;

    void *parent_node = get_page(pager, old_parent_page_num);
    uint32_t separator_key =
        leaf_node_key_at_slot(left_page, *leaf_node_num_cells(left_page) - 1);

    pager_mark_dirty(pager, old_parent_page_num);

    if (*internal_node_num_keys(parent_node) >= INTERNAL_NODE_MAX_KEYS) {
      internal_node_split(pager, old_parent_page_num, separator_key,
                          left_page_num, right_page_num);
      return;
    }

    internal_node_insert_key(pager, old_parent_page_num, separator_key,
                             left_page_num, right_page_num);
  } else {
    uint32_t root_page_num = cursor->page_num;
    *node_parent(left_page) = root_page_num;
    *node_parent(right_page) = root_page_num;

    initialize_internal_node(node);
    *node_is_root_value(node) = 1;
    *internal_node_num_keys(node) = 1;
    *internal_node_pointer(node, 0) = left_page_num;
    *internal_node_rightmost_pointer(node) = right_page_num;
    *internal_node_key(node, 0) =
        leaf_node_key_at_slot(left_page, *leaf_node_num_cells(left_page) - 1);

    pager_mark_dirty(pager, root_page_num);
  }
}

/* Inserts a new cell into a leaf, splitting first when free space is exhausted.
 */
void leaf_node_insert(Cursor *cursor, uint32_t key, Record *record) {
  void *node = get_page(cursor->table->pager, cursor->page_num);

  uint32_t free_start = *leaf_node_free_start(node);
  uint32_t free_end = *leaf_node_free_end(node);
  uint32_t required_space = LEAF_NODE_CELL_SIZE + LEAF_NODE_SLOT_SIZE;

  if (free_end < free_start || (free_end - free_start) < required_space) {
    leaf_node_split(cursor, node, key, record);
    return;
  }

  uint32_t num_cells = *leaf_node_num_cells(node);
  uint32_t slot_index = cursor->slot_num;

  uint32_t insertion_point = *leaf_node_free_end(node) - LEAF_NODE_CELL_SIZE;

  for (uint32_t i = num_cells; i > slot_index; i--) {
    *leaf_node_offset_value(node, (uint16_t)i) =
        *leaf_node_offset_value(node, (uint16_t)(i - 1));
  }
  *(leaf_node_offset_value(node, slot_index)) = insertion_point;

  *(leaf_node_num_cells(node)) += 1;
  *(leaf_node_free_start(node)) += LEAF_NODE_SLOT_SIZE;
  *(leaf_node_free_end(node)) -= LEAF_NODE_CELL_SIZE;

  uint8_t *cell = (uint8_t *)node + insertion_point;
  memcpy(cell, &key, LEAF_NODE_CELL_KEY_SIZE);
  write_serialized_record(record, cell + LEAF_NODE_CELL_KEY_SIZE);

  pager_mark_dirty(cursor->table->pager, cursor->page_num);
}

/* Reads and prints the record currently addressed by the cursor, emitting only
 * the projected columns in order. A count of 0 prints every column. */
void print_row(Record record, const ColumnId *projection,
               size_t projection_count) {
  if (projection_count == 0) {
    printf("(%d, %s, %s)\n", record.id, record.username, record.email);
    return;
  }

  printf("(");
  for (size_t i = 0; i < projection_count; i++) {
    if (i > 0)
      printf(", ");
    switch (projection[i]) {
    case COLUMN_ID:
      printf("%d", record.id);
      break;
    case COLUMN_USERNAME:
      printf("%s", record.username);
      break;
    case COLUMN_EMAIL:
      printf("%s", record.email);
      break;
    }
  }
  printf(")\n");
}

/* Traverses the B+ tree to locate a key. Returns a Cursor pointing to the
   key's position (or insertion point if absent). Sets *key_exists to 1 if the
   key was found, 0 otherwise. Caller is responsible for freeing the cursor. */
Cursor *find_key_cursor(Table *table, uint32_t key, int *key_exists) {
  Cursor *cursor = new_cursor_start(table);
  void *node = get_page(table->pager, cursor->page_num);

  while (*node_type_value(node) != NODE_TYPE_LEAF) {
    uint32_t left = 0;
    uint32_t right = *internal_node_num_keys(node);

    while (left < right) {
      uint32_t middle_index = left + (right - left) / 2;
      uint32_t middle_key = *internal_node_key(node, middle_index);

      if (key <= middle_key) {
        right = middle_index;
      } else {
        left = middle_index + 1;
      }
    }

    if (left == *internal_node_num_keys(node)) {
      cursor->page_num = *internal_node_rightmost_pointer(node);
    } else {
      cursor->page_num = *internal_node_pointer(node, left);
    }

    node = get_page(table->pager, cursor->page_num);
  }

  uint32_t leaf_page_num = cursor->page_num;
  free(cursor);

  Cursor *leaf_cursor = leaf_node_offset_find(table, leaf_page_num, key);
  uint32_t num_cells = *leaf_node_num_cells(node);
  *key_exists = (leaf_cursor->slot_num < num_cells &&
                 leaf_node_key_at_slot(node, leaf_cursor->slot_num) == key);

  return leaf_cursor;
}

void flush_to_wal(Pager *pager) {
  for (int i = 0; i < TABLE_MAX_PAGES; i++) {
    if (pager->dirty[i] != 0) {
      wal_append_page(pager->wal, pager->wal->curr_txid, i, pager->pages[i]);
      pager->dirty[i] = 0;
    }
  }
  wal_commit(pager->wal, pager->wal->curr_txid);
  pager->wal->curr_txid++;
}

/* Inserts a record into the B-tree, redistributing nodes and updating parent
 keys as needed.*/
ExecuteStatus execute_insert(Statement *statement, Table *table) {
  Record record = statement->record_to_insert;

  int key_exists;
  Cursor *cursor = find_key_cursor(table, record.id, &key_exists);
  if (key_exists) {
    free(cursor);
    return EXECUTE_DUPLICATE_KEY;
  }

  leaf_node_insert(cursor, record.id, &record);

  flush_to_wal(table->pager);

  free(cursor);
  return EXECUTE_SUCCESS;
}

uint8_t row_matches(Expr *where_expr, Record record) {
  switch (where_expr->kind) {
  case COMPARISON:
    switch (where_expr->col) {
    case COLUMN_ID:
      return record.id == where_expr->intval;
    case COLUMN_EMAIL:
      return strcmp(record.email, where_expr->strval) == 0;
    case COLUMN_USERNAME:
      return strcmp(record.username, where_expr->strval) == 0;
    }
    return 0;
  case AND_EXPR:
    return row_matches(where_expr->left, record) &&
           row_matches(where_expr->right, record);
  case OR_EXPR:
    return row_matches(where_expr->left, record) ||
           row_matches(where_expr->right, record);
  }
  return 0;
}

// Recursively frees a WHERE expression tree
void free_expr(Expr *expr) {
  if (expr == NULL)
    return;
  free_expr(expr->left);
  free_expr(expr->right);
  if (expr->kind == COMPARISON)
    free(expr->strval);
  free(expr);
}

/* Print all records in the table in ascending key order. */
void execute_select(Statement *statement, Table *table) {
  Cursor *cursor = new_cursor_start(table);

  void *node = get_page(table->pager, cursor->page_num);

  while (*node_type_value(node) != NODE_TYPE_LEAF) {
    uint32_t child_page_num = *internal_node_pointer(node, 0);
    node = get_page(table->pager, child_page_num);
    cursor->page_num = child_page_num;
  }

  while (!(cursor->is_end_of_table)) {
    Record record;
    read_deserialized_record(get_record_start(cursor), &record);
    if (!statement->has_where || row_matches(statement->where_expr, record))
      print_row(record, statement->projection, statement->projection_count);
    advance_cursor(cursor);
  }

  if (statement->has_where)
    free_expr(statement->where_expr);
  free(cursor);
}

/* Reads all cells from a leaf node into parallel key/record arrays.
   Returns the number of cells read. */
uint32_t leaf_node_read_all_cells(void *node, uint32_t *keys_out,
                                  Record *records_out) {
  uint32_t num_cells = *leaf_node_num_cells(node);
  for (uint32_t i = 0; i < num_cells; i++) {
    keys_out[i] = leaf_node_key_at_slot(node, i);
    uint16_t cell_offset = *leaf_node_offset_value(node, (uint16_t)i);
    read_deserialized_record((uint8_t *)node + cell_offset +
                                 LEAF_NODE_CELL_KEY_SIZE,
                             &records_out[i]);
  }
  return num_cells;
}

/* Rebuilds a leaf node from scratch using the given key/record arrays.
   Preserves is_root and parent_ptr. Sets next_pointer to next_ptr.
   This reclaims fragmented cell-payload space left by simple deletes. */
void leaf_node_rebuild(void *node, uint32_t *keys, Record *records,
                       uint32_t count, uint32_t next_ptr) {
  uint8_t is_root = *node_is_root_value(node);
  uint32_t parent_page = *node_parent(node);

  initialize_leaf_node(node);
  *node_is_root_value(node) = is_root;
  *node_parent(node) = parent_page;
  *leaf_node_next_pointer(node) = next_ptr;

  for (uint32_t i = 0; i < count; i++) {
    uint32_t insertion_point = *leaf_node_free_end(node) - LEAF_NODE_CELL_SIZE;
    *leaf_node_offset_value(node, (uint16_t)i) = (uint16_t)insertion_point;

    uint8_t *cell = (uint8_t *)node + insertion_point;
    memcpy(cell, &keys[i], LEAF_NODE_CELL_KEY_SIZE);
    write_serialized_record(&records[i], cell + LEAF_NODE_CELL_KEY_SIZE);

    *leaf_node_num_cells(node) += 1;
    *leaf_node_free_start(node) += LEAF_NODE_SLOT_SIZE;
    *leaf_node_free_end(node) -= LEAF_NODE_CELL_SIZE;
  }
}

/* Finds the index of child_page among the children of an internal node.
   Returns 0..num_keys-1 for pointer[i], or num_keys for the rightmost
   pointer. Returns -1 if not found (should not happen in a valid tree). */
int32_t find_child_index_in_parent(void *parent, uint32_t child_page) {
  uint32_t num_keys = *internal_node_num_keys(parent);
  for (uint32_t i = 0; i < num_keys; i++) {
    if (*internal_node_pointer(parent, i) == child_page)
      return (int32_t)i;
  }
  if (*internal_node_rightmost_pointer(parent) == child_page)
    return (int32_t)num_keys;
  return -1;
}

/* Returns the page number of the i-th child of an internal node.
   child_index = 0..num_keys-1 returns pointer[i];
   child_index = num_keys returns the rightmost pointer. */
uint32_t internal_node_child(void *node, uint32_t child_index) {
  uint32_t num_keys = *internal_node_num_keys(node);
  if (child_index < num_keys)
    return *internal_node_pointer(node, child_index);
  return *internal_node_rightmost_pointer(node);
}

/* Removes the separator key at key_index and the child pointer to its right
   from an internal node. Shifts remaining keys/pointers to fill the gap.

   After removal the node has (old_num_keys - 1) keys and correspondingly
   one fewer child. The caller must check for underflow separately. */
void internal_node_remove_key(void *node, uint32_t key_index) {
  uint32_t num_keys = *internal_node_num_keys(node);

  if (key_index == num_keys - 1) {
    /* Removing the last key: the right child of this key is the rightmost
       pointer. Replace rightmost with pointer[num_keys-1] (the left child
       of the removed key). */
    *internal_node_rightmost_pointer(node) =
        *internal_node_pointer(node, num_keys - 1);
  } else {
    /* Shift keys left to fill the gap at key_index. */
    for (uint32_t i = key_index; i < num_keys - 1; i++) {
      *internal_node_key(node, i) = *internal_node_key(node, i + 1);
    }
    /* Shift child pointers left: we are removing pointer[key_index + 1]
       (the right child of the deleted separator). */
    for (uint32_t i = key_index + 1; i < num_keys - 1; i++) {
      *internal_node_pointer(node, i) = *internal_node_pointer(node, i + 1);
    }
  }

  *internal_node_num_keys(node) = num_keys - 1;
}

/* When the root internal node has 0 keys and only a single child (the
   rightmost pointer), copy that child into page 0 to shrink the tree
   by one level. Updates grandchildren's parent pointers if needed. */
void collapse_root(Table *table) {
  void *root = get_page(table->pager, table->root_page_num);

  if (*node_type_value(root) != NODE_TYPE_INTERNAL)
    return;
  if (*internal_node_num_keys(root) > 0)
    return;

  uint32_t child_page_num = *internal_node_rightmost_pointer(root);
  void *child = get_page(table->pager, child_page_num);

  memcpy(root, child, PAGE_SIZE);
  *node_is_root_value(root) = 1;
  *node_parent(root) = 0;
  pager_mark_dirty(table->pager, table->root_page_num);

  /* If the promoted child was internal, fix its children's parent ptrs. */
  if (*node_type_value(root) == NODE_TYPE_INTERNAL) {
    uint32_t num_keys = *internal_node_num_keys(root);
    for (uint32_t i = 0; i < num_keys; i++) {
      void *grandchild =
          get_page(table->pager, *internal_node_pointer(root, i));
      *node_parent(grandchild) = table->root_page_num;
      pager_mark_dirty(table->pager, *internal_node_pointer(root, i));
    }
    void *last = get_page(table->pager, *internal_node_rightmost_pointer(root));
    *node_parent(last) = table->root_page_num;
    pager_mark_dirty(table->pager, *internal_node_rightmost_pointer(root));
  }
}

/*
  Redistribute cells between a leaf node and one of its siblings.
  Called when a leaf drops below LEAF_NODE_MIN_CELLS but a sibling has
  more cells than LEAF_NODE_MIN_CELLS (so it can spare one).
*/
void leaf_node_redistribute(Pager *pager, uint32_t node_page_num,
                            uint32_t sibling_page_num, uint32_t separator_index,
                            int sibling_is_left) {
  void *node = get_page(pager, node_page_num);
  void *sibling = get_page(pager, sibling_page_num);

  uint32_t node_keys[LEAF_NODE_MAX_CELLS + 1];
  Record node_records[LEAF_NODE_MAX_CELLS + 1];
  uint32_t node_n = leaf_node_read_all_cells(node, node_keys, node_records);

  uint32_t sib_keys[LEAF_NODE_MAX_CELLS];
  Record sib_records[LEAF_NODE_MAX_CELLS];
  uint32_t sib_n = leaf_node_read_all_cells(sibling, sib_keys, sib_records);

  /* Move one cell from the sibling to the node. */
  if (sibling_is_left) {
    memmove(node_keys + 1, node_keys, node_n * sizeof(uint32_t));
    memmove(node_records + 1, node_records, node_n * sizeof(Record));
    node_keys[0] = sib_keys[sib_n - 1];
    node_records[0] = sib_records[sib_n - 1];
  } else {
    node_keys[node_n] = sib_keys[0];
    node_records[node_n] = sib_records[0];
    memmove(sib_keys, sib_keys + 1, sib_n * sizeof(uint32_t));
    memmove(sib_records, sib_records + 1, sib_n * sizeof(Record));
  }
  ++node_n;
  --sib_n;

  /* Rebuild both pages from the modified arrays, redistribution does
     not change the leaf linked-list order. */
  uint32_t node_next = *leaf_node_next_pointer(node);
  uint32_t sib_next = *leaf_node_next_pointer(sibling);
  leaf_node_rebuild(node, node_keys, node_records, node_n, node_next);
  leaf_node_rebuild(sibling, sib_keys, sib_records, sib_n, sib_next);

  /* Update the parent's separator key. */
  void *parent = get_page(pager, *node_parent(node));

  void *left_child = sibling_is_left ? sibling : node;
  uint32_t left_num_cells = *leaf_node_num_cells(left_child);

  *internal_node_key(parent, separator_index) =
      leaf_node_key_at_slot(left_child, left_num_cells - 1);

  pager_mark_dirty(pager, node_page_num);
  pager_mark_dirty(pager, sibling_page_num);
  pager_mark_dirty(pager, *node_parent(node));
}

/* Merge the RIGHT leaf into the LEFT leaf */
void leaf_node_merge(Pager *pager, uint32_t left_page_num,
                     uint32_t right_page_num, uint32_t separator_index) {
  void *left_node = get_page(pager, left_page_num);
  void *right_node = get_page(pager, right_page_num);

  uint32_t left_node_keys[LEAF_NODE_MAX_CELLS + 1];
  Record left_node_records[LEAF_NODE_MAX_CELLS + 1];
  uint32_t left_n =
      leaf_node_read_all_cells(left_node, left_node_keys, left_node_records);

  uint32_t right_node_keys[LEAF_NODE_MAX_CELLS + 1];
  Record right_node_records[LEAF_NODE_MAX_CELLS + 1];
  uint32_t right_n =
      leaf_node_read_all_cells(right_node, right_node_keys, right_node_records);

  uint32_t all_keys[LEAF_NODE_MAX_CELLS + 1];
  Record all_records[LEAF_NODE_MAX_CELLS + 1];

  for (uint32_t i = 0; i < left_n + right_n; ++i) {
    if (i < left_n) {
      all_keys[i] = left_node_keys[i];
      all_records[i] = left_node_records[i];
    } else {
      all_keys[i] = right_node_keys[i - left_n];
      all_records[i] = right_node_records[i - left_n];
    }
  }

  uint32_t next_ptr = *leaf_node_next_pointer(right_node);
  leaf_node_rebuild(left_node, all_keys, all_records, left_n + right_n,
                    next_ptr);

  uint32_t parent_page_num = *node_parent(left_node);
  void *parent = get_page(pager, parent_page_num);
  internal_node_remove_key(parent, separator_index);

  pager_mark_dirty(pager, left_page_num);
  pager_mark_dirty(pager, parent_page_num);

  /* Evict the now-orphaned right page from the pager cache. Clear its dirty
     flag first so flush_to_wal won't log a freed page. */
  pager->dirty[right_page_num] = 0;
  pager->pages[right_page_num] = NULL;
  free(right_node);
}

/* Number of children in an internal node is always num_keys + 1. */
uint32_t internal_node_num_children(void *node) {
  return *internal_node_num_keys(node) + 1;
}

/* Redistribution for internal-node underflow. */
void internal_node_redistribute(Pager *pager, uint32_t node_page_num,
                                uint32_t sibling_page_num,
                                uint32_t separator_index, int sibling_is_left) {
  void *node = get_page(pager, node_page_num);
  void *sibling = get_page(pager, sibling_page_num);

  uint32_t parent_page_num = *node_parent(node);
  void *parent = get_page(pager, parent_page_num);

  uint32_t node_num_keys = *internal_node_num_keys(node);
  uint32_t sibling_num_keys = *internal_node_num_keys(sibling);

  if (sibling_is_left) {
    memmove((uint8_t *)node + INTERNAL_NODE_HEADER_SIZE +
                INTERNAL_NODE_PAIR_SIZE,
            (uint8_t *)node + INTERNAL_NODE_HEADER_SIZE,
            node_num_keys * INTERNAL_NODE_PAIR_SIZE);

    uint32_t moved_child = *internal_node_rightmost_pointer(sibling);
    *internal_node_pointer(node, 0) = moved_child;

    // Bring parent's separator key down into node as node's new first key.
    *internal_node_key(node, 0) = *internal_node_key(parent, separator_index);

    // Move sibling's last key up to parent[separator_index].
    *internal_node_key(parent, separator_index) =
        *internal_node_key(sibling, sibling_num_keys - 1);

    // Move sibling's rightmost child to become node's new first child.
    *internal_node_rightmost_pointer(sibling) =
        *internal_node_pointer(sibling, sibling_num_keys - 1);

    *node_parent(get_page(pager, moved_child)) = node_page_num;
    pager_mark_dirty(pager, moved_child);
  } else {
    uint32_t moved_child = internal_node_child(sibling, 0);
    uint32_t old_rightmost_child = *internal_node_rightmost_pointer(node);

    // Bring parent's separator key down into node as node's new last key.
    *internal_node_key(node, node_num_keys) =
        *internal_node_key(parent, separator_index);

    *internal_node_pointer(node, node_num_keys) = old_rightmost_child;

    // Move sibling's leftmost child to become node's new rightmost child.
    *internal_node_rightmost_pointer(node) = moved_child;

    // Move sibling's first key up to parent[separator_index].
    *internal_node_key(parent, separator_index) =
        *internal_node_key(sibling, 0);

    memmove((uint8_t *)sibling + INTERNAL_NODE_HEADER_SIZE,
            (uint8_t *)sibling + INTERNAL_NODE_HEADER_SIZE +
                INTERNAL_NODE_PAIR_SIZE,
            (sibling_num_keys - 1) * INTERNAL_NODE_PAIR_SIZE);

    *node_parent(get_page(pager, moved_child)) = node_page_num;
    pager_mark_dirty(pager, moved_child);
  }

  *internal_node_num_keys(node) = node_num_keys + 1;
  *internal_node_num_keys(sibling) = sibling_num_keys - 1;

  pager_mark_dirty(pager, node_page_num);
  pager_mark_dirty(pager, sibling_page_num);
  pager_mark_dirty(pager, parent_page_num);
}

/* Implement merge for internal-node underflow. */
void internal_node_merge(Pager *pager, uint32_t left_page_num,
                         uint32_t right_page_num, uint32_t separator_index) {
  void *left_node = get_page(pager, left_page_num);
  void *right_node = get_page(pager, right_page_num);

  uint32_t parent_node_num = *node_parent(left_node);
  void *parent = get_page(pager, parent_node_num);

  uint32_t left_node_num_keys = *internal_node_num_keys(left_node);
  uint32_t right_node_num_keys = *internal_node_num_keys(right_node);

  /* Place bridge key at pair[L] and left's old rightmost as its left child. */
  *internal_node_key(left_node, left_node_num_keys) =
      *internal_node_key(parent, separator_index);
  *internal_node_pointer(left_node, left_node_num_keys) =
      *internal_node_rightmost_pointer(left_node);

  /* Append right's keys and children starting at pair[L+1]. */
  for (uint32_t i = 0; i < right_node_num_keys; ++i) {
    *internal_node_key(left_node, left_node_num_keys + 1 + i) =
        *internal_node_key(right_node, i);

    uint32_t node_num_to_append = *internal_node_pointer(right_node, i);
    *internal_node_pointer(left_node, left_node_num_keys + 1 + i) =
        node_num_to_append;

    void *node_to_append = get_page(pager, node_num_to_append);
    *node_parent(node_to_append) = left_page_num;
    pager_mark_dirty(pager, node_num_to_append);
  }

  /* Transfer right's rightmost child to left, update its parent pointer. */
  uint32_t right_rightmost = *internal_node_rightmost_pointer(right_node);
  *internal_node_rightmost_pointer(left_node) = right_rightmost;
  *node_parent(get_page(pager, right_rightmost)) = left_page_num;
  pager_mark_dirty(pager, right_rightmost);

  /* Update left's key count: bridge key + all of right's keys. */
  *internal_node_num_keys(left_node) =
      left_node_num_keys + 1 + right_node_num_keys;

  internal_node_remove_key(parent, separator_index);

  pager_mark_dirty(pager, left_page_num);
  pager_mark_dirty(pager, parent_node_num);

  /* Evict the right page from the pager cache without a double-free. Clear its
     dirty flag first so flush_to_wal won't log a freed page. */
  pager->dirty[right_page_num] = 0;
  pager->pages[right_page_num] = NULL;
  free(right_node);
}

void handle_internal_node_underflow(Pager *pager, uint32_t page_num,
                                    void *parent, int32_t child_idx,
                                    uint32_t num_parent_keys) {
  int has_left = (child_idx > 0);
  int has_right = ((uint32_t)child_idx < num_parent_keys);

  uint32_t left_sib_page = 0, right_sib_page = 0;
  uint32_t left_sep = 0, right_sep = 0;

  if (has_left) {
    left_sib_page = internal_node_child(parent, child_idx - 1);
    left_sep = (uint32_t)child_idx - 1;
  }
  if (has_right) {
    right_sib_page = internal_node_child(parent, child_idx + 1);
    right_sep = (uint32_t)child_idx;
  }

  if (has_left && *internal_node_num_keys(get_page(pager, left_sib_page)) >
                      INTERNAL_NODE_MIN_KEYS) {
    internal_node_redistribute(pager, page_num, left_sib_page, left_sep, 1);
    return;
  }
  if (has_right && *internal_node_num_keys(get_page(pager, right_sib_page)) >
                       INTERNAL_NODE_MIN_KEYS) {
    internal_node_redistribute(pager, page_num, right_sib_page, right_sep, 0);
    return;
  }

  if (has_left) {
    internal_node_merge(pager, left_sib_page, page_num, left_sep);
  } else {
    internal_node_merge(pager, page_num, right_sib_page, right_sep);
  }
}

/* Handle underflow after a deletion causes a node to drop below
   minimum occupancy. */
void handle_underflow(Pager *pager, Table *table, uint32_t page_num) {
  void *node = get_page(pager, page_num);

  if (*node_is_root_value(node)) {
    if (*node_type_value(node) == NODE_TYPE_LEAF)
      return;
    else
      collapse_root(table);
    return;
  }

  uint32_t parent_page = *node_parent(node);
  void *parent = get_page(pager, parent_page);
  int32_t child_idx = find_child_index_in_parent(parent, page_num);
  uint32_t num_parent_keys = *internal_node_num_keys(parent);

  /* Identify siblings. Not every node has both. */
  int has_left = (child_idx > 0);
  int has_right = ((uint32_t)child_idx < num_parent_keys);

  uint32_t left_sib_page = 0, right_sib_page = 0;
  uint32_t left_sep = 0, right_sep = 0;

  if (has_left) {
    left_sib_page = internal_node_child(parent, child_idx - 1);
    left_sep = child_idx - 1;
  }
  if (has_right) {
    right_sib_page = internal_node_child(parent, child_idx + 1);
    right_sep = child_idx;
  }

  if (*node_type_value(node) == NODE_TYPE_LEAF) {
    /* Try redistribute first: pick a sibling above minimum. */
    if (has_left && *leaf_node_num_cells(get_page(pager, left_sib_page)) >
                        LEAF_NODE_MIN_CELLS) {
      leaf_node_redistribute(pager, page_num, left_sib_page, left_sep, 1);
      return;
    }
    if (has_right && *leaf_node_num_cells(get_page(pager, right_sib_page)) >
                         LEAF_NODE_MIN_CELLS) {
      leaf_node_redistribute(pager, page_num, right_sib_page, right_sep, 0);
      return;
    }

    /* No sibling can spare — merge. Convention: left receives, right is
       discarded. Decide which role the underflowed node plays. */
    if (has_left) {
      leaf_node_merge(pager, left_sib_page, page_num, left_sep);
    } else {
      leaf_node_merge(pager, page_num, right_sib_page, right_sep);
    }
  } else if (*node_type_value(node) == NODE_TYPE_INTERNAL) {
    handle_internal_node_underflow(pager, page_num, parent, child_idx,
                                   num_parent_keys);
  }

  /* After a merge the parent lost a key — check for parent underflow. */
  uint32_t parent_keys = *internal_node_num_keys(parent);
  if (*node_is_root_value(parent) && parent_keys == 0)
    collapse_root(table);
  else if (!*node_is_root_value(parent) && parent_keys < INTERNAL_NODE_MIN_KEYS)
    handle_underflow(pager, table, parent_page);
}

void execute_delete(Statement *statement, Table *table) {
  uint32_t key = statement->id_to_delete;

  int key_exists;
  Cursor *cursor = find_key_cursor(table, key, &key_exists);
  if (!key_exists) {
    printf("Key %d does not exist\n", key);
    free(cursor);
    return;
  }

  void *node = get_page(table->pager, cursor->page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);
  uint32_t deleted_page = cursor->page_num;

  /* Remove the slot entry by shifting later slots left. */
  for (uint32_t i = cursor->slot_num + 1; i < num_cells; ++i) {
    *leaf_node_offset_value(node, i - 1) = *leaf_node_offset_value(node, i);
  }
  *leaf_node_num_cells(node) -= 1;
  *leaf_node_free_start(node) -= LEAF_NODE_SLOT_SIZE;

  pager_mark_dirty(table->pager, deleted_page);

  /* Check for underflow: non-root leaf dropped below minimum occupancy. */
  if (!*node_is_root_value(node) &&
      *leaf_node_num_cells(node) < LEAF_NODE_MIN_CELLS) {
    handle_underflow(table->pager, table, deleted_page);
  }

  flush_to_wal(table->pager);

  free(cursor);
}

/* Dispatches a prepared statement to its corresponding executor. */
ExecuteStatus execute_statement(Statement *statement, Table *table) {
  if (statement->statement_type == STATEMENT_INSERT) {
    return execute_insert(statement, table);
  }
  if (statement->statement_type == STATEMENT_SELECT) {
    execute_select(statement, table);
  }
  if (statement->statement_type == STATEMENT_DELETE) {
    execute_delete(statement, table);
  }

  return EXECUTE_SUCCESS;
}

/* Maps a column-name identifier to its ColumnId. Returns 1 on success. */
int resolve_column(Token token, ColumnId *out) {
  if (token.len_lexeme == 2 && strncmp(token.start_lexeme, "id", 2) == 0)
    *out = COLUMN_ID;
  else if (token.len_lexeme == 4 && strncmp(token.start_lexeme, "name", 4) == 0)
    *out = COLUMN_USERNAME;
  else if (token.len_lexeme == 5 &&
           strncmp(token.start_lexeme, "email", 5) == 0)
    *out = COLUMN_EMAIL;
  else
    return 0;
  return 1;
}

/* A value (in an INSERT) is any bare word: an identifier or an integer. The
 * unquoted grammar can't tell "alice" the name from a column name lexically, so
 * the parser accepts either in value position. */
int is_value_token(TokenType type) {
  return type == TOKEN_IDENTIFIER || type == TOKEN_INT_LITERAL;
}

Expr *parse_comparison(Token *tokens, uint32_t *pos) {
  ColumnId where_col;
  if (!resolve_column(tokens[*pos], &where_col))
    return NULL;
  (*pos)++;

  if (tokens[*pos].type != TOKEN_OP_EQUAL)
    return NULL;
  (*pos)++;

  if (!is_value_token(tokens[*pos].type))
    return NULL;

  Expr *expr = malloc(sizeof(Expr));
  expr->kind = COMPARISON;
  expr->left = NULL;
  expr->right = NULL;
  expr->strval = NULL;
  expr->col = where_col;
  expr->operator = TOKEN_OP_EQUAL;

  if (tokens[*pos].type == TOKEN_INT_LITERAL) {
    /* Only the integer column may be compared against an int literal. */
    if (where_col != COLUMN_ID) {
      free_expr(expr);
      return NULL;
    }
    expr->intval = (uint32_t)strtol(tokens[*pos].start_lexeme, NULL, 10);
  } else {
    /* String literals only match the string columns. */
    if (where_col == COLUMN_ID) {
      free_expr(expr);
      return NULL;
    }
    expr->strval = malloc(sizeof(char) * (tokens[*pos].len_lexeme + 1));
    memcpy(expr->strval, tokens[*pos].start_lexeme, tokens[*pos].len_lexeme);
    expr->strval[tokens[*pos].len_lexeme] = '\0';
  }
  (*pos)++;
  return expr;
}

Expr *parse_and(Token *tokens, uint32_t *pos) {
  Expr *left = parse_comparison(tokens, pos);
  if (left == NULL)
    return NULL;

  while (tokens[*pos].type == TOKEN_KW_AND) {
    (*pos)++; /* consume AND */
    Expr *right = parse_comparison(tokens, pos);
    if (right == NULL) {
      free_expr(left);
      return NULL;
    }
    Expr *node = malloc(sizeof(Expr));
    node->kind = AND_EXPR;
    node->left = left;
    node->right = right;
    node->strval = NULL;
    left = node;
  }

  return left;
}

Expr *parse_or(Token *tokens, uint32_t *pos) { return parse_and(tokens, pos); }

Expr *parse_expr(Token *tokens, uint32_t *pos) { return parse_or(tokens, pos); }

PrepareStatus parse_statement(Token *tokens, Statement *statement) {
  switch (tokens[0].type) {
  case TOKEN_KW_SELECT: {
    statement->statement_type = STATEMENT_SELECT;
    statement->projection_count = 0;
    statement->has_where = 0;

    /* Bare SELECT projects every column. */
    if (tokens[1].type == TOKEN_EOF)
      return PREPARE_SUCCESS;
    if (tokens[1].type == TOKEN_OP_ALL) {
      if (tokens[2].type == TOKEN_KW_WHERE) {
        uint32_t wpos = 3; /* first token after WHERE */
        Expr *where_expr = parse_expr(tokens, &wpos);
        if (where_expr == NULL || tokens[wpos].type != TOKEN_EOF) {
          free_expr(where_expr);
          return PREPARE_FAILURE;
        }
        statement->has_where = 1;
        statement->where_expr = where_expr;
        return PREPARE_SUCCESS;
      } else if (tokens[2].type == TOKEN_EOF)
        return PREPARE_SUCCESS;
    }

    /* Otherwise parse a comma-separated list of column identifiers. */
    size_t pos = 1;
    while (1) {
      if (tokens[pos].type != TOKEN_IDENTIFIER ||
          statement->projection_count >= MAX_SELECT_COLUMNS)
        return PREPARE_FAILURE;

      ColumnId col;
      if (!resolve_column(tokens[pos], &col))
        return PREPARE_FAILURE;
      statement->projection[statement->projection_count++] = col;
      pos++;

      if (tokens[pos].type == TOKEN_EOF)
        return PREPARE_SUCCESS;
      if (tokens[pos].type == TOKEN_KW_WHERE) {
        uint32_t wpos = pos + 1; /* first token after WHERE */
        Expr *where_expr = parse_expr(tokens, &wpos);
        if (where_expr == NULL || tokens[wpos].type != TOKEN_EOF) {
          free_expr(where_expr);
          return PREPARE_FAILURE;
        }
        statement->has_where = 1;
        statement->where_expr = where_expr;
        return PREPARE_SUCCESS;
      }
      if (tokens[pos].type != TOKEN_COMMA)
        return PREPARE_FAILURE;
      pos++; /* consume comma; another column must follow */
    }
  }

  case TOKEN_KW_INSERT: {
    if (tokens[1].type != TOKEN_INT_LITERAL ||
        !is_value_token(tokens[2].type) || !is_value_token(tokens[3].type) ||
        tokens[4].type != TOKEN_EOF) {
      printf("Incorrect arguments for insert\n");
      return PREPARE_FAILURE;
    }

    Record *record = &statement->record_to_insert;
    /* Need room for a trailing '\0', so len must be strictly less than the
     * field capacity. */
    if (tokens[2].len_lexeme >= sizeof(record->username) ||
        tokens[3].len_lexeme >= sizeof(record->email)) {
      printf("Incorrect arguments for insert\n");
      return PREPARE_FAILURE;
    }

    statement->statement_type = STATEMENT_INSERT;
    record->id = (uint32_t)strtol(tokens[1].start_lexeme, NULL, 10);
    memcpy(record->username, tokens[2].start_lexeme, tokens[2].len_lexeme);
    record->username[tokens[2].len_lexeme] = '\0';
    memcpy(record->email, tokens[3].start_lexeme, tokens[3].len_lexeme);
    record->email[tokens[3].len_lexeme] = '\0';
    return PREPARE_SUCCESS;
  }

  case TOKEN_KW_DELETE:
    if (tokens[1].type != TOKEN_INT_LITERAL || tokens[2].type != TOKEN_EOF) {
      printf("Incorrect arguments for delete\n");
      return PREPARE_FAILURE;
    }
    statement->statement_type = STATEMENT_DELETE;
    statement->id_to_delete =
        (uint32_t)strtol(tokens[1].start_lexeme, NULL, 10);
    return PREPARE_SUCCESS;

  default:
    return PREPARE_UNRECOGNIZED_COMMAND;
  }
}

// Classifies a word into a token:
Token classify_word(const char *start, size_t len) {
  if (len == 6 && strncmp(start, "select", 6) == 0)
    return (Token){TOKEN_KW_SELECT, (char *)start, len};
  if (len == 6 && strncmp(start, "insert", 6) == 0)
    return (Token){TOKEN_KW_INSERT, (char *)start, len};
  if (len == 6 && strncmp(start, "delete", 6) == 0)
    return (Token){TOKEN_KW_DELETE, (char *)start, len};
  if (len == 5 && strncmp(start, "where", 5) == 0)
    return (Token){TOKEN_KW_WHERE, (char *)start, len};
  if (len == 3 && strncmp(start, "and", 3) == 0)
    return (Token){TOKEN_KW_AND, (char *)start, len};
  if (len == 2 && strncmp(start, "or", 2) == 0)
    return (Token){TOKEN_KW_OR, (char *)start, len};

  for (size_t i = 0; i < len; i++) {
    if (!isdigit((unsigned char)start[i]))
      return (Token){TOKEN_IDENTIFIER, (char *)start, len};
  }
  return (Token){TOKEN_INT_LITERAL, (char *)start, len};
}

// Tokenizes one input line.
Token *lexer(const char *line) {
  size_t line_len = strlen(line);

  /* Upper bound: at most one token per char, plus the EOF terminator. */
  Token *tokens = malloc(sizeof(Token) * (line_len + 1));
  if (tokens == NULL) {
    printf("Unable to allocate tokens\n");
    exit(EXIT_FAILURE);
  }

  size_t curr_token = 0;
  size_t start = 0;
  for (size_t i = 0; i <= line_len; i++) {
    char c = line[i]; /* line[line_len] is the terminating '\0' */
    int at_end = (i == line_len);
    int is_punct = (c == ',' || c == '=' || c == '*');

    /* A word token ends at whitespace, at single-char punctuation, or at the
     * terminating '\0'. Punctuation is then emitted as its own token. */
    if (at_end || isspace((unsigned char)c) || is_punct) {
      if (i > start)
        tokens[curr_token++] = classify_word(line + start, i - start);
      if (is_punct) {
        TokenType type;
        switch (c) {
        case ',':
          type = TOKEN_COMMA;
          break;
        case '=':
          type = TOKEN_OP_EQUAL;
          break;
        case '*':
          type = TOKEN_OP_ALL;
          break;
        }
        tokens[curr_token++] = (Token){type, (char *)(line + i), 1};
      }
      start = i + 1;
    }
  }

  tokens[curr_token] = (Token){TOKEN_EOF, NULL, 0};
  return tokens;
}

/* Parses user input into an executable SQL-like statement structure. */
PrepareStatus prepare_statement(InputBuffer *input_buffer,
                                Statement *statement) {
  Token *tokens = lexer(input_buffer->buffer);
  PrepareStatus status = parse_statement(tokens, statement);

  free(tokens);
  return status;
}

/* Program entry point: opens DB, runs REPL loop, and executes statements. */
int main(int argc, char *argv[]) {
  if (argc < 2) {
    printf("You must supply a db file!\n");
    exit(EXIT_FAILURE);
  }

  InputBuffer *input_buffer = new_input_buffer();

  char *filename = argv[1];
  Table *table = open_db(filename);

  while (1) {
    print_prompt();
    read_input(input_buffer);

    if (input_buffer->buffer[0] == '.') {
      switch (execute_meta_command(input_buffer, table)) {
      case META_COMMAND_SUCCESS:
        break;

      case META_COMMAND_UNRECOGNIZED_COMMAND:
        printf("Unrecognized command\n");
        continue;
        break;
      }
    }

    Statement statement;
    switch (prepare_statement(input_buffer, &statement)) {
    case PREPARE_SUCCESS:
      break;

    case PREPARE_FAILURE:
      continue;
      break;

    case PREPARE_UNRECOGNIZED_COMMAND:
      continue;
      break;
    }

    switch (execute_statement(&statement, table)) {
    case EXECUTE_SUCCESS:
      printf("Executed\n");
      break;

    case EXECUTE_DUPLICATE_KEY:
      printf("Error: Duplicate key.\n");
      break;
    }
  }
}