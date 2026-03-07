#include <fcntl.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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

typedef enum {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandStatus;

typedef enum { STATEMENT_INSERT, STATEMENT_SELECT } StatementType;

typedef struct {
  StatementType statement_type;
  Record record_to_insert;
} Statement;

typedef struct {
  void *pages[TABLE_MAX_PAGES];
  off_t file_length;
  uint32_t num_pages;
  char *filename;
  int fd;
} Pager;

typedef struct {
  Pager *pager;
  uint32_t root_page_num;
} Table;

typedef struct {
  uint32_t page_num;
  uint32_t cell_num;
  Table *table;
  int is_end_of_table;
} Cursor;

// Leaf node header
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_HEADER_SIZE = LEAF_NODE_NUM_CELLS_SIZE;

// Leaf node body
const uint16_t LEAF_NODE_OFFSET_SIZE = sizeof(uint16_t);
const uint32_t LEAF_NODE_SIZE = PAGE_SIZE;
const uint32_t LEAF_NODE_CELL_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_CELL_VALUE_SIZE = RECORD_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS =
    LEAF_NODE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE =
    LEAF_NODE_CELL_KEY_SIZE + LEAF_NODE_CELL_VALUE_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS =
    LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

uint32_t *leaf_node_num_cells(void *node) { return node; }

void *leaf_node_cell(void *node, uint32_t cell_num) {
  return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

uint32_t *leaf_node_cell_key(void *node, uint32_t cell_num) {
  return leaf_node_cell(node, cell_num);
}

void *leaf_node_cell_value(void *node, uint32_t cell_num) {
  return leaf_node_cell(node, cell_num) + LEAF_NODE_CELL_KEY_SIZE;
}

uint16_t *leaf_node_offset_value(void *node, uint16_t offset_num) {
  return node + LEAF_NODE_HEADER_SIZE + offset_num * LEAF_NODE_OFFSET_SIZE;
};

typedef struct {
  Pager *pager;
} BTree;

Pager *new_pager(char *filename) {
  int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);

  if (fd == -1) {
    printf("Unable to open file\n");
    exit(EXIT_FAILURE);
  }

  off_t file_length = lseek(fd, 0, SEEK_END);

  Pager *pager = (Pager *)malloc(sizeof(Pager));
  pager->fd = fd;
  pager->filename = filename;
  pager->file_length = file_length;
  pager->num_pages = file_length / LEAF_NODE_SIZE;

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
    pager->pages[i] = NULL;

  return pager;
}

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

void close_db(Table *table) {
  Pager *pager = table->pager;

  for (uint32_t i = 0; i < table->pager->num_pages; i++) {
    if (pager->pages[i] == NULL)
      continue;

    flush_page(pager, i);
    free(pager->pages[i]);
    pager->pages[i] = NULL;
  }

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    void *page = pager->pages[i];
    if (page) {
      free(page);
      pager->pages[i] = NULL;
    }
  }

  close(pager->fd);
  free(pager);
  free(table);
}

void free_table(Table *table) {
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
    free(table->pager->pages[i]);
  free(table);
}

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

void *get_record_start(Cursor *cursor) {
  void *node = get_page(cursor->table->pager, cursor->page_num);

  uint32_t cell_offset = LEAF_NODE_HEADER_SIZE +
                         cursor->cell_num * LEAF_NODE_CELL_SIZE +
                         LEAF_NODE_CELL_KEY_SIZE;

  return node + cell_offset;
}

Cursor *new_cursor_start(Table *table) {
  Cursor *new_cursor = (Cursor *)malloc(sizeof(Cursor));
  new_cursor->page_num = table->root_page_num;
  new_cursor->cell_num = 0;
  new_cursor->table = table;
  void *node = get_page(table->pager, new_cursor->page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);
  new_cursor->is_end_of_table = (num_cells == 0);

  return new_cursor;
}

Cursor *new_cursor_end(Table *table) {
  Cursor *new_cursor = (Cursor *)malloc(sizeof(Cursor));
  new_cursor->page_num = table->pager->num_pages - 1;
  void *node = get_page(table->pager, table->pager->num_pages - 1);
  new_cursor->cell_num = *leaf_node_num_cells(node);
  new_cursor->table = table;
  new_cursor->is_end_of_table = 1;

  return new_cursor;
}

void advance_cursor(Cursor *cursor) {
  cursor->cell_num += 1;

  if (cursor->cell_num >= LEAF_NODE_MAX_CELLS) {
    printf("Maximum insertions achieved\n");
    return;
  } else {
    if (cursor->cell_num == LEAF_NODE_MAX_CELLS - 1) {
      cursor->is_end_of_table = 1;
    }
  }
}

InputBuffer *new_input_buffer() {
  InputBuffer *new_buffer = malloc(sizeof(InputBuffer));
  new_buffer->buffer = NULL;
  new_buffer->buffer_length = 0;
  new_buffer->input_length = 0;

  return new_buffer;
}

void initialize_leaf_node(void *node) { *leaf_node_num_cells(node) = 0; }

Cursor *leaf_node_find(Table *table, uint32_t page_num, uint32_t key) {
  void *node = get_page(table->pager, page_num);

  Cursor *cursor = (Cursor *)malloc(sizeof(Cursor));
  cursor->page_num = page_num;
  cursor->table = table;

  uint32_t left = 0;
  uint32_t right = *leaf_node_num_cells(node);

  while (left < right) {
    uint32_t middle = left + (right - left) / 2;
    uint32_t curr_key = *leaf_node_cell_key(node, middle);

    if (key == curr_key) {
      cursor->cell_num = middle;
      return cursor;
    }
    if (key < curr_key) {
      right = middle;
    } else {
      left = middle + 1;
    }
  }

  cursor->cell_num = left;
  return cursor;
}

Table *open_db(char *filename) {
  Pager *pager = new_pager(filename);

  Table *new_table = (Table *)malloc(sizeof(Table));
  new_table->pager = pager;
  new_table->root_page_num = 0;

  if (pager->num_pages == 0) {
    void *root_node = get_page(pager, 0);
    initialize_leaf_node(root_node);
  }

  new_table->pager->num_pages = 1;

  return new_table;
}

void print_prompt() { printf("db > "); }

void free_input_buffer(InputBuffer *input_buffer) {
  free(input_buffer->buffer);
  free(input_buffer);
}

MetaCommandStatus execute_meta_command(InputBuffer *input_buffer,
                                       Table *table) {
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
    free_input_buffer(input_buffer);
    close_db(table);
    exit(EXIT_SUCCESS);
  }

  return META_COMMAND_UNRECOGNIZED_COMMAND;
}

PrepareStatus prepare_statement(InputBuffer *input_buffer,
                                Statement *statement) {
  if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
    statement->statement_type = STATEMENT_INSERT;

    int args_assigned = sscanf(input_buffer->buffer, "insert %d %31s %254s",
                               &(statement->record_to_insert.id),
                               statement->record_to_insert.username,
                               statement->record_to_insert.email);

    if (args_assigned < 3) {
      printf("Incorrect arguments for insert\n");
      return PREPARE_FAILURE;
    }

    return PREPARE_SUCCESS;
  }
  if (strcmp(input_buffer->buffer, "select") == 0) {
    statement->statement_type = STATEMENT_SELECT;
    return PREPARE_SUCCESS;
  }

  return PREPARE_UNRECOGNIZED_COMMAND;
}

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

void write_serialized_record(Record *source, void *destination) {
  memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
  memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
  memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void read_deserialized_record(void *source, Record *destination) {
  memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
  memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
  memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

void leaf_node_insert(Cursor *cursor, uint32_t key, Record *record) {
  void *node = get_page(cursor->table->pager, cursor->page_num);

  uint32_t num_cells = *leaf_node_num_cells(node);
  uint32_t insertion_point = LEAF_NODE_MAX_CELLS - 1 - num_cells;

  *(leaf_node_num_cells(node)) += 1;
  uint8_t *node_bytes = (uint8_t *)node;
  uint8_t *cell_bytes = (uint8_t *)leaf_node_cell(node, insertion_point);

  *(leaf_node_offset_value(node, num_cells)) = cell_bytes - node_bytes;
  *(leaf_node_cell_key(node, insertion_point)) = key;
  write_serialized_record(record, leaf_node_cell_value(node, insertion_point));
}

void access_row(Cursor *cursor) {
  Record record;
  read_deserialized_record(get_record_start(cursor), &record);
  printf("(%d, %s, %s)\n", record.id, record.username, record.email);
}

void execute_insert(Statement *statement, Table *table) {
  Cursor *cursor = new_cursor_end(table);
  void *node = get_page(table->pager, table->pager->num_pages - 1);
  Record record = statement->record_to_insert;

  leaf_node_insert(cursor, record.id, &record);

  free(cursor);
}

void execute_select(Statement *statement, Table *table) {
  Cursor *cursor = new_cursor_start(table);

  while (!(cursor->is_end_of_table)) {
    access_row(cursor);
    advance_cursor(cursor);
  }

  free(cursor);
}

void execute_statement(Statement *statement, Table *table) {
  if (statement->statement_type == STATEMENT_INSERT) {
    execute_insert(statement, table);
  }
  if (statement->statement_type == STATEMENT_SELECT) {
    execute_select(statement, table);
  }
}

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

    execute_statement(&statement, table);
    printf("Executed\n");
  }
}