#include "pager.h"
#include "btree.h"
#include "common.h"
#include "lexer.h"
#include "parser.h"
#include <_string.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

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

  free(table->schema->column_types);
  free(table->schema->column_names);
  free(table->schema);
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

/* TEMPORARY  */
static Schema *build_users_schema(void) {
  Schema *schema = malloc(sizeof(Schema));
  schema->num_columns = 3;
  schema->pk_column = 0;

  schema->column_types = malloc(sizeof(ColumnType) * schema->num_columns);
  schema->column_types[0] = INT;
  schema->column_types[1] = TEXT;
  schema->column_types[2] = TEXT;

  schema->column_names = malloc(sizeof(char *) * schema->num_columns);
  schema->column_names[0] = "id";
  schema->column_names[1] = "name";
  schema->column_names[2] = "email";

  return schema;
}

void load_catalog(Database *database, Pager *pager) {
  Schema *schema = malloc(sizeof(Schema));
  schema->num_columns = 4;
  schema->pk_column = UINT32_MAX;

  schema->column_types = malloc(sizeof(ColumnType) * schema->num_columns);
  schema->column_types[0] = INT;
  schema->column_types[1] = TEXT;
  schema->column_types[2] = TEXT;
  schema->column_types[3] = INT;

  schema->column_names = malloc(sizeof(char *) * schema->num_columns);
  schema->column_names[0] = "type";
  schema->column_names[1] = "name";
  schema->column_names[2] = "create_statement";
  schema->column_names[3] = "root_page_num";

  database->num_tables = 1;
  Table *catalog_table = malloc(sizeof(Table));
  catalog_table->table_name = strndup("catalog", 7);
  catalog_table->pager = pager;
  catalog_table->schema = schema;
  catalog_table->root_page_num = 0;
  catalog_table->rowid_counter =
      pager->num_pages > 0 ? get_rightmost_rowid(catalog_table) : 0;
  database->tables[0] = catalog_table;
}

/* Opens the database file and ensures the root page is initialized. */
Database *open_db(char *filename) {
  Pager *pager = new_pager(filename); /* Global pager */

  wal_recover(pager);

  Database *database = (Database *)malloc(sizeof(Database));
  load_catalog(database, pager);

  if (pager->num_pages == 0) {
    void *catalog_node = get_page(pager, 0);
    initialize_leaf_node(catalog_node);
    *node_is_root_value(catalog_node) = 1;
    pager->num_pages = 1;
  } else {
    /* read catalog */
    void *catalog_node = get_page(pager, 0);
    uint32_t num_cells = *leaf_node_num_cells(catalog_node);
    uint32_t node_keys[num_cells];
    Record node_records[num_cells];
    Schema *schema = database->tables[0]->schema;
    leaf_node_read_all_cells(catalog_node, node_keys, node_records, schema);

    for (int i = 0; i < num_cells; i++) {
      database->num_tables++;
      database->tables[i + 1] = malloc(sizeof(Table));
      Statement statement;
      Token *tokens = lexer(node_records[i].values[2].text_val.str);
      parse_statement(tokens, &statement);
      database->tables[i + 1]->pager = pager;
      database->tables[i + 1]->schema = malloc(sizeof(Schema));
      memcpy(database->tables[i + 1]->schema, &(statement.schema),
             sizeof(Schema));
      database->tables[i + 1]->table_name =
          strndup(node_records[i].values[1].text_val.str,
                  node_records[i].values[1].text_val.len);
      database->tables[i + 1]->root_page_num =
          node_records[i].values[3].int_val;
      database->tables[i + 1]->rowid_counter =
          get_rightmost_rowid(database->tables[i + 1]);
    }
  }

  return database;
}
