#include "pager.h"
#include "btree.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
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

/* Opens the database file and ensures the root page is initialized. */
Table *open_db(char *filename) {
  Pager *pager = new_pager(filename);

  wal_recover(pager);

  Table *new_table = (Table *)malloc(sizeof(Table));
  new_table->pager = pager;
  new_table->root_page_num = 0;
  new_table->schema = build_users_schema();

  if (pager->num_pages == 0) {
    void *root_node = get_page(pager, 0);
    initialize_leaf_node(root_node);
    *node_is_root_value(root_node) = 1;
    new_table->pager->num_pages = 1;
  }
  new_table->rowid_counter = get_rightmost_rowid(new_table) + 1;

  return new_table;
}
