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
  char *filename;
  int fd;
} Pager;

typedef struct {
  Pager *pager;
  size_t records_length;
} Table;

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

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
    pager->pages[i] = NULL;

  return pager;
}

Table *open_db(char *filename) {
  Pager *pager = new_pager(filename);

  Table *new_table = (Table *)malloc(sizeof(Table));
  new_table->records_length = pager->file_length / RECORD_SIZE;
  new_table->pager = pager;

  return new_table;
}

void flush_page(Pager *pager, uint32_t num_page, size_t size) {
  if (lseek(pager->fd, num_page * PAGE_SIZE, SEEK_SET) == -1) {
    printf("Error seeking while flushing page %d\n", num_page);
    exit(EXIT_FAILURE);
  }

  ssize_t bytes_written = write(pager->fd, pager->pages[num_page], size);
  if (bytes_written == -1) {
    printf("Error writing page %d\n", num_page);
    exit(EXIT_FAILURE);
  }
}

void close_db(Table *table) {
  Pager *pager = table->pager;

  uint32_t num_full_pages = table->records_length / RECORDS_PER_PAGE;

  for (uint32_t i = 0; i < num_full_pages; i++) {
    if (pager->pages[i] == NULL)
      continue;

    flush_page(pager, i, PAGE_SIZE);
    free(pager->pages[i]);
    pager->pages[i] = NULL;
  }

  uint32_t num_additional_records = table->records_length % RECORDS_PER_PAGE;
  if (num_additional_records > 0) {
    uint32_t page_num = num_full_pages;

    if (pager->pages[page_num] != NULL) {
      uint32_t size = num_additional_records * RECORD_SIZE;
      flush_page(pager, page_num, size);
      free(pager->pages[page_num]);
      pager->pages[page_num] = NULL;
    }
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
  if (page_num > TABLE_MAX_PAGES) {
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

void *get_record_start(Pager *pager, uint32_t record_num) {
  uint32_t page_num = record_num / RECORDS_PER_PAGE;

  void *page = get_page(pager, page_num);

  uint32_t record_offset = record_num % RECORDS_PER_PAGE;
  uint32_t byte_offset = RECORD_SIZE * record_offset;

  return page + byte_offset;
}

InputBuffer *new_input_buffer() {
  InputBuffer *new_buffer = malloc(sizeof(InputBuffer));
  new_buffer->buffer = NULL;
  new_buffer->buffer_length = 0;
  new_buffer->input_length = 0;

  return new_buffer;
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
  memcpy(destination + 0, &(source->id), ID_SIZE);
  memcpy(destination + 32, &(source->username), USERNAME_SIZE);
  memcpy(destination + 64, &(source->email), EMAIL_SIZE);
}

void read_deserialized_record(void *source, Record *destination) {
  memcpy(&(destination->id), source + 0, ID_SIZE);
  memcpy(&(destination->username), source + 32, USERNAME_SIZE);
  memcpy(&(destination->email), source + 64, EMAIL_SIZE);
}

void execute_insert(Statement *statement, Table *table) {
  void *slot = get_record_start(table->pager, table->records_length);
  write_serialized_record(&(statement->record_to_insert), slot);
  table->records_length += 1;
}

void execute_select(Table *table) {
  Record record;
  for (size_t i = 0; i < table->records_length; i++) {
    read_deserialized_record(get_record_start(table->pager, i), &record);
    printf("(%d, %s, %s)\n", record.id, record.username, record.email);
  }
}

void execute_statement(Statement *statement, Table *table) {
  if (statement->statement_type == STATEMENT_INSERT) {
    execute_insert(statement, table);
  }
  if (statement->statement_type == STATEMENT_SELECT) {
    execute_select(table);
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