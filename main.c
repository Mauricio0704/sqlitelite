#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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
  size_t records_length;
} Table;

Table *new_table() {
  Table *new_table = (Table *)malloc(sizeof(Table));
  new_table->records_length = 0;

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
    new_table->pages[i] = NULL;

  return new_table;
}

void free_table(Table *table) {
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
    free(table->pages[i]);
  free(table);
}

void *get_record_start(Table *table, uint32_t record_num) {
  uint32_t page_num = record_num / RECORDS_PER_PAGE;
  void *page = table->pages[page_num];

  if (page == NULL) {
    table->pages[page_num] = malloc(PAGE_SIZE);
    page = table->pages[page_num];
  }

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

MetaCommandStatus execute_meta_command(InputBuffer *input_buffer) {
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
    printf("Closing database\n");
    free_input_buffer(input_buffer);
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
  memcpy(destination + 0, &(source->id), 32);
  memcpy(destination + 32, &(source->username), USERNAME_SIZE);
  memcpy(destination + 64, &(source->email), EMAIL_SIZE);
}

void read_deserialized_record(void *source, Record *destination) {
  memcpy(&(destination->id), source + 0, 32);
  memcpy(&(destination->username), source + 32, USERNAME_SIZE);
  memcpy(&(destination->email), source + 64, EMAIL_SIZE);
}

void execute_insert(Statement *statement, Table *table) {
  void *slot = get_record_start(table, table->records_length);

  void *serialized_record;
  write_serialized_record(&(statement->record_to_insert), slot);
  table->records_length += 1;
}

void execute_select(Table *table) {
  Record record;
  for (int i = 0; i < table->records_length; i++) {
      read_deserialized_record(get_record_start(table, i),  &record);
      printf("(%d, %s, %s)\n", record.id, record.username, record.email);
  }
}

void execute_statement(Statement *statement, Table *table) {
  if (statement->statement_type == STATEMENT_INSERT) {
    printf("Doing an insert...\n");
    execute_insert(statement, table);
    printf("Data inserted correctly\n");
  }
  if (statement->statement_type == STATEMENT_SELECT) {
    printf("Doing a select...\n");
    execute_select(table);
  }
}

int main(int argc, char *argv[]) {
  InputBuffer *input_buffer = new_input_buffer();
  Table *table = new_table();

  while (1) {
    print_prompt();
    read_input(input_buffer);

    if (input_buffer->buffer[0] == '.') {
      switch (execute_meta_command(input_buffer)) {
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