#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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
  int id;
  char username[32];
  char email[255];
} Record;

typedef struct {
  StatementType statement_type;
  Record record_to_insert;
} Statement;

typedef struct {
  Record *records;
  size_t records_length;
} Table;

Table *new_table() {
  Table *new_table = malloc(sizeof(Table));
  new_table->records = NULL;
  new_table->records_length = 0;

  return new_table;
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

void execute_insert(Statement *statement, Table *table) {
  size_t new_length = table->records_length + 1;
  table->records = malloc(new_length * sizeof(Record));

  table->records[table->records_length].id = statement->record_to_insert.id;
  strncpy(table->records[table->records_length].email,
          statement->record_to_insert.email,
          sizeof(table->records[table->records_length].email) - 1);
  table->records[table->records_length]
      .email[sizeof(table->records[table->records_length].email) - 1] = '\0';

  strncpy(table->records[table->records_length].username,
          statement->record_to_insert.username,
          sizeof(table->records[table->records_length].username) - 1);
  table->records[table->records_length]
      .username[sizeof(table->records[table->records_length].username) - 1] =
      '\0';

  table->records_length = new_length;
}

void execute_select(Table *table) {
  for (int i = 0; i < table->records_length; i++) {
    Record record = table->records[i];
    printf("%d | %d %s %s\n", i + 1, record.id, record.username, record.email);
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