#include "common.h"
#include "executor.h"
#include "pager.h"
#include "parser.h"
#include <ctype.h>
#include <fcntl.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

typedef struct {
  char *buffer;
  size_t input_length;
  ssize_t buffer_length;
} InputBuffer;

/* Allocates and initializes a reusable input buffer structure. */
InputBuffer *new_input_buffer() {
  InputBuffer *new_buffer = malloc(sizeof(InputBuffer));
  new_buffer->buffer = NULL;
  new_buffer->buffer_length = 0;
  new_buffer->input_length = 0;

  return new_buffer;
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

/* Program entry point: opens DB, runs REPL loop, and executes statements. */
int main(int argc, char *argv[]) {
  if (argc < 2) {
    printf("You must supply a db file!\n");
    exit(EXIT_FAILURE);
  }

  InputBuffer *input_buffer = new_input_buffer();

  char *filename = argv[1];
  Database *database = open_db(filename);

  while (1) {
    print_prompt();
    read_input(input_buffer);

    if (input_buffer->buffer[0] == '.') {
      switch (execute_meta_command(input_buffer, database->tables[0])) {
      case META_COMMAND_SUCCESS:
        break;

      case META_COMMAND_UNRECOGNIZED_COMMAND:
        printf("Unrecognized command\n");
        continue;
        break;
      }
    }

    Statement statement;
    switch (prepare_statement(input_buffer->buffer, &statement)) {
    case PREPARE_SUCCESS:
      break;

    case PREPARE_FAILURE:
      printf("Failure in statement\n");
      continue;
      break;

    case PREPARE_UNRECOGNIZED_COMMAND:
      printf("Unrecognized command in statement \n");
      continue;
      break;
    }

    switch (execute_statement(&statement, database)) {
    case EXECUTE_SUCCESS:
      printf("Executed\n");
      break;

    case EXECUTE_DUPLICATE_KEY:
      printf("Error: Duplicate key.\n");
      break;

    case EXECUTE_SCHEMA_MISMATCH:
      printf("Error: Column types do not match with schema types\n");
      break;

    case EXECUTE_TABLE_NOT_FOUND:
      printf("Error: Table not found\n");
      break;
    }
  }
}