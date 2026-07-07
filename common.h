#ifndef COMMON_H
#define COMMON_H

#include <stddef.h>
#include <stdint.h>

/* Shared vocabulary used across more than one module */
#define TABLE_MAX_PAGES 100
#define PAGE_SIZE 4096

typedef struct {
  uint32_t id;
  uint32_t len_username;
  uint32_t len_email;
  char *username;
  char *email;
} Record;

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
  STATEMENT_INSERT,
  STATEMENT_SELECT,
  STATEMENT_DELETE
} StatementType;

#endif /* COMMON_H */
