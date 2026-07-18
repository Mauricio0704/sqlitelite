#ifndef COMMON_H
#define COMMON_H

#include <stddef.h>
#include <stdint.h>

/* Shared vocabulary used across more than one module */
#define TABLE_MAX_PAGES 100
#define PAGE_SIZE 4096
#define MAX_TABLES 5

typedef enum { INT, TEXT } ColumnType;

typedef struct {
  ColumnType type;
  union {
    uint32_t int_val;
    struct {
      uint32_t len;
      char *str;
    } text_val;
  };
} Value;

typedef struct {
  Value *vals;
  size_t n_vals;
} Record;

typedef enum { TABLE } CatalogType;

typedef struct {
  char *table_name;
  uint32_t root_page_num;
  CatalogType type;
  char *create_stmt;
} CatalogEntry;

typedef struct Schema {
  ColumnType *col_types;
  char **col_names;
  size_t n_cols;
  uint32_t pk_idx;
} Schema;

typedef enum {
  PREPARE_SUCCESS,
  PREPARE_FAILURE,
  PREPARE_UNRECOGNIZED_COMMAND
} PrepareStatus;

typedef enum {
  EXECUTE_SUCCESS,
  EXECUTE_DUPLICATE_KEY,
  EXECUTE_SCHEMA_MISMATCH,
  EXECUTE_TABLE_NOT_FOUND
} ExecuteStatus;

typedef enum {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandStatus;

typedef enum {
  STATEMENT_INSERT,
  STATEMENT_SELECT,
  STATEMENT_DELETE,
  STATEMENT_CREATE,
  STATEMENT_UPDATE
} StatementType;

#endif /* COMMON_H */
