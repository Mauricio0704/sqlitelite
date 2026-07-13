#include "analyzer.h"
#include "common.h"
#include "pager.h"
#include "parser.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

int record_matches_schema(Record record, Schema *schema) {
  (void)record;
  (void)schema;

  if (record.num_values != schema->n_cols)
    return 0;

  for (int i = 0; i < record.num_values; i++) {
    ColumnType record_type = record.values[i].type;
    ColumnType schema_type = schema->col_types[i];

    if (record_type != schema_type)
      return 0;
  }

  return 1;
}

/* Finds the column named 'name' in schema, writing its index to *out. */
int resolve_col_in_schema(const char *name, Schema *schema, int *out) {
  for (int j = 0; j < schema->n_cols; j++) {
    if (strcmp(name, schema->col_names[j]) == 0) {
      *out = j;
      return 1;
    }
  }
  return 0;
}

int columns_match_schema(SelectStmt *stmt, Schema *schema) {
  for (int i = 0; i < stmt->projection_count; i++) {
    if (!resolve_col_in_schema(stmt->projection_names[i], schema,
                               &stmt->projection_idxs[i]))
      return 0;
  }
  return 1;
}

/* Resolves every column name in a WHERE tree. */
int where_matches_schema(Expr *expr, Schema *schema) {
  if (expr == NULL)
    return 1;
  switch (expr->kind) {
  case COMPARISON: {
    int idx;
    if (!resolve_col_in_schema(expr->col_name, schema, &idx))
      return 0;
    expr->col_idx = idx;
    ColumnType literal_type = expr->strval != NULL ? TEXT : INT;
    return literal_type == schema->col_types[idx];
  }
  case AND_EXPR:
  case OR_EXPR:
    return where_matches_schema(expr->left, schema) &&
           where_matches_schema(expr->right, schema);
  }
  return 0;
}

ExecuteStatus analyze(Statement *stmt, Database *db, Table **out) {
  uint8_t found = 0;
  for (int i = 0; i < db->num_tables; i++) {
    if (strcmp(db->tables[i]->table_name, stmt->table_name) == 0) {
      *out = db->tables[i];
      found = 1;
    }
  }
  if (found == 0) {
    return EXECUTE_TABLE_NOT_FOUND;
  }
  if (stmt->type == STATEMENT_INSERT) {
    if (record_matches_schema(stmt->insert_stmt->record, (*out)->schema) == 0)
      return EXECUTE_SCHEMA_MISMATCH;
  } else if (stmt->type == STATEMENT_SELECT) {
    if (columns_match_schema(stmt->select_stmt, (*out)->schema) == 0)
      return EXECUTE_SCHEMA_MISMATCH;
    if (stmt->select_stmt->has_where &&
        where_matches_schema(stmt->select_stmt->where_expr, (*out)->schema) ==
            0)
      return EXECUTE_SCHEMA_MISMATCH;
  }
  return EXECUTE_SUCCESS;
}
