#include "analyzer.h"
#include "common.h"
#include "pager.h"
#include "parser.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

int record_matches_schema(Record record, Schema *schema) {
  if (record.n_vals != schema->n_cols)
    return 0;

  for (size_t i = 0; i < record.n_vals; i++) {
    ColumnType record_type = record.vals[i].type;
    ColumnType schema_type = schema->col_types[i];

    if (record_type != schema_type)
      return 0;
  }

  return 1;
}

/* Finds the column named 'name' in schema, writing its index to *out. */
int resolve_col_in_schema(const char *name, Schema *schema, int *out) {
  for (size_t j = 0; j < schema->n_cols; j++) {
    if (strcmp(name, schema->col_names[j]) == 0) {
      *out = j;
      return 1;
    }
  }
  return 0;
}

int columns_match_schema(char **names, int *idxs, int count, Schema *schema) {
  for (int i = 0; i < count; i++) {
    if (!resolve_col_in_schema(names[i], schema, &idxs[i]))
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

int values_match_column_types(UpdateStmt *upd, Schema *schema) {
  for (size_t i = 0; i < upd->new_vals_count; i++) {
    if (upd->new_vals[i].type != schema->col_types[upd->new_vals_idxs[i]])
      return 0;
  }
  return 1;
}

ExecuteStatus analyze(Statement *stmt, Database *db, Table **out) {
  uint8_t found = 0;
  for (uint32_t i = 0; i < db->num_tables; i++) {
    if (strcmp(db->tables[i]->table_name, stmt->table_name) == 0) {
      *out = db->tables[i];
      found = 1;
    }
  }
  if (found == 0)
    return EXECUTE_TABLE_NOT_FOUND;

  switch (stmt->type) {
  case STATEMENT_INSERT:
    if (record_matches_schema(stmt->insert_stmt->record, (*out)->schema) == 0)
      return EXECUTE_SCHEMA_MISMATCH;
    break;
  case STATEMENT_SELECT: {
    SelectStmt *sel = stmt->select_stmt;
    if (columns_match_schema(sel->projection_names, sel->projection_idxs,
                             sel->projection_count, (*out)->schema) == 0)
      return EXECUTE_SCHEMA_MISMATCH;
    if (stmt->select_stmt->has_where &&
        where_matches_schema(stmt->select_stmt->where_expr, (*out)->schema) ==
            0)
      return EXECUTE_SCHEMA_MISMATCH;
    break;
  }
  case STATEMENT_DELETE:
    if (stmt->delete_stmt->has_where &&
        where_matches_schema(stmt->delete_stmt->where_expr, (*out)->schema) ==
            0)
      return EXECUTE_SCHEMA_MISMATCH;
    break;
  case STATEMENT_UPDATE: {
    UpdateStmt *upd = stmt->update_stmt;
    if (columns_match_schema(upd->new_vals_cols, upd->new_vals_idxs,
                             upd->new_vals_count, (*out)->schema) == 0)
      return EXECUTE_SCHEMA_MISMATCH;
    if (values_match_column_types(upd, (*out)->schema) == 0)
      return EXECUTE_SCHEMA_MISMATCH;
    if (upd->has_where &&
        where_matches_schema(upd->where_expr, (*out)->schema) == 0)
      return EXECUTE_SCHEMA_MISMATCH;
    break;
  }
  default:
    break;
  }

  return EXECUTE_SUCCESS;
}
