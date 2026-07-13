#include "analyzer.h"
#include "common.h"
#include "pager.h"
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
  }
  return EXECUTE_SUCCESS;
}