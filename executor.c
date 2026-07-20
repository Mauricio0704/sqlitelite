#include "executor.h"
#include "analyzer.h"
#include "btree.h"
#include "common.h"
#include "pager.h"
#include "parser.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Reads and prints the record currently addressed by the cursor, emitting only
 * the projected columns in order. A count of 0 prints every column. */
void print_row(Record record, int *projection_idxs, size_t projection_count) {
  if (projection_count == 0) {
    printf("(");
    for (size_t i = 0; i < record.n_vals; i++) {
      if (i > 0)
        printf(", ");
      Value curr_value = record.vals[i];
      switch (curr_value.type) {
      case INT:
        printf("%d", curr_value.int_val);
        break;
      case TEXT:
        printf("%s", curr_value.text_val.str);
        break;
      default:
        break;
      }
    }
    printf(")\n");
    return;
  }

  printf("(");
  for (size_t i = 0; i < projection_count; i++) {
    if (i > 0)
      printf(", ");
    Value curr_value = record.vals[projection_idxs[i]];
    switch (curr_value.type) {
    case INT:
      printf("%d", curr_value.int_val);
      break;
    case TEXT:
      printf("%s", curr_value.text_val.str);
      break;
    default:
      break;
    }
  }
  printf(")\n");
}

/* Inserts a record into the B-tree, redistributing nodes and updating parent
 keys as needed.*/
ExecuteStatus execute_insert(InsertStmt *stmt, Table *table) {
  Record record = stmt->record;

  uint32_t key;
  uint32_t pk_col = table->schema->pk_idx;

  /* If PK is not defined or it is TEXT, should use rowid */
  if (pk_col == UINT32_MAX || record.vals[pk_col].type == TEXT)
    key = table->rowid_counter;
  else
    key = record.vals[pk_col].int_val;

  int key_exists;
  Cursor *cursor = find_key_cursor(table, key, &key_exists);
  if (key_exists) {
    free(cursor);
    return EXECUTE_DUPLICATE_KEY;
  }

  leaf_node_insert(cursor, key, &record);

  flush_to_wal(table->pager);

  /* Keep the counter above every key inserted, so auto-assigned rowids never
     collide with or regress behind an explicit id. */
  if (key >= table->rowid_counter)
    table->rowid_counter = key + 1;

  free(cursor);
  return EXECUTE_SUCCESS;
}

uint8_t apply_operator(TokenType operator, int cmp) {
  switch (operator) {
  case TOKEN_OP_SMALLER:
    return cmp < 0;
  case TOKEN_OP_GREATER:
    return cmp > 0;
  case TOKEN_OP_GEQ:
    return cmp >= 0;
  case TOKEN_OP_SEQ:
    return cmp <= 0;
  default: /* TOKEN_OP_EQUAL */
    return cmp == 0;
  }
}

uint8_t row_matches(Expr *where_expr, Record record) {
  switch (where_expr->kind) {
  case COMPARISON: {
    Value curr_val = record.vals[where_expr->col_idx];
    switch (curr_val.type) {
    case INT: {
      int cmp = (curr_val.int_val > where_expr->intval) -
                (curr_val.int_val < where_expr->intval);
      return apply_operator(where_expr->op_type, cmp);
    }
    case TEXT:
      return apply_operator(where_expr->op_type,
                            strcmp(curr_val.text_val.str, where_expr->strval));
    default:
      break;
    }
    return 0;
  }
  case AND_EXPR:
    return row_matches(where_expr->left, record) &&
           row_matches(where_expr->right, record);
  case OR_EXPR:
    return row_matches(where_expr->left, record) ||
           row_matches(where_expr->right, record);
  }
  return 0;
}

void execute_select(SelectStmt *stmt, Table *table) {
  Cursor *cursor = new_cursor_start(table);

  while (!(cursor->is_end_of_table)) {
    Record record;
    read_deserialized_record(get_record_start(cursor), &record, table->schema);
    if (!stmt->has_where || row_matches(stmt->where_expr, record))
      print_row(record, stmt->projection_idxs, stmt->projection_count);
    free_record(&record);
    advance_cursor(cursor);
  }

  free(cursor);
}

void delete_keys(uint32_t *keys, uint32_t n_keys, Table *table) {
  for (uint32_t i = 0; i < n_keys; i++) {
    int key_exists;
    Cursor *del_cursor = find_key_cursor(table, keys[i], &key_exists);
    delete_record(del_cursor);
    free(del_cursor);
  }
}

void execute_delete(DeleteStmt *stmt, Table *table) {
  Cursor *cursor = new_cursor_start(table);
  uint32_t capacity = 16;
  uint32_t matches = 0;
  uint32_t *keys = malloc(sizeof(uint32_t) * capacity);

  while (!(cursor->is_end_of_table)) {
    Record record;
    read_deserialized_record(get_record_start(cursor), &record, table->schema);
    if (!stmt->has_where || row_matches(stmt->where_expr, record)) {
      if (matches == capacity) {
        capacity *= 2;
        keys = realloc(keys, sizeof(uint32_t) * capacity);
      }
      void *node = get_page(table->pager, cursor->page_num);
      keys[matches++] = leaf_node_key_at_slot(node, cursor->slot_num);
    }
    free_record(&record);
    advance_cursor(cursor);
  }
  free(cursor);

  delete_keys(keys, matches, table);
  free(keys);
}

void execute_update(UpdateStmt *stmt, Table *table) {
  Cursor *cursor = new_cursor_start(table);
  uint32_t capacity = 16;
  uint32_t matches = 0;
  uint32_t *keys = malloc(sizeof(uint32_t) * capacity);
  Record *records = malloc(sizeof(Record) * capacity);

  while (!(cursor->is_end_of_table)) {
    Record record;
    read_deserialized_record(get_record_start(cursor), &record, table->schema);
    if (!stmt->has_where || row_matches(stmt->where_expr, record)) {
      if (matches == capacity) {
        capacity *= 2;
        keys = realloc(keys, sizeof(uint32_t) * capacity);
        records = realloc(records, sizeof(Record) * capacity);
      }
      void *node = get_page(table->pager, cursor->page_num);
      keys[matches] = leaf_node_key_at_slot(node, cursor->slot_num);
      records[matches] = record;
      matches++;
    } else {
      free_record(&record);
    }
    advance_cursor(cursor);
  }
  free(cursor);

  delete_keys(keys, matches, table);
  for (uint32_t i = 0; i < matches; i++) {
    Record record = records[i];
    for (size_t j = 0; j < stmt->new_vals_count; j++) {
      int idx = stmt->new_vals_idxs[j];
      if (record.vals[idx].type == TEXT)
        free(record.vals[idx].text_val.str);
      record.vals[idx] = stmt->new_vals[j];
      if (stmt->new_vals[j].type == TEXT)
        record.vals[idx].text_val.str = strndup(
            stmt->new_vals[j].text_val.str, stmt->new_vals[j].text_val.len);
    }
    InsertStmt ins = {.record = record};
    execute_insert(&ins, table);
    free_record(&record);
  }

  free(keys);
  free(records);
}

Record get_new_table_record(CreateStmt *stmt, uint32_t root_page_num) {
  Record record;
  record.n_vals = 4;
  record.vals = malloc(sizeof(Value) * 4);

  record.vals[0] = (Value){INT, {TABLE}};
  record.vals[1].type = TEXT;
  record.vals[1].text_val.len = strlen(stmt->new_table_name);
  record.vals[1].text_val.str =
      strndup(stmt->new_table_name, record.vals[1].text_val.len);
  record.vals[2].type = TEXT;
  if (stmt->raw_stmt != NULL) {
    record.vals[2].text_val.str = strdup(stmt->raw_stmt);
    record.vals[2].text_val.len = (uint32_t)strlen(record.vals[2].text_val.str);
  } else {
    record.vals[2].text_val.str = NULL;
    record.vals[2].text_val.len = 0;
  }
  record.vals[3] = (Value){INT, {root_page_num}};
  return record;
}

void execute_create(CreateStmt *stmt, Table *table) {
  void *catalog_node = get_page(table->pager, 0);
  uint32_t num_tables = *leaf_node_num_cells(catalog_node);
  if (num_tables >= MAX_TABLES)
    return;

  uint32_t n_table_root_page_num = table->pager->num_pages;
  InsertStmt ins_stmt;
  ins_stmt.record = get_new_table_record(stmt, n_table_root_page_num);
  execute_insert(&ins_stmt, table);

  void *new_table_node = get_page(table->pager, n_table_root_page_num);
  initialize_leaf_node(new_table_node);
  *node_is_root_value(new_table_node) = 1;
}

/* Dispatches a prepared statement to its corresponding executor. */
ExecuteStatus execute_statement(Statement *stmt, Database *db) {
  Table *table;
  ExecuteStatus status = analyze(stmt, db, &table);
  if (status != EXECUTE_SUCCESS) {
    switch (stmt->type) {
    case STATEMENT_SELECT:
      free_select_stmt(stmt->select_stmt);
      break;
    case STATEMENT_DELETE:
      free_delete_stmt(stmt->delete_stmt);
      break;
    case STATEMENT_INSERT:
      free_insert_stmt(stmt->insert_stmt);
      break;
    case STATEMENT_CREATE:
      free_create_stmt(stmt->create_stmt);
      break;
    case STATEMENT_UPDATE:
      free_update_stmt(stmt->update_stmt);
      break;
    }
    return status;
  }

  switch (stmt->type) {
  case STATEMENT_INSERT: {
    ExecuteStatus insert_status = execute_insert(stmt->insert_stmt, table);
    free_insert_stmt(stmt->insert_stmt);
    return insert_status;
  }
  case STATEMENT_SELECT:
    execute_select(stmt->select_stmt, table);
    free_select_stmt(stmt->select_stmt);
    break;
  case STATEMENT_DELETE: {
    DeleteStmt *del = stmt->delete_stmt;
    execute_delete(del, table);
    free_delete_stmt(stmt->delete_stmt);
    break;
  }
  case STATEMENT_CREATE:
    execute_create(stmt->create_stmt,
                   table); /* Adds new record to catalog table */
    Pager *pager = table->pager;

    db->tables[db->num_tables] =
        new_table_from_stmt(pager, stmt, pager->num_pages);
    pager->num_pages++;
    db->num_tables++;
    free_create_stmt(stmt->create_stmt);
    break;
  case STATEMENT_UPDATE:
    execute_update(stmt->update_stmt, table);
    free_update_stmt(stmt->update_stmt);
    break;
  }
  return EXECUTE_SUCCESS;
}