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

  void *node = get_page(table->pager, cursor->page_num);

  while (*node_type_value(node) != NODE_TYPE_LEAF) {
    uint32_t child_page_num = *internal_node_pointer(node, 0);
    node = get_page(table->pager, child_page_num);
    cursor->page_num = child_page_num;
  }

  while (!(cursor->is_end_of_table)) {
    Record record;
    read_deserialized_record(get_record_start(cursor), &record, table->schema);
    if (!stmt->has_where || row_matches(stmt->where_expr, record))
      print_row(record, stmt->projection_idxs, stmt->projection_count);
    advance_cursor(cursor);
  }

  free(cursor);
}

void execute_delete(DeleteStmt *stmt, Table *table) {
  uint32_t key = stmt->id_to_delete;

  int key_exists;
  Cursor *cursor = find_key_cursor(table, key, &key_exists);
  if (!key_exists) {
    printf("Key %d does not exist\n", key);
    free(cursor);
    return;
  }

  void *node = get_page(table->pager, cursor->page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);
  uint32_t deleted_page = cursor->page_num;

  /* Remove the slot entry by shifting later slots left. */
  for (uint32_t i = cursor->slot_num + 1; i < num_cells; ++i) {
    *leaf_node_offset_value(node, i - 1) = *leaf_node_offset_value(node, i);
  }
  *leaf_node_num_cells(node) -= 1;
  *leaf_node_free_start(node) -= LEAF_NODE_SLOT_SIZE;

  pager_mark_dirty(table->pager, deleted_page);

  /* Check for underflow: non-root leaf dropped below minimum occupancy. */
  if (!*node_is_root_value(node) &&
      *leaf_node_num_cells(node) < LEAF_NODE_MIN_CELLS) {
    handle_underflow(table->pager, table, deleted_page);
  }

  flush_to_wal(table->pager);

  free(cursor);
}

Record get_new_table_record(CreateStmt *stmt, uint32_t root_page_num) {
  Record record;
  record.n_vals = 4;
  record.vals = malloc(sizeof(Value) * 4);

  record.vals[0] = (Value){INT, TABLE};
  record.vals[1].type = TEXT;
  record.vals[1].text_val.len = strlen(stmt->new_table_name);
  record.vals[1].text_val.str =
      strndup(stmt->new_table_name, record.vals[1].text_val.len);
  record.vals[2].type = TEXT;
  if (stmt->raw_stmt != NULL) {
    record.vals[2].text_val.str = strdup(stmt->raw_stmt);
    record.vals[2].text_val.len =
        (uint32_t)strlen(record.vals[2].text_val.str);
  } else {
    record.vals[2].text_val.str = NULL;
    record.vals[2].text_val.len = 0;
  }
  record.vals[3] = (Value){INT, root_page_num};
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
    if (stmt->type == STATEMENT_SELECT)
      free_select_stmt(stmt->select_stmt);
    return status;
  }

  switch (stmt->type) {
  case STATEMENT_INSERT:
    return execute_insert(stmt->insert_stmt, table);
  case STATEMENT_SELECT:
    execute_select(stmt->select_stmt, table);
    free_select_stmt(stmt->select_stmt);
    break;
  case STATEMENT_DELETE:
    execute_delete(stmt->delete_stmt, table);
    break;
  case STATEMENT_CREATE:
    execute_create(stmt->create_stmt,
                   table); /* Adds new record to catalog table */
    Pager *pager = table->pager;

    db->tables[db->num_tables] = new_table_from_stmt(pager, stmt);
    pager->num_pages++;
    db->num_tables++;
    break;
  }
  return EXECUTE_SUCCESS;
}