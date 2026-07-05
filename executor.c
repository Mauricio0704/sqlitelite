#include "executor.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/* Reads and prints the record currently addressed by the cursor, emitting only
 * the projected columns in order. A count of 0 prints every column. */
void print_row(Record record, const ColumnId *projection,
               size_t projection_count) {
  if (projection_count == 0) {
    printf("(%d, %s, %s)\n", record.id, record.username, record.email);
    return;
  }

  printf("(");
  for (size_t i = 0; i < projection_count; i++) {
    if (i > 0)
      printf(", ");
    switch (projection[i]) {
    case COLUMN_ID:
      printf("%d", record.id);
      break;
    case COLUMN_USERNAME:
      printf("%s", record.username);
      break;
    case COLUMN_EMAIL:
      printf("%s", record.email);
      break;
    }
  }
  printf(")\n");
}

/* Inserts a record into the B-tree, redistributing nodes and updating parent
 keys as needed.*/
ExecuteStatus execute_insert(Statement *statement, Table *table) {
  Record record = statement->record_to_insert;

  int key_exists;
  Cursor *cursor = find_key_cursor(table, record.id, &key_exists);
  if (key_exists) {
    free(cursor);
    return EXECUTE_DUPLICATE_KEY;
  }

  leaf_node_insert(cursor, record.id, &record);

  flush_to_wal(table->pager);

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
  case COMPARISON:
    switch (where_expr->col) {
    case COLUMN_ID: {
      int cmp =
          (record.id > where_expr->intval) - (record.id < where_expr->intval);
      return apply_operator(where_expr->op_type, cmp);
    }
    case COLUMN_EMAIL:
      return apply_operator(where_expr->op_type,
                            strcmp(record.email, where_expr->strval));
    case COLUMN_USERNAME:
      return apply_operator(where_expr->op_type,
                            strcmp(record.username, where_expr->strval));
    }
    return 0;
  case AND_EXPR:
    return row_matches(where_expr->left, record) &&
           row_matches(where_expr->right, record);
  case OR_EXPR:
    return row_matches(where_expr->left, record) ||
           row_matches(where_expr->right, record);
  }
  return 0;
}

void execute_select(Statement *statement, Table *table) {
  Cursor *cursor = new_cursor_start(table);

  void *node = get_page(table->pager, cursor->page_num);

  while (*node_type_value(node) != NODE_TYPE_LEAF) {
    uint32_t child_page_num = *internal_node_pointer(node, 0);
    node = get_page(table->pager, child_page_num);
    cursor->page_num = child_page_num;
  }

  while (!(cursor->is_end_of_table)) {
    Record record;
    read_deserialized_record(get_record_start(cursor), &record);
    if (!statement->has_where || row_matches(statement->where_expr, record))
      print_row(record, statement->projection, statement->projection_count);
    advance_cursor(cursor);
  }

  if (statement->has_where)
    free_expr(statement->where_expr);
  free(cursor);
}

void execute_delete(Statement *statement, Table *table) {
  uint32_t key = statement->id_to_delete;

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

/* Dispatches a prepared statement to its corresponding executor. */
ExecuteStatus execute_statement(Statement *statement, Table *table) {
  if (statement->statement_type == STATEMENT_INSERT) {
    return execute_insert(statement, table);
  }
  if (statement->statement_type == STATEMENT_SELECT) {
    execute_select(statement, table);
  }
  if (statement->statement_type == STATEMENT_DELETE) {
    execute_delete(statement, table);
  }

  return EXECUTE_SUCCESS;
}