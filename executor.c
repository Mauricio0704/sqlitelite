#include "executor.h"
#include "btree.h"
#include "common.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Reads and prints the record currently addressed by the cursor, emitting only
 * the projected columns in order. A count of 0 prints every column. */
void print_row(Record record, const ColumnId *projection_idxs,
               size_t projection_count) {
  if (projection_count == 0) {
    printf("(");
    for (size_t i = 0; i < record.num_values; i++) {
      if (i > 0)
        printf(", ");
      Value curr_value = record.values[i];
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
    Value curr_value = record.values[projection_idxs[i]];
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
ExecuteStatus execute_insert(Statement *statement, Table *table) {
  Record record = statement->record_to_insert;

  /* INTEGER PRIMARY KEY uses the supplied value, otherwise rowid. */
  Value pk_value = record.values[table->schema->pk_column];
  uint32_t key =
      (pk_value.type == INT) ? pk_value.int_val : table->rowid_counter;

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
    Value curr_val = record.values[where_expr->col_idx];
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
    read_deserialized_record(get_record_start(cursor), &record, table->schema);
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