#ifndef BTREE_H
#define BTREE_H

#include "common.h"
#include "pager.h"

#include <stdint.h>

typedef enum {
  NODE_TYPE_INTERNAL,
  NODE_TYPE_LEAF,
  NODE_TYPE_INDEFINITE
} NodeType;

typedef struct {
  uint32_t page_num;
  uint32_t slot_num;
  Table *table;
  int is_end_of_table;
} Cursor;

/* Node layout constants (all compile-time; see note above on enum vs const). */
enum {
  NODE_IS_ROOT_SIZE = sizeof(uint8_t),
  NODE_IS_ROOT_OFFSET = 0,
  NODE_TYPE_SIZE = sizeof(uint8_t),
  NODE_TYPE_OFFSET = NODE_IS_ROOT_OFFSET + NODE_IS_ROOT_SIZE,
  NODE_PARENT_POINTER_SIZE = sizeof(uint32_t),
  NODE_PARENT_POINTER_OFFSET = NODE_TYPE_OFFSET + NODE_TYPE_SIZE,
  COMMON_NODE_HEADER_SIZE =
      NODE_IS_ROOT_SIZE + NODE_TYPE_SIZE + NODE_PARENT_POINTER_SIZE,

  /* Internal node header */
  INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t),
  INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE,
  INTERNAL_NODE_RIGHTMOST_POINTER_SIZE = sizeof(uint32_t),
  INTERNAL_NODE_RIGHTMOST_POINTER_OFFSET =
      INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE,
  INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE +
                              INTERNAL_NODE_NUM_KEYS_SIZE +
                              INTERNAL_NODE_RIGHTMOST_POINTER_SIZE,
  INTERNAL_NODE_MAX_KEYS = 3,

  /* Internal node body */
  INTERNAL_NODE_POINTER_SIZE = sizeof(uint32_t),
  INTERNAL_NODE_POINTER_OFFSET = 0,
  INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t),
  INTERNAL_NODE_KEY_OFFSET =
      INTERNAL_NODE_POINTER_OFFSET + INTERNAL_NODE_POINTER_SIZE,
  INTERNAL_NODE_PAIR_SIZE = INTERNAL_NODE_KEY_SIZE + INTERNAL_NODE_POINTER_SIZE,

  /* Leaf node header */
  LEAF_NODE_NEXT_POINTER_SIZE = sizeof(uint32_t),
  LEAF_NODE_NEXT_POINTER_OFFSET = COMMON_NODE_HEADER_SIZE,
  LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t),
  LEAF_NODE_NUM_CELLS_OFFSET =
      LEAF_NODE_NEXT_POINTER_OFFSET + LEAF_NODE_NEXT_POINTER_SIZE,
  LEAF_NODE_FREE_START_SIZE = sizeof(uint16_t),
  LEAF_NODE_FREE_START_OFFSET =
      LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE,
  LEAF_NODE_FREE_END_SIZE = sizeof(uint16_t),
  LEAF_NODE_FREE_END_OFFSET =
      LEAF_NODE_FREE_START_OFFSET + LEAF_NODE_FREE_START_SIZE,
  LEAF_NODE_HEADER_SIZE =
      COMMON_NODE_HEADER_SIZE + LEAF_NODE_NEXT_POINTER_SIZE +
      LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_FREE_START_SIZE +
      LEAF_NODE_FREE_END_SIZE,

  LEAF_NODE_SIZE = PAGE_SIZE,

  /* Leaf node body */
  LEAF_NODE_SLOT_SIZE = sizeof(uint16_t),
  LEAF_NODE_CELL_KEY_SIZE = sizeof(uint32_t),
  LEAF_NODE_CELL_KEY_OFFSET = 0,
  LEAF_NODE_SPACE_FOR_CELLS = LEAF_NODE_SIZE - LEAF_NODE_HEADER_SIZE,

  /* Worst case */
  LEAF_NODE_MIN_CELL_SIZE =
      LEAF_NODE_CELL_KEY_SIZE + 3 * sizeof(uint32_t) + LEAF_NODE_SLOT_SIZE,
  LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_MIN_CELL_SIZE,
  LEAF_NODE_MIN_CELLS = 4, /* Placeholder */
  INTERNAL_NODE_MIN_KEYS = INTERNAL_NODE_MAX_KEYS / 2,
};

uint8_t *node_is_root_value(void *node);
uint8_t *node_type_value(void *node);
uint32_t *node_parent(void *node);
uint32_t *internal_node_num_keys(void *node);
uint32_t *internal_node_rightmost_pointer(void *node);
uint32_t *internal_node_key(void *node, uint32_t key_num);
uint32_t *internal_node_pointer(void *node, uint32_t key_num);
uint32_t *leaf_node_num_cells(void *node);
uint16_t *leaf_node_free_start(void *node);
uint16_t *leaf_node_free_end(void *node);
uint32_t *leaf_node_next_pointer(void *node);
uint16_t *leaf_node_offset_value(void *node, uint16_t slot_num);
uint32_t leaf_node_key_at_slot(void *node, uint32_t slot_num);
void *get_record_start(Cursor *cursor);
Cursor *new_cursor_start(Table *table);
void advance_cursor(Cursor *cursor);
void initialize_node(void *node, NodeType node_type);
void initialize_internal_node(void *node);
void initialize_leaf_node(void *node);
Cursor *leaf_node_offset_find(Table *table, uint32_t page_num, uint32_t key);
uint32_t get_rightmost_rowid(Table *table);
void delete_record(Cursor *cursor);
void free_record(Record *record);
void write_serialized_record(Record *source, void *destination);
void read_deserialized_record(void *source, Record *destination, Schema *schema);
void internal_node_insert_key(Pager *pager, uint32_t parent_page_num,
                              uint32_t promoted_key, uint32_t left_child_page,
                              uint32_t right_child_page);
void leaf_node_insert(Cursor *cursor, uint32_t key, Record *record);
Cursor *find_key_cursor(Table *table, uint32_t key, int *key_exists);
uint32_t leaf_node_read_all_cells(void *node, uint32_t *keys_out,
                                  Record *records_out, Schema *schema);
int32_t find_child_index_in_parent(void *parent, uint32_t child_page);
uint32_t internal_node_child(void *node, uint32_t child_index);
void internal_node_remove_key(void *node, uint32_t key_index);
uint32_t internal_node_num_children(void *node);

#endif /* BTREE_H */
