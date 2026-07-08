#include "btree.h"
#include "common.h"
#include "pager.h"
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

/* Returns a pointer to the node root flag field. */
uint8_t *node_is_root_value(void *node) { return node + NODE_IS_ROOT_OFFSET; }

/* Returns a pointer to the node type field. */
uint8_t *node_type_value(void *node) { return node + NODE_TYPE_OFFSET; }

/* Returns a pointer to the parent page number field. */
uint32_t *node_parent(void *node) { return node + NODE_PARENT_POINTER_OFFSET; }

/* Returns a pointer to the number-of-keys field of an internal node. */
uint32_t *internal_node_num_keys(void *node) {
  return node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}

/* Returns a pointer to the rightmost child pointer field of an internal node.
 */
uint32_t *internal_node_rightmost_pointer(void *node) {
  return node + INTERNAL_NODE_RIGHTMOST_POINTER_OFFSET;
}

/* Returns a pointer to the separator key at key_num in an internal node. */
uint32_t *internal_node_key(void *node, uint32_t key_num) {
  return node + INTERNAL_NODE_HEADER_SIZE + key_num * INTERNAL_NODE_PAIR_SIZE +
         INTERNAL_NODE_KEY_OFFSET;
}

/* Returns a pointer to the child pointer at key_num in an internal node. */
uint32_t *internal_node_pointer(void *node, uint32_t key_num) {
  return node + INTERNAL_NODE_HEADER_SIZE + key_num * INTERNAL_NODE_PAIR_SIZE +
         INTERNAL_NODE_POINTER_OFFSET;
}

/* Returns a pointer to the number-of-cells field of a leaf node. */
uint32_t *leaf_node_num_cells(void *node) {
  return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

/* Returns a pointer to the first free byte in the slot directory region. */
uint16_t *leaf_node_free_start(void *node) {
  return node + LEAF_NODE_FREE_START_OFFSET;
}

/* Returns a pointer to the first free byte in the cell payload region. */
uint16_t *leaf_node_free_end(void *node) {
  return node + LEAF_NODE_FREE_END_OFFSET;
}

/* Returns a pointer to the next leaf page number in the leaf linked list. */
uint32_t *leaf_node_next_pointer(void *node) {
  return node + LEAF_NODE_NEXT_POINTER_OFFSET;
}

/* Returns a pointer to the slot directory entry for slot_num. */
uint16_t *leaf_node_offset_value(void *node, uint16_t slot_num) {
  return node + LEAF_NODE_HEADER_SIZE + slot_num * LEAF_NODE_SLOT_SIZE;
};

/* Reads and returns the key stored at a given leaf slot. */
uint32_t leaf_node_key_at_slot(void *node, uint32_t slot_num) {
  uint16_t cell_offset = *leaf_node_offset_value(node, (uint16_t)slot_num);
  uint32_t key;
  memcpy(&key, (uint8_t *)node + cell_offset, sizeof(key));
  return key;
}

/* Returns the address of the serialized record payload for the cursor slot. */
void *get_record_start(Cursor *cursor) {
  void *node = get_page(cursor->table->pager, cursor->page_num);

  return node + *leaf_node_offset_value(node, cursor->slot_num) +
         LEAF_NODE_CELL_KEY_SIZE;
}

/* Creates a cursor positioned at slot 0 of the current root page. */
Cursor *new_cursor_start(Table *table) {
  Cursor *new_cursor = (Cursor *)malloc(sizeof(Cursor));
  new_cursor->page_num = table->root_page_num;
  new_cursor->slot_num = 0;
  new_cursor->table = table;
  void *node = get_page(table->pager, new_cursor->page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);
  new_cursor->is_end_of_table = (num_cells == 0);

  return new_cursor;
}

/* Advances the cursor to the next slot, crossing leaf pages as needed. */
void advance_cursor(Cursor *cursor) {
  void *node = get_page(cursor->table->pager, cursor->page_num);
  cursor->slot_num += 1;

  if (cursor->slot_num >= *leaf_node_num_cells(node)) {
    if (*leaf_node_next_pointer(node) == 0) {
      cursor->is_end_of_table = 1;
      return;
    }
    cursor->page_num = *leaf_node_next_pointer(node);
    cursor->slot_num = 0;
  }

  return;
}

/* Initializes fields common to all node types. */
void initialize_node(void *node, NodeType node_type) {
  *node_is_root_value(node) = 0;
  *node_type_value(node) = node_type;
  *node_parent(node) = 0;
}

/* Initializes an internal node header. */
void initialize_internal_node(void *node) {
  initialize_node(node, NODE_TYPE_INTERNAL);
  *internal_node_num_keys(node) = 0;
}

/* Initializes a leaf node header and free-space boundaries. */
void initialize_leaf_node(void *node) {
  initialize_node(node, NODE_TYPE_LEAF);
  *leaf_node_next_pointer(node) = 0;
  *leaf_node_num_cells(node) = 0;
  *leaf_node_free_start(node) =
      LEAF_NODE_HEADER_SIZE + *leaf_node_num_cells(node) * LEAF_NODE_SLOT_SIZE;
  *leaf_node_free_end(node) = LEAF_NODE_SIZE;
}

/* Binary-searches a leaf node for key and returns insertion/search cursor. */
Cursor *leaf_node_offset_find(Table *table, uint32_t page_num, uint32_t key) {
  void *node = get_page(table->pager, page_num);
  Cursor *cursor = (Cursor *)malloc(sizeof(Cursor));
  cursor->page_num = page_num;
  cursor->table = table;

  uint32_t left = 0;
  uint32_t right = *leaf_node_num_cells(node);

  while (left < right) {
    uint32_t middle = left + (right - left) / 2;
    uint32_t curr_key = leaf_node_key_at_slot(node, middle);

    if (key == curr_key) {
      cursor->slot_num = middle;
      return cursor;
    }
    if (key < curr_key) {
      right = middle;
    } else {
      left = middle + 1;
    }
  }

  cursor->slot_num = left;
  return cursor;
}

/* Returns the largest key currently stored. */
uint32_t get_rightmost_rowid(Table *table) {
  void *node = get_page(table->pager, table->root_page_num);

  while (*node_type_value(node) == NODE_TYPE_INTERNAL) {
    node = get_page(table->pager, *internal_node_rightmost_pointer(node));
  }

  uint32_t num_cells = *leaf_node_num_cells(node);
  if (num_cells == 0)
    return 0;
  return leaf_node_key_at_slot(node, num_cells - 1);
}

/* Serializes an in-memory Record into its fixed-width on-page layout. */
void write_serialized_record(Record *source, void *destination) {
  uint32_t CURR_OFFSET = 0;

  for (size_t i = 0; i < source->num_values; i++) {
    Value curr_value = source->values[i];
    switch (curr_value.type) {
    case INT:
      memcpy(destination + CURR_OFFSET, &(curr_value.int_val),
             sizeof(uint32_t));
      CURR_OFFSET += sizeof(uint32_t);
      break;
    case TEXT:
      memcpy(destination + CURR_OFFSET, &(curr_value.text_val.len),
             sizeof(uint32_t));
      CURR_OFFSET += sizeof(uint32_t);
      size_t text_value_size = sizeof(char) * curr_value.text_val.len;
      memcpy(destination + CURR_OFFSET, curr_value.text_val.str,
             text_value_size);
      CURR_OFFSET += text_value_size;
      break;
    default:
      break;
    }
  }
}

/* Deserializes a fixed-width on-page record into an in-memory Record. */
void read_deserialized_record(void *source, Record *destination,
                              Schema *schema) {
  uint32_t CURR_OFFSET = 0;
  uint32_t num_columns = schema->num_columns;
  destination->num_values = num_columns;
  destination->values = malloc(sizeof(Value) * destination->num_values);

  for (size_t i = 0; i < num_columns; i++) {
    ColumnType curr_type = schema->column_types[i];
    Value *curr_value = &(destination->values[i]);
    switch (curr_type) {
    case INT:
      curr_value->type = INT;
      memcpy(&(curr_value->int_val), source + CURR_OFFSET, sizeof(uint32_t));
      CURR_OFFSET += sizeof(uint32_t);
      break;
    case TEXT:
      curr_value->type = TEXT;
      memcpy(&(curr_value->text_val.len), source + CURR_OFFSET,
             sizeof(uint32_t));
      CURR_OFFSET += sizeof(uint32_t);
      size_t text_value_size = sizeof(char) * curr_value->text_val.len;
      curr_value->text_val.str = malloc(text_value_size + 1);
      memcpy(curr_value->text_val.str, source + CURR_OFFSET, text_value_size);
      curr_value->text_val.str[curr_value->text_val.len] = '\0';
      CURR_OFFSET += text_value_size;
      break;
    default:
      break;
    }
  }
}

/* Inserts a new key+child into a parent internal node that has room. */
void internal_node_insert_key(Pager *pager, uint32_t parent_page_num,
                              uint32_t promoted_key, uint32_t left_child_page,
                              uint32_t right_child_page) {
  void *parent = get_page(pager, parent_page_num);
  uint32_t parent_num_keys = *internal_node_num_keys(parent);

  int32_t parent_slot = -1;
  for (uint32_t i = 0; i < parent_num_keys; i++) {
    if (*internal_node_pointer(parent, i) == left_child_page) {
      parent_slot = (int32_t)i;
      break;
    }
  }

  if (parent_slot == -1) {
    /* left_child_page is the rightmost pointer. */
    *internal_node_num_keys(parent) += 1;
    *internal_node_key(parent, parent_num_keys) = promoted_key;
    *internal_node_pointer(parent, parent_num_keys) = left_child_page;
    *internal_node_rightmost_pointer(parent) = right_child_page;
  } else {
    uint32_t old_sep = *internal_node_key(parent, parent_slot);
    *internal_node_key(parent, parent_slot) = promoted_key;

    *internal_node_num_keys(parent) += 1;
    for (uint32_t i = parent_num_keys; i > (uint32_t)(parent_slot + 1); i--) {
      *internal_node_key(parent, i) = *internal_node_key(parent, i - 1);
      *internal_node_pointer(parent, i) = *internal_node_pointer(parent, i - 1);
    }
    *internal_node_key(parent, parent_slot + 1) = old_sep;
    *internal_node_pointer(parent, parent_slot + 1) = right_child_page;
  }
}

/* Splits a full internal node, promotes the middle key, and redistributes
   keys/children. Handles both root and non-root nodes correctly. */
void internal_node_split(Pager *pager, uint32_t node_page_num, uint32_t new_key,
                         uint32_t new_right_child) {
  void *node = get_page(pager, node_page_num);
  uint32_t old_num_keys = *internal_node_num_keys(node);
  uint32_t total_keys = old_num_keys + 1;
  uint8_t was_root = *node_is_root_value(node);
  uint32_t old_parent_page = *node_parent(node);

  uint32_t old_children[INTERNAL_NODE_MAX_KEYS + 1];
  for (uint32_t i = 0; i < old_num_keys; i++) {
    old_children[i] = *internal_node_pointer(node, i);
  }
  old_children[old_num_keys] = *internal_node_rightmost_pointer(node);

  uint32_t insert_pos = old_num_keys;
  for (uint32_t i = 0; i < old_num_keys; i++) {
    if (new_key < *internal_node_key(node, i)) {
      insert_pos = i;
      break;
    }
  }

  uint32_t all_keys[INTERNAL_NODE_MAX_KEYS + 1];
  for (uint32_t i = 0, j = 0; i < total_keys; i++) {
    if (i == insert_pos) {
      all_keys[i] = new_key;
    } else {
      all_keys[i] = *internal_node_key(node, j);
      j++;
    }
  }

  uint32_t all_children[INTERNAL_NODE_MAX_KEYS + 2];
  for (uint32_t i = 0, j = 0; i <= total_keys; i++) {
    if (i == insert_pos + 1) {
      all_children[i] = new_right_child;
    } else {
      all_children[i] = old_children[j];
      j++;
    }
  }

  uint32_t promote_index = total_keys / 2;
  uint32_t promoted_key = all_keys[promote_index];
  uint32_t left_num_keys = promote_index;
  uint32_t right_num_keys = total_keys - promote_index - 1;

  if (was_root) {
    /* Root split: create two new child pages, reinitialize original as root. */
    uint32_t left_page_num = pager->num_pages;
    pager->num_pages += 1;
    uint32_t right_page_num = pager->num_pages;
    pager->num_pages += 1;

    void *left_node = get_page(pager, left_page_num);
    void *right_node = get_page(pager, right_page_num);

    pager_mark_dirty(pager, left_page_num);
    pager_mark_dirty(pager, right_page_num);

    initialize_internal_node(left_node);
    initialize_internal_node(right_node);

    *internal_node_num_keys(left_node) = left_num_keys;
    for (uint32_t i = 0; i < left_num_keys; i++) {
      *internal_node_key(left_node, i) = all_keys[i];
      *internal_node_pointer(left_node, i) = all_children[i];
    }
    *internal_node_rightmost_pointer(left_node) = all_children[promote_index];

    *internal_node_num_keys(right_node) = right_num_keys;
    for (uint32_t i = 0; i < right_num_keys; i++) {
      *internal_node_key(right_node, i) = all_keys[promote_index + 1 + i];
      *internal_node_pointer(right_node, i) =
          all_children[promote_index + 1 + i];
    }
    *internal_node_rightmost_pointer(right_node) = all_children[total_keys];

    *node_parent(left_node) = node_page_num;
    *node_parent(right_node) = node_page_num;
    pager_mark_dirty(pager, node_page_num);

    for (uint32_t i = 0; i <= left_num_keys; i++) {
      uint32_t child_page = (i < left_num_keys)
                                ? *internal_node_pointer(left_node, i)
                                : *internal_node_rightmost_pointer(left_node);
      *node_parent(get_page(pager, child_page)) = left_page_num;
    }
    for (uint32_t i = 0; i <= right_num_keys; i++) {
      uint32_t child_page = (i < right_num_keys)
                                ? *internal_node_pointer(right_node, i)
                                : *internal_node_rightmost_pointer(right_node);
      *node_parent(get_page(pager, child_page)) = right_page_num;
    }

    initialize_internal_node(node);
    *node_is_root_value(node) = 1;
    *internal_node_num_keys(node) = 1;
    *internal_node_key(node, 0) = promoted_key;
    *internal_node_pointer(node, 0) = left_page_num;
    *internal_node_rightmost_pointer(node) = right_page_num;
  } else {
    /* Non-root split: keep original page as left, create new right page,
       and propagate the promoted key up to the parent. */
    uint32_t right_page_num = pager->num_pages;
    pager->num_pages += 1;
    void *right_node = get_page(pager, right_page_num);
    pager_mark_dirty(pager, right_page_num);
    initialize_internal_node(right_node);

    /* Rebuild the original node as the left half. */
    initialize_internal_node(node);
    *node_parent(node) = old_parent_page;
    *internal_node_num_keys(node) = left_num_keys;
    for (uint32_t i = 0; i < left_num_keys; i++) {
      *internal_node_key(node, i) = all_keys[i];
      *internal_node_pointer(node, i) = all_children[i];
    }
    *internal_node_rightmost_pointer(node) = all_children[promote_index];

    /* Fill the right half. */
    *internal_node_num_keys(right_node) = right_num_keys;
    for (uint32_t i = 0; i < right_num_keys; i++) {
      *internal_node_key(right_node, i) = all_keys[promote_index + 1 + i];
      *internal_node_pointer(right_node, i) =
          all_children[promote_index + 1 + i];
    }
    *internal_node_rightmost_pointer(right_node) = all_children[total_keys];

    /* Update children's parent pointers. */
    for (uint32_t i = 0; i <= left_num_keys; i++) {
      uint32_t child_page = (i < left_num_keys)
                                ? *internal_node_pointer(node, i)
                                : *internal_node_rightmost_pointer(node);
      *node_parent(get_page(pager, child_page)) = node_page_num;
    }
    *node_parent(right_node) = old_parent_page;
    for (uint32_t i = 0; i <= right_num_keys; i++) {
      uint32_t child_page = (i < right_num_keys)
                                ? *internal_node_pointer(right_node, i)
                                : *internal_node_rightmost_pointer(right_node);
      *node_parent(get_page(pager, child_page)) = right_page_num;
    }

    /* Insert promoted key into parent. */
    void *parent = get_page(pager, old_parent_page);
    if (*internal_node_num_keys(parent) >= INTERNAL_NODE_MAX_KEYS) {
      internal_node_split(pager, old_parent_page, promoted_key, right_page_num);
    } else {
      internal_node_insert_key(pager, old_parent_page, promoted_key,
                               node_page_num, right_page_num);
    }
  }
}

/* Inserts a key/value pair into a leaf node at the cursor position. */
void leaf_node_insert(Cursor *cursor, uint32_t key, Record *record);

/* Splits a full leaf node, redistributes cells, and updates parent links. */
void leaf_node_split(Cursor *cursor, void *node, uint32_t key, Record *record) {
  uint32_t old_num_cells = *leaf_node_num_cells(node);
  uint32_t total_cells = old_num_cells + 1;
  uint32_t insertion_slot = cursor->slot_num;
  const uint32_t SPLIT_POINT = (total_cells + 1) / 2;
  uint8_t was_root = *node_is_root_value(node);
  uint32_t old_parent_page_num = *node_parent(node);

  uint32_t all_keys[LEAF_NODE_MAX_CELLS + 1];
  Record all_records[LEAF_NODE_MAX_CELLS + 1];

  for (uint32_t i = 0, j = 0; i < total_cells; i++) {
    if (i == insertion_slot) {
      all_keys[i] = key;
      all_records[i] = *record;
      continue;
    }

    all_keys[i] = leaf_node_key_at_slot(node, j);
    uint16_t cell_offset = *leaf_node_offset_value(node, (uint16_t)j);
    read_deserialized_record((uint8_t *)node + cell_offset +
                                 LEAF_NODE_CELL_KEY_SIZE,
                             &all_records[i], cursor->table->schema);
    j++;
  }

  Pager *pager = cursor->table->pager;
  uint32_t left_page_num;
  uint32_t right_page_num;
  void *left_page;
  void *right_page;

  if (!was_root) {
    left_page_num = cursor->page_num;
    right_page_num = pager->num_pages;
    pager->num_pages += 1;

    left_page = node;
  } else {
    left_page_num = pager->num_pages;
    right_page_num = pager->num_pages + 1;
    pager->num_pages += 2;

    left_page = get_page(pager, left_page_num);
  }
  uint32_t old_next = *leaf_node_next_pointer(node);
  right_page = get_page(pager, right_page_num);
  initialize_leaf_node(left_page);
  initialize_leaf_node(right_page);

  Cursor left_cursor = {
      .page_num = left_page_num,
      .slot_num = 0,
      .table = cursor->table,
      .is_end_of_table = 0,
  };

  Cursor right_cursor = {
      .page_num = right_page_num,
      .slot_num = 0,
      .table = cursor->table,
      .is_end_of_table = 0,
  };

  for (uint32_t i = 0; i < total_cells; i++) {
    if (i < SPLIT_POINT) {
      left_cursor.slot_num = *leaf_node_num_cells(left_page);
      leaf_node_insert(&left_cursor, all_keys[i], &all_records[i]);
    } else {
      right_cursor.slot_num = *leaf_node_num_cells(right_page);
      leaf_node_insert(&right_cursor, all_keys[i], &all_records[i]);
    }
  }

  *leaf_node_next_pointer(left_page) = right_page_num;
  *leaf_node_next_pointer(right_page) = old_next;

  pager_mark_dirty(pager, left_page_num);
  pager_mark_dirty(pager, right_page_num);

  if (!was_root) {
    *node_parent(left_page) = old_parent_page_num;
    *node_parent(right_page) = old_parent_page_num;

    void *parent_node = get_page(pager, old_parent_page_num);
    uint32_t separator_key =
        leaf_node_key_at_slot(left_page, *leaf_node_num_cells(left_page) - 1);

    pager_mark_dirty(pager, old_parent_page_num);

    if (*internal_node_num_keys(parent_node) >= INTERNAL_NODE_MAX_KEYS) {
      internal_node_split(pager, old_parent_page_num, separator_key,
                          right_page_num);
      return;
    }

    internal_node_insert_key(pager, old_parent_page_num, separator_key,
                             left_page_num, right_page_num);
  } else {
    uint32_t root_page_num = cursor->page_num;
    *node_parent(left_page) = root_page_num;
    *node_parent(right_page) = root_page_num;

    initialize_internal_node(node);
    *node_is_root_value(node) = 1;
    *internal_node_num_keys(node) = 1;
    *internal_node_pointer(node, 0) = left_page_num;
    *internal_node_rightmost_pointer(node) = right_page_num;
    *internal_node_key(node, 0) =
        leaf_node_key_at_slot(left_page, *leaf_node_num_cells(left_page) - 1);

    pager_mark_dirty(pager, root_page_num);
  }
}

uint32_t leaf_node_cell_size(Record *record) {
  uint32_t cell_size = LEAF_NODE_CELL_KEY_SIZE;
  for (size_t i = 0; i < record->num_values; i++) {
    Value curr_value = record->values[i];
    switch (curr_value.type) {
    case INT:
      cell_size += sizeof(uint32_t);
      break;
    case TEXT:
      cell_size += sizeof(uint32_t);
      cell_size += sizeof(char) * curr_value.text_val.len;
      break;
    default:
      break;
    }
  }
  return cell_size;
}

/* Inserts a new cell into a leaf, splitting first when free space is exhausted.
 */
void leaf_node_insert(Cursor *cursor, uint32_t key, Record *record) {
  void *node = get_page(cursor->table->pager, cursor->page_num);

  uint32_t free_start = *leaf_node_free_start(node);
  uint32_t free_end = *leaf_node_free_end(node);
  uint32_t cell_size = leaf_node_cell_size(record);
  uint32_t required_space = cell_size + LEAF_NODE_SLOT_SIZE;

  if (free_end < free_start || (free_end - free_start) < required_space) {
    leaf_node_split(cursor, node, key, record);
    return;
  }

  uint32_t num_cells = *leaf_node_num_cells(node);
  uint32_t slot_index = cursor->slot_num;

  uint32_t insertion_point = *leaf_node_free_end(node) - cell_size;

  for (uint32_t i = num_cells; i > slot_index; i--) {
    *leaf_node_offset_value(node, (uint16_t)i) =
        *leaf_node_offset_value(node, (uint16_t)(i - 1));
  }
  *(leaf_node_offset_value(node, slot_index)) = insertion_point;

  *(leaf_node_num_cells(node)) += 1;
  *(leaf_node_free_start(node)) += LEAF_NODE_SLOT_SIZE;
  *(leaf_node_free_end(node)) -= cell_size;

  uint8_t *cell = (uint8_t *)node + insertion_point;
  memcpy(cell, &key, LEAF_NODE_CELL_KEY_SIZE);
  write_serialized_record(record, cell + LEAF_NODE_CELL_KEY_SIZE);

  pager_mark_dirty(cursor->table->pager, cursor->page_num);
}

/* Traverses the B+ tree to locate a key. Returns a Cursor pointing to the
   key's position (or insertion point if absent). Sets *key_exists to 1 if the
   key was found, 0 otherwise. Caller is responsible for freeing the cursor. */
Cursor *find_key_cursor(Table *table, uint32_t key, int *key_exists) {
  Cursor *cursor = new_cursor_start(table);
  void *node = get_page(table->pager, cursor->page_num);

  while (*node_type_value(node) != NODE_TYPE_LEAF) {
    uint32_t left = 0;
    uint32_t right = *internal_node_num_keys(node);

    while (left < right) {
      uint32_t middle_index = left + (right - left) / 2;
      uint32_t middle_key = *internal_node_key(node, middle_index);

      if (key <= middle_key) {
        right = middle_index;
      } else {
        left = middle_index + 1;
      }
    }

    if (left == *internal_node_num_keys(node)) {
      cursor->page_num = *internal_node_rightmost_pointer(node);
    } else {
      cursor->page_num = *internal_node_pointer(node, left);
    }

    node = get_page(table->pager, cursor->page_num);
  }

  uint32_t leaf_page_num = cursor->page_num;
  free(cursor);

  Cursor *leaf_cursor = leaf_node_offset_find(table, leaf_page_num, key);
  uint32_t num_cells = *leaf_node_num_cells(node);
  *key_exists = (leaf_cursor->slot_num < num_cells &&
                 leaf_node_key_at_slot(node, leaf_cursor->slot_num) == key);

  return leaf_cursor;
}

/* Reads all cells from a leaf node into parallel key/record arrays.
   Returns the number of cells read. */
uint32_t leaf_node_read_all_cells(void *node, uint32_t *keys_out,
                                  Record *records_out, Schema *schema) {
  uint32_t num_cells = *leaf_node_num_cells(node);
  for (uint32_t i = 0; i < num_cells; i++) {
    keys_out[i] = leaf_node_key_at_slot(node, i);
    uint16_t cell_offset = *leaf_node_offset_value(node, (uint16_t)i);
    read_deserialized_record((uint8_t *)node + cell_offset +
                                 LEAF_NODE_CELL_KEY_SIZE,
                             &records_out[i], schema);
  }
  return num_cells;
}

/* Rebuilds a leaf node from scratch using the given key/record arrays.
   Preserves is_root and parent_ptr. Sets next_pointer to next_ptr.
   This reclaims fragmented cell-payload space left by simple deletes. */
void leaf_node_rebuild(void *node, uint32_t *keys, Record *records,
                       uint32_t count, uint32_t next_ptr) {
  uint8_t is_root = *node_is_root_value(node);
  uint32_t parent_page = *node_parent(node);

  initialize_leaf_node(node);
  *node_is_root_value(node) = is_root;
  *node_parent(node) = parent_page;
  *leaf_node_next_pointer(node) = next_ptr;

  for (uint32_t i = 0; i < count; i++) {
    uint32_t cell_size = leaf_node_cell_size(&records[i]);
    uint32_t insertion_point = *leaf_node_free_end(node) - cell_size;
    *leaf_node_offset_value(node, (uint16_t)i) = (uint16_t)insertion_point;

    uint8_t *cell = (uint8_t *)node + insertion_point;
    memcpy(cell, &keys[i], LEAF_NODE_CELL_KEY_SIZE);
    write_serialized_record(&records[i], cell + LEAF_NODE_CELL_KEY_SIZE);

    *leaf_node_num_cells(node) += 1;
    *leaf_node_free_start(node) += LEAF_NODE_SLOT_SIZE;
    *leaf_node_free_end(node) -= cell_size;
  }
}

/* Finds the index of child_page among the children of an internal node.
   Returns 0..num_keys-1 for pointer[i], or num_keys for the rightmost
   pointer. Returns -1 if not found (should not happen in a valid tree). */
int32_t find_child_index_in_parent(void *parent, uint32_t child_page) {
  uint32_t num_keys = *internal_node_num_keys(parent);
  for (uint32_t i = 0; i < num_keys; i++) {
    if (*internal_node_pointer(parent, i) == child_page)
      return (int32_t)i;
  }
  if (*internal_node_rightmost_pointer(parent) == child_page)
    return (int32_t)num_keys;
  return -1;
}

/* Returns the page number of the i-th child of an internal node.
   child_index = 0..num_keys-1 returns pointer[i];
   child_index = num_keys returns the rightmost pointer. */
uint32_t internal_node_child(void *node, uint32_t child_index) {
  uint32_t num_keys = *internal_node_num_keys(node);
  if (child_index < num_keys)
    return *internal_node_pointer(node, child_index);
  return *internal_node_rightmost_pointer(node);
}

/* Removes the separator key at key_index and the child pointer to its right
   from an internal node. Shifts remaining keys/pointers to fill the gap.

   After removal the node has (old_num_keys - 1) keys and correspondingly
   one fewer child. The caller must check for underflow separately. */
void internal_node_remove_key(void *node, uint32_t key_index) {
  uint32_t num_keys = *internal_node_num_keys(node);

  if (key_index == num_keys - 1) {
    /* Removing the last key: the right child of this key is the rightmost
       pointer. Replace rightmost with pointer[num_keys-1] (the left child
       of the removed key). */
    *internal_node_rightmost_pointer(node) =
        *internal_node_pointer(node, num_keys - 1);
  } else {
    /* Shift keys left to fill the gap at key_index. */
    for (uint32_t i = key_index; i < num_keys - 1; i++) {
      *internal_node_key(node, i) = *internal_node_key(node, i + 1);
    }
    /* Shift child pointers left: we are removing pointer[key_index + 1]
       (the right child of the deleted separator). */
    for (uint32_t i = key_index + 1; i < num_keys - 1; i++) {
      *internal_node_pointer(node, i) = *internal_node_pointer(node, i + 1);
    }
  }

  *internal_node_num_keys(node) = num_keys - 1;
}

/* When the root internal node has 0 keys and only a single child (the
   rightmost pointer), copy that child into page 0 to shrink the tree
   by one level. Updates grandchildren's parent pointers if needed. */
void collapse_root(Table *table) {
  void *root = get_page(table->pager, table->root_page_num);

  if (*node_type_value(root) != NODE_TYPE_INTERNAL)
    return;
  if (*internal_node_num_keys(root) > 0)
    return;

  uint32_t child_page_num = *internal_node_rightmost_pointer(root);
  void *child = get_page(table->pager, child_page_num);

  memcpy(root, child, PAGE_SIZE);
  *node_is_root_value(root) = 1;
  *node_parent(root) = 0;
  pager_mark_dirty(table->pager, table->root_page_num);

  /* If the promoted child was internal, fix its children's parent ptrs. */
  if (*node_type_value(root) == NODE_TYPE_INTERNAL) {
    uint32_t num_keys = *internal_node_num_keys(root);
    for (uint32_t i = 0; i < num_keys; i++) {
      void *grandchild =
          get_page(table->pager, *internal_node_pointer(root, i));
      *node_parent(grandchild) = table->root_page_num;
      pager_mark_dirty(table->pager, *internal_node_pointer(root, i));
    }
    void *last = get_page(table->pager, *internal_node_rightmost_pointer(root));
    *node_parent(last) = table->root_page_num;
    pager_mark_dirty(table->pager, *internal_node_rightmost_pointer(root));
  }
}

/*
  Redistribute cells between a leaf node and one of its siblings.
  Called when a leaf drops below LEAF_NODE_MIN_CELLS but a sibling has
  more cells than LEAF_NODE_MIN_CELLS (so it can spare one).
*/
void leaf_node_redistribute(Schema *schema, Pager *pager,
                            uint32_t node_page_num, uint32_t sibling_page_num,
                            uint32_t separator_index, int sibling_is_left) {
  void *node = get_page(pager, node_page_num);
  void *sibling = get_page(pager, sibling_page_num);

  uint32_t node_keys[LEAF_NODE_MAX_CELLS + 1];
  Record node_records[LEAF_NODE_MAX_CELLS + 1];
  uint32_t node_n =
      leaf_node_read_all_cells(node, node_keys, node_records, schema);

  uint32_t sib_keys[LEAF_NODE_MAX_CELLS];
  Record sib_records[LEAF_NODE_MAX_CELLS];
  uint32_t sib_n =
      leaf_node_read_all_cells(sibling, sib_keys, sib_records, schema);

  /* Move one cell from the sibling to the node. */
  if (sibling_is_left) {
    memmove(node_keys + 1, node_keys, node_n * sizeof(uint32_t));
    memmove(node_records + 1, node_records, node_n * sizeof(Record));
    node_keys[0] = sib_keys[sib_n - 1];
    node_records[0] = sib_records[sib_n - 1];
  } else {
    node_keys[node_n] = sib_keys[0];
    node_records[node_n] = sib_records[0];
    memmove(sib_keys, sib_keys + 1, sib_n * sizeof(uint32_t));
    memmove(sib_records, sib_records + 1, sib_n * sizeof(Record));
  }
  ++node_n;
  --sib_n;

  /* Rebuild both pages from the modified arrays, redistribution does
     not change the leaf linked-list order. */
  uint32_t node_next = *leaf_node_next_pointer(node);
  uint32_t sib_next = *leaf_node_next_pointer(sibling);
  leaf_node_rebuild(node, node_keys, node_records, node_n, node_next);
  leaf_node_rebuild(sibling, sib_keys, sib_records, sib_n, sib_next);

  /* Update the parent's separator key. */
  void *parent = get_page(pager, *node_parent(node));

  void *left_child = sibling_is_left ? sibling : node;
  uint32_t left_num_cells = *leaf_node_num_cells(left_child);

  *internal_node_key(parent, separator_index) =
      leaf_node_key_at_slot(left_child, left_num_cells - 1);

  pager_mark_dirty(pager, node_page_num);
  pager_mark_dirty(pager, sibling_page_num);
  pager_mark_dirty(pager, *node_parent(node));
}

/* Merge the RIGHT leaf into the LEFT leaf */
void leaf_node_merge(Schema *schema, Pager *pager, uint32_t left_page_num,
                     uint32_t right_page_num, uint32_t separator_index) {
  void *left_node = get_page(pager, left_page_num);
  void *right_node = get_page(pager, right_page_num);

  uint32_t left_node_keys[LEAF_NODE_MAX_CELLS + 1];
  Record left_node_records[LEAF_NODE_MAX_CELLS + 1];
  uint32_t left_n = leaf_node_read_all_cells(left_node, left_node_keys,
                                             left_node_records, schema);

  uint32_t right_node_keys[LEAF_NODE_MAX_CELLS + 1];
  Record right_node_records[LEAF_NODE_MAX_CELLS + 1];
  uint32_t right_n = leaf_node_read_all_cells(right_node, right_node_keys,
                                              right_node_records, schema);

  uint32_t all_keys[LEAF_NODE_MAX_CELLS + 1];
  Record all_records[LEAF_NODE_MAX_CELLS + 1];

  for (uint32_t i = 0; i < left_n + right_n; ++i) {
    if (i < left_n) {
      all_keys[i] = left_node_keys[i];
      all_records[i] = left_node_records[i];
    } else {
      all_keys[i] = right_node_keys[i - left_n];
      all_records[i] = right_node_records[i - left_n];
    }
  }

  uint32_t next_ptr = *leaf_node_next_pointer(right_node);
  leaf_node_rebuild(left_node, all_keys, all_records, left_n + right_n,
                    next_ptr);

  uint32_t parent_page_num = *node_parent(left_node);
  void *parent = get_page(pager, parent_page_num);
  internal_node_remove_key(parent, separator_index);

  pager_mark_dirty(pager, left_page_num);
  pager_mark_dirty(pager, parent_page_num);

  /* Evict the now-orphaned right page from the pager cache. Clear its dirty
     flag first so flush_to_wal won't log a freed page. */
  pager->dirty[right_page_num] = 0;
  pager->pages[right_page_num] = NULL;
  free(right_node);
}

/* Number of children in an internal node is always num_keys + 1. */
uint32_t internal_node_num_children(void *node) {
  return *internal_node_num_keys(node) + 1;
}

/* Redistribution for internal-node underflow. */
void internal_node_redistribute(Pager *pager, uint32_t node_page_num,
                                uint32_t sibling_page_num,
                                uint32_t separator_index, int sibling_is_left) {
  void *node = get_page(pager, node_page_num);
  void *sibling = get_page(pager, sibling_page_num);

  uint32_t parent_page_num = *node_parent(node);
  void *parent = get_page(pager, parent_page_num);

  uint32_t node_num_keys = *internal_node_num_keys(node);
  uint32_t sibling_num_keys = *internal_node_num_keys(sibling);

  if (sibling_is_left) {
    memmove((uint8_t *)node + INTERNAL_NODE_HEADER_SIZE +
                INTERNAL_NODE_PAIR_SIZE,
            (uint8_t *)node + INTERNAL_NODE_HEADER_SIZE,
            node_num_keys * INTERNAL_NODE_PAIR_SIZE);

    uint32_t moved_child = *internal_node_rightmost_pointer(sibling);
    *internal_node_pointer(node, 0) = moved_child;

    // Bring parent's separator key down into node as node's new first key.
    *internal_node_key(node, 0) = *internal_node_key(parent, separator_index);

    // Move sibling's last key up to parent[separator_index].
    *internal_node_key(parent, separator_index) =
        *internal_node_key(sibling, sibling_num_keys - 1);

    // Move sibling's rightmost child to become node's new first child.
    *internal_node_rightmost_pointer(sibling) =
        *internal_node_pointer(sibling, sibling_num_keys - 1);

    *node_parent(get_page(pager, moved_child)) = node_page_num;
    pager_mark_dirty(pager, moved_child);
  } else {
    uint32_t moved_child = internal_node_child(sibling, 0);
    uint32_t old_rightmost_child = *internal_node_rightmost_pointer(node);

    // Bring parent's separator key down into node as node's new last key.
    *internal_node_key(node, node_num_keys) =
        *internal_node_key(parent, separator_index);

    *internal_node_pointer(node, node_num_keys) = old_rightmost_child;

    // Move sibling's leftmost child to become node's new rightmost child.
    *internal_node_rightmost_pointer(node) = moved_child;

    // Move sibling's first key up to parent[separator_index].
    *internal_node_key(parent, separator_index) =
        *internal_node_key(sibling, 0);

    memmove((uint8_t *)sibling + INTERNAL_NODE_HEADER_SIZE,
            (uint8_t *)sibling + INTERNAL_NODE_HEADER_SIZE +
                INTERNAL_NODE_PAIR_SIZE,
            (sibling_num_keys - 1) * INTERNAL_NODE_PAIR_SIZE);

    *node_parent(get_page(pager, moved_child)) = node_page_num;
    pager_mark_dirty(pager, moved_child);
  }

  *internal_node_num_keys(node) = node_num_keys + 1;
  *internal_node_num_keys(sibling) = sibling_num_keys - 1;

  pager_mark_dirty(pager, node_page_num);
  pager_mark_dirty(pager, sibling_page_num);
  pager_mark_dirty(pager, parent_page_num);
}

/* Implement merge for internal-node underflow. */
void internal_node_merge(Pager *pager, uint32_t left_page_num,
                         uint32_t right_page_num, uint32_t separator_index) {
  void *left_node = get_page(pager, left_page_num);
  void *right_node = get_page(pager, right_page_num);

  uint32_t parent_node_num = *node_parent(left_node);
  void *parent = get_page(pager, parent_node_num);

  uint32_t left_node_num_keys = *internal_node_num_keys(left_node);
  uint32_t right_node_num_keys = *internal_node_num_keys(right_node);

  /* Place bridge key at pair[L] and left's old rightmost as its left child. */
  *internal_node_key(left_node, left_node_num_keys) =
      *internal_node_key(parent, separator_index);
  *internal_node_pointer(left_node, left_node_num_keys) =
      *internal_node_rightmost_pointer(left_node);

  /* Append right's keys and children starting at pair[L+1]. */
  for (uint32_t i = 0; i < right_node_num_keys; ++i) {
    *internal_node_key(left_node, left_node_num_keys + 1 + i) =
        *internal_node_key(right_node, i);

    uint32_t node_num_to_append = *internal_node_pointer(right_node, i);
    *internal_node_pointer(left_node, left_node_num_keys + 1 + i) =
        node_num_to_append;

    void *node_to_append = get_page(pager, node_num_to_append);
    *node_parent(node_to_append) = left_page_num;
    pager_mark_dirty(pager, node_num_to_append);
  }

  /* Transfer right's rightmost child to left, update its parent pointer. */
  uint32_t right_rightmost = *internal_node_rightmost_pointer(right_node);
  *internal_node_rightmost_pointer(left_node) = right_rightmost;
  *node_parent(get_page(pager, right_rightmost)) = left_page_num;
  pager_mark_dirty(pager, right_rightmost);

  /* Update left's key count: bridge key + all of right's keys. */
  *internal_node_num_keys(left_node) =
      left_node_num_keys + 1 + right_node_num_keys;

  internal_node_remove_key(parent, separator_index);

  pager_mark_dirty(pager, left_page_num);
  pager_mark_dirty(pager, parent_node_num);

  /* Evict the right page from the pager cache without a double-free. Clear its
     dirty flag first so flush_to_wal won't log a freed page. */
  pager->dirty[right_page_num] = 0;
  pager->pages[right_page_num] = NULL;
  free(right_node);
}

void handle_internal_node_underflow(Pager *pager, uint32_t page_num,
                                    void *parent, int32_t child_idx,
                                    uint32_t num_parent_keys) {
  int has_left = (child_idx > 0);
  int has_right = ((uint32_t)child_idx < num_parent_keys);

  uint32_t left_sib_page = 0, right_sib_page = 0;
  uint32_t left_sep = 0, right_sep = 0;

  if (has_left) {
    left_sib_page = internal_node_child(parent, child_idx - 1);
    left_sep = (uint32_t)child_idx - 1;
  }
  if (has_right) {
    right_sib_page = internal_node_child(parent, child_idx + 1);
    right_sep = (uint32_t)child_idx;
  }

  if (has_left && *internal_node_num_keys(get_page(pager, left_sib_page)) >
                      INTERNAL_NODE_MIN_KEYS) {
    internal_node_redistribute(pager, page_num, left_sib_page, left_sep, 1);
    return;
  }
  if (has_right && *internal_node_num_keys(get_page(pager, right_sib_page)) >
                       INTERNAL_NODE_MIN_KEYS) {
    internal_node_redistribute(pager, page_num, right_sib_page, right_sep, 0);
    return;
  }

  if (has_left) {
    internal_node_merge(pager, left_sib_page, page_num, left_sep);
  } else {
    internal_node_merge(pager, page_num, right_sib_page, right_sep);
  }
}

/* Handle underflow after a deletion causes a node to drop below
   minimum occupancy. */
void handle_underflow(Pager *pager, Table *table, uint32_t page_num) {
  void *node = get_page(pager, page_num);

  if (*node_is_root_value(node)) {
    if (*node_type_value(node) == NODE_TYPE_LEAF)
      return;
    else
      collapse_root(table);
    return;
  }

  uint32_t parent_page = *node_parent(node);
  void *parent = get_page(pager, parent_page);
  int32_t child_idx = find_child_index_in_parent(parent, page_num);
  uint32_t num_parent_keys = *internal_node_num_keys(parent);

  /* Identify siblings. Not every node has both. */
  int has_left = (child_idx > 0);
  int has_right = ((uint32_t)child_idx < num_parent_keys);

  uint32_t left_sib_page = 0, right_sib_page = 0;
  uint32_t left_sep = 0, right_sep = 0;

  if (has_left) {
    left_sib_page = internal_node_child(parent, child_idx - 1);
    left_sep = child_idx - 1;
  }
  if (has_right) {
    right_sib_page = internal_node_child(parent, child_idx + 1);
    right_sep = child_idx;
  }

  if (*node_type_value(node) == NODE_TYPE_LEAF) {
    /* Try redistribute first: pick a sibling above minimum. */
    if (has_left && *leaf_node_num_cells(get_page(pager, left_sib_page)) >
                        LEAF_NODE_MIN_CELLS) {
      leaf_node_redistribute(table->schema, pager, page_num, left_sib_page,
                             left_sep, 1);
      return;
    }
    if (has_right && *leaf_node_num_cells(get_page(pager, right_sib_page)) >
                         LEAF_NODE_MIN_CELLS) {
      leaf_node_redistribute(table->schema, pager, page_num, right_sib_page,
                             right_sep, 0);
      return;
    }

    /* No sibling can spare — merge. Convention: left receives, right is
       discarded. Decide which role the underflowed node plays. */
    if (has_left) {
      leaf_node_merge(table->schema, pager, left_sib_page, page_num, left_sep);
    } else {
      leaf_node_merge(table->schema, pager, page_num, right_sib_page,
                      right_sep);
    }
  } else if (*node_type_value(node) == NODE_TYPE_INTERNAL) {
    handle_internal_node_underflow(pager, page_num, parent, child_idx,
                                   num_parent_keys);
  }

  /* After a merge the parent lost a key — check for parent underflow. */
  uint32_t parent_keys = *internal_node_num_keys(parent);
  if (*node_is_root_value(parent) && parent_keys == 0)
    collapse_root(table);
  else if (!*node_is_root_value(parent) && parent_keys < INTERNAL_NODE_MIN_KEYS)
    handle_underflow(pager, table, parent_page);
}