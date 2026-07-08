#ifndef PAGER_H
#define PAGER_H

#include "common.h"
#include "wal.h"

#include <stdint.h>

typedef struct Pager {
  void *pages[TABLE_MAX_PAGES];
  uint8_t dirty[TABLE_MAX_PAGES];
  off_t file_length;
  uint32_t num_pages;
  char *filename;
  int fd;
  WAL *wal;
} Pager;

typedef struct {
  Pager *pager;
  Schema *schema;
  uint32_t root_page_num;
  uint32_t rowid_counter;
} Table;

Pager *new_pager(char *filename);
void  *get_page(Pager *pager, uint32_t page_num);
void   flush_page(Pager *pager, uint32_t num_page);
void   pager_mark_dirty(Pager *pager, uint32_t page_num);
Table *open_db(char *filename);
void   close_db(Table *table);

#endif /* PAGER_H */
