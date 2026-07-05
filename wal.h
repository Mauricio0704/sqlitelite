#ifndef WAL_H
#define WAL_H

#include <stdint.h>
#include <sys/types.h>

typedef struct Pager Pager; /* forward declaration; defined in pager.h */


typedef enum { COMMIT, CHECKPOINT, PAGE_CHANGE } WALEntryType;

/* In-memory view of one WAL record's fixed header. Payload is handled
 * separately */
typedef struct {
  WALEntryType type;
  uint32_t checksum;
  off_t lsn;
  uint32_t txid;
  uint32_t page_num;
  uint32_t length;
} WALEntry;

/* WAL on-disk header layout. enum (not `const`) so these compile-time constants
 * have no linkage and don't collide across the TUs that include this header. */
enum {
  WAL_CHECKSUM_SIZE = sizeof(uint32_t),
  WAL_CHECKSUM_OFFSET = 0,
  WAL_TYPE_SIZE = sizeof(uint8_t),
  WAL_TYPE_OFFSET = WAL_CHECKSUM_OFFSET + WAL_CHECKSUM_SIZE,
  WAL_LSN_SIZE = sizeof(off_t),
  WAL_LSN_OFFSET = WAL_TYPE_OFFSET + WAL_TYPE_SIZE,
  WAL_TXID_SIZE = sizeof(uint32_t),
  WAL_TXID_OFFSET = WAL_LSN_OFFSET + WAL_LSN_SIZE,
  WAL_PAGE_NUM_SIZE = sizeof(uint32_t),
  WAL_PAGE_NUM_OFFSET = WAL_TXID_OFFSET + WAL_TXID_SIZE,
  WAL_LENGTH_SIZE = sizeof(uint32_t),
  WAL_LENGTH_OFFSET = WAL_PAGE_NUM_OFFSET + WAL_PAGE_NUM_SIZE,
  WAL_COMMON_HEADER_SIZE = WAL_LENGTH_OFFSET + WAL_LENGTH_SIZE,
};

typedef struct {
  off_t file_length;
  char *filename;
  int fd;
  uint32_t curr_txid;
} WAL;

WAL *new_wal(char *db_filename);
uint32_t wal_checksum(void *data, uint32_t length);
void write_serialized_wal_entry(WALEntry *src, void *dst);
void read_deserialized_wal_entry(void *src, WALEntry *dst);
off_t wal_append_page(WAL *wal, uint32_t txid, uint32_t page_num, void *page_image);
void wal_commit(WAL *wal, uint32_t txid);
void wal_clean(WAL *wal);              // truncate after checkpoint
void wal_recover(Pager *pager);
void wal_close(WAL *wal);
void flush_to_wal(Pager *pager);

#endif /* WAL_H */
