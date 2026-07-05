#include "wal.h"
#include "pager.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// Opens or creates the WAL file.
WAL *new_wal(char *db_filename) {
  size_t len = strlen(db_filename) + 4 + 1;
  char *wal_filename = malloc(len);
  if (!wal_filename)
    return NULL;

  snprintf(wal_filename, len, "%s-wal", db_filename);

  int fd = open(wal_filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
  if (fd == -1) {
    printf("Unable to open WAL file\n");
    exit(EXIT_FAILURE);
  }

  off_t file_length = lseek(fd, 0, SEEK_END);

  WAL *wal = (WAL *)malloc(sizeof(WAL));
  wal->fd = fd;
  wal->filename = wal_filename;
  wal->file_length = file_length;
  wal->curr_txid = 0;

  return wal;
}

void write_serialized_wal_entry(WALEntry *source, void *destination) {
  memcpy(destination + WAL_TYPE_OFFSET, &(source->type), WAL_TYPE_SIZE);
  memcpy(destination + WAL_LSN_OFFSET, &(source->lsn), WAL_LSN_SIZE);
  memcpy(destination + WAL_TXID_OFFSET, &(source->txid), WAL_TXID_SIZE);
  memcpy(destination + WAL_PAGE_NUM_OFFSET, &(source->page_num),
         WAL_PAGE_NUM_SIZE);
  memcpy(destination + WAL_LENGTH_OFFSET, &(source->length), WAL_LENGTH_SIZE);
  memcpy(destination + WAL_CHECKSUM_OFFSET, &(source->checksum),
         WAL_CHECKSUM_SIZE);
}

void read_deserialized_wal_entry(void *source, WALEntry *destination) {
  memcpy(&(destination->type), source + WAL_TYPE_OFFSET, WAL_TYPE_SIZE);
  memcpy(&(destination->lsn), source + WAL_LSN_OFFSET, WAL_LSN_SIZE);
  memcpy(&(destination->txid), source + WAL_TXID_OFFSET, WAL_TXID_SIZE);
  memcpy(&(destination->page_num), source + WAL_PAGE_NUM_OFFSET,
         WAL_PAGE_NUM_SIZE);
  memcpy(&(destination->length), source + WAL_LENGTH_OFFSET, WAL_LENGTH_SIZE);
  memcpy(&(destination->checksum), source + WAL_CHECKSUM_OFFSET,
         WAL_CHECKSUM_SIZE);
}

/* Additive byte-sum over length bytes, used to detect torn/corrupt records. */
uint32_t wal_checksum(void *data, uint32_t length) {
  uint32_t sum = 0;
  uint8_t *bytes = (uint8_t *)data;
  for (uint32_t i = 0; i < length; i++) {
    sum += bytes[i];
  }
  return sum;
}

/* Appends ONE page-change record to the WAL. Returns the LSN the
 record was written at. */
off_t wal_append_page(WAL *wal, uint32_t txid, uint32_t page_num,
                      void *page_image) {
  WALEntry entry;
  entry.lsn = wal->file_length;
  entry.txid = txid;
  entry.type = PAGE_CHANGE;
  entry.length = PAGE_SIZE;
  entry.page_num = page_num;

  uint8_t *wal_record = malloc(WAL_COMMON_HEADER_SIZE);
  write_serialized_wal_entry(&entry, wal_record);

  uint32_t checksum = wal_checksum(wal_record + WAL_CHECKSUM_SIZE,
                                   WAL_COMMON_HEADER_SIZE - WAL_CHECKSUM_SIZE) +
                      wal_checksum(page_image, PAGE_SIZE);
  memcpy(wal_record + WAL_CHECKSUM_OFFSET, &checksum, WAL_CHECKSUM_SIZE);

  pwrite(wal->fd, wal_record, WAL_COMMON_HEADER_SIZE, wal->file_length);
  pwrite(wal->fd, page_image, PAGE_SIZE,
         wal->file_length + WAL_COMMON_HEADER_SIZE);

  off_t lsn = wal->file_length;
  wal->file_length += WAL_COMMON_HEADER_SIZE + PAGE_SIZE;

  free(wal_record);

  return lsn;
}

/* Writes the COMMIT record for txid and issues the single fsync that makes
 the whole transaction durable. */
void wal_commit(WAL *wal, uint32_t txid) {
  WALEntry entry;
  entry.lsn = wal->file_length;
  entry.txid = txid;
  entry.type = COMMIT;
  entry.length = 0;
  entry.page_num = -1;

  uint8_t *wal_record = malloc(WAL_COMMON_HEADER_SIZE);

  write_serialized_wal_entry(&entry, wal_record);

  /* Header-only record: checksum covers the header minus the checksum field. */
  uint32_t checksum = wal_checksum(wal_record + WAL_CHECKSUM_SIZE,
                                   WAL_COMMON_HEADER_SIZE - WAL_CHECKSUM_SIZE);
  memcpy(wal_record + WAL_CHECKSUM_OFFSET, &checksum, WAL_CHECKSUM_SIZE);

  pwrite(wal->fd, wal_record, WAL_COMMON_HEADER_SIZE, wal->file_length);
  fsync(wal->fd);

  free(wal_record);
  wal->file_length += WAL_COMMON_HEADER_SIZE;
}

void wal_clean(WAL *wal) {
  ftruncate(wal->fd, 0);
  lseek(wal->fd, 0, SEEK_SET);
  wal->file_length = 0;
}

void wal_recover(Pager *pager) {
  uint8_t header[WAL_COMMON_HEADER_SIZE];
  uint8_t payload[PAGE_SIZE];
  off_t wal_offset = 0;

  typedef struct {
    uint32_t page_num;
    uint8_t image[PAGE_SIZE];
  } BufferedPage;

  struct PagesBuffer {
    ssize_t size;
    ssize_t capacity;
    BufferedPage *bufferedPages;
  } PageBuffer;

  PageBuffer.size = 0;
  PageBuffer.capacity = 2;
  PageBuffer.bufferedPages = malloc(2 * sizeof(BufferedPage));

  while (1) {
    ssize_t n =
        pread(pager->wal->fd, header, WAL_COMMON_HEADER_SIZE, wal_offset);
    if (n < (ssize_t)WAL_COMMON_HEADER_SIZE) {
      break;
    }

    WALEntry entry = {0};
    read_deserialized_wal_entry(header, &entry);

    // Length must match the record type
    uint32_t expected_length;
    if (entry.type == PAGE_CHANGE) {
      expected_length = PAGE_SIZE;
    } else if (entry.type == COMMIT) {
      expected_length = 0;
    } else {
      break;
    }
    if (entry.length != expected_length) {
      break;
    }

    if (entry.length > 0) {
      n = pread(pager->wal->fd, payload, entry.length,
                wal_offset + WAL_COMMON_HEADER_SIZE);
      if (n < (ssize_t)entry.length) {
        break;
      }
    }

    // Recompute the checksum exactly as it was written
    uint32_t computed = wal_checksum(
        header + WAL_CHECKSUM_SIZE, WAL_COMMON_HEADER_SIZE - WAL_CHECKSUM_SIZE);
    if (entry.length > 0) {
      computed += wal_checksum(payload, entry.length);
    }
    if (computed != entry.checksum) {
      break;
    }

    if (entry.type == COMMIT) {
      for (int i = 0; i < PageBuffer.size; i++) {
        uint32_t page_num = PageBuffer.bufferedPages[i].page_num;
        void *image = PageBuffer.bufferedPages[i].image;
        pwrite(pager->fd, image, PAGE_SIZE, PAGE_SIZE * page_num);
      }
      PageBuffer.size = 0;
    } else {
      if (PageBuffer.size >= PageBuffer.capacity) {
        ssize_t new_capacity = PageBuffer.capacity * 2;
        void *temp = realloc(PageBuffer.bufferedPages,
                             new_capacity * sizeof(BufferedPage));

        if (temp == NULL) {
          printf("Error realocating buffered pages");
          exit(EXIT_FAILURE);
        }

        PageBuffer.bufferedPages = temp;
        PageBuffer.capacity = new_capacity;
      }
      PageBuffer.bufferedPages[PageBuffer.size].page_num = entry.page_num;
      memcpy(PageBuffer.bufferedPages[PageBuffer.size].image, payload,
             PAGE_SIZE);
      PageBuffer.size++;
    }

    wal_offset += WAL_COMMON_HEADER_SIZE + entry.length;
  }
  pager->file_length = lseek(pager->fd, 0, SEEK_END);
  pager->num_pages = pager->file_length / PAGE_SIZE;

  fsync(pager->fd);
  free(PageBuffer.bufferedPages);
  wal_clean(pager->wal);
}

/* Frees WAL resources. */
void wal_close(WAL *wal) {
  close(wal->fd);
  free(wal->filename);
  free(wal);
}

void flush_to_wal(Pager *pager) {
  for (int i = 0; i < TABLE_MAX_PAGES; i++) {
    if (pager->dirty[i] != 0) {
      wal_append_page(pager->wal, pager->wal->curr_txid, i, pager->pages[i]);
      pager->dirty[i] = 0;
    }
  }
  wal_commit(pager->wal, pager->wal->curr_txid);
  pager->wal->curr_txid++;
}