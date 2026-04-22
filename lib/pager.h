/// Copyright 2026 Theo Lincke
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.

#pragma once

#include "c_specx/concurrency/periodic_task.h"
#include "errors.h"
#include "lockt/lock_table.h"
#include "os_pager/os_pager.h"
#include "pager/page_h.h"
#include "txns/txn.h"
#include "wal/os_wal.h"
#include "wal/wal_rec_hdr.h"

/**
 * Database structure:
 *
 * Every FS_BTMP_NPGS pages starts with a free space
 * map, which tracks it's section's free pages. It tracks
 * pages [self, self + FS_BTMP_NPGS)
 *
 * 0                         - FSM [0, FS_BTMP_NPGS)
 * 1                         - Root
 * 2                         - PAGE
 * 3                         - PAGE
 * 4                         - PAGE
 *
 * ...
 *
 * FS_BTMP_NPGS              - FSM [FS_BTMP_NPGS, 2 * FS_BTMP_NPGS)
 * FS_BTMP_NPGS + 1          - PAGE
 * FS_BTMP_NPGS + 2          - PAGE
 *
 * ...
 *
 * 2 * FS_BTMP_NPGS          - FSM [2 * FS_BTMP_NPGS, 3 * FS_BTMP_NPGS)
 * 2 * FS_BTMP_NPGS + 1      - PAGE
 * 2 * FS_BTMP_NPGS + 2      - PAGE
 *
 * ...
 */

// Special Page Numbers
#define ROOT_PGNO ((pgno)1)  // Root page
#define VHASH_PGNO ((pgno)2) // Free space map start

/**
 * Flags to help the buffer manager know
 * which pages are occupied, accessed or writable
 */
enum
{
  PW_ACCESS = 1u << 0, // meaningless for X pages
  PW_DIRTY = 1u << 1,  // meaningless for X pages
  PW_PRESENT = 1u << 2,
  PW_X = 1u << 3,
};

/**
 * Pager property flags
 */
enum
{
  PGR_ISNEW = 1u << 0,        // This is a pager on a new database file
  PGR_ISRESTARTING = 1u << 1, // Pager is currently restarting
};

// Robin hood hash table for buffer pool
#define KTYPE pgno
#define VTYPE u32
#define SUFFIX idx
#include "c_specx/ds/robin_hood_ht.h"
#undef KTYPE
#undef VTYPE
#undef SUFFIX

struct pager
{
  // Resources
  struct os_pager *fp; // OS pager abstraction (e.g. file_pager)
  struct os_wal *ww;   // Write-ahead log abstraction
  struct lockt *lt;    // Lock table

  bool iown_fp;
  bool iown_ww;
  bool iown_lt;

  struct dpg_table *dpt;  // Dirty Page Table
  struct txn_table *tnxt; // Transaction Table

  /**
   * A hash table of pgno -> index within the buffer pool
   *
   * It's a static hash table because you can never
   * have more pages in this hash table than there are
   * pages in the buffer pool
   */
  hash_table_idx pgno_to_value;
  hentry_idx _hdata[MEMORY_PAGE_LEN];

  /**
   * The actual buffer pool
   *
   * This is very large and takes up the most of the space
   * of pager. Pager should never be stack allocated because of
   * the buffer pool
   */
  struct page_frame pages[MEMORY_PAGE_LEN];

  /**
   * The pager uses a poor man's LRU algorithm
   * for evicting pages. Clock just cycles through
   * the buffer pool and finds the next available page
   */
  u32 clock;

  // Transaction control
  txid next_tid;

  int flags;

  struct periodic_task checkpoint_task;
};

DEFINE_DBG_ASSERT (struct pager, pager, p, {
  ASSERT (p);
  ASSERT (p->clock < MEMORY_PAGE_LEN);
})

struct pager *pgr_open_single_file (const char *dbname, error *e);
err_t pgr_delete_single_file (const char *dbname, error *e);
struct pager *pgr_open (struct os_pager *fp, struct os_wal *ww, struct lockt *lt, error *e);
err_t pgr_close (struct pager *p, error *e);
bool pgr_isnew (const struct pager *p);
void pgr_attach_lock_table (struct pager *p, struct lockt *lt);

// Utils
p_size pgr_get_npages (const struct pager *p);
void i_log_page_table (int log_level, bool only_present, const struct pager *p);

// Transaction control
err_t pgr_begin_txn (struct txn *tx, struct pager *p, error *e);
err_t pgr_commit (struct pager *p, struct txn *tx, error *e);
// slsn pgr_savepoint (struct pager *p, struct txn *t, error *e);
err_t pgr_rollback (struct pager *p, struct txn *tx, lsn save_lsn, error *e);

err_t pgr_checkpoint (struct pager *p, error *e);
err_t pgr_deletion_blocking_checkpoint (struct pager *p, error *e);
err_t pgr_launch_checkpoint_thread (struct pager *p, u64 msec, error *e);

/**
 * Gets a page in read mode.
 *
 * Pass in [flags] to verify that the page is what you
 * want to read. Pass PG_PERMISSVE if you don't want to
 * do any validation checks when reading
 *
 * This page is read in in READ MODE (S lock). To
 * make it writable, either call pgr_make_writable
 * or use pgr_get_writable instead
 */
err_t pgr_get (page_h *dest, int flags, pgno pgno, struct pager *p, error *e);

/**
 * Creates a new page.
 *
 * Pass in [ptype] to initialize the page
 * for the type. Note that page initialization
 * functions do NOT ensure valid pages. So
 * you usually can't just do
 *
 * pgr_new()
 * pgr_release();
 *
 * You'll need to modify the page in some way to make it
 * valid
 */
err_t pgr_new (
    page_h *dest,
    struct pager *p,
    struct txn *tx,
    enum page_type ptype,
    error *e);

/**
 * Upgrades a readable page to a writable page
 *
 * This page must be readable
 *
 * Under the hood, this copies the page into a new spot in the buffer pool.
 * So you may get buffer pool out of memory errors
 */
err_t pgr_make_writable (struct pager *p, struct txn *tx, page_h *h, error *e);

// Permissive - page can already be writable
err_t pgr_maybe_make_writable (
    struct pager *p,
    struct txn *tx,
    page_h *cur,
    error *e);

// Gets page in writable mode
err_t pgr_get_writable (
    page_h *dest,
    struct txn *tx,
    int flags,
    pgno pg,
    struct pager *p,
    error *e);

// Deletes this page and releases it in the buffer pool
err_t pgr_delete_and_release (
    struct pager *p,
    struct txn *tx,
    page_h *h,
    error *e);

/**
 * Pages are owned by functions. All pages must be released
 * on exit. If the page is in write mode (X lock), then
 *
 * 1. The page is checked to see if it's valid based on [flags]
 * 2. an UPDATE record is appended to the log
 *
 * Then the page is released from the buffer pool
 *
 * Otherwise, the page is just released if it's in read mode
 */
err_t pgr_release (struct pager *p, page_h *h, int flags, error *e);

/**
 * This method is unsafe and should not be used outside tests
 * because it puts the buffer pool in an unknown state if
 * pgr_flush fails
 */
err_t pgr_release_with_flush (struct pager *p, page_h *h, int flags, error *e);
err_t pgr_release_with_evict (struct pager *p, page_h *h, int flags, error *e);

// Release the page if it's not NONE
err_t pgr_release_if_exists (struct pager *p, page_h *h, int flags, error *e);

/**
 * This is the most explicit release - you can specify your
 * own WAL record. The exception is using this method.
 *
 * SmartFiles operates on the premise that most WAL updates are
 * physical page updates because data is so tightly packed.
 *
 * Smaller non physical updates must use this function instead
 */
err_t pgr_release_with_log (
    struct pager *p,
    page_h *h,
    int flags,
    struct wal_update_write *record, // if NULL, appends a physical page update
    error *e);

err_t pgr_release_without_log (struct pager *p, page_h *h, int flags, error *e);

void pgr_unfix (struct pager *p, page_h *h, int flags);

/**
 * When error handling, use this instead of pgr_release, if you want
 * to cancel a page write
 */
void pgr_cancel (const struct pager *p, page_h *h);
void pgr_cancel_if_exists (struct pager *p, page_h *h);

/**
 * Finds the next open and available spot to
 * create a new page. This is thread unsafe because
 * it simply returns the index without setting it or anything.
 *
 * So it's the callers job to lock the pager before running this
 * and also to set the page PRESENT flag accordingly
 */
i32 pgr_reserve_at_clock_thread_unsafe (struct pager *p, error *e);

/**
 * Page eviction:
 *
 * 1. Flush the contents of the page to disk
 * 2. Say that this page is no longer occupied for the next
 *    clock pass around
 *
 * Method is thread unsafe
 */
err_t pgr_evict (struct pager *p, struct page_frame *mp, error *e);

/**
 * Page flush:
 *
 * - If page is dirty
 *    - If not restarting:
 *      - Flush page_lsn to the wal
 *    - Flush page to database file
 *    - remove the page from the dirty page table
 */
err_t pgr_flush (const struct pager *p, struct page_frame *mp, error *e);

/**
 * Extends the file and appends a Nested Top Action WAL record
 *
 * 1. Append NTE record
 * 2. Extend the file to [npages]
 */
err_t pgr_extend_file (const struct pager *p, pgno npages, struct txn *tx, error *e);

/**
 * These are used mostly in tests
 */
err_t pgr_flush_wall (const struct pager *p, error *e);
err_t pgr_crash (struct pager *p, error *e);
