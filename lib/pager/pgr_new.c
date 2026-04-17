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

#include "pager.h"
#include "pager/page_fixture.h"
#include "pager/page_h.h"
#include "pages/data_list.h"
#include "pages/fsm_page.h"
#include "pages/page.h"
#include "c_specx.h"
#include "wal/wal_rec_hdr.h"

/*
 * Allocate a buffer pair for a page number that does not yet exist on disk.
 *
 * Unlike pgr_get(), this variant does not read from disk.  It reserves two
 * adjacent slots in the buffer pool — one read frame and one write frame —
 * and initializes the read frame's type to PG_TRASH (a sentinel meaning "no
 * real content").  The write frame is initialized with page_init_empty() for
 * the requested type.  Both frames are registered under [pg] in the hash
 * table.
 *
 * This is used by pgr_new() after FSM allocation has assigned a page number.
 * It is "unsafe" because the page number must have been validated by the
 * caller (taken from the FSM) and the pager lock must be held.
 */
static err_t
pgr_new_from_uninitialized_unsafe (page_h *dest, struct pager *p,
                                   struct txn *tx, const enum page_type type,
                                   const pgno pg, error *e)
{
  DBG_ASSERT (pager, p);
  DBG_ASSERT (page_h, dest);
  ASSERT (dest->mode == PHM_NONE);

  struct page_frame *pgr = NULL, *pgw = NULL;

  // Reserve the read page spot
  const i32 rclock = pgr_reserve_at_clock_thread_unsafe (p, e);
  if (rclock < 0)
    {
      goto theend;
    }

  // Initialize read node
  pgr = &p->pages[rclock];
  pgr->pin = 1;
  pgr->flags = PW_DIRTY | PW_ACCESS | PW_PRESENT;
  pgr->wsibling = -1;
  page_set_type (&pgr->page, PG_TRASH);

  // Reserve the write page spot
  const i32 wclock = pgr_reserve_at_clock_thread_unsafe (p, e);
  if (wclock < 0)
    {
      // ROLLBACK pgr
      pgr->pin = 0;
      pgr->flags = 0;
      goto theend;
    }

  pgr->wsibling = wclock;
  pgw = &p->pages[wclock];
  pgw->pin = 1;
  pgw->flags = PW_PRESENT | PW_X;
  pgw->wsibling = -1;

  page_init_empty (&pgw->page, type);
  pgr->page.pg = pg;
  pgw->page.pg = pg;

  // Insert page into the hash table
  const hdata_idx hd = (hdata_idx){ .key = pg, .value = rclock };
  ht_insert_expect_idx (&p->pgno_to_value, hd);

  // Initialize page_h
  dest->pgr = pgr;
  dest->pgw = pgw;
  dest->mode = PHM_X;
  dest->tx = tx;

  // Lock the page

theend:
  return error_trace (e);
}

/*
 * Allocate a new page from the free space map (FSM) and return it in write
 * mode.
 *
 * Scans each FSM page (one per FS_BTMP_NPGS-page section) for the first
 * clear bit.  When a free slot is found, the bit is set and the FSM is
 * released with a WUP_FSM log record (more compact than a full physical
 * record).  The target page is then fetched, upgraded to write mode, and
 * initialized with page_init_empty().
 *
 * If all existing FSM sections are full, a new FSM page is created via
 * pgr_new_from_uninitialized_unsafe() and the database file is extended with
 * a WUP_FEXT Nested Top Action record.  A fresh FSM always has bit 0 set
 * (the FSM page itself) and bit 1 as the first available page.
 *
 * On any failure, all held pages (including any partially-allocated pages)
 * are cancelled rather than released, leaving the buffer pool in a clean
 * state.
 */
err_t
pgr_new (page_h *dest, struct pager *p, struct txn *tx, const enum page_type type,
         error *e)
{
  page_h fsm = page_h_create (); // The currently used free space map
  pgno ret = 0;                  // The return page
  pgno fsmpg = 0;                // The free space map page

  // Iterate through existing FSM's to see if there's a free lockable
  // page
  for (; fsmpg < pgr_get_npages (p); fsmpg += FS_BTMP_NPGS)
    {
      // Fetch this free space map
      if (pgr_get (&fsm, PG_FREE_SPACE_MAP, fsmpg, p, e))
        {
          goto failed;
        }

      const page *_fsm = page_h_ro (&fsm);

      // Find the next free slot
      const sp_size next = fsm_next_freebit (_fsm, 0);

      if (next != -1)
        {
          // FOUND!
          ret = next + fsmpg; // The actual page number
          if (pgr_make_writable (p, tx, &fsm, e))
            {
              goto failed;
            }

          fsm_set_bit (page_h_w (&fsm), next);

          if (pgr_release_with_log (
                  p,
                  &fsm,
                  PG_FREE_SPACE_MAP,
                  &(struct wal_update_write){
                      .type = WUP_FSM,
                      .tid = tx->tid,
                      .prev = tx->data.last_lsn,
                      .fsm = {
                          .pg = ret,
                          .undo = 0,
                          .redo = 1,
                      },
                  },
                  e))
            {
              goto failed;
            }

          // Fetch this page - don't care about
          // the data in it
          if (pgr_get (dest, PG_PERMISSIVE, ret, p, e))
            {
              goto failed;
            }

          if (pgr_make_writable (p, tx, dest, e))
            {
              goto failed;
            }

          // Initialize the new page
          page_init_empty (page_h_w (dest), type);

          goto theend;
        }
      else
        {
          // NOT FOUND!
          ASSERT (fsm.mode == PHM_S);
          if (pgr_release (p, &fsm, PG_FREE_SPACE_MAP, e))
            {
              goto failed;
            }
          continue;
        }
    }

  // Exceeded the number of free space maps we have - Need to create a
  // new free space map
  if (pgr_new_from_uninitialized_unsafe (&fsm, p, tx, PG_FREE_SPACE_MAP, fsmpg,
                                         e))
    {
      goto failed;
    }

  // Creating a new FSM means we are tracking this many pages
  if (pgr_extend_file (p, fsmpg + FS_BTMP_NPGS, tx, e))
    {
      goto failed;
    }

  // the next free bit must be 1 because this is a new fsm
  ASSERT (fsm_next_freebit (page_h_ro (&fsm), 0) == 1);
  ret = fsmpg + 1;

  fsm_set_bit (page_h_w (&fsm), pgtoidx (ret));

  if (pgr_release_with_log (p, &fsm, PG_FREE_SPACE_MAP, &(struct wal_update_write){
                                                            .type = WUP_FSM,
                                                            .tid = tx->tid,
                                                            .prev = tx->data.last_lsn,
                                                            .fsm = {
                                                                .pg = ret,
                                                                .undo = 0,
                                                                .redo = 1,
                                                            },
                                                        },
                            e))
    {
      goto failed;
    }

  if (pgr_new_from_uninitialized_unsafe (dest, p, tx, type, ret, e))
    {
      goto failed;
    }

theend:
  return SUCCESS;

failed:
  pgr_cancel_if_exists (p, dest);
  pgr_cancel_if_exists (p, &fsm);
  return error_trace (e);
}

#ifndef NTEST
TEST (pgr_new_get_save)
{
  struct pgr_fixture f;
  page_h h = page_h_create ();
  pgr_fixture_create (&f);

  struct txn tx;
  pgr_begin_txn (&tx, f.p, &f.e);

  pgr_new (&h, f.p, &tx, PG_DATA_LIST, &f.e);
  test_assert_int_equal ((int)pgr_get_npages (f.p), FS_BTMP_NPGS);

  // Make it valid
  dl_set_used (page_h_w (&h), DL_DATA_SIZE);

  pgr_release (f.p, &h, PG_DATA_LIST, &f.e);

  pgr_commit (f.p, &tx, &f.e);

  pgr_fixture_teardown (&f);
}

// TODO
TEST (pgr_new_multiple_fsm)
{
  /* TODO: this test used a thread pool (f.tp) which has been removed
   * from pager deps */
  struct pgr_fixture f;
  pgr_fixture_create (&f);
  pgr_fixture_teardown (&f);
}
#endif
