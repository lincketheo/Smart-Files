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

#include "c_specx.h"
#include "pager.h"
#include "pager/page_fixture.h"
#include "pager/page_h.h"
#include "pages/fsm_page.h"
#include "pages/page.h"

/*
 * Free a page and release its handle.
 *
 * Freeing a page means clearing its bit in the FSM so that pgr_new() can
 * reuse it.  Two WAL records are written:
 *
 *   1. The page itself is released with PG_PERMISSIVE and NULL update, which
 *      writes a PHYSICAL WAL record for the page content.  This undo image
 *      lets recovery restore the page if the transaction is rolled back.
 *
 *   2. The FSM page update is logged as WUP_FSM with undo=1 (the bit was 1,
 *      meaning allocated) and redo=0 (after commit the bit is 0, meaning
 *      free).  This compact record avoids logging the full FSM bitmap.
 *
 * The pgno-to-FSM mapping: FSM page for pgno is at (pgno / FS_BTMP_NPGS) *
 * FS_BTMP_NPGS, and the bit index within that FSM page is pgno % FS_BTMP_NPGS.
 */
err_t
pgr_delete_and_release (struct pager *p, struct txn *tx, page_h *h, error *e)
{
  DBG_ASSERT (pager, p);
  page_h fsm = page_h_create ();

  // Truncate to 8 * FS_BTMP_NPGS to get the tracking fsm
  const pgno pg = page_h_pgno (h);
  // FSM page that tracks this pgno lives at the aligned section base
  pgno fsmpg = page_h_pgno (h) / FS_BTMP_NPGS;
  fsmpg *= FS_BTMP_NPGS;

  // Bit index within that FSM page's bitmap
  const p_size idx = page_h_pgno (h) % FS_BTMP_NPGS;

  if (pgr_get_writable (&fsm, tx, PG_FREE_SPACE_MAP, fsmpg, p, e))
    {
      goto failed;
    }

  // Mark the page as free in the bitmap before logging
  fsm_clr_bit (page_h_w (&fsm), idx);

  // Log the freed page with a physical record (NULL = full page image as
  // undo)
  if (pgr_release_with_log (p, h, PG_PERMISSIVE, NULL, e))
    {
      goto failed;
    }

  // Log the FSM change compactly: undo=1 (was allocated), redo=0 (now free)
  if (pgr_release_with_log (p, &fsm, PG_FREE_SPACE_MAP, &(struct wal_update_write){
                                                            .type = WUP_FSM,
                                                            .tid = tx->tid,
                                                            .prev = tx->data.last_lsn,
                                                            .fsm = {
                                                                .pg = pg,
                                                                .undo = 1,
                                                                .redo = 0,
                                                            },
                                                        },
                            e))
    {
      goto failed;
    }

  return SUCCESS;

failed:
  pgr_cancel_if_exists (p, &fsm);

  return error_trace (e);
}

#ifndef NTEST
TEST (pgr_delete)
{
  struct pgr_fixture f;
  error *e = &f.e;
  pgr_fixture_create (&f);

  struct txn tx;
  pgr_begin_txn (&tx, f.p, e);

  page_h a = page_h_create ();
  page_h b = page_h_create ();
  page_h c = page_h_create ();
  page_h d = page_h_create ();

  pgr_new (&a, f.p, &tx, PG_DATA_LIST, e);
  pgr_new (&b, f.p, &tx, PG_DATA_LIST, e);
  pgr_new (&c, f.p, &tx, PG_DATA_LIST, e);
  pgr_new (&d, f.p, &tx, PG_DATA_LIST, e);

  const pgno apg = page_h_pgno (&a);
  const pgno bpg = page_h_pgno (&b);
  const pgno cpg = page_h_pgno (&c);
  const pgno dpg = page_h_pgno (&d);

  pgr_delete_and_release (f.p, &tx, &a, e);
  pgr_delete_and_release (f.p, &tx, &b, e);
  pgr_delete_and_release (f.p, &tx, &c, e);
  pgr_delete_and_release (f.p, &tx, &d, e);

  pgr_new (&a, f.p, &tx, PG_DATA_LIST, e);
  pgr_new (&b, f.p, &tx, PG_DATA_LIST, e);
  pgr_new (&c, f.p, &tx, PG_DATA_LIST, e);
  pgr_new (&d, f.p, &tx, PG_DATA_LIST, e);

  test_assert_equal (page_h_pgno (&a), apg);
  test_assert_equal (page_h_pgno (&b), bpg);
  test_assert_equal (page_h_pgno (&c), cpg);
  test_assert_equal (page_h_pgno (&d), dpg);

  pgr_delete_and_release (f.p, &tx, &a, e);
  pgr_delete_and_release (f.p, &tx, &b, e);
  pgr_delete_and_release (f.p, &tx, &c, e);
  pgr_delete_and_release (f.p, &tx, &d, e);

  pgr_new (&a, f.p, &tx, PG_DATA_LIST, e);
  pgr_new (&b, f.p, &tx, PG_DATA_LIST, e);
  pgr_new (&c, f.p, &tx, PG_DATA_LIST, e);
  pgr_new (&d, f.p, &tx, PG_DATA_LIST, e);

  test_assert_equal (page_h_pgno (&a), apg);
  test_assert_equal (page_h_pgno (&b), bpg);
  test_assert_equal (page_h_pgno (&c), cpg);
  test_assert_equal (page_h_pgno (&d), dpg);

  dl_set_used (page_h_w (&a), DL_DATA_SIZE);
  dl_set_used (page_h_w (&b), DL_DATA_SIZE);
  dl_set_used (page_h_w (&c), DL_DATA_SIZE);
  dl_set_used (page_h_w (&d), DL_DATA_SIZE);

  pgr_release (f.p, &a, PG_DATA_LIST, e);
  pgr_delete_and_release (f.p, &tx, &b, e);
  pgr_delete_and_release (f.p, &tx, &c, e);
  pgr_release (f.p, &d, PG_DATA_LIST, e);

  pgr_commit (f.p, &tx, &f.e);

  pgr_fixture_teardown (&f);
}
#endif
