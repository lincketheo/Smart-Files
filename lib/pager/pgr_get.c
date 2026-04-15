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
#include "pages/page.h"
#include "tlclib_dev.h"

/*
 * Fetch a page in shared (read) mode.
 *
 * First checks whether the page is already in the buffer pool via the hash
 * table.  A hit simply increments the pin count.  On a miss, the clock
 * algorithm finds a free slot (evicting an unpinned page if necessary), reads
 * the page from disk, validates it against [flags], and inserts it into the
 * hash table.
 *
 * The returned page_h is in PHM_S mode.  To modify the page, call
 * pgr_make_writable() afterwards, or use pgr_get_writable() directly.
 *
 * Passing PG_PERMISSIVE in [flags] skips page-type and checksum validation,
 * which is required when reading a page whose type is not yet known (e.g.,
 * newly allocated pages that have not been initialized yet).
 */
err_t
pgr_get (page_h *dest, const int flags, const pgno pg, struct pager *p, error *e)
{
  struct page_frame *pgr = NULL;
  hdata_idx data;
  switch (ht_get_idx (&p->pgno_to_value, &data, pg))
    {
    case HTAR_SUCCESS:
      {
        pgr = &p->pages[data.value];
        pgr->pin++;
        break;
      }
    case HTAR_DOESNT_EXIST:
      {
        const i32 clock = pgr_reserve_at_clock_thread_unsafe (p, e);
        if (clock < 0)
          {
            goto failed;
          }

        pgr = &p->pages[clock];
        if (ospgr_read (p->fp, pgr->page.raw, pg, e))
          {
            goto failed;
          }

        if (page_validate_for_db (&pgr->page, flags, e))
          {
            return error_trace (e);
          }

        pgr->pin = 1;
        pgr->flags = PW_ACCESS | PW_PRESENT;
        pgr->wsibling = -1;
        pgr->page.pg = pg;
        ht_insert_expect_idx (&p->pgno_to_value,
                              (hdata_idx){ .key = pg, .value = clock });
        break;
      }
    }

  dest->pgr = pgr;
  dest->pgw = NULL;
  dest->mode = PHM_S;

failed:
  return error_trace (e);
}

err_t
pgr_get_writable (
    page_h *dest,
    struct txn *tx,
    const int flags,
    const pgno pg,
    struct pager *p,
    error *e)
{
  if (pgr_get (dest, flags, pg, p, e))
    {
      return error_trace (e);
    }

  if (pgr_make_writable (p, tx, dest, e))
    {
      pgr_cancel (p, dest);
    }

  return error_trace (e);
}

#ifndef NTEST
TEST (pgr_get_invalid_checksum)
{
  page_h pg = page_h_create ();
  error e = error_create ();
  struct pgr_fixture pf;
  pgr_fixture_create (&pf);

  struct txn tx;
  pgr_begin_txn (&tx, pf.p, &pf.e);

  pgr_new (&pg, pf.p, &tx, PG_DATA_LIST, &pf.e);
  dl_make_valid (page_h_w (&pg));

  const pgno _pg = page_h_pgno (&pg);
  pgr_release_with_flush (pf.p, &pg, PG_DATA_LIST, &pf.e);

  pgr_commit (pf.p, &tx, &pf.e);

  pgr_get (&pg, PG_DATA_LIST, _pg, pf.p, &pf.e);
  test_assert_int_equal (page_get_checksum (page_h_ro (&pg)),
                         page_compute_checksum (page_h_ro (&pg)));

  // Force checksum to be different
  page fake_page;
  memcpy (fake_page.raw, page_h_ro (&pg)->raw, PAGE_SIZE);
  fake_page.pg = page_h_pgno (&pg);
  page_set_checksum (&fake_page, page_get_checksum (&fake_page) + 1);

  pgr_release_with_evict (pf.p, &pg, PG_DATA_LIST, &pf.e);

  // Force a invalid write
  ospgr_write (pf.p->fp, fake_page.raw, fake_page.pg, &pf.e);

  // This one will fail
  test_err_t_check (pgr_get (&pg, PG_DATA_LIST, _pg, pf.p, &pf.e), ERR_CORRUPT,
                    &pf.e);

  pgr_fixture_teardown (&pf);
}
#endif
