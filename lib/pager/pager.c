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
#include "c_specx.h"

bool
pgr_isnew (const struct pager *p)
{
  return p->flags & PGR_ISNEW;
}

p_size
pgr_get_npages (const struct pager *p)
{
  DBG_ASSERT (pager, p);
  return ospgr_get_npages (p->fp);
}

err_t
pgr_flush_wall (const struct pager *p, error *e)
{
  return oswal_flush_all (p->ww, e);
}

void
pgr_attach_lock_table (struct pager *p, struct lockt *lt)
{
  DBG_ASSERT (pager, p);
  ASSERT (p->lt == NULL);
  p->lt = lt;
}

#ifndef NTEST

TEST (pager_fill_ht)
{
  struct pgr_fixture f;
  pgr_fixture_create (&f);

  struct txn tx;
  pgr_begin_txn (&tx, f.p, &f.e);

  page_h pgs[MEMORY_PAGE_LEN];
  page_h bad = page_h_create ();

  {
    // Fill up - there is already one page in the pool, the root
    u32 i = 0;
    for (; i < MEMORY_PAGE_LEN / 2; ++i)
      {
        pgs[i] = page_h_create ();
        pgr_new (&pgs[i], f.p, &tx, PG_DATA_LIST, &f.e);
        test_assert_equal (pgs[i].mode, PHM_X);
      }

    do
      {
        const err_t __ret = (err_t)pgr_new (&bad, f.p, &tx, PG_DATA_LIST, &f.e);
        test_assert_int_equal (__ret, ERR_PAGER_FULL);
        (&f.e)->cause_code = SUCCESS;
      }
    while (0);

    // Release them all
    for (i = 0; i < MEMORY_PAGE_LEN / 2; ++i)
      {
        dl_set_used (page_h_w (&pgs[i]), DL_DATA_SIZE);
        pgr_release (f.p, &pgs[i], PG_DATA_LIST, &f.e);
      }
  }

  // Repeat above
  {
    // Fill half way up - good
    for (u32 i = 0; i < MEMORY_PAGE_LEN / 2; ++i)
      {
        pgr_new (&pgs[i], f.p, &tx, PG_DATA_LIST, &f.e);
        test_assert_equal (pgs[i].mode, PHM_X);
      }

    // Next page will be full
    test_err_t_check (pgr_new (&bad, f.p, &tx, PG_DATA_LIST, &f.e),
                      ERR_PAGER_FULL, &f.e);

    // Release them all
    for (u32 i = 0; i < MEMORY_PAGE_LEN / 2; ++i)
      {
        dl_set_used (page_h_w (&pgs[i]), DL_DATA_SIZE);
        pgr_release (f.p, &pgs[i], PG_DATA_LIST, &f.e);
      }
  }

  pgr_commit (f.p, &tx, &f.e);

  pgr_fixture_teardown (&f);
}

TEST (wal_int)
{
  struct pgr_fixture f;
  page_h h = page_h_create ();
  pgr_fixture_create (&f);

  struct txn tx;
  pgr_begin_txn (&tx, f.p, &f.e);

  pgr_new (&h, f.p, &tx, PG_DATA_LIST, &f.e);

  dl_set_used (page_h_w (&h), DL_DATA_SIZE);
  pgr_release (f.p, &h, PG_DATA_LIST, &f.e);

  pgr_commit (f.p, &tx, &f.e);

  pgr_fixture_teardown (&f);
}
#endif

void
i_log_page_table (const int log_level, bool only_present, const struct pager *p)

{
  DBG_ASSERT (pager, p);
  i_log (log_level, "Page Table:\n");
  for (u32 i = 0; i < MEMORY_PAGE_LEN; ++i)
    {
      const struct page_frame *mp = &p->pages[i];
      if (mp->flags & PW_PRESENT)
        {
          i_printf (log_level,
                    "%u |(PAGE)    pg: %" PRpgno
                    " pin: %d ax: %d drt: %d prsn: %d "
                    "sib: %d type: %d|\n",
                    i, mp->page.pg, mp->pin, mp->flags & PW_ACCESS,
                    mp->flags & PW_DIRTY, mp->flags & PW_PRESENT, mp->wsibling,
                    page_get_type (&mp->page));
        }
      else if (!only_present)
        {
          i_printf (log_level, "%u | |\n", i);
        }
    }
  i_log_dpgt (log_level, p->dpt);
  i_log_txnt (log_level, p->tnxt);
}

err_t
pgr_crash (struct pager *p, error *e)
{
  oswal_crash (p->ww, e);
  ospgr_crash (p->fp, e);

  txnt_crash (p->tnxt);
  dpgt_crash (p->dpt);

  i_free (p);

  return error_trace (e);
}
