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

#include "tlclib/memory/alloc.h"
#include "tlclib/memory/chunk_alloc.h"
#include "tlclib/core/macros.h"
#include "tlclib/core/random.h"
#include "tlclib/intf/logging.h"
#include "tlclib/intf/os/memory.h"
#include "tlclib/intf/os/threading.h"
#include "numstore/errors.h"
#include "paging/dpgt/dirty_page_table.h"
#include "paging/txns/txn_table.h"
#include "paging/wal/wal.h"
#include "paging/wal/wal_rec_hdr.h"
#include "test/testing.h"

#include <stdatomic.h>
#include <string.h>

#ifndef NTEST

struct wal_queue
{
  i_semaphore sync;
  struct wal *ww;
  atomic_int idx;
  struct wal_rec_hdr_read *read;
  const int len;
  atomic_int ret;
};

static void *
wal_thread (void *ctx)
{
  error e = error_create ();
  struct wal_queue *q = ctx;

  i_semaphore_wait (&q->sync);

  while (true)
    {
      const int idx = atomic_fetch_add (&q->idx, 1);
      if (idx >= q->len)
        {
          goto theend;
        }
      struct wal_rec_hdr_write write = wrhw_from_wrhr (&q->read[idx]);

      const slsn l = wal_append_log (q->ww, &write, &e);
      if (l < 0)
        {
          goto theend;
        }

      if (wal_flush_to (q->ww, l, &e))
        {
          goto theend;
        }
    }

theend:
  if (e.cause_code)
    {
      atomic_store (&q->ret, e.cause_code);
    }
  return NULL;
}

TEST (TT_UNIT, wal_multi_threaded)
{
  error e = error_create ();
  struct wal *ww = wal_open ("test.wal", &e);

  struct wal_rec_hdr_read *read = i_malloc (1000, sizeof *read, &e);
  struct alloc alloc;
  chunk_alloc_create_default (&alloc._calloc);
  alloc.type = AT_CHNK_ALLOC;

  for (u32 i = 0; i < 1000; ++i)
    {
      wal_rec_hdr_read_random (&read[i], &alloc, &e);
    }

  struct wal_queue ctx = {
    .ww = ww,
    .idx = 0,
    .read = read,
    .len = 1000,
  };

  i_semaphore_create (&ctx.sync, 10, &e);

  u32 nthreads;
  i_thread threads[10];
  for (nthreads = 0; nthreads < arrlen (threads); ++nthreads)
    {
      i_thread_create (&threads[nthreads], wal_thread, &ctx, &e);
    }

  for (u32 i = 0; i < 10; ++i)
    {
      i_semaphore_post (&ctx.sync);
    }

  i_log_info ("Threads active\n");

  for (; nthreads > 0; --nthreads)
    {
      i_thread_join (&threads[nthreads - 1], &e);
    }

  lsn read_lsn = 0;
  u32 finger = 0;

  for (u32 i = 0; i < 1000; ++i)
    {
      struct wal_rec_hdr_read *actual = wal_read_next (ww, &read_lsn, &e);
      test_assert (actual->type != WL_EOF);

      bool found = false;
      for (u32 k = 0; k < 1000; ++k)
        {
          const u32 idx = (finger + k) % 1000;
          if (wal_rec_hdr_read_equal (actual, &read[idx]))
            {
              finger = (idx + 1) % 1000;
              read[idx].type = WL_EOF;
              found = true;
              break;
            }
        }
      test_assert (found);
    }

  const struct wal_rec_hdr_read *actual = wal_read_next (ww, &read_lsn, &e);
  test_assert_int_equal (actual->type, WL_EOF);
}

struct wal_test_params
{
  const char *fname;
  struct wal_rec_hdr_read *batch1;
  u32 batch1_len;
  struct wal_rec_hdr_read *batch2;
  u32 batch2_len;
};

static void
wal_test_fill_batch (struct wal_rec_hdr_read *batch, const u32 len, struct alloc *a,
                     error *e)
{
  for (u32 i = 0; i < len; i++)
    {
      struct wal_rec_hdr_read *r = &batch[i];

      switch (r->type)
        {
        case WL_UPDATE:
          {
            rand_bytes (r->update.phys.undo, PAGE_SIZE);
            rand_bytes (r->update.phys.redo, PAGE_SIZE);
            break;
          }
        case WL_CLR:
          {
            rand_bytes (r->clr.phys.redo, PAGE_SIZE);
            break;
          }
        case WL_CKPT_END:
          {
            r->ckpt_end.att = txnt_open (e);
            r->ckpt_end.dpt = dpgt_open (e);
            txnt_determ_populate (r->ckpt_end.att, a, e);
            dpgt_rand_populate (r->ckpt_end.dpt, e);
            break;
          }
        default:
          {
            break;
          }
        }
    }
}

static void
wal_test_free_batch (const struct wal_rec_hdr_read *batch, const u32 len)
{
  for (u32 i = 0; i < len; i++)
    {
      const struct wal_rec_hdr_read *r = &batch[i];

      if (r->type == WL_CKPT_END)
        {
          txnt_close (r->ckpt_end.att);
          dpgt_close (r->ckpt_end.dpt);
        }
    }
}

static void
run_wal_test (const struct wal_test_params *p)
{
  error e = error_create ();

  i_remove_quiet (p->fname, &e);
  struct wal *ww = wal_open (p->fname, &e);
  /**
   * Write all the input logs
   */
  {
    slsn l = -1;
    for (u32 i = 0; i < p->batch1_len; i++)
      {
        struct wal_rec_hdr_write out = wrhw_from_wrhr (&p->batch1[i]);
        slsn nextl = wal_append_log (ww, &out, &e);
        test_assert (nextl >= 0);
        test_assert (nextl > l);
        l = nextl;
      }
    wal_flush_all (ww, &e);
  }

  /**
   * Read all the input logs and expect
   * that they are the same as the
   * first batch written ones
   */
  {
    for (u32 i = 0; i < p->batch1_len; i++)
      {
        lsn read_lsn;
        struct wal_rec_hdr_read *next = NULL;
        if (i == 0)
          {
            next = wal_read_entry (ww, 0, &e);
          }
        else
          {
            next = wal_read_next (ww, &read_lsn, &e);
          }
        test_assert (wal_rec_hdr_read_equal (next, &p->batch1[i]));

        if (next->type == WL_CKPT_END)
          {
            txnt_close (next->ckpt_end.att);
            dpgt_close (next->ckpt_end.dpt);
            i_free (next->ckpt_end.txn_bank);
          }
      }
  }

  /**
   * Write a second batch of input logs
   */
  {
    slsn l = 0;
    for (u32 i = 0; i < p->batch2_len; i++)
      {
        struct wal_rec_hdr_write out = wrhw_from_wrhr (&p->batch2[i]);
        l = wal_append_log (ww, &out, &e);
      }
    wal_flush_all (ww, &e);
  }

  /**
   * Read from the start and confirm all the logs
   */
  {
    for (u32 i = 0; i < p->batch1_len; i++)
      {
        lsn read_lsn;
        struct wal_rec_hdr_read *next = NULL;
        if (i == 0)
          {
            next = wal_read_entry (ww, 0, &e);
          }
        else
          {
            next = wal_read_next (ww, &read_lsn, &e);
          }
        test_assert (wal_rec_hdr_read_equal (next, &p->batch1[i]));

        if (next->type == WL_CKPT_END)
          {
            txnt_close (next->ckpt_end.att);
            dpgt_close (next->ckpt_end.dpt);
            i_free (next->ckpt_end.txn_bank);
          }
      }

    for (u32 i = 0; i < p->batch2_len; i++)
      {
        lsn read_lsn;
        struct wal_rec_hdr_read *next = wal_read_next (ww, &read_lsn, &e);
        test_assert (wal_rec_hdr_read_equal (next, &p->batch2[i]));

        if (next->type == WL_CKPT_END)
          {
            txnt_close (next->ckpt_end.att);
            dpgt_close (next->ckpt_end.dpt);
            i_free (next->ckpt_end.txn_bank);
          }
      }
  }

  wal_close (ww, &e);
}

////////////////////////////////////////////////////////////
// WAL test cases

TEST (TT_UNIT, wal)
{
  error e = error_create ();
  struct alloc a = { .type = AT_CHNK_ALLOC };
  chunk_alloc_create_default (&a._calloc);

  struct wal_rec_hdr_read batch1_full[] = {
    { .type = WL_BEGIN, .begin = { .tid = 1 } },
    { .type = WL_COMMIT, .commit = { .tid = 3, .prev = 20 } },
    { .type = WL_END, .end = { .tid = 4, .prev = 30 } },
    { .type = WL_UPDATE,
      .update = { .type = WUP_PHYSICAL,
                  .tid = 5,
                  .prev = 40,
                  .phys = { .pg = 111 } } },
    { .type = WL_CLR,
      .clr = { .type = WCLR_PHYSICAL,
               .tid = 6,
               .prev = 50,
               .undo_next = 42,
               .phys = { .pg = 222 } } },
    { .type = WL_CKPT_BEGIN },
    { .type = WL_CKPT_END },
    { .type = WL_CKPT_END },
  };

  struct wal_rec_hdr_read batch2_full[] = {
    { .type = WL_BEGIN, .begin = { .tid = 2 } },
    { .type = WL_UPDATE,
      .update = { .type = WUP_PHYSICAL,
                  .tid = 6,
                  .prev = 41,
                  .phys = { .pg = 112 } } },
  };

  struct wal_rec_hdr_read batch1_begin_only[] = {
    { .type = WL_BEGIN, .begin = { .tid = 1 } },
  };

  struct wal_rec_hdr_read batch2_begin_only[] = {
    { .type = WL_BEGIN, .begin = { .tid = 2 } },
  };

  struct wal_rec_hdr_read batch1_no_ckpt[] = {
    { .type = WL_BEGIN, .begin = { .tid = 1 } },
    { .type = WL_COMMIT, .commit = { .tid = 3, .prev = 20 } },
    { .type = WL_END, .end = { .tid = 4, .prev = 30 } },
    { .type = WL_UPDATE,
      .update = { .type = WUP_PHYSICAL,
                  .tid = 5,
                  .prev = 40,
                  .phys = { .pg = 111 } } },
    { .type = WL_CLR,
      .clr = { .type = WCLR_PHYSICAL,
               .tid = 6,
               .prev = 50,
               .undo_next = 42,
               .phys = { .pg = 222 } } },
  };

  struct wal_test_params cases[] = {
    {
        .fname = "test_full.wal",
        .batch1 = batch1_full,
        .batch1_len = arrlen (batch1_full),
        .batch2 = batch2_full,
        .batch2_len = arrlen (batch2_full),
    },
    {
        .fname = "test_begin_only.wal",
        .batch1 = batch1_begin_only,
        .batch1_len = arrlen (batch1_begin_only),
        .batch2 = batch2_begin_only,
        .batch2_len = arrlen (batch2_begin_only),
    },
    {
        .fname = "test_no_ckpt.wal",
        .batch1 = batch1_no_ckpt,
        .batch1_len = arrlen (batch1_no_ckpt),
        .batch2 = batch2_full,
        .batch2_len = arrlen (batch2_full),
    },
  };

  for (u32 i = 0; i < arrlen (cases); i++)
    {
      TEST_CASE ("Wal: %d", i)
      {
        const struct wal_test_params *c = &cases[i];

        wal_test_fill_batch (c->batch1, c->batch1_len, &a, &e);
        wal_test_fill_batch (c->batch2, c->batch2_len, &a, &e);

        run_wal_test (c);

        wal_test_free_batch (c->batch1, c->batch1_len);
        wal_test_free_batch (c->batch2, c->batch2_len);
      }
    }

  chunk_alloc_free_all (&a._calloc);
}

TEST (TT_UNIT, wal_single_entry)
{
  error e = error_create ();
  struct alloc a = { .type = AT_CHNK_ALLOC };
  chunk_alloc_create_default (&a._calloc);

  struct wal_rec_hdr_read cases[] = {
    { .type = WL_BEGIN, .begin = { .tid = 1 } },
    { .type = WL_COMMIT, .commit = { .tid = 2, .prev = 10 } },
    { .type = WL_END, .end = { .tid = 3, .prev = 20 } },
    { .type = WL_UPDATE,
      .update = { .type = WUP_PHYSICAL,
                  .tid = 4,
                  .prev = 30,
                  .phys = { .pg = 111 } } },
    { .type = WL_CLR,
      .clr = { .type = WCLR_PHYSICAL,
               .tid = 5,
               .prev = 40,
               .undo_next = 42,
               .phys = { .pg = 222 } } },
    { .type = WL_CKPT_BEGIN },
    { .type = WL_CKPT_END },
  };

  for (u32 i = 0; i < arrlen (cases); i++)
    {
      TEST_CASE ("wal_single_entry: %d", i)
      {
        struct wal_rec_hdr_read *c = &cases[i];

        wal_test_fill_batch (c, 1, &a, &e);

        i_remove_quiet ("test_single_entry.wal", &e);
        struct wal *ww = wal_open ("test_single_entry.wal", &e);
        // WRITE
        struct wal_rec_hdr_write out = wrhw_from_wrhr (c);
        const slsn l = wal_append_log (ww, &out, &e);
        test_assert (l >= 0);

        wal_flush_all (ww, &e);

        // READ
        struct wal_rec_hdr_read *next = wal_read_entry (ww, 0, &e);
        test_assert (wal_rec_hdr_read_equal (next, c));

        if (next->type == WL_CKPT_END)
          {
            txnt_close (next->ckpt_end.att);
            dpgt_close (next->ckpt_end.dpt);
            i_free (next->ckpt_end.txn_bank);
          }

        wal_close (ww, &e);

        wal_test_free_batch (c, 1);
      }
    }

  chunk_alloc_free_all (&a._calloc);
}

#endif
