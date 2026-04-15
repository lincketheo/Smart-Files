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

#include "intf/os/threading.h"
#include "numstore/errors.h"

#ifndef NTEST

struct thread_ctx
{
  struct wal *w;
  i_semaphore *begin;
  err_t ret;
};

/**
static void *
simple_pager_ops (void *_ctx)
{
  struct thread_ctx *ctx = _ctx;
  struct pager *p = ctx->p;
  struct txn tx;
  error e = error_create ();

  page_h a = page_h_create ();
  page_h b = page_h_create ();
  page_h c = page_h_create ();
  page_h d = page_h_create ();

  i_semaphore_wait (ctx->begin);

  if (unlikely((pgr_begin_txn (&tx, p, &e)) < SUCCESS)) { goto theend; }

  if (unlikely((pgr_new (&a, p, &tx, PG_DATA_LIST, &e)) < SUCCESS)) { goto
theend; } if (unlikely((pgr_new (&b, p, &tx, PG_DATA_LIST, &e)) < SUCCESS)) {
goto theend; } if (unlikely((pgr_new (&c, p, &tx, PG_DATA_LIST, &e)) <
SUCCESS)) { goto theend; } if (unlikely((pgr_new (&d, p, &tx, PG_DATA_LIST,
&e)) < SUCCESS)) { goto theend; }

  dl_make_valid (page_h_w (&a));
  dl_make_valid (page_h_w (&b));
  dl_make_valid (page_h_w (&c));
  dl_make_valid (page_h_w (&d));

  pgno ap = page_h_pgno (&a);
  pgno bp = page_h_pgno (&a);
  pgno cp = page_h_pgno (&a);
  pgno dp = page_h_pgno (&a);

  if (unlikely((pgr_release (p, &a, PG_DATA_LIST, &e)) < SUCCESS)) { goto
theend; } if (unlikely((pgr_release (p, &b, PG_DATA_LIST, &e)) < SUCCESS)) {
goto theend; } if (unlikely((pgr_release (p, &c, PG_DATA_LIST, &e)) < SUCCESS))
{ goto theend; } if (unlikely((pgr_release (p, &d, PG_DATA_LIST, &e)) <
SUCCESS)) { goto theend; }

  if (unlikely((pgr_get (&a, PG_DATA_LIST, ap, p, &e)) < SUCCESS)) { goto
theend; } if (unlikely((pgr_get (&b, PG_DATA_LIST, ap, p, &e)) < SUCCESS)) {
goto theend; } if (unlikely((pgr_get (&c, PG_DATA_LIST, ap, p, &e)) < SUCCESS))
{ goto theend; } if (unlikely((pgr_get (&d, PG_DATA_LIST, ap, p, &e)) <
SUCCESS)) { goto theend; }

  if (unlikely((pgr_release (p, &a, PG_DATA_LIST, &e)) < SUCCESS)) { goto
theend; } if (unlikely((pgr_release (p, &b, PG_DATA_LIST, &e)) < SUCCESS)) {
goto theend; } if (unlikely((pgr_release (p, &c, PG_DATA_LIST, &e)) < SUCCESS))
{ goto theend; } if (unlikely((pgr_release (p, &d, PG_DATA_LIST, &e)) <
SUCCESS)) { goto theend; }

theend:
  pgr_cancel_if_exists (p, &a);
  pgr_cancel_if_exists (p, &b);
  pgr_cancel_if_exists (p, &c);
  pgr_cancel_if_exists (p, &d);

  ctx->ret = e.cause_code;

  return NULL;
}

TEST (TT_UNIT, pager_mt)
{
  struct pgr_fixture pf;
  pgr_fixture_create (&pf);

  i_semaphore begin;

  struct thread_ctx ctx[10] = {
    { .p = pf.p, .begin = &begin },
    { .p = pf.p, .begin = &begin },
    { .p = pf.p, .begin = &begin },
    { .p = pf.p, .begin = &begin },
    { .p = pf.p, .begin = &begin },
    { .p = pf.p, .begin = &begin },
    { .p = pf.p, .begin = &begin },
    { .p = pf.p, .begin = &begin },
    { .p = pf.p, .begin = &begin },
    { .p = pf.p, .begin = &begin },
  };

  i_thread threads[10];

  i_semaphore_create (&begin, arrlen (threads), &pf.e);

  for (u32 i = 0; i < arrlen (threads); ++i)
    {
      i_thread_create (&threads[i], simple_pager_ops, &ctx, &pf.e);
    }

  for (u32 i = 0; i < arrlen (threads); ++i)
    {
      i_semaphore_post (&begin);
    }

  for (u32 i = 0; i < arrlen (threads); ++i)
    {
      i_thread_join (&threads[i], &pf.e);
    }

  for (u32 i = 0; i < arrlen (threads); ++i)
    {
      test_assert_int_equal (ctx[i].ret, SUCCESS);
    }
}
*/
#endif
