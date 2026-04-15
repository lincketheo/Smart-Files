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

#include "dpgt/dirty_page_table.h"

#ifndef NTEST
struct dpgt_thread_ctx
{
  struct dpg_table *table;
  volatile int counter;
  pgno start_pg;
  int count;
};

static void *
dpgt_insert_thread (void *arg)
{
  struct dpgt_thread_ctx *ctx = arg;
  error e = error_create ();

  for (int i = 0; i < ctx->count; i++)
    {
      const pgno pg = ctx->start_pg + i;
      const lsn rec_lsn = ctx->start_pg + i;

      if (dpgt_add (ctx->table, pg, rec_lsn, &e) == SUCCESS)
        {
          ctx->counter += 1;
        }
    }
  return NULL;
}

static void *
dpgt_reader_thread (void *arg)
{
  struct dpgt_thread_ctx *ctx = arg;

  for (int i = 0; i < ctx->count; i++)
    {
      lsn rec_lsn;
      if (dpgt_get (&rec_lsn, ctx->table, ctx->start_pg + i))
        {
          ctx->counter += 1;
        }
    }
  return NULL;
}

static void *
dpgt_updater_thread (void *arg)
{
  struct dpgt_thread_ctx *ctx = arg;

  for (int i = 0; i < ctx->count; i++)
    {
      const pgno pg = ctx->start_pg + i;

      if (dpgt_exists (ctx->table, pg))
        {
          dpgt_update (ctx->table, pg, ctx->start_pg + i + 1000);
          ctx->counter += 1;
        }
    }

  return NULL;
}

static void *
dpgt_remove_thread (void *arg)
{
  struct dpgt_thread_ctx *ctx = arg;

  for (int i = 0; i < ctx->count; i++)
    {
      bool removed;
      dpgt_remove (&removed, ctx->table, ctx->start_pg + i);

      if (removed)
        {
          ctx->counter += 1;
        }
    }

  return NULL;
}

TEST (dpgt_concurrent)
{
  TEST_CASE ("concurrent inserts")
  {
    error e = error_create ();
    struct dpg_table *t = dpgt_open (&e);
    struct dpgt_thread_ctx ctx1 = {
      .table = t,
      .start_pg = 0,
      .count = 100,
      .counter = 0,
    };
    struct dpgt_thread_ctx ctx2 = {
      .table = t,
      .start_pg = 100,
      .count = 100,
      .counter = 0,
    };
    struct dpgt_thread_ctx ctx3 = {
      .table = t,
      .start_pg = 200,
      .count = 100,
      .counter = 0,
    };

    i_thread t1, t2, t3;
    test_assert_equal (i_thread_create (&t1, dpgt_insert_thread, &ctx1, &e),
                       SUCCESS);
    test_assert_equal (i_thread_create (&t2, dpgt_insert_thread, &ctx2, &e),
                       SUCCESS);
    test_assert_equal (i_thread_create (&t3, dpgt_insert_thread, &ctx3, &e),
                       SUCCESS);

    i_thread_join (&t1, &e);
    i_thread_join (&t2, &e);
    i_thread_join (&t3, &e);

    int total_inserts = ctx1.counter + ctx2.counter + ctx3.counter;
    test_assert_equal (total_inserts, 300);

    for (pgno pg = 0; pg < 300; pg++)
      {
        test_assert (dpgt_exists (t, pg));
      }

    dpgt_close (t);
  }

  TEST_CASE ("concurrent readers")
  {
    error e = error_create ();
    struct dpg_table *t = dpgt_open (&e);
    // Pre-populate
    for (pgno pg = 0; pg < 200; pg++)
      {
        dpgt_add (t, pg, pg * 10, &e);
      }

    struct dpgt_thread_ctx ctx1 = {
      .table = t,
      .start_pg = 0,
      .count = 100,
      .counter = 0,
    };
    struct dpgt_thread_ctx ctx2 = {
      .table = t,
      .start_pg = 50,
      .count = 100,
      .counter = 0,
    };
    struct dpgt_thread_ctx ctx3 = {
      .table = t,
      .start_pg = 100,
      .count = 100,
      .counter = 0,
    };

    i_thread t1, t2, t3;
    test_assert_equal (i_thread_create (&t1, dpgt_reader_thread, &ctx1, &e),
                       SUCCESS);
    test_assert_equal (i_thread_create (&t2, dpgt_reader_thread, &ctx2, &e),
                       SUCCESS);
    test_assert_equal (i_thread_create (&t3, dpgt_reader_thread, &ctx3, &e),
                       SUCCESS);

    i_thread_join (&t1, &e);
    i_thread_join (&t2, &e);
    i_thread_join (&t3, &e);

    int total_reads = ctx1.counter + ctx2.counter + ctx3.counter;
    test_assert_equal (total_reads, 300);

    dpgt_close (t);
  }

  TEST_CASE ("concurrent updates")
  {
    error e = error_create ();
    struct dpg_table *t = dpgt_open (&e);
    // Pre-populate
    for (pgno pg = 0; pg < 300; pg++)
      {
        dpgt_add (t, pg, pg * 10, &e);
      }

    struct dpgt_thread_ctx ctx1 = {
      .table = t,
      .start_pg = 0,
      .count = 100,
      .counter = 0,
    };
    struct dpgt_thread_ctx ctx2 = {
      .table = t,
      .start_pg = 100,
      .count = 100,
      .counter = 0,
    };
    struct dpgt_thread_ctx ctx3 = {
      .table = t,
      .start_pg = 200,
      .count = 100,
      .counter = 0,
    };

    i_thread t1, t2, t3;
    test_assert_equal (i_thread_create (&t1, dpgt_updater_thread, &ctx1, &e),
                       SUCCESS);
    test_assert_equal (i_thread_create (&t2, dpgt_updater_thread, &ctx2, &e),
                       SUCCESS);
    test_assert_equal (i_thread_create (&t3, dpgt_updater_thread, &ctx3, &e),
                       SUCCESS);

    i_thread_join (&t1, &e);
    i_thread_join (&t2, &e);
    i_thread_join (&t3, &e);

    int total_updates = ctx1.counter + ctx2.counter + ctx3.counter;
    test_assert_equal (total_updates, 300);

    // Verify updates
    for (pgno pg = 0; pg < 300; pg++)
      {
        lsn rec_lsn;
        test_assert (dpgt_get (&rec_lsn, t, pg));
        test_assert_equal (rec_lsn, pg + 1000);
      }

    dpgt_close (t);
  }

  TEST_CASE ("concurrent removes")
  {
    error e = error_create ();
    struct dpg_table *t = dpgt_open (&e);
    // Pre-populate
    for (pgno pg = 0; pg < 300; pg++)
      {
        dpgt_add (t, pg, pg * 10, &e);
      }

    struct dpgt_thread_ctx ctx1 = {
      .table = t,
      .start_pg = 0,
      .count = 100,
      .counter = 0,
    };
    struct dpgt_thread_ctx ctx2 = {
      .table = t,
      .start_pg = 100,
      .count = 100,
      .counter = 0,
    };
    struct dpgt_thread_ctx ctx3 = {
      .table = t,
      .start_pg = 200,
      .count = 100,
      .counter = 0,
    };

    i_thread t1, t2, t3;
    test_assert_equal (i_thread_create (&t1, dpgt_remove_thread, &ctx1, &e),
                       SUCCESS);
    test_assert_equal (i_thread_create (&t2, dpgt_remove_thread, &ctx2, &e),
                       SUCCESS);
    test_assert_equal (i_thread_create (&t3, dpgt_remove_thread, &ctx3, &e),
                       SUCCESS);

    i_thread_join (&t1, &e);
    i_thread_join (&t2, &e);
    i_thread_join (&t3, &e);

    int total_removes = ctx1.counter + ctx2.counter + ctx3.counter;
    test_assert_equal (total_removes, 300);

    for (pgno pg = 0; pg < 300; pg++)
      {
        test_assert (!dpgt_exists (t, pg));
      }

    dpgt_close (t);
  }

  TEST_CASE ("concurrent insert and read")
  {
    error e = error_create ();
    struct dpg_table *t = dpgt_open (&e);
    // Pre-populate half so readers have something to find
    for (pgno pg = 0; pg < 100; pg++)
      {
        dpgt_add (t, pg, pg * 10, &e);
      }

    struct dpgt_thread_ctx insert_ctx = {
      .table = t,
      .start_pg = 100,
      .count = 100,
      .counter = 0,
    };
    struct dpgt_thread_ctx read_ctx1 = {
      .table = t,
      .start_pg = 0,
      .count = 100,
      .counter = 0,
    };
    struct dpgt_thread_ctx read_ctx2 = {
      .table = t,
      .start_pg = 0,
      .count = 100,
      .counter = 0,
    };

    i_thread t1, t2, t3;
    test_assert_equal (
        i_thread_create (&t1, dpgt_insert_thread, &insert_ctx, &e), SUCCESS);
    test_assert_equal (
        i_thread_create (&t2, dpgt_reader_thread, &read_ctx1, &e), SUCCESS);
    test_assert_equal (
        i_thread_create (&t3, dpgt_reader_thread, &read_ctx2, &e), SUCCESS);

    i_thread_join (&t1, &e);
    i_thread_join (&t2, &e);
    i_thread_join (&t3, &e);

    // All inserts must have succeeded
    test_assert_equal (insert_ctx.counter, 100);

    // All pre-populated pages must still be present
    for (pgno pg = 0; pg < 100; pg++)
      {
        test_assert (dpgt_exists (t, pg));
      }

    // All inserted pages must be present
    for (pgno pg = 100; pg < 200; pg++)
      {
        test_assert (dpgt_exists (t, pg));
      }

    dpgt_close (t);
  }
}
#endif
