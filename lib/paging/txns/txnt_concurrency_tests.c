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

#include "intf/os/time.h"
#include "paging/txns/txn.h"
#include "paging/txns/txn_table.h"
#include "test/testing.h"

#ifndef NTEST
#include <stdatomic.h>
struct txnt_thread_ctx
{
  struct txn_table *table;
  struct txn *txn_bank; // Pre-allocated transactions
  _Atomic int counter;
  txid start_tid;
  int count;
};

static void *
txnt_insert_thread (void *arg)
{
  struct txnt_thread_ctx *ctx = arg;
  error e = error_create ();

  for (int i = 0; i < ctx->count; i++)
    {
      txn_init (&ctx->txn_bank[i], ctx->start_tid + i,
                (struct txn_data){
                    .last_lsn = ctx->start_tid + i,
                    .undo_next_lsn = ctx->start_tid + i - 1,
                    .state = TX_RUNNING,
                });

      txnt_insert_txn (ctx->table, &ctx->txn_bank[i]);

      atomic_fetch_add (&ctx->counter, 1);
    }

  return NULL;
}

static void *
txnt_reader_thread (void *arg)
{
  struct txnt_thread_ctx *ctx = arg;

  for (int i = 0; i < ctx->count; i++)
    {
      struct txn *retrieved;
      if (txnt_get (&retrieved, ctx->table, ctx->start_tid + i))
        {
          atomic_fetch_add (&ctx->counter, 1);
        }
    }

  return NULL;
}

static void *
txnt_updater_thread (void *arg)
{
  struct txnt_thread_ctx *ctx = arg;

  for (int i = 0; i < ctx->count; i++)
    {
      struct txn *retrieved;
      if (txnt_get (&retrieved, ctx->table, ctx->start_tid + i))
        {
          struct txn_data new_data = retrieved->data;
          new_data.last_lsn = ctx->start_tid + i + 1000;
          txn_update_data (retrieved, new_data);
          atomic_fetch_add (&ctx->counter, 1);
        }
    }

  return NULL;
}

static void *
txnt_state_transition_thread (void *arg)
{
  struct txnt_thread_ctx *ctx = arg;

  for (int i = 0; i < ctx->count; i++)
    {
      struct txn *retrieved;
      if (txnt_get (&retrieved, ctx->table, ctx->start_tid + i))
        {
          // TX_RUNNING -> TX_CANDIDATE_FOR_UNDO
          struct txn_data new_data = retrieved->data;
          new_data.state = TX_CANDIDATE_FOR_UNDO;
          txn_update_data (retrieved, new_data);

          atomic_fetch_add (&ctx->counter, 1);
          i_sleep_us (100);

          // -> TX_COMMITTED
          new_data.state = TX_COMMITTED;
          txn_update_data (retrieved, new_data);
        }
    }

  return NULL;
}

TEST (TT_UNIT, txnt_concurrent)
{
  TEST_CASE ("txnt_concurrent_inserts")
  {
    error e = error_create ();
    struct txn_table *t = txnt_open (&e);
    struct txn txn_bank1[100], txn_bank2[100], txn_bank3[100];

    struct txnt_thread_ctx ctx1 = {
      .table = t,
      .txn_bank = txn_bank1,
      .start_tid = 0,
      .count = 100,
      .counter = 0,
    };
    struct txnt_thread_ctx ctx2 = {
      .table = t,
      .txn_bank = txn_bank2,
      .start_tid = 100,
      .count = 100,
      .counter = 0,
    };
    struct txnt_thread_ctx ctx3 = {
      .table = t,
      .txn_bank = txn_bank3,
      .start_tid = 200,
      .count = 100,
      .counter = 0,
    };

    i_thread t1, t2, t3;
    test_assert_equal (i_thread_create (&t1, txnt_insert_thread, &ctx1, &e),
                       SUCCESS);
    test_assert_equal (i_thread_create (&t2, txnt_insert_thread, &ctx2, &e),
                       SUCCESS);
    test_assert_equal (i_thread_create (&t3, txnt_insert_thread, &ctx3, &e),
                       SUCCESS);

    i_thread_join (&t1, &e);
    i_thread_join (&t2, &e);
    i_thread_join (&t3, &e);

    int total_inserts = ctx1.counter + ctx2.counter + ctx3.counter;
    test_assert_equal (total_inserts, 300);

    for (txid tid = 0; tid < 300; tid++)
      {
        test_assert (txn_exists (t, tid));
      }

    txnt_close (t);
  }

  TEST_CASE ("txnt_concurrent_readers")
  {
    error e = error_create ();
    struct txn_table *t = txnt_open (&e);
    // Pre-populate
    struct txn txns[200];
    for (int i = 0; i < 200; i++)
      {
        txn_init (&txns[i], i,
                  (struct txn_data){
                      .last_lsn = i,
                      .undo_next_lsn = i - 1,
                      .state = TX_RUNNING,
                  });
        txnt_insert_txn (t, &txns[i]);
      }

    struct txnt_thread_ctx ctx1 = {
      .table = t,
      .start_tid = 0,
      .count = 100,
      .counter = 0,
    };
    struct txnt_thread_ctx ctx2 = {
      .table = t,
      .start_tid = 50,
      .count = 100,
      .counter = 0,
    };
    struct txnt_thread_ctx ctx3 = {
      .table = t,
      .start_tid = 100,
      .count = 100,
      .counter = 0,
    };

    i_thread t1, t2, t3;
    test_assert_equal (i_thread_create (&t1, txnt_reader_thread, &ctx1, &e),
                       SUCCESS);
    test_assert_equal (i_thread_create (&t2, txnt_reader_thread, &ctx2, &e),
                       SUCCESS);
    test_assert_equal (i_thread_create (&t3, txnt_reader_thread, &ctx3, &e),
                       SUCCESS);

    i_thread_join (&t1, &e);
    i_thread_join (&t2, &e);
    i_thread_join (&t3, &e);

    int total_reads = ctx1.counter + ctx2.counter + ctx3.counter;
    test_assert_equal (total_reads, 300);

    txnt_close (t);
  }

  TEST_CASE ("txnt_update_undo_next")
  {
    error e = error_create ();
    struct txn_table *t = txnt_open (&e);
    struct txn tx;
    txn_init (&tx, 700,
              (struct txn_data){
                  .last_lsn = 100,
                  .undo_next_lsn = 80,
                  .state = TX_RUNNING,
              });

    txnt_insert_txn (t, &tx);

    struct txn *retrieved;
    bool found = txnt_get (&retrieved, t, 700);
    test_assert (found);

    struct txn_data new_data = retrieved->data;
    new_data.undo_next_lsn = 150;
    txn_update_data (retrieved, new_data);

    found = txnt_get (&retrieved, t, 700);
    test_assert (found);
    test_assert (retrieved->data.undo_next_lsn == 150);

    txnt_close (t);
  }

  TEST_CASE ("txnt_update_state")
  {
    error e = error_create ();
    struct txn_table *t = txnt_open (&e);
    struct txn tx;
    txn_init (&tx, 800,
              (struct txn_data){
                  .last_lsn = 100,
                  .undo_next_lsn = 90,
                  .state = TX_RUNNING,
              });

    txnt_insert_txn (t, &tx);

    struct txn *retrieved;
    bool found = txnt_get (&retrieved, t, 800);
    test_assert (found);

    struct txn_data new_data = retrieved->data;
    new_data.state = TX_COMMITTED;
    txn_update_data (retrieved, new_data);

    found = txnt_get (&retrieved, t, 800);
    test_assert (found);
    test_assert (retrieved->data.state == TX_COMMITTED);

    txnt_close (t);
  }

  TEST_CASE ("txnt_concurrent_updates")
  {
    error e = error_create ();
    struct txn_table *t = txnt_open (&e);
    // Pre-populate
    struct txn txns[300];
    for (int i = 0; i < 300; i++)
      {
        txn_init (&txns[i], i,
                  (struct txn_data){
                      .last_lsn = i,
                      .undo_next_lsn = i - 1,
                      .state = TX_RUNNING,
                  });
        txnt_insert_txn (t, &txns[i]);
      }

    struct txnt_thread_ctx ctx1 = {
      .table = t,
      .start_tid = 0,
      .count = 100,
      .counter = 0,
    };
    struct txnt_thread_ctx ctx2 = {
      .table = t,
      .start_tid = 100,
      .count = 100,
      .counter = 0,
    };
    struct txnt_thread_ctx ctx3 = {
      .table = t,
      .start_tid = 200,
      .count = 100,
      .counter = 0,
    };

    i_thread t1, t2, t3;
    test_assert_equal (i_thread_create (&t1, txnt_updater_thread, &ctx1, &e),
                       SUCCESS);
    test_assert_equal (i_thread_create (&t2, txnt_updater_thread, &ctx2, &e),
                       SUCCESS);
    test_assert_equal (i_thread_create (&t3, txnt_updater_thread, &ctx3, &e),
                       SUCCESS);

    i_thread_join (&t1, &e);
    i_thread_join (&t2, &e);
    i_thread_join (&t3, &e);

    int total_updates = ctx1.counter + ctx2.counter + ctx3.counter;
    test_assert_equal (total_updates, 300);

    // Verify updates
    for (txid tid = 0; tid < 300; tid++)
      {
        struct txn *retrieved;
        test_assert (txnt_get (&retrieved, t, tid));
        test_assert_equal (retrieved->data.last_lsn, tid + 1000);
      }

    txnt_close (t);
  }

  TEST_CASE ("txnt_concurrent_state_transitions")
  {
    error e = error_create ();
    struct txn_table *t = txnt_open (&e);
    // Pre-populate with running transactions
    struct txn txns[150];
    for (int i = 0; i < 150; i++)
      {
        txn_init (&txns[i], i,
                  (struct txn_data){
                      .last_lsn = i,
                      .undo_next_lsn = i - 1,
                      .state = TX_RUNNING,
                  });
        txnt_insert_txn (t, &txns[i]);
      }

    struct txnt_thread_ctx ctx1 = {
      .table = t,
      .start_tid = 0,
      .count = 50,
      .counter = 0,
    };
    struct txnt_thread_ctx ctx2 = {
      .table = t,
      .start_tid = 50,
      .count = 50,
      .counter = 0,
    };
    struct txnt_thread_ctx ctx3 = {
      .table = t,
      .start_tid = 100,
      .count = 50,
      .counter = 0,
    };

    i_thread t1, t2, t3;
    test_assert_equal (
        i_thread_create (&t1, txnt_state_transition_thread, &ctx1, &e),
        SUCCESS);
    test_assert_equal (
        i_thread_create (&t2, txnt_state_transition_thread, &ctx2, &e),
        SUCCESS);
    test_assert_equal (
        i_thread_create (&t3, txnt_state_transition_thread, &ctx3, &e),
        SUCCESS);

    i_thread_join (&t1, &e);
    i_thread_join (&t2, &e);
    i_thread_join (&t3, &e);

    int total_transitions = ctx1.counter + ctx2.counter + ctx3.counter;
    test_assert_equal (total_transitions, 150);

    // Verify all are committed
    for (txid tid = 0; tid < 150; tid++)
      {
        struct txn *retrieved;
        test_assert (txnt_get (&retrieved, t, tid));
        test_assert_equal (retrieved->data.state, TX_COMMITTED);
      }

    txnt_close (t);
  }
}

#endif
