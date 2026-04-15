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

#include "paging/txns/txn_table.h"

#include "tlclib/memory/alloc.h"
#include "tlclib/dev/assert.h"
#include "tlclib/ds/dbl_buffer.h"
#include "tlclib/memory/deserializer.h"
#include "tlclib/dev/error.h"
#include "tlclib/ds/hash_table.h"
#include "tlclib/concurrency/latch.h"
#include "tlclib/core/random.h"
#include "tlclib/memory/serializer.h"
#include "tlclib/memory/slab_alloc.h"
#include "tlclib/intf/logging.h"
#include "tlclib/intf/os/memory.h"
#include "numstore/stdtypes.h"
#include "paging/txns/txn.h"
#include "test/testing.h"

DEFINE_DBG_ASSERT (struct txn_table, txn_table, t, { ASSERT (t); })

#define TXN_SERIAL_UNIT (sizeof (txid) + sizeof (lsn) + sizeof (lsn))

struct txn_table
{
  latch l;
  struct htable *t;

  bool isfrozen;
};

struct txn_table *
txnt_open (error *e)
{
  struct txn_table *dest = i_malloc (1, sizeof *dest, e);
  if (dest == NULL)
    {
      goto failed;
    }

  dest->t = htable_create (512, e);
  if (dest->t == NULL)
    {
      goto dest_failed;
    }

  latch_init (&dest->l);

  return dest;

dest_failed:
  i_free (dest);
failed:
  return NULL;
}

void
txnt_close (struct txn_table *t)
{
  DBG_ASSERT (txn_table, t);
  latch_lock (&t->l);
  htable_free (t->t);
  latch_unlock (&t->l);
  i_free (t);
}

#ifndef NTEST
TEST (TT_UNIT, txnt_open)
{
  TEST_CASE ("basic")
  {
    error e = error_create ();
    struct txn_table *t = txnt_open (&e);
    txnt_close (t);
  }

  TEST_CASE ("open multiple")
  {
    error e = error_create ();
    for (int i = 0; i < 4; ++i)
      {
        struct txn_table *t = txnt_open (&e);
        txnt_close (t);
      }
  }
}
#endif

static const char *
txn_state_to_str (const int state)
{
  switch (state)
    {
      case_ENUM_RETURN_STRING (TX_RUNNING);
      case_ENUM_RETURN_STRING (TX_CANDIDATE_FOR_UNDO);
      case_ENUM_RETURN_STRING (TX_COMMITTED);
      case_ENUM_RETURN_STRING (TX_DONE);
    }

  UNREACHABLE ();
}

static void
i_log_txn_in_txnt (struct hnode *node, void *_log_level)
{
  const int *log_level = _log_level;
  struct txn *tx = container_of (node, struct txn, node);

  latch_lock (&tx->l);

  i_printf (*log_level,
            "| %d | %" PRtxid " | %" PRpgno " | %" PRpgno " | %s |\n",
            tx->node.hcode, tx->tid, tx->data.last_lsn, tx->data.undo_next_lsn,
            txn_state_to_str (tx->data.state));

  latch_unlock (&tx->l);
}

void
i_log_txnt (int log_level, struct txn_table *t)
{
  latch_lock (&t->l);

  i_log (log_level, "============ TXN TABLE START ===============\n");
  htable_foreach (t->t, i_log_txn_in_txnt, &log_level);
  i_log (log_level, "============ TXN TABLE END   ===============\n");

  latch_unlock (&t->l);
}

struct merge_ctx
{
  struct txn_table *dest;
  error *e;
  struct dbl_buffer *txn_dest;
  struct slab_alloc *alloc;
};

static void
merge_txn (struct txn *tx, void *vctx)
{
  const struct merge_ctx *ctx = vctx;
  ASSERT (ctx->txn_dest == NULL || ctx->txn_dest->size == sizeof (struct txn));

  // Fail fast on an error
  if (ctx->e->cause_code)
    {
      return;
    }

  latch_lock (&tx->l);

  // Skip duplicate transactions
  if (txn_exists (ctx->dest, tx->tid))
    {
      goto theend;
    }

  // Handle in case we copy transaction over
  struct txn *target_txn = tx;

  // If provided, allocate a new transaction to copy over
  if (ctx->txn_dest && ctx->alloc)
    {
      target_txn = slab_alloc_alloc (ctx->alloc, ctx->e);
      if (target_txn == NULL)
        {
          goto theend;
        }
      if (dblb_append (ctx->txn_dest, &target_txn, 1, ctx->e))
        {
          goto theend;
        }
      txn_init (target_txn, tx->tid, tx->data);
    }

  // Insert into the table
  txnt_insert_txn (ctx->dest, target_txn);

theend:
  latch_unlock (&tx->l);
}

err_t
txnt_merge_into (struct txn_table *dest, struct txn_table *src,
                 struct dbl_buffer *txn_dest, struct slab_alloc *alloc,
                 error *e)
{
  struct merge_ctx ctx = {
    .dest = dest,
    .e = e,
    .txn_dest = txn_dest,
  };

  latch_lock (&src->l);
  txnt_foreach (src, merge_txn, &ctx);
  latch_unlock (&src->l);

  return ctx.e->cause_code;
}

#ifndef NTEST
TEST (TT_UNIT, txnt_merge_into)
{
  TEST_CASE ("Empty to Empty")
  {
    error e = error_create ();
    struct txn_table *src = txnt_open (&e);
    struct txn_table *dest = txnt_open (&e);
    const err_t result = txnt_merge_into (dest, src, NULL, NULL, &e);
    test_assert (result == SUCCESS);

    txnt_close (dest);
    txnt_close (src);
  }

  TEST_CASE ("Data")
  {
    error e = error_create ();
    struct txn_table *dest = txnt_open (&e);
    struct txn_table *src = txnt_open (&e);
    // Add to dest
    struct txn dest_txns[5];
    for (int i = 0; i < 5; i++)
      {
        txn_init (&dest_txns[i], i + 1,
                  (struct txn_data){
                      .last_lsn = (i + 1) * 10,
                      .undo_next_lsn = (i + 1) * 10 - 1,
                      .state = TX_RUNNING,
                  });
        txnt_insert_txn (dest, &dest_txns[i]);
      }

    // Add to src (different tids)
    struct txn src_txns[5];
    for (int i = 0; i < 5; i++)
      {
        txn_init (&src_txns[i], i + 6,
                  (struct txn_data){
                      .last_lsn = (i + 6) * 10,
                      .undo_next_lsn = (i + 6) * 10 - 1,
                      .state = TX_CANDIDATE_FOR_UNDO,
                  });
        txnt_insert_txn (src, &src_txns[i]);
      }

    const err_t result = txnt_merge_into (dest, src, NULL, NULL, &e);
    test_assert (result == SUCCESS);

    // Verify all exist in dest
    for (txid tid = 1; tid <= 10; tid++)
      {
        test_assert (txn_exists (dest, tid));
      }

    txnt_close (dest);
    txnt_close (src);
  }

  TEST_CASE ("no duplicate insert")
  {
    error e = error_create ();

    struct txn_table *dest = txnt_open (&e);
    struct txn_table *src = txnt_open (&e);
    // Add same tid to both with different values
    struct txn dest_tx;
    txn_init (&dest_tx, 42,
              (struct txn_data){
                  .last_lsn = 100,
                  .undo_next_lsn = 90,
                  .state = TX_RUNNING,
              });
    txnt_insert_txn (dest, &dest_tx);

    struct txn src_tx;
    txn_init (&src_tx, 42,
              (struct txn_data){
                  .last_lsn = 200,
                  .undo_next_lsn = 190,
                  .state = TX_COMMITTED,
              });
    txnt_insert_txn (src, &src_tx);

    txnt_merge_into (dest, src, NULL, NULL, &e);

    // Dest should keep its original value
    struct txn *retrieved;
    bool found = txnt_get (&retrieved, dest, 42);
    test_assert (found);
    test_assert_int_equal (retrieved->data.last_lsn, 100);
    test_assert_int_equal (retrieved->data.state, TX_RUNNING);

    txnt_close (dest);
    txnt_close (src);
  }
}
#endif

static void
find_max_undo (struct txn *tx, void *vctx)
{
  slsn *max = vctx;

  latch_lock (&tx->l);

  if (tx->data.state == TX_CANDIDATE_FOR_UNDO)
    {
      if ((slsn)tx->data.undo_next_lsn > *max)
        {
          *max = tx->data.undo_next_lsn;
        }
    }

  latch_unlock (&tx->l);
}

slsn
txnt_max_u_undo_lsn (struct txn_table *t)
{
  slsn max = -1;

  latch_lock (&t->l);
  txnt_foreach (t, find_max_undo, &max);
  latch_unlock (&t->l);

  return max;
}

#ifndef NTEST
TEST (TT_UNIT, txnt_max_u_undo_lsn)
{
  TEST_CASE ("empty")
  {
    error e = error_create ();
    struct txn_table *t = txnt_open (&e);
    slsn max = txnt_max_u_undo_lsn (t);
    test_assert (max == -1);

    txnt_close (t);
  }

  TEST_CASE ("candidates")
  {
    error e = error_create ();
    struct txn_table *t = txnt_open (&e);
    struct txn tx1, tx2, tx3, tx4;

    // Running - should be ignored
    txn_init (&tx1, 1,
              (struct txn_data){ .last_lsn = 100,
                                 .undo_next_lsn = 100,
                                 .state = TX_RUNNING });

    // Candidates
    txn_init (&tx2, 2,
              (struct txn_data){ .last_lsn = 50,
                                 .undo_next_lsn = 40,
                                 .state = TX_CANDIDATE_FOR_UNDO });
    txn_init (&tx3, 3,
              (struct txn_data){ .last_lsn = 80,
                                 .undo_next_lsn = 75,
                                 .state = TX_CANDIDATE_FOR_UNDO });
    txn_init (&tx4, 4,
              (struct txn_data){ .last_lsn = 60,
                                 .undo_next_lsn = 55,
                                 .state = TX_CANDIDATE_FOR_UNDO });

    txnt_insert_txn (t, &tx1);
    txnt_insert_txn (t, &tx2);
    txnt_insert_txn (t, &tx3);
    txnt_insert_txn (t, &tx4);

    slsn max = txnt_max_u_undo_lsn (t);
    test_assert (max == 75);

    txnt_close (t);
  }

  TEST_CASE ("only running txns")
  {
    error e = error_create ();
    struct txn_table *t = txnt_open (&e);
    struct txn tx1, tx2, tx3;
    txn_init (&tx1, 1,
              (struct txn_data){
                  .last_lsn = 100, .undo_next_lsn = 90, .state = TX_RUNNING });
    txn_init (&tx2, 2,
              (struct txn_data){ .last_lsn = 200,
                                 .undo_next_lsn = 190,
                                 .state = TX_RUNNING });
    txn_init (&tx3, 3,
              (struct txn_data){ .last_lsn = 300,
                                 .undo_next_lsn = 290,
                                 .state = TX_COMMITTED });

    txnt_insert_txn (t, &tx1);
    txnt_insert_txn (t, &tx2);
    txnt_insert_txn (t, &tx3);

    slsn max = txnt_max_u_undo_lsn (t);
    test_assert (max == -1);

    txnt_close (t);
  }
}
#endif

static void
find_min_lsn (struct txn *tx, void *vctx)
{
  slsn *min = vctx;

  latch_lock (&tx->l);

  if (*min == -1 || (slsn)tx->data.min_lsn < *min)
    {
      *min = tx->data.min_lsn;
    }

  latch_unlock (&tx->l);
}

slsn
txnt_min_lsn (struct txn_table *t)
{
  slsn min = -1; // Actually a big number b/c unsigned

  latch_lock (&t->l);
  txnt_foreach (t, find_min_lsn, &min);
  latch_unlock (&t->l);

  return min;
}

#ifndef NTEST
TEST (TT_UNIT, txnt_min_lsn)
{
  TEST_CASE ("empty")
  {
    error e = error_create ();
    struct txn_table *t = txnt_open (&e);
    const slsn min = txnt_min_lsn (t);
    test_assert (min == -1);

    txnt_close (t);
  }

  TEST_CASE ("candidates")
  {
    error e = error_create ();
    struct txn_table *t = txnt_open (&e);
    struct txn tx1, tx2, tx3, tx4;

    txn_init (&tx1, 1, (struct txn_data){ .min_lsn = 100 });
    txn_init (&tx2, 2, (struct txn_data){ .min_lsn = 40 });
    txn_init (&tx3, 3, (struct txn_data){ .min_lsn = 75 });
    txn_init (&tx4, 4, (struct txn_data){ .min_lsn = 55 });

    txnt_insert_txn (t, &tx1);
    txnt_insert_txn (t, &tx2);
    txnt_insert_txn (t, &tx3);
    txnt_insert_txn (t, &tx4);

    const slsn min = txnt_min_lsn (t);
    test_assert (min == 40);

    txnt_close (t);
  }
}
#endif

struct foreach_ctx
{
  void (*action) (struct txn *, void *ctx);
  void *ctx;
};

static void
hnode_foreach (struct hnode *node, void *ctx)
{
  const struct foreach_ctx *_ctx = ctx;
  _ctx->action (container_of (node, struct txn, node), _ctx->ctx);
}

void
txnt_foreach (const struct txn_table *t,
              void (*action) (struct txn *, void *ctx),
              void *ctx)
{
  struct foreach_ctx _ctx = {
    .action = action,
    .ctx = ctx,
  };
  htable_foreach (t->t, hnode_foreach, &_ctx);
}

u32
txnt_get_size (const struct txn_table *dest)
{
  return htable_size (dest->t);
}

static bool
txn_equals_for_exists (const struct hnode *left, const struct hnode *right)
{
  // Might have passed the exact same ref as exists in the htable
  if (left == right)
    {
      return true;
    }

  // Otherwise, passed a key with just relevant information
  else
    {
      struct txn *_left = container_of (left, struct txn, node);
      struct txn *_right = container_of (right, struct txn, node);

      latch_lock (&_left->l);
      latch_lock (&_right->l);

      bool ret = _left->tid == _right->tid;

      latch_unlock (&_right->l);
      latch_unlock (&_left->l);

      return ret;
    }
}

bool
txn_exists (const struct txn_table *t, const txid tid)
{
  struct txn tx;
  txn_key_init (&tx, tid);

  struct hnode **ret = htable_lookup (t->t, &tx.node, txn_equals_for_exists);

  return ret != NULL;
}

#ifndef NTEST
TEST (TT_UNIT, txnt_exists)
{
  TEST_CASE ("txnt_exists")
  {
    error e = error_create ();
    struct txn_table *t = txnt_open (&e);
    test_assert (!txn_exists (t, 1100));

    struct txn tx;
    txn_init (&tx, 1100,
              (struct txn_data){
                  .last_lsn = 100,
                  .undo_next_lsn = 90,
                  .state = TX_RUNNING,
              });

    txnt_insert_txn (t, &tx);
    test_assert (txn_exists (t, 1100));

    txnt_close (t);
  }
}
#endif

void
txnt_insert_txn (struct txn_table *t, struct txn *tx)
{
  DBG_ASSERT (txn_table, t);
  ASSERT (!txn_exists (t, tx->tid));

  latch_lock (&t->l);
  htable_insert (t->t, &tx->node);
  latch_unlock (&t->l);
}

void
txnt_insert_txn_if_not_exists (struct txn_table *t, struct txn *tx)
{
  DBG_ASSERT (txn_table, t);

  latch_lock (&t->l);

  if (txn_exists (t, tx->tid))
    {
      goto theend;
    }

  htable_insert (t->t, &tx->node);

theend:
  latch_unlock (&t->l);
}

#ifndef NTEST
TEST (TT_UNIT, txnt_insert)
{
  TEST_CASE ("new")
  {
    error e = error_create ();
    struct txn_table *t = txnt_open (&e);
    struct txn tx;
    txn_init (&tx, 900,
              (struct txn_data){
                  .last_lsn = 100,
                  .undo_next_lsn = 90,
                  .state = TX_RUNNING,
              });

    txnt_insert_txn_if_not_exists (t, &tx);

    struct txn *retrieved;
    bool found = txnt_get (&retrieved, t, 900);
    test_assert (found);
    test_assert (retrieved->tid == 900);

    txnt_close (t);
  }

  TEST_CASE ("if not exists but exists")
  {
    error e = error_create ();
    struct txn_table *t = txnt_open (&e);
    struct txn tx1;
    txn_init (&tx1, 1000,
              (struct txn_data){
                  .last_lsn = 100,
                  .undo_next_lsn = 90,
                  .state = TX_RUNNING,
              });

    txnt_insert_txn (t, &tx1);

    struct txn tx2;
    txn_init (&tx2, 1000,
              (struct txn_data){
                  .last_lsn = 200,
                  .undo_next_lsn = 180,
                  .state = TX_COMMITTED,
              });

    txnt_insert_txn_if_not_exists (t, &tx2);

    struct txn *retrieved;
    bool found = txnt_get (&retrieved, t, 1000);
    test_assert (found);
    test_assert (retrieved->data.last_lsn == 100);
    test_assert (retrieved->data.state == TX_RUNNING);

    txnt_close (t);
  }

  TEST_CASE ("different states")
  {
    error e = error_create ();
    struct txn_table *t = txnt_open (&e);
    struct txn tx1, tx2, tx3;

    txn_init (&tx1, 1,
              (struct txn_data){
                  .last_lsn = 10, .undo_next_lsn = 9, .state = TX_RUNNING });
    txn_init (&tx2, 2,
              (struct txn_data){ .last_lsn = 20,
                                 .undo_next_lsn = 19,
                                 .state = TX_CANDIDATE_FOR_UNDO });
    txn_init (&tx3, 3,
              (struct txn_data){ .last_lsn = 30,
                                 .undo_next_lsn = 29,
                                 .state = TX_COMMITTED });

    txnt_insert_txn (t, &tx1);
    txnt_insert_txn (t, &tx2);
    txnt_insert_txn (t, &tx3);

    struct txn *retrieved;

    test_assert (txnt_get (&retrieved, t, 1));
    test_assert (retrieved->data.state == TX_RUNNING);

    test_assert (txnt_get (&retrieved, t, 2));
    test_assert (retrieved->data.state == TX_CANDIDATE_FOR_UNDO);

    test_assert (txnt_get (&retrieved, t, 3));
    test_assert (retrieved->data.state == TX_COMMITTED);

    txnt_close (t);
  }
}
#endif

bool
txnt_get (struct txn **dest, struct txn_table *t, const txid tid)
{
  DBG_ASSERT (txn_table, t);

  struct txn key;
  txn_key_init (&key, tid);

  latch_lock (&t->l);

  struct hnode **node = htable_lookup (t->t, &key.node, txn_equals_for_exists);
  if (node)
    {
      *dest = container_of (*node, struct txn, node);
    }

  latch_unlock (&t->l);

  return node != NULL;
}

void
txnt_get_expect (struct txn **dest, struct txn_table *t, const txid tid)
{
  DBG_ASSERT (txn_table, t);

  struct txn key;
  txn_key_init (&key, tid);

  latch_lock (&t->l);

  struct hnode **node = htable_lookup (t->t, &key.node, txn_equals_for_exists);
  ASSERT (node);
  *dest = container_of (*node, struct txn, node);

  latch_unlock (&t->l);
}

#ifndef NTEST
TEST (TT_UNIT, txnt_get)
{
  TEST_CASE ("nonexistent returns false")
  {
    error e = error_create ();
    struct txn_table *t = txnt_open (&e);
    struct txn *retrieved;
    bool found = txnt_get (&retrieved, t, 9999);
    test_assert (!found);

    txnt_close (t);
  }

  TEST_CASE ("and get tx running")
  {
    error e = error_create ();
    struct txn_table *t = txnt_open (&e);
    struct txn tx;
    txn_init (&tx, 100,
              (struct txn_data){
                  .last_lsn = 50,
                  .undo_next_lsn = 40,
                  .state = TX_RUNNING,
              });

    txnt_insert_txn (t, &tx);

    struct txn *retrieved;
    bool found = txnt_get (&retrieved, t, 100);
    test_assert (found);
    test_assert (txn_data_equal_unsafe (&retrieved->data, &tx.data));

    txnt_close (t);
  }

  TEST_CASE ("and get tx candidate for undo")
  {
    error e = error_create ();
    struct txn_table *t = txnt_open (&e);
    struct txn tx;
    txn_init (&tx, 200,
              (struct txn_data){
                  .last_lsn = 100,
                  .undo_next_lsn = 90,
                  .state = TX_CANDIDATE_FOR_UNDO,
              });

    txnt_insert_txn (t, &tx);

    struct txn *retrieved;
    bool found = txnt_get (&retrieved, t, 200);
    test_assert (found);
    test_assert (retrieved->tid == 200);
    test_assert (retrieved->data.last_lsn == 100);
    test_assert (retrieved->data.undo_next_lsn == 90);
    test_assert (retrieved->data.state == TX_CANDIDATE_FOR_UNDO);

    txnt_close (t);
  }

  TEST_CASE ("update last lsn")
  {
    error e = error_create ();
    struct txn_table *t = txnt_open (&e);
    struct txn tx;
    txn_init (&tx, 600,
              (struct txn_data){
                  .last_lsn = 100,
                  .undo_next_lsn = 90,
                  .state = TX_RUNNING,
              });

    txnt_insert_txn (t, &tx);

    // Fetch, update, verify
    struct txn *retrieved;
    bool found = txnt_get (&retrieved, t, 600);
    test_assert (found);

    struct txn_data new_data = retrieved->data;
    new_data.last_lsn = 200;
    txn_update_data (retrieved, new_data);

    // Verify update
    found = txnt_get (&retrieved, t, 600);
    test_assert (found);
    test_assert (retrieved->data.last_lsn == 200);

    txnt_close (t);
  }

  TEST_CASE ("txnt state transitions all types")
  {
    error e = error_create ();
    struct txn_table *t = txnt_open (&e);
    struct txn tx;
    txn_init (&tx, 2000,
              (struct txn_data){
                  .last_lsn = 100,
                  .undo_next_lsn = 99,
                  .state = TX_RUNNING,
              });

    txnt_insert_txn (t, &tx);

    struct txn key;
    struct txn *retrieved = &key;

    test_assert (txnt_get (&retrieved, t, 2000));
    test_assert (retrieved->data.state == TX_RUNNING);

    // Transition to CANDIDATE_FOR_UNDO
    struct txn_data new_data = retrieved->data;
    new_data.state = TX_CANDIDATE_FOR_UNDO;
    txn_update_data (retrieved, new_data);

    test_assert (txnt_get (&retrieved, t, 2000));
    test_assert (retrieved->data.state == TX_CANDIDATE_FOR_UNDO);

    // Transition to COMMITTED
    new_data = retrieved->data;
    new_data.state = TX_COMMITTED;
    txn_update_data (retrieved, new_data);

    test_assert (txnt_get (&retrieved, t, 2000));
    test_assert (retrieved->data.state == TX_COMMITTED);

    txnt_close (t);
  }
}
#endif

void
txnt_remove_txn (bool *exists, struct txn_table *t, const struct txn *tx)
{
  DBG_ASSERT (txn_table, t);

  latch_lock (&t->l);

  struct hnode **node = htable_lookup (t->t, &tx->node, txn_equals_for_exists);

  if (node == NULL)
    {
      *exists = false;
      goto theend;
    }

  *exists = true;

  htable_delete (t->t, node);

theend:
  latch_unlock (&t->l);
}

void
txnt_remove_txn_expect (struct txn_table *t, const struct txn *tx)
{
  DBG_ASSERT (txn_table, t);

  latch_lock (&t->l);

  struct hnode **node = htable_lookup (t->t, &tx->node, txn_equals_for_exists);

  ASSERT (node != NULL);

  htable_delete (t->t, node);

  latch_unlock (&t->l);
}

#ifndef NTEST
TEST (TT_UNIT, txnt_remove)
{
  TEST_CASE ("txnt_remove_existing_txn")
  {
    error e = error_create ();
    struct txn_table *t = txnt_open (&e);
    struct txn tx;
    txn_init (&tx, 400,
              (struct txn_data){
                  .last_lsn = 100,
                  .undo_next_lsn = 90,
                  .state = TX_RUNNING,
              });

    txnt_insert_txn (t, &tx);

    bool removed;
    txnt_remove_txn (&removed, t, &tx);
    test_assert (removed);

    struct txn *retrieved;
    bool found = txnt_get (&retrieved, t, 400);
    test_assert (!found);

    txnt_close (t);
  }

  TEST_CASE ("txnt_remove_nonexistent_txn")
  {
    error e = error_create ();
    struct txn_table *t = txnt_open (&e);
    struct txn tx;
    txn_key_init (&tx, 500);

    bool removed;
    txnt_remove_txn (&removed, t, &tx);
    test_assert (!removed);

    txnt_close (t);
  }

  TEST_CASE ("txnt_double_remove_same_transaction")
  {
    error e = error_create ();
    struct txn_table *t = txnt_open (&e);
    struct txn tx;
    txn_init (&tx, 100,
              (struct txn_data){
                  .last_lsn = 50,
                  .undo_next_lsn = 49,
                  .state = TX_RUNNING,
              });

    txnt_insert_txn (t, &tx);

    bool removed;
    txnt_remove_txn (&removed, t, &tx);
    test_assert (removed);

    txnt_remove_txn (&removed, t, &tx);
    test_assert (!removed);

    txnt_close (t);
  }

  TEST_CASE ("txnt_operations_after_remove")
  {
    error e = error_create ();
    struct txn_table *t = txnt_open (&e);
    struct txn tx;
    txn_init (&tx, 200,
              (struct txn_data){
                  .last_lsn = 50,
                  .undo_next_lsn = 49,
                  .state = TX_RUNNING,
              });

    txnt_insert_txn (t, &tx);

    bool removed;
    txnt_remove_txn (&removed, t, &tx);
    test_assert (removed);

    // Get should fail
    struct txn *retrieved;
    bool found = txnt_get (&retrieved, t, 200);
    test_assert (!found);

    txnt_close (t);
  }
}
#endif

void
txnt_freeze_active_txns_for_serialization (struct txn_table *t)
{
  latch_lock (&t->l);
  t->isfrozen = true;
}

void
txnt_unfreeze (struct txn_table *t)
{
  ASSERT (t->isfrozen);
  latch_unlock (&t->l);
}

u32
txnt_get_serialize_size (const struct txn_table *t)
{
  return htable_size (t->t) * TXN_SERIAL_UNIT;
}

struct txn_serialize_ctx
{
  struct serializer s;
};

static void
hnode_foreach_serialize (struct hnode *node, void *ctx)
{
  struct txn_serialize_ctx *_ctx = ctx;

  const struct txn *tx = container_of (node, struct txn, node);

  txid tid;
  lsn last_lsn;
  lsn undo_next_lsn;

  tid = tx->tid;
  last_lsn = tx->data.last_lsn;
  undo_next_lsn = tx->data.undo_next_lsn;

  srlizr_write_expect (&_ctx->s, &tid, sizeof (tid));
  srlizr_write_expect (&_ctx->s, &last_lsn, sizeof (last_lsn));
  srlizr_write_expect (&_ctx->s, &undo_next_lsn, sizeof (undo_next_lsn));
}

u32
txnt_serialize (u8 *dest, const u32 dlen, struct txn_table *t)
{
  ASSERT (t->isfrozen);

  struct txn_serialize_ctx ctx = {
    .s = srlizr_create (dest, dlen),
  };

  htable_foreach (t->t, hnode_foreach_serialize, &ctx);

  // Unfreeze
  latch_unlock (&t->l);

  return ctx.s.dlen;
}

struct txn_table *
txnt_deserialize (const u8 *src, struct txn *txn_bank, const u32 slen, error *e)
{
  struct txn_table *dest = txnt_open (e);
  if (dest == NULL)
    {
      goto failed;
    }

  if (slen == 0)
    {
      return dest;
    }

  struct deserializer d = dsrlizr_create (src, slen);

  ASSERT (slen % TXN_SERIAL_UNIT == 0);
  const u32 tlen = slen / TXN_SERIAL_UNIT;

  for (u32 i = 0; i < tlen; ++i)
    {
      txid tid = 0;
      lsn last_lsn = 0;
      lsn undo_next_lsn = 0;

      dsrlizr_read_expect (&tid, sizeof (tid), &d);
      dsrlizr_read_expect (&last_lsn, sizeof (last_lsn), &d);
      dsrlizr_read_expect (&undo_next_lsn, sizeof (undo_next_lsn), &d);

      txn_init (&txn_bank[i], tid,
                (struct txn_data){
                    .last_lsn = last_lsn,
                    .undo_next_lsn = undo_next_lsn,
                    .state = TX_CANDIDATE_FOR_UNDO,
                });

      txnt_insert_txn (dest, &txn_bank[i]);
    }

  return dest;

failed:
  return NULL;
}

u32
txnlen_from_serialized (const u32 slen)
{
  ASSERT (slen % TXN_SERIAL_UNIT == 0);
  return slen / TXN_SERIAL_UNIT;
}

#ifndef NTEST
TEST (TT_UNIT, txnt_serialize)
{
  TEST_CASE ("txnt_serialize_deserialize_empty")
  {
    error e = error_create ();
    struct txn_table *t = txnt_open (&e);
    txnt_freeze_active_txns_for_serialization (t);
    u8 buffer[4096];
    const u32 size = txnt_serialize (buffer, sizeof (buffer), t);
    test_assert (size == 0);

    struct txn txn_bank[10];
    struct txn_table *t2 = txnt_deserialize (buffer, txn_bank, size, &e);
    test_assert (txnt_equal_ignore_state (t, t2));

    txnt_close (t);
    txnt_close (t2);
  }

  TEST_CASE ("txnt_serialize_deserialize_with_data")
  {
    error e = error_create ();
    struct txn_table *t = txnt_open (&e);
    struct txn txns[10];
    for (int i = 0; i < 10; i++)
      {
        txn_init (&txns[i], i + 1,
                  (struct txn_data){
                      .last_lsn = (i + 1) * 100,
                      .undo_next_lsn = (i + 1) * 100 - 10,
                      .state = TX_RUNNING,
                  });

        txnt_insert_txn (t, &txns[i]);
      }

    txnt_freeze_active_txns_for_serialization (t);
    u8 buffer[4096];
    const u32 size = txnt_serialize (buffer, sizeof (buffer), t);
    test_assert (size > 0);

    struct txn txn_bank[10];
    struct txn_table *t2 = txnt_deserialize (buffer, txn_bank, size, &e);
    for (txid tid = 1; tid <= 10; tid++)
      {
        struct txn *retrieved;
        bool found = txnt_get (&retrieved, t2, tid);
        test_assert (found);
        test_assert (retrieved->data.state == TX_CANDIDATE_FOR_UNDO);
      }

    txnt_close (t);
    txnt_close (t2);
  }
}
#endif

struct txnt_eq_ctx
{
  struct txn_table *other;
  bool ret;
};

static void
txnt_eq_foreach (struct hnode *node, void *_ctx)
{
  struct txnt_eq_ctx *ctx = _ctx;
  if (ctx->ret == false)
    {
      return;
    }

  struct txn *tx = container_of (node, struct txn, node);
  struct txn candidate;

  latch_lock (&tx->l);
  {
    txn_key_init (&candidate, tx->tid);

    struct hnode **other_node = htable_lookup (ctx->other->t, &candidate.node,
                                               txn_equals_for_exists);

    if (other_node == NULL)
      {
        ctx->ret = false;
        latch_unlock (&tx->l);
        return;
      }

    struct txn *other_tx = container_of (*other_node, struct txn, node);

    latch_lock (&other_tx->l);
    {
      bool equal = true;

      equal = equal && tx->data.last_lsn == other_tx->data.last_lsn;
      equal = equal && tx->data.undo_next_lsn == other_tx->data.undo_next_lsn;

      ctx->ret = equal;
    }
    latch_unlock (&other_tx->l);
  }
  latch_unlock (&tx->l);
}

bool
txnt_equal_ignore_state (struct txn_table *left, struct txn_table *right)
{
  latch_lock (&left->l);
  latch_lock (&right->l);

  if (htable_size (left->t) != htable_size (right->t))
    {
      latch_unlock (&right->l);
      latch_unlock (&left->l);
      return false;
    }

  struct txnt_eq_ctx ctx = {
    .other = right,
    .ret = true,
  };
  htable_foreach (left->t, txnt_eq_foreach, &ctx);

  latch_unlock (&right->l);
  latch_unlock (&left->l);

  return ctx.ret;
}

#ifndef NTEST
TEST (TT_UNIT, txnt_equal_ignore_state)
{
  TEST_CASE ("txnt_equal_ignore_state_empty_tables")
  {
    error e = error_create ();
    struct txn_table *t1 = txnt_open (&e);
    struct txn_table *t2 = txnt_open (&e);
    test_assert (txnt_equal_ignore_state (t1, t2));

    txnt_close (t1);
    txnt_close (t2);
  }

  TEST_CASE ("txnt_equal_ignore_state_same_content")
  {
    error e = error_create ();
    struct txn_table *t1 = txnt_open (&e);
    struct txn_table *t2 = txnt_open (&e);
    struct txn t1_txns[5], t2_txns[5];
    for (int i = 0; i < 5; i++)
      {
        txn_init (&t1_txns[i], i + 1,
                  (struct txn_data){
                      .last_lsn = (i + 1) * 10,
                      .undo_next_lsn = (i + 1) * 10 - 1,
                      .state = TX_RUNNING,
                  });
        txn_init (&t2_txns[i], i + 1,
                  (struct txn_data){
                      .last_lsn = (i + 1) * 10,
                      .undo_next_lsn = (i + 1) * 10 - 1,
                      .state = TX_RUNNING,
                  });
        txnt_insert_txn (t1, &t1_txns[i]);
        txnt_insert_txn (t2, &t2_txns[i]);
      }

    test_assert (txnt_equal_ignore_state (t1, t2));

    txnt_close (t1);
    txnt_close (t2);
  }

  TEST_CASE ("txnt_not_equal_different_content")
  {
    error e = error_create ();
    struct txn_table *t1 = txnt_open (&e);
    struct txn_table *t2 = txnt_open (&e);
    struct txn tx1, tx2;
    txn_init (&tx1, 1,
              (struct txn_data){
                  .last_lsn = 10, .undo_next_lsn = 9, .state = TX_RUNNING });
    txn_init (&tx2, 1,
              (struct txn_data){
                  .last_lsn = 20, .undo_next_lsn = 19, .state = TX_RUNNING });

    txnt_insert_txn (t1, &tx1);
    txnt_insert_txn (t2, &tx2);

    test_assert (!txnt_equal_ignore_state (t1, t2));

    txnt_close (t1);
    txnt_close (t2);
  }
}
#endif

err_t
txnt_rand_populate (struct txn_table *t, struct alloc *alloc, error *e)
{
  latch_lock (&t->l);
  const u32 len = htable_size (t->t);

  txid tid = 0;

  for (u32 i = 0; i < 100 - len; ++i, tid += randu32r (0, 100))
    {
      struct txn *tx = alloc_alloc (alloc, 1, sizeof *tx, NULL);
      if (tx == NULL)
        {
          goto theend;
        }

      txn_init (tx, tid,
                (struct txn_data){
                    .last_lsn = randu32r (0, 1000),
                    .undo_next_lsn = randu32r (0, 1000),
                    .state = TX_RUNNING,
                });

      htable_insert (t->t, &tx->node);
    }

theend:
  latch_unlock (&t->l);
  return error_trace (e);
}

err_t
txnt_determ_populate (struct txn_table *t, struct alloc *alloc, error *e)
{
  latch_lock (&t->l);
  const u32 len = htable_size (t->t);

  txid tid = 0;

  for (u32 i = 0; i < 1000 - len; ++i, tid++)
    {
      struct txn *tx = alloc_alloc (alloc, 1, sizeof *tx, NULL);
      if (tx == NULL)
        {
          goto theend;
        }

      txn_init (tx, tid,
                (struct txn_data){
                    .last_lsn = i,
                    .undo_next_lsn = i,
                    .state = TX_RUNNING,
                });

      htable_insert (t->t, &tx->node);
    }

theend:
  latch_unlock (&t->l);
  return error_trace (e);
}

void
txnt_crash (struct txn_table *t)
{
  DBG_ASSERT (txn_table, t);
  htable_free (t->t);
  i_free (t);
}
