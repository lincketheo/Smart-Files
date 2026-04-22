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

#include "txns/txn.h"

#include "c_specx.h"
#include "c_specx_dev.h"
#include "lockt/lt_lock.h"

void txn_key_init (struct txn *dest, txid tid);

void
txn_init (struct txn *dest, const txid tid, const struct txn_data data)
{
  dest->data = data;
  dest->tid = tid;
  dest->locks = NULL;
  hnode_init (&dest->node, tid);
  latch_init (&dest->l);
  slab_alloc_init (&dest->lock_alloc, sizeof (struct txn_lock), 512);
}

void
txn_key_init (struct txn *dest, const txid tid)
{
  dest->tid = tid;
  hnode_init (&dest->node, tid);
  latch_init (&dest->l);
}

void
txn_update_data (struct txn *t, const struct txn_data data)
{
  latch_lock (&t->l);
  t->data = data;
  latch_unlock (&t->l);
}

void
txn_update (struct txn *t, enum tx_state state, const lsn last, const lsn undo_next)
{
  latch_lock (&t->l);
  t->data = (struct txn_data){
    .state = TX_CANDIDATE_FOR_UNDO,
    .last_lsn = last,
    .undo_next_lsn = undo_next,
  };
  latch_unlock (&t->l);
}

void
txn_update_state (struct txn *t, const enum tx_state new_state)
{
  latch_lock (&t->l);
  t->data.state = new_state;
  latch_unlock (&t->l);
}

void
txn_update_last_undo (struct txn *t, const lsn last_lsn, const lsn undo_next_lsn)
{
  latch_lock (&t->l);
  t->data.last_lsn = last_lsn;
  t->data.undo_next_lsn = undo_next_lsn;
  latch_unlock (&t->l);
}

void
txn_update_last_state (struct txn *t, const lsn last_lsn, const enum tx_state new_state)
{
  latch_lock (&t->l);
  t->data.last_lsn = last_lsn;
  t->data.state = new_state;
  latch_unlock (&t->l);
}

void
txn_update_last (struct txn *t, const lsn last_lsn)
{
  latch_lock (&t->l);
  t->data.last_lsn = last_lsn;
  latch_unlock (&t->l);
}

void
txn_update_undo_next (struct txn *t, const lsn undo_next)
{
  latch_lock (&t->l);
  t->data.undo_next_lsn = undo_next;
  latch_unlock (&t->l);
}

bool
txn_data_equal_unsafe (const struct txn_data *left, const struct txn_data *right)
{
  bool equal = true;

  equal = equal && left->last_lsn == right->last_lsn;
  equal = equal && left->undo_next_lsn == right->undo_next_lsn;
  equal = equal && left->state == right->state;

  return equal;
}

static bool
txn_haslock_unsafe (const struct txn *t, const struct lt_lock lock)
{
  bool ret = false;

  const struct txn_lock *curr = t->locks;
  while (curr != NULL)
    {
      if (lt_lock_equal (curr->lock, lock))
        {
          ret = true;
          goto theend;
        }
      curr = curr->next;
    }

theend:
  return ret;
}

err_t
txn_newlock (struct txn *t, const struct lt_lock lock, const enum lock_mode mode, error *e)
{
  latch_lock (&t->l);

  if (txn_haslock_unsafe (t, lock))
    {
      latch_unlock (&t->l);
      return SUCCESS;
    }

  struct txn_lock *next = slab_alloc_alloc (&t->lock_alloc, e);
  if (next == NULL)
    {
      latch_unlock (&t->l);
      return error_trace (e);
    }

  next->lock = lock;
  next->mode = mode;

  next->next = t->locks;
  t->locks = next;
  latch_unlock (&t->l);

  return SUCCESS;
}

bool
txn_haslock (struct txn *t, const struct lt_lock lock)
{
  latch_lock (&t->l);

  bool ret = false;

  const struct txn_lock *curr = t->locks;
  while (curr != NULL)
    {
      if (lt_lock_equal (curr->lock, lock))
        {
          ret = true;
          goto theend;
        }
      curr = curr->next;
    }

theend:
  latch_unlock (&t->l);
  return ret;
}

void
txn_close (struct txn *t)
{
  latch_lock (&t->l);

  struct txn_lock *curr = t->locks;
  while (curr != NULL)
    {
      struct txn_lock *next = curr->next;
      slab_alloc_free (&t->lock_alloc, curr);
      curr = next;
    }

  slab_alloc_destroy (&t->lock_alloc);

  latch_unlock (&t->l);
}

void
txn_foreach_lock (struct txn *t, const lock_func func, void *ctx)
{
  latch_lock (&t->l);

  const struct txn_lock *curr = t->locks;
  while (curr != NULL)
    {
      func (curr->lock, curr->mode, ctx);
      curr = curr->next;
    }

  latch_unlock (&t->l);
}

void
i_log_txn (const int log_level, struct txn *tx)
{
  latch_lock (&tx->l);

  i_log_info ("txn:\n");
  i_printf (log_level, "|%" PRtxid "| ", tx->tid);

  switch (tx->data.state)
    {
    case TX_RUNNING:
      {
        i_printf (log_level, "TX_RUNNING ");
        break;
      }
    case TX_CANDIDATE_FOR_UNDO:
      {
        i_printf (log_level, "TX_CANDIDATE_FOR_UNDO ");
        break;
      }
    case TX_COMMITTED:
      {
        i_printf (log_level, "TX_COMMITTED ");
        break;
      }
    case TX_DONE:
      {
        i_printf (log_level, "TX_DONE ");
        break;
      }
    }

  i_printf (log_level,
            "|last_lsn = %" PRtxid " undo_next_lsn = %" PRtxid "|\n",
            tx->data.last_lsn, tx->data.undo_next_lsn);

  const struct txn_lock *curr = tx->locks;
  while (curr)
    {
      i_printf (log_level, "     |%3s| ", gr_lock_mode_name (curr->mode));
      i_print_lt_lock (log_level, curr->lock);
      curr = curr->next;
    }

  i_log_info ("txn end\n");

  latch_unlock (&tx->l);
}

#ifndef NTEST

static void *
txn_newlock_test (void *_tx)
{
  error e = error_create ();
  struct txn *tx = _tx;
  struct lt_lock lock;

#define MAYBE_ADD_LOCK(type, r)                                              \
  lock = r;                                                                  \
  if (txn_newlock (tx, lock, LM_X, &e))                                      \
    {                                                                        \
      goto failed;                                                           \
    }                                                                        \
  if (!txn_haslock (tx, lock))                                               \
    {                                                                        \
      error_causef (&e, ERR_INVALID_ARGUMENT, "Transaction must have lock"); \
      goto failed;                                                           \
    }

  LT_LOCK_FOR_EACH_RANDOM (MAYBE_ADD_LOCK);

failed:
  return NULL;
}

TEST (txn_basic)
{
  error e = error_create ();

  TEST_CASE ("txn_newlock single threaded")
  {
    struct txn tx;
    txn_init (&tx, 10,
              (struct txn_data){
                  .state = TX_RUNNING,
                  .last_lsn = 10,
                  .undo_next_lsn = 5,
              });

    for (u32 i = 0; i < 1000; ++i)
      {
        txn_newlock_test (&tx);
      }

    txn_close (&tx);
  }

  TEST_CASE ("txn_newlock multi threaded")
  {
    struct txn tx;
    txn_init (&tx, 10,
              (struct txn_data){
                  .state = TX_RUNNING,
                  .last_lsn = 10,
                  .undo_next_lsn = 5,
              });

    i_thread threads[100];

    for (u32 i = 0; i < 100; ++i)
      {
        i_thread_create (&threads[i], txn_newlock_test, &tx, &e);
      }

    for (u32 i = 0; i < 100; ++i)
      {
        i_thread_join (&threads[i], &e);
      }

    txn_close (&tx);
  }
}
#endif
