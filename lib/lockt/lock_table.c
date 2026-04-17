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

#include "lockt/lock_table.h"

#include "lockt/lt_lock.h"
#include "pager.h"
#include "c_specx.h"
#include "txns/txn.h"

struct lockt_frame
{
  struct lt_lock key;
  struct gr_lock lock;
  struct hnode node;
  int refcount;
};

static err_t
lockt_frame_init (struct lockt_frame *dest, const struct lt_lock key, error *e)
{
  WRAP (gr_lock_init (&dest->lock, e));

  dest->key = key;
  dest->refcount = 0;
  hnode_init (&dest->node, lt_lock_key (key));

  return SUCCESS;
}

static void
lockt_frame_destroy (struct lockt_frame *dest)
{
  gr_lock_destroy (&dest->lock);
}

static void
lockt_frame_init_key (struct lockt_frame *dest, const struct lt_lock key)
{
  dest->key = key;
  hnode_init (&dest->node, lt_lock_key (key));
}

static bool
lockt_frame_eq (const struct hnode *left, const struct hnode *right)
{
  const struct lockt_frame *_left = container_of (left, struct lockt_frame, node);
  const struct lockt_frame *_right = container_of (right, struct lockt_frame, node);
  return lt_lock_equal (_left->key, _right->key);
}

static void
frame_ref (struct lockt_frame *frame)
{
  frame->refcount++;
}

static void
frame_unref (struct lockt *t, struct lockt_frame *frame)
{
  ASSERT (frame->refcount > 0);
  frame->refcount--;

  if (frame->refcount == 0)
    {
      struct hnode **found = htable_lookup (t->table, &frame->node, lockt_frame_eq);
      htable_delete (t->table, found);
      lockt_frame_destroy (frame);
      slab_alloc_free (&t->lock_alloc, frame);
    }
}

err_t
lockt_init (struct lockt *t, error *e)
{
  slab_alloc_init (&t->lock_alloc, sizeof (struct lockt_frame), 1000);

  t->table = htable_create (1000, e);
  if (t->table == NULL)
    {
      return e->cause_code;
    }

  latch_init (&t->l);

  return SUCCESS;
}

void
lockt_destroy (struct lockt *t)
{
  slab_alloc_destroy (&t->lock_alloc);
  htable_free (t->table);
}

static err_t
lockt_lock_once (
    struct lockt *t,
    const struct lt_lock lock,
    const enum lock_mode mode,
    struct txn *tx,
    error *e)
{
  /**
  // Fast path: if this transaction already holds this lock, skip.
  if (tx && txn_haslock (tx, lock))
    {
      return SUCCESS;
    }
  */

  latch_lock (&t->l);

  // Find the existing lock
  struct lockt_frame key;
  lockt_frame_init_key (&key, lock);
  struct hnode **node = htable_lookup (t->table, &key.node, lockt_frame_eq);

  // Result pointer
  struct lockt_frame *frame;

  if (node != NULL)
    {
      // Existing frame
      frame = container_of (*node, struct lockt_frame, node);
    }
  else
    {
      // Allocate and insert a new frame
      frame = slab_alloc_alloc (&t->lock_alloc, e);
      if (e->cause_code != SUCCESS)
        {
          latch_unlock (&t->l);
          return error_trace (e);
        }

      if (lockt_frame_init (frame, lock, e) != SUCCESS)
        {
          slab_alloc_free (&t->lock_alloc, frame);
          latch_unlock (&t->l);
          return error_trace (e);
        }

      htable_insert (t->table, &frame->node);
    }

  frame_ref (frame);

  // Record in the transaction while we still hold the latch.
  if (tx)
    {
      if (txn_newlock (tx, lock, mode, e) != SUCCESS)
        {
          frame_unref (t, frame);
          latch_unlock (&t->l);
          return error_trace (e);
        }
    }

  latch_unlock (&t->l);

  // Call the lock function - this is the big stuff
  if (gr_lock (&frame->lock, mode, e) != SUCCESS)
    {
      latch_lock (&t->l);
      frame_unref (t, frame);
      latch_unlock (&t->l);
      return error_trace (e);
    }

  return SUCCESS;
}

err_t
lockt_lock (
    struct lockt *t,
    const struct lt_lock lock,
    const enum lock_mode mode,
    struct txn *tx,
    error *e)
{
  // Fetch and lock the parent lock first if there is one
  struct lt_lock parent;
  if (get_parent (&parent, lock))
    {
      const enum lock_mode pmode = get_parent_mode (mode);
      // TODO - error handling here for failed locks
      WRAP (lockt_lock (t, parent, pmode, tx, e));
    }

  return lockt_lock_once (t, lock, mode, tx, e);
}

static void
lockt_unlock_once (struct lockt *t, const struct lt_lock lock, const enum lock_mode mode)
{
  latch_lock (&t->l);
  {
    // Search for the node
    struct lockt_frame key;
    lockt_frame_init_key (&key, lock);
    struct hnode **found = htable_lookup (t->table, &key.node, lockt_frame_eq);
    ASSERT (found);
    struct lockt_frame *frame = container_of (*found, struct lockt_frame, node);

    // Unlock it
    gr_unlock (&frame->lock, mode);

    // Unreference it
    frame_unref (t, frame);
  }
  latch_unlock (&t->l);
}

err_t
lockt_unlock (
    struct lockt *t,
    const struct lt_lock lock,
    const enum lock_mode mode,
    error *e)
{
  // Unlock the child first (bottom-up).
  lockt_unlock_once (t, lock, mode);

  // Then unlock the parent.
  struct lt_lock parent;
  if (get_parent (&parent, lock))
    {
      const enum lock_mode pmode = get_parent_mode (mode);
      WRAP (lockt_unlock (t, parent, pmode, e));
    }

  return SUCCESS;
}

struct unlock_ctx
{
  struct lockt *t;
};

static void
unlock_single_lock (const struct lt_lock lock, const enum lock_mode mode, void *ctx)
{
  const struct unlock_ctx *c = ctx;
  struct lockt *t = c->t;

  struct lockt_frame key;
  lockt_frame_init_key (&key, lock);

  struct hnode **found = htable_lookup (t->table, &key.node, lockt_frame_eq);
  ASSERT (found);

  struct lockt_frame *frame = container_of (*found, struct lockt_frame, node);

  gr_unlock (&frame->lock, mode);
  frame_unref (t, frame);
}

void
lockt_unlock_tx (struct lockt *t, struct txn *tx)
{
  ASSERT (t);
  ASSERT (tx);

  latch_lock (&t->l);

  struct unlock_ctx ctx = { .t = t };
  txn_foreach_lock (tx, unlock_single_lock, &ctx);

  latch_unlock (&t->l);
  txn_close (tx);
}

static void
i_log_lockt_frame_hnode (struct hnode *node, void *ctx)
{
  const int *log_level = ctx;
  const struct lockt_frame *frame = container_of (node, struct lockt_frame, node);
  i_print_lt_lock (*log_level, frame->key);
}

void
i_log_lockt (int log_level, const struct lockt *t)
{
  i_log (log_level, "================== LOCK TABLE START ==================\n");
  htable_foreach (t->table, i_log_lockt_frame_hnode, &log_level);
  i_log (log_level, "================== LOCK TABLE END ==================\n");
}

#ifndef NTEST

struct test_case
{
  struct lockt *lt;
  int counter;
  struct lt_lock key1;
  struct pager *p;
};

static void *
writer_thread_locks_type1_x (void *args)
{
  struct test_case *c = args;
  error e = error_create ();

  for (int i = 0; i < 100; i++)
    {

      struct txn tx;
      if (unlikely (pgr_begin_txn (&tx, c->p, &e) < SUCCESS))
        {
          panic ("Test Failed");
        }

      if (unlikely (lockt_lock (c->lt, c->key1, LM_X, &tx, &e) < SUCCESS))
        {
          panic ("Test Failed");
        }

      {
        int counter = c->counter;
        counter++;
        c->counter = counter;
      }

      if (unlikely (pgr_commit (c->p, &tx, &e) < SUCCESS))
        {
          panic ("TODO");
        }
    }

  return NULL;
}

TEST (lock_table_exclusivity)
{
  error e = error_create ();

  i_rm_rf ("testdb", &e);

  struct lockt lt;
  lockt_init (&lt, &e);
  struct pager *p = pgr_open_single_file ("testdb", &e);
  pgr_attach_lock_table (p, &lt);
  struct test_case c = {
    .lt = &lt,
    .counter = 0,
    .key1 = {
        .type = LOCK_DB,
        .data = { 0 },
    },
    .p = p,
  };

  i_thread threads[20];
  for (u32 i = 0; i < arrlen (threads); ++i)
    {
      i_thread_create (&threads[i], writer_thread_locks_type1_x, &c, &e);
    }
  for (u32 i = 0; i < arrlen (threads); ++i)
    {
      i_thread_join (&threads[i], &e);
    }

  test_assert_int_equal (c.counter, 100 * arrlen (threads));

  pgr_close (p, &e);
  lockt_destroy (&lt);
}

#endif
