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

#include "tlclib/ds/hash_table.h"

#include "tlclib/concurrency/latch.h"
#include "tlclib/core/random.h"
#include "tlclib/dev/error.h"
#include "tlclib/dev/testing.h"

#include <string.h>

DEFINE_DBG_ASSERT (struct htable, htable, t, {
  ASSERT (t);
  ASSERT (t->cap > 0);
  ASSERT (t->table);
})

static bool
default_equals (const struct hnode *left,
                const struct hnode *right)
{
  return left->hcode == right->hcode;
}

struct htable *
htable_create (const u32 n, error *e)
{
  struct htable *ret = i_malloc (1, sizeof (struct htable) + n * sizeof (struct hnode *), e);

  if (ret == NULL)
    {
      return ret;
    }

  memset (ret->table, 0, n * sizeof (struct hnode *));

  ret->cap = n;
  ret->size = 0;
  latch_init (&ret->latch);

  DBG_ASSERT (htable, ret);

  return ret;
}

void
htable_free (struct htable *t)
{
  DBG_ASSERT (htable, t);

  i_free (t);
}

void
htable_insert (struct htable *t, struct hnode *node)
{
  latch_lock (&t->latch);

  DBG_ASSERT (htable, t);

  const u32 pos = node->hcode % t->cap;
  struct hnode *next = t->table[pos];
  node->next = next;
  t->table[pos] = node;
  t->size++;

  latch_unlock (&t->latch);
}

struct hnode **
htable_lookup (struct htable *t, const struct hnode *key,
               bool (*eq) (const struct hnode *,
                           const struct hnode *))
{
  if (eq == NULL)
    {
      eq = default_equals;
    }

  latch_lock (&t->latch);

  DBG_ASSERT (htable, t);

  const u32 pos = key->hcode % t->cap;

  struct hnode **from = &t->table[pos];

  for (struct hnode *cur; (cur = *from) != NULL; from = &cur->next)
    {
      if (cur->hcode == key->hcode && eq (cur, key))
        {
          latch_unlock (&t->latch);
          return from;
        }
    }

  latch_unlock (&t->latch);
  return NULL;
}

struct hnode *
htable_delete (struct htable *t, struct hnode **from)
{
  latch_lock (&t->latch);

  DBG_ASSERT (htable, t);

  struct hnode *node = *from;
  *from = node->next;
  t->size--;

  latch_unlock (&t->latch);

  return node;
}

struct hnode **
htable_random (struct htable *t)
{
  const u32 target_idx = randu32r (0, t->size - 1);
  u32 idx = 0;

  latch_lock (&t->latch);

  DBG_ASSERT (htable, t);

  for (u32 pos = 0; pos < t->cap; ++pos)
    {
      struct hnode **from = &t->table[pos];

      for (struct hnode *cur; (cur = *from) != NULL; from = &cur->next)
        {
          if (idx == target_idx)
            {
              latch_unlock (&t->latch);
              return from;
            }
          idx++;
        }
    }

  latch_unlock (&t->latch);
  return NULL;
}

void
htable_foreach (const struct htable *t,
                void (*action) (struct hnode *v, void *ctx), void *ctx)
{
  latch_lock (&((struct htable *)t)->latch);

  for (u32 i = 0; i < t->cap; ++i)
    {
      struct hnode *cur = t->table[i];
      while (cur)
        {
          struct hnode *next = cur->next;
          action (cur, ctx);
          cur = next;
        }
    }

  latch_unlock (&((struct htable *)t)->latch);
}

#ifndef NTEST
struct hdata
{
  struct hnode node;
  int data;
  int value;
};

static bool
hdata_eq (const struct hnode *left, const struct hnode *right)
{
  const struct hdata *_left = container_of (left, struct hdata, node);
  const struct hdata *_right = container_of (right, struct hdata, node);

  return _left->data == _right->data;
}

TEST (htable)
{
  error e = error_create ();
  struct htable *t = htable_create (100, &e);
  struct hdata data[1000];

  int k = 0;
  for (int i = 0; i < 1000; ++i)
    {
      k += randu32r (1, 10000);
      data[i].data = k;
      data[i].value = randu32 ();
      data[i].node.hcode = k;
      htable_insert (t, &data[i].node);
    }

  test_assert_int_equal (t->size, 1000);

  for (int i = 0; i < 1000; ++i)
    {
      struct hnode **node = htable_random (t);
    }

  for (int i = 0; i < 1000; ++i)
    {
      struct hdata key;
      key.data = data[i].data;
      key.node.hcode = data[i].node.hcode;
      key.value = data[i].value; // Expected value (normally
                                 // we wouldn't have this)

      ////// LOOKUP
      struct hnode **found = htable_lookup (t, &key.node, hdata_eq);
      const struct hdata *_found = container_of (*found, struct hdata, node);

      // These two are obvious
      test_assert_int_equal (_found->data, key.data);
      test_assert_int_equal (_found->node.hcode, key.node.hcode);

      // This one is the real test
      test_assert_int_equal (_found->value, key.value);

      ////// DELETE
      struct hnode *deleted = htable_delete (t, found);
      const struct hdata *_deleted = container_of (deleted, struct hdata, node);

      // These two are obvious
      test_assert_int_equal (_deleted->data, key.data);
      test_assert_int_equal (_deleted->node.hcode, key.node.hcode);

      // This one is the real test
      test_assert_int_equal (_deleted->value, key.value);
    }

  test_assert_int_equal (t->size, 0);

  htable_free (t);
}

#endif
