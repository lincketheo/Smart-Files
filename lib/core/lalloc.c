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

#include "core/lalloc.h"

#include "core/assert.h"
#include "core/bounds.h"
#include "core/error.h"
#include "test/testing.h"

#include <string.h>

DEFINE_DBG_ASSERT (struct lalloc, lalloc, l, {
  ASSERT (l);
  ASSERT (l->data);
  ASSERT (l->used <= l->limit);
})

#define MIN(a, b) ((a) < (b) ? (a) : (b))

struct lalloc
lalloc_create (u8 *data, const u32 limit)
{
  ASSERT (limit > 0);
  struct lalloc ret = {
    .used = 0,
    .limit = limit,
    .data = data,
  };
  latch_init (&ret.latch);
  DBG_ASSERT (lalloc, &ret);
  return ret;
}

u32
lalloc_get_state (struct lalloc *l)
{
  latch_lock (&l->latch);

  const u32 result = l->used;
  latch_unlock (&l->latch);

  return result;
}

void
lalloc_reset_to_state (struct lalloc *l, const u32 state)
{
  latch_lock (&l->latch);

  l->used = state;

  latch_unlock (&l->latch);
}

void *
lmalloc (struct lalloc *a, const u32 req, const u32 size, error *e)
{
  latch_lock (&a->latch);

  DBG_ASSERT (lalloc, a);
  ASSERT (req > 0);
  ASSERT (size > 0);

  u32 total;
  if (!safe_mul_u32 (&total, req, size))
    {
      error_causef (e, ERR_NOMEM, "alloc %d*%d: overflow", req, size);
      latch_unlock (&a->latch);
      return NULL;
    }

  const u32 avail = a->limit - a->used;
  if (avail <= total)
    {
      error_causef (e, ERR_NOMEM, "linear alloc %d bytes: only %d remaining",
                    total, avail);
      latch_unlock (&a->latch);
      return NULL;
    }

  void *ret = &a->data[a->used];
  a->used = a->used + total;

  latch_unlock (&a->latch);

  return ret;
}

void *
lcalloc (struct lalloc *a, const u32 req, const u32 size, error *e)
{
  void *ret = lmalloc (a, req, size, e);
  if (ret == NULL)
    {
      return ret;
    }

  memset (ret, 0, req * size);

  return ret;
}

void
lalloc_reset (struct lalloc *a)
{
  latch_lock (&a->latch);

  DBG_ASSERT (lalloc, a);
  a->used = 0;

  latch_unlock (&a->latch);
}

#ifndef NTEST
TEST (TT_UNIT, lalloc_edge_cases)
{
  u8 mem[64];
  struct lalloc a = lalloc_create (mem, sizeof (mem));
  error e = error_create ();

  test_assert_int_equal (lalloc_get_state (&a), 0);
  test_assert_int_equal (a.limit, sizeof (mem));

  TEST_CASE ("first allocation (1 byte) must succeed and be correctly aligned")
  {
    void *p1 = lmalloc (&a, 1, 1, &e);
    const size_t align = sizeof (void *);
    test_assert_int_equal (((uintptr_t)p1) % align, 0);
  }

  const u32 s1 = lalloc_get_state (&a);

  TEST_CASE ("lcalloc must zero the returned memory")
  {
    const int *p2 = lcalloc (&a, 4, sizeof (int), &e);
    for (int i = 0; i < 4; ++i)
      {
        test_assert_int_equal (p2[i], 0);
      }
  }

  TEST_CASE ("rewind with lalloc_reset_to_state")
  {
    lalloc_reset_to_state (&a, s1);
    test_assert_int_equal (lalloc_get_state (&a), s1);
  }

  TEST_CASE ("allocate until only one byte is left - should still succeed")
  {
    const u32 left = a.limit - a.used;
    void *p3 = lmalloc (&a, left - 1, 1, &e);
  }

  u32 before_fail;
  TEST_CASE ("allocator now “full” – further request must fail AND keep state")
  {
    before_fail = lalloc_get_state (&a);
    const void *p_fail = lmalloc (&a, 2, 1, &e);
    test_assert_int_equal (p_fail == NULL, true);
    test_assert_int_equal (e.cause_code, ERR_NOMEM);
    e.cause_code = SUCCESS;
    test_assert_int_equal (lalloc_get_state (&a), before_fail);
  }

  TEST_CASE ("overflow protection: extremely large request must return NULL")
  {
    before_fail = lalloc_get_state (&a);
    const void *p_over = lmalloc (&a, UINT32_MAX, 16, &e);
    test_assert_int_equal (p_over == NULL, true);
    test_assert_int_equal (e.cause_code, ERR_NOMEM);
    e.cause_code = SUCCESS;
    test_assert_int_equal (lalloc_get_state (&a), before_fail);
  }

  TEST_CASE ("lalloc_reset should clear all usage")
  {
    lalloc_reset (&a);
    test_assert_int_equal (lalloc_get_state (&a), 0);
  }
}
#endif
