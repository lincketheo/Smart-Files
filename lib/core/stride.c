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

#include "core/stride.h"

#include "core/assert.h"
#include "core/error.h"
#include "test/testing.h"

bool
ustride_equal (const struct user_stride left, const struct user_stride right)
{
  const int mask = STOP_PRESENT | STEP_PRESENT | START_PRESENT;
  const int left_present = left.present & mask;
  const int right_present = right.present & mask;

  if (left_present != right_present)
    {
      return false;
    }

  if ((left_present & START_PRESENT) && left.start != right.start)
    {
      return false;
    }

  if ((left_present & STEP_PRESENT) && left.step != right.step)
    {
      return false;
    }

  if ((left_present & STOP_PRESENT) && left.stop != right.stop)
    {
      return false;
    }

  return true;
}

void
stride_resolve_expect (struct stride *dest, const struct user_stride src,
                       const b_size arrlen)
{
  const sb_size step = (src.present & STEP_PRESENT) ? src.step : 1;

  ASSERT (step > 0);

  if (arrlen == 0)
    {
      dest->start = 0;
      dest->stride = (u32)step;
      dest->nelems = 0;
      return;
    }

  sb_size start, stop;

  if (src.present & START_PRESENT)
    {
      start = src.start;
      if (start < 0)
        {
          start += arrlen;
        }

      // Clamp [0, arrlen]
      if (start < 0)
        {
          start = 0;
        }
      if (start > (sb_size)arrlen)
        {
          start = arrlen;
        }
    }
  else
    {
      start = 0;
    }

  if (src.present & STOP_PRESENT)
    {
      stop = src.stop;
      if (stop < 0)
        {
          stop += arrlen;
        }

      // Clamp [0, arrlen]
      if (stop < 0)
        {
          stop = 0;
        }
      if (stop > (sb_size)arrlen)
        {
          stop = arrlen;
        }
    }
  else
    {
      stop = arrlen;
    }

  b_size nelems;
  if (stop <= start)
    {
      nelems = 0;
    }
  else
    {
      nelems = (stop - start + step - 1) / step;
    }

  // Populate destination
  dest->start = (b_size)start;
  dest->stride = (u32)step;
  dest->nelems = nelems;

  return;
}

err_t
stride_resolve (struct stride *dest, const struct user_stride src, const b_size arrlen,
                error *e)
{
  const sb_size step = (src.present & STEP_PRESENT) ? src.step : 1;

  if (step <= 0)
    {
      return error_causef (e, ERR_INVALID_ARGUMENT,
                           "stride step must be positive");
    }

  stride_resolve_expect (dest, src, arrlen);

  return SUCCESS;
}

#ifndef NTEST
TEST (TT_UNIT, stride_resolve)
{
  struct stride result;
  error e = error_create ();

  TEST_CASE ("Full slice [::] on length 10 ")
  {
    struct user_stride full = { 0 };
    stride_resolve (&result, full, 10, &e);
    test_assert_int_equal (result.start, 0);
    test_assert_int_equal (result.stride, 1);
    test_assert_int_equal (result.nelems, 10);
  }

  TEST_CASE ("Slice with step [::2] on length 10 ")
  {
    struct user_stride step2 = { .step = 2, .present = STEP_PRESENT };
    stride_resolve (&result, step2, 10, &e);
    test_assert_int_equal (result.start, 0);
    test_assert_int_equal (result.stride, 2);
    test_assert_int_equal (result.nelems, 5);
  }

  TEST_CASE ("Start only [5:] on length 10 ")
  {
    struct user_stride from5 = { .start = 5, .present = START_PRESENT };
    stride_resolve (&result, from5, 10, &e);
    test_assert_int_equal (result.start, 5);
    test_assert_int_equal (result.stride, 1);
    test_assert_int_equal (result.nelems, 5);
  }

  TEST_CASE ("Stop only [:5] on length 10 ")
  {
    struct user_stride to5 = { .stop = 5, .present = STOP_PRESENT };
    stride_resolve (&result, to5, 10, &e);
    test_assert_int_equal (result.start, 0);
    test_assert_int_equal (result.stride, 1);
    test_assert_int_equal (result.nelems, 5);
  }

  TEST_CASE ("Range [2:8] on length 10 ")
  {
    struct user_stride range
        = { .start = 2, .stop = 8, .present = START_PRESENT | STOP_PRESENT };
    stride_resolve (&result, range, 10, &e);
    test_assert_int_equal (result.start, 2);
    test_assert_int_equal (result.stride, 1);
    test_assert_int_equal (result.nelems, 6);
  }

  TEST_CASE ("Range with step [1:9:2] on length 10 ")
  {
    struct user_stride range_step
        = { .start = 1,
            .stop = 9,
            .step = 2,
            .present = START_PRESENT | STOP_PRESENT | STEP_PRESENT };
    stride_resolve (&result, range_step, 10, &e);
    test_assert_int_equal (result.start, 1);
    test_assert_int_equal (result.stride, 2);
    test_assert_int_equal (result.nelems, 4); // indices 1,3,5,7
  }

  TEST_CASE ("Negative start index [-3:] on length 10 -> [7:] ")
  {
    struct user_stride neg_start = { .start = -3, .present = START_PRESENT };
    stride_resolve (&result, neg_start, 10, &e);
    test_assert_int_equal (result.start, 7);
    test_assert_int_equal (result.stride, 1);
    test_assert_int_equal (result.nelems, 3);
  }

  TEST_CASE ("Negative stop index [:-2] on length 10 -> [:8] ")
  {
    struct user_stride neg_stop = { .stop = -2, .present = STOP_PRESENT };
    stride_resolve (&result, neg_stop, 10, &e);
    test_assert_int_equal (result.start, 0);
    test_assert_int_equal (result.stride, 1);
    test_assert_int_equal (result.nelems, 8);
  }

  TEST_CASE ("Both negative [-5:-2] on length 10 -> [5:8] ")
  {
    struct user_stride both_neg
        = { .start = -5, .stop = -2, .present = START_PRESENT | STOP_PRESENT };
    stride_resolve (&result, both_neg, 10, &e);
    test_assert_int_equal (result.start, 5);
    test_assert_int_equal (result.stride, 1);
    test_assert_int_equal (result.nelems, 3);
  }

  TEST_CASE ("Out of bounds start [20:] on length 10 ")
  {
    struct user_stride oob_start = { .start = 20, .present = START_PRESENT };
    stride_resolve (&result, oob_start, 10, &e);
    test_assert_int_equal (result.start, 10);
    test_assert_int_equal (result.stride, 1);
    test_assert_int_equal (result.nelems, 0);
  }

  TEST_CASE ("Empty slice [5:2] on length 10 ")
  {
    struct user_stride empty
        = { .start = 5, .stop = 2, .present = START_PRESENT | STOP_PRESENT };
    stride_resolve (&result, empty, 10, &e);
    test_assert_int_equal (result.nelems, 0);
  }

  TEST_CASE ("Empty array ")
  {
    struct user_stride on_empty = { 0 };
    stride_resolve (&result, on_empty, 0, &e);
    test_assert_int_equal (result.start, 0);
    test_assert_int_equal (result.stride, 1);
    test_assert_int_equal (result.nelems, 0);
  }

  TEST_CASE ("Invalid step = 0 ")
  {
    struct user_stride zero_step = { .step = 0, .present = STEP_PRESENT };
    test_err_t_check (stride_resolve (&result, zero_step, 10, &e),
                      ERR_INVALID_ARGUMENT, &e);
  }

  TEST_CASE ("Invalid negative step ")
  {
    struct user_stride neg_step = { .step = -1, .present = STEP_PRESENT };
    test_err_t_check (stride_resolve (&result, neg_step, 10, &e),
                      ERR_INVALID_ARGUMENT, &e);
  }
}
#endif

DEFINE_DBG_ASSERT (struct mus_builder, mus_builder, s, { ASSERT (s); })

void
musb_create (struct mus_builder *dest, struct chunk_alloc *temp,
             struct chunk_alloc *persistent)
{
  *dest = (struct mus_builder){
    .head = NULL,
    .temp = temp,
    .persistent = persistent,
  };
}

err_t
musb_accept_key (struct mus_builder *eb, const struct user_stride stride, error *e)
{
  DBG_ASSERT (mus_builder, eb);

  struct mus_llnode *node = chunk_malloc (eb->temp, 1, sizeof *node, e);
  if (!node)
    {
      return error_trace (e);
    }

  llnode_init (&node->link);
  node->stride = stride;

  if (!eb->head)
    {
      eb->head = &node->link;
    }
  else
    {
      list_append (&eb->head, &node->link);
    }

  return SUCCESS;
}

err_t
musb_build (struct multi_user_stride *dest, const struct mus_builder *eb, error *e)
{
  DBG_ASSERT (mus_builder, eb);
  ASSERT (dest);

  const u32 len = list_length (eb->head);
  if (len == 0)
    {
      return error_causef (e, ERR_INTERP, "no strides to build");
    }

  struct user_stride *strides
      = chunk_malloc (eb->persistent, len, sizeof *strides, e);
  if (!strides)
    {
      return error_trace (e);
    }

  u32 i = 0;
  for (struct llnode *it = eb->head; it; it = it->next)
    {
      const struct mus_llnode *mn = container_of (it, struct mus_llnode, link);
      strides[i++] = mn->stride;
    }

  dest->strides = strides;
  dest->len = len;
  return SUCCESS;
}
