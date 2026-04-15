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

#include "core/ext_array.h"

#include "core/data_validator.h"
#include "core/data_writer.h"
#include "intf/logging.h"
#include "intf/os/memory.h"
#include "test/testing.h"

#include <string.h>

struct ext_array
ext_array_create (void)
{
  return (struct ext_array){
    .data = NULL,
    .len = 0,
    .cap = 0,
  };
}

void
ext_array_free (struct ext_array *r)
{
  if (r->data)
    {
      i_free (r->data);
    }
  r->data = NULL;
  r->len = 0;
  r->cap = 0;
}

static err_t
ext_array_reserve (struct ext_array *r, const u32 cap, error *e)
{
  if (cap > r->cap)
    {
      u8 *data = i_realloc_right (r->data, r->cap, cap * 2, 1, e);
      if (data == NULL)
        {
          return error_trace (e);
        }
      r->data = data;
      r->cap = cap * 2;
    }
  return SUCCESS;
}

err_t
ext_array_insert (struct ext_array *r, const u32 ofst, const void *src, const u32 slen,
                  error *e)
{
  ASSERT (ofst <= r->len);
  if (ext_array_reserve (r, r->len + slen, e))
    {
      return error_trace (e);
    }

  const u32 tlen = r->len - ofst;
  if (tlen > 0)
    {
      u8 *tail = i_malloc (tlen, 1, e);
      if (tail == NULL)
        {
          return error_trace (e);
        }
      memcpy (tail, r->data + ofst, tlen);
      memcpy (r->data + ofst, src, slen);
      memcpy (r->data + ofst + slen, tail, tlen);
      i_free (tail);
    }
  else
    {
      memcpy (r->data + ofst, src, slen);
    }

  r->len += slen;
  return SUCCESS;
}

i64
ext_array_read (const struct ext_array *r, const struct stride str, const u32 size, void *_dest,
                error *e)
{
  u8 *dest = _dest;
  u32 total_read = 0;
  u32 bidx = str.start * size;

  while (total_read < str.nelems)
    {
      if (bidx + size > r->len)
        {
          return total_read;
        }
      memcpy (dest, r->data + bidx, size);
      dest += size;
      bidx += str.stride * size;
      total_read++;
    }

  return total_read;
}

i64
ext_array_write (const struct ext_array *r, const struct stride str, const u32 size,
                 const void *_src, error *e)
{
  const u8 *src = _src;
  u32 total_written = 0;
  u32 bidx = str.start * size;

  while (total_written < str.nelems)
    {
      if (bidx + size > r->len)
        {
          return total_written;
        }
      memcpy (r->data + bidx, src, size);
      src += size;
      bidx += str.stride * size;
      total_written++;
    }

  return total_written;
}

i64
ext_array_remove (struct ext_array *r, const struct stride str, const u32 size,
                  void *_dest, error *e)
{
  u8 *dest = _dest;
  u32 total_removed = 0;
  u32 wpos = 0;
  u32 rpos = 0;
  u32 next_remove = str.start;

  while (rpos * size < r->len)
    {
      if (total_removed < str.nelems && rpos == next_remove)
        {
          if (dest)
            {
              memcpy (dest, r->data + rpos * size, size);
              dest += size;
            }
          total_removed++;
          next_remove += str.stride;
        }
      else
        {
          if (wpos != rpos)
            {
              memmove (r->data + wpos * size, r->data + rpos * size, size);
            }
          wpos++;
        }
      rpos++;
    }

  r->len = wpos * size;
  return total_removed;
}

u64
ext_array_get_len (const struct ext_array *r)
{
  return r->len;
}

static err_t
ext_array_insert_func (void *ctx, const u32 ofst, const void *src, const u32 slen,
                       error *e)
{
  struct ext_array *arr = ctx;
  return ext_array_insert (arr, ofst, src, slen, e);
}
static i64
ext_array_read_func (void *ctx, const struct stride str, const u32 size, void *dest,
                     error *e)
{
  struct ext_array *arr = ctx;
  return ext_array_read (arr, str, size, dest, e);
}
static i64
ext_array_write_func (void *ctx, const struct stride str, const u32 size, const void *src,
                      error *e)
{
  struct ext_array *arr = ctx;
  return ext_array_write (arr, str, size, src, e);
}
static i64
ext_array_remove_func (void *ctx, const struct stride str, const u32 size, void *dest,
                       error *e)
{
  struct ext_array *arr = ctx;
  return ext_array_remove (arr, str, size, dest, e);
}
static i64
ext_array_getlen_func (void *ctx, error *e)
{
  struct ext_array *arr = ctx;
  return ext_array_get_len (arr);
}

static const struct data_writer_functions funcs = {
  .insert = ext_array_insert_func,
  .read = ext_array_read_func,
  .write = ext_array_write_func,
  .remove = ext_array_remove_func,
  .getlen = ext_array_getlen_func,
};

void
ext_array_data_writer (struct data_writer *dest, struct ext_array *arr)
{
  dest->functions = funcs;
  dest->ctx = arr;
}

#ifndef NTEST
TEST (TT_UNIT, ext_array_insert_read)
{
  TEST_CASE ("basic sequential")
  {
    error e = error_create ();
    struct ext_array a = ext_array_create ();

    const u32 src[] = { 1, 2, 3, 4, 5 };
    ext_array_insert (&a, 0, src, sizeof (src), &e);

    u32 dest[5] = { 0 };
    const i64 n = ext_array_read (&a,
                                  (struct stride){
                                      .start = 0,
                                      .stride = 1,
                                      .nelems = 5,
                                  },
                                  sizeof (u32), dest, &e);

    test_assert (n == 5);
    test_assert_memequal (src, dest, arrlen (src));
    ext_array_free (&a);
  }

  TEST_CASE ("insert at end (append)")
  {
    error e = error_create ();
    struct ext_array a = ext_array_create ();

    const u32 first[] = { 1, 2, 3 };
    const u32 second[] = { 4, 5, 6 };
    ext_array_insert (&a, 0, first, sizeof (first), &e);
    ext_array_insert (&a, sizeof (first), second, sizeof (second), &e);

    u32 dest[6] = { 0 };
    ext_array_read (&a,
                    (struct stride){
                        .start = 0,
                        .stride = 1,
                        .nelems = 6,
                    },
                    sizeof (u32), dest, &e);

    const u32 expected[] = { 1, 2, 3, 4, 5, 6 };
    test_assert_memequal (expected, dest, arrlen (expected));
    ext_array_free (&a);
  }

  TEST_CASE ("insert in middle")
  {
    error e = error_create ();
    struct ext_array a = ext_array_create ();

    const u32 initial[] = { 1, 3, 4 };
    const u32 inserted[] = { 2 };
    ext_array_insert (&a, 0, initial, sizeof (initial), &e);
    ext_array_insert (&a, sizeof (u32), inserted, sizeof (inserted), &e);

    u32 dest[4] = { 0 };
    ext_array_read (&a,
                    (struct stride){
                        .start = 0,
                        .stride = 1,
                        .nelems = 4,
                    },
                    sizeof (u32), dest, &e);

    const u32 expected[] = { 1, 2, 3, 4 };
    test_assert_memequal (expected, dest, arrlen (expected));
    ext_array_free (&a);
  }

  TEST_CASE ("read strided")
  {
    error e = error_create ();
    struct ext_array a = ext_array_create ();

    const u32 src[] = { 1, 2, 3, 4, 5, 6 };
    ext_array_insert (&a, 0, src, sizeof (src), &e);

    u32 dest[3] = { 0 };
    const i64 n = ext_array_read (&a,
                                  (struct stride){
                                      .start = 0,
                                      .stride = 2,
                                      .nelems = 3,
                                  },
                                  sizeof (u32), dest, &e);

    test_assert (n == 3);
    const u32 expected[] = { 1, 3, 5 };
    test_assert_memequal (expected, dest, arrlen (expected));
    ext_array_free (&a);
  }

  TEST_CASE ("read past end")
  {
    error e = error_create ();
    struct ext_array a = ext_array_create ();

    const u32 src[] = { 10, 20, 30 };
    ext_array_insert (&a, 0, src, sizeof (src), &e);

    u32 dest[10] = { 0 };
    const i64 n = ext_array_read (&a,
                                  (struct stride){
                                      .start = 1,
                                      .stride = 1,
                                      .nelems = 10,
                                  },
                                  sizeof (u32), dest, &e);

    test_assert (n == 2);
    const u32 expected[] = { 20, 30 };
    test_assert_memequal (expected, dest, 2);
    ext_array_free (&a);
  }
}

TEST (TT_UNIT, ext_array_write)
{
  TEST_CASE ("overwrite single middle")
  {
    error e = error_create ();
    struct ext_array a = ext_array_create ();

    const u32 src[] = { 1, 2, 3, 4, 5 };
    ext_array_insert (&a, 0, src, sizeof (src), &e);

    const u32 patch[] = { 99 };
    const i64 n = ext_array_write (&a,
                                   (struct stride){
                                       .start = 2,
                                       .stride = 1,
                                       .nelems = 1,
                                   },
                                   sizeof (u32), patch, &e);

    test_assert (n == 1);

    u32 dest[5] = { 0 };
    ext_array_read (&a,
                    (struct stride){ .start = 0, .stride = 1, .nelems = 5 },
                    sizeof (u32), dest, &e);

    const u32 expected[] = { 1, 2, 99, 4, 5 };
    test_assert_memequal (expected, dest, arrlen (expected));
    ext_array_free (&a);
  }

  TEST_CASE ("overwrite strided")
  {
    error e = error_create ();
    struct ext_array a = ext_array_create ();

    const u32 src[] = { 1, 2, 3, 4, 5, 6 };
    ext_array_insert (&a, 0, src, sizeof (src), &e);

    const u32 patch[] = { 0, 0, 0 };
    const i64 n = ext_array_write (&a,
                                   (struct stride){
                                       .start = 0,
                                       .stride = 2,
                                       .nelems = 3,
                                   },
                                   sizeof (u32), patch, &e);

    test_assert (n == 3);

    u32 dest[6] = { 0 };
    ext_array_read (&a,
                    (struct stride){
                        .start = 0,
                        .stride = 1,
                        .nelems = 6,
                    },
                    sizeof (u32), dest, &e);

    const u32 expected[] = { 0, 2, 0, 4, 0, 6 };
    test_assert_memequal (expected, dest, arrlen (expected));
    ext_array_free (&a);
  }

  TEST_CASE ("write past end returns short count")
  {
    error e = error_create ();
    struct ext_array a = ext_array_create ();

    const u32 src[] = { 1, 2, 3 };
    ext_array_insert (&a, 0, src, sizeof (src), &e);

    const u32 patch[] = { 9, 9, 9 };
    const i64 n = ext_array_write (&a,
                                   (struct stride){
                                       .start = 2,
                                       .stride = 1,
                                       .nelems = 3,
                                   },
                                   sizeof (u32), patch, &e);

    test_assert (n == 1);
    ext_array_free (&a);
  }
}

TEST (TT_UNIT, ext_array_remove)
{
  TEST_CASE ("remove from middle")
  {
    error e = error_create ();
    struct ext_array a = ext_array_create ();

    const u32 src[] = { 1, 2, 3, 4, 5 };
    ext_array_insert (&a, 0, src, sizeof (src), &e);

    u32 removed = 0;
    const i64 n = ext_array_remove (&a,
                                    (struct stride){
                                        .start = 2,
                                        .stride = 1,
                                        .nelems = 1,
                                    },
                                    sizeof (u32), &removed, &e);

    test_assert (n == 1);
    u32 expected_removed = 3;
    test_assert_memequal (&expected_removed, &removed, 1);

    u32 dest[4] = { 0 };
    ext_array_read (&a,
                    (struct stride){
                        .start = 0,
                        .stride = 1,
                        .nelems = 4,
                    },
                    sizeof (u32), dest, &e);

    const u32 expected[] = { 1, 2, 4, 5 };
    test_assert_memequal (expected, dest, arrlen (expected));
    ext_array_free (&a);
  }

  TEST_CASE ("remove first")
  {
    error e = error_create ();
    struct ext_array a = ext_array_create ();

    const u32 src[] = { 10, 20, 30 };
    ext_array_insert (&a, 0, src, sizeof (src), &e);

    u32 removed = 0;
    ext_array_remove (&a,
                      (struct stride){
                          .start = 0,
                          .stride = 1,
                          .nelems = 1,
                      },
                      sizeof (u32), &removed, &e);

    u32 dest[2] = { 0 };
    ext_array_read (&a,
                    (struct stride){
                        .start = 0,
                        .stride = 1,
                        .nelems = 2,
                    },
                    sizeof (u32), dest, &e);

    const u32 expected[] = { 20, 30 };
    test_assert_memequal (expected, dest, arrlen (expected));
    ext_array_free (&a);
  }

  TEST_CASE ("remove last")
  {
    error e = error_create ();
    struct ext_array a = ext_array_create ();

    const u32 src[] = { 10, 20, 30 };
    ext_array_insert (&a, 0, src, sizeof (src), &e);

    u32 removed = 0;
    ext_array_remove (&a,
                      (struct stride){
                          .start = 2,
                          .stride = 1,
                          .nelems = 1,
                      },
                      sizeof (u32), &removed, &e);

    u32 dest[2] = { 0 };
    ext_array_read (&a,
                    (struct stride){
                        .start = 0,
                        .stride = 1,
                        .nelems = 2,
                    },
                    sizeof (u32), dest, &e);

    const u32 expected[] = { 10, 20 };
    test_assert_memequal (expected, dest, arrlen (expected));
    ext_array_free (&a);
  }

  TEST_CASE ("remove strided")
  {
    error e = error_create ();
    struct ext_array a = ext_array_create ();

    const u32 src[] = { 1, 2, 3, 4, 5, 6 };
    ext_array_insert (&a, 0, src, sizeof (src), &e);

    u32 removed[3] = { 0 };
    const i64 n = ext_array_remove (&a,
                                    (struct stride){
                                        .start = 0,
                                        .stride = 2,
                                        .nelems = 3,
                                    },
                                    sizeof (u32), removed, &e);

    test_assert (n == 3);
    const u32 expected_removed[] = { 1, 3, 5 };
    test_assert_memequal (expected_removed, removed,
                          arrlen (expected_removed));

    u32 dest[3] = { 0 };
    ext_array_read (&a,
                    (struct stride){
                        .start = 0,
                        .stride = 1,
                        .nelems = 3,
                    },
                    sizeof (u32), dest, &e);

    const u32 expected[] = { 2, 4, 6 };
    test_assert_memequal (expected, dest, arrlen (expected));
    ext_array_free (&a);
  }
}

TEST (TT_UNIT, ext_array_random)
{
  error e = error_create ();
  u32 size = 1;

  /**
  struct ext_array ext_arr_1 = ext_array_create ();
  struct ext_array ext_arr_2 = ext_array_create ();

  struct data_writer ref;
  struct data_writer sut;

  ext_array_data_writer (&ref, &ext_arr_1);
  ext_array_data_writer (&sut, &ext_arr_2);

  struct dvalidtr d = {
  .sut = sut,
  .ref = ref,
  };

  dvalidtr_random_test (&d, 1, 1000, 10000, &e);

  ext_array_free (&ext_arr_1);

  ext_array_free (&ext_arr_2);
  */

  // Block sizes to test
  const u32 niters[]
      = { 100, 100, 100, 100, 100, 100, 1000, 1000, 1000, 1000, 10000 };

  for (u32 i = 0; i < arrlen (niters); ++i)
    {
      i_log_info ("Block random test: %d\n", i);

      struct ext_array ext_arr_1 = ext_array_create ();
      struct ext_array ext_arr_2 = ext_array_create ();

      struct data_writer ref;
      struct data_writer sut;

      ext_array_data_writer (&ref, &ext_arr_1);
      ext_array_data_writer (&sut, &ext_arr_2);

      struct dvalidtr d = {
        .sut = sut,
        .ref = ref,
        .isvalid = NULL,
      };

      dvalidtr_random_test (&d, 1, niters[i], 1000, &e);

      ext_array_free (&ext_arr_1);
      ext_array_free (&ext_arr_2);
    }
}
#endif
