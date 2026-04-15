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

#include "core/block_array.h"

#include "core/assert.h"
#include "core/data_validator.h"
#include "core/data_writer.h"
#include "core/ext_array.h"
#include "core/slab_alloc.h"
#include "intf/logging.h"
#include "intf/os/memory.h"
#include "test/testing.h"

#include <stdlib.h>
#include <string.h>

struct block_array *
block_array_create (const u32 cap_per_node, error *e)
{
  ASSERT (cap_per_node > 0);

  struct block_array *ret
      = i_malloc (1, sizeof (struct block_array) + cap_per_node, e);
  if (ret == NULL)
    {
      return ret;
    }

  slab_alloc_init (&ret->block_alloc, sizeof (struct block) + cap_per_node,
                   512);
  ret->cap_per_node = cap_per_node;
  ret->head = NULL;
  ret->tlen = 0;

  return ret;
}

void
block_array_free (struct block_array *r)
{
  slab_alloc_destroy (&r->block_alloc);
  i_free (r);
}

static struct block *
block_alloc_empty (struct block_array *r, struct block *prev, error *e)
{
  struct block *ret = slab_alloc_alloc (&r->block_alloc, e);
  if (ret == NULL)
    {
      return NULL;
    }

  ret->len = 0;
  if (prev)
    {
      ret->next = prev->next;
      prev->next = ret;
      if (ret->next)
        {
          ret->next->prev = ret;
        }
    }
  else
    {
      ret->next = NULL;
    }
  ret->prev = prev;

  return ret;
}

err_t
block_array_insert (struct block_array *r, u32 ofst, const void *_src,
                    u32 slen, error *e)
{
  ASSERT (slen > 0);

  const u8 *src = _src;

  // Allocate head if it's empty
  if (r->head == NULL)
    {
      r->head = block_alloc_empty (r, r->head, e);
      if (r->head == NULL)
        {
          panic ("ROLLBACK");
        }
    }

  // Seek
  struct block *cur = r->head;
  while (ofst > cur->len)
    {
      ASSERT (cur->next != NULL); // Don't allow buffer overflows
      ofst -= cur->len;
      cur = cur->next;
    }

  // Save the tail
  r->tlen = cur->len - ofst;
  if (r->tlen > 0)
    {
      memcpy (r->tail, cur->data + ofst, r->tlen);
      cur->len = ofst;
    }

  struct block *last = cur->next;

  // Write out source data
  while (slen > 0)
    {
      // Advance forward
      if (cur->len == r->cap_per_node)
        {
          cur = block_alloc_empty (r, cur, e);
          if (cur == NULL)
            {
              panic ("ROLLBACK");
            }
        }

      // Append to cur
      u32 next_write = r->cap_per_node - cur->len; // Writable
      next_write = MIN (next_write, slen);         // Readable
      memcpy (&cur->data[cur->len], src, next_write);

      ASSERT (next_write <= slen);

      src += next_write;
      slen -= next_write;
      cur->len += next_write;
    }

  // Write out tail data
  u32 twritten = 0;
  while (twritten < r->tlen)
    {
      // Advance forward
      if (cur->len == r->cap_per_node)
        {
          cur = block_alloc_empty (r, cur, e);
          if (cur == NULL)
            {
              panic ("ROLLBACK");
            }
        }

      // Append to cur
      u32 next_write = r->cap_per_node - cur->len;       // Writable
      next_write = MIN (next_write, r->tlen - twritten); // Readable
      memcpy (&cur->data[cur->len], &r->tail[twritten], next_write);

      twritten += next_write;
      cur->len += next_write;
    }

  // Link the last node
  cur->next = last;
  r->tlen = 0;

  return SUCCESS;
}

struct stride_state
{
  u32 next;
  bool active;
};

u64
block_array_read (const struct block_array *r, const struct stride str, const u32 size,
                  void *_dest)
{
  u8 *dest = _dest;

  // Seek
  const struct block *cur = r->head;
  u32 bidx = str.start * size;

  while (true)
    {
      if (cur == NULL)
        {
          return 0;
        }
      else if (bidx >= cur->len)
        {
          bidx -= cur->len;
          cur = cur->next;
        }
      else
        {
          break;
        }
    }

  struct stride_state state = {
    .next = size,
    .active = true,
  };

  u32 total_read = 0;

  while (total_read < str.nelems)
    {
      if (state.active)
        {
          const u32 next_read = MIN (state.next, cur->len - bidx);
          if (next_read > 0)
            {
              memcpy (dest, &cur->data[bidx], next_read);
              dest += next_read;
            }
          bidx += next_read;
          state.next -= next_read;

          if (state.next == 0)
            {
              total_read++;

              if (str.stride == 1)
                {
                  state = (struct stride_state){
                    .next = size,
                    .active = true,
                  };
                }
              else
                {
                  state = (struct stride_state){
                    .next = (str.stride - 1) * size,
                    .active = false,
                  };
                }
            }
        }
      else
        {
          const u32 next = MIN (state.next, cur->len - bidx);
          bidx += next;
          state.next -= next;

          if (state.next == 0)
            {
              state = (struct stride_state){ .next = size, .active = true };
            }
        }

      if (bidx == cur->len)
        {
          cur = cur->next;
          bidx = 0;
          if (cur == NULL)
            {
              return total_read;
            }
        }
    }

  return total_read;
}

u64
block_array_write (const struct block_array *r, const struct stride str, const u32 size,
                   const void *_src)
{
  const u8 *src = _src;

  // Seek
  struct block *cur = r->head;
  u32 bidx = str.start * size;

  while (true)
    {
      if (cur == NULL)
        {
          return 0;
        }
      else if (bidx >= cur->len)
        {
          bidx -= cur->len;
          cur = cur->next;
        }
      else
        {
          break;
        }
    }

  struct stride_state state = {
    .next = size,
    .active = true,
  };

  u32 total_written = 0;

  while (total_written < str.nelems)
    {
      if (state.active)
        {
          const u32 next = MIN (state.next, cur->len - bidx);
          if (next > 0)
            {
              memcpy (&cur->data[bidx], src, next);
            }
          src += next;
          bidx += next;
          state.next -= next;

          if (state.next == 0)
            {
              total_written++;

              if (str.stride == 1)
                {
                  state = (struct stride_state){
                    .next = size,
                    .active = true,
                  };
                }
              else
                {
                  state = (struct stride_state){
                    .next = (str.stride - 1) * size,
                    .active = false,
                  };
                }
            }
        }
      else
        {
          const u32 next = MIN (state.next, cur->len - bidx);
          bidx += next;
          state.next -= next;

          if (state.next == 0)
            {
              state = (struct stride_state){ .next = size, .active = true };
            }
        }

      if (bidx == cur->len)
        {
          cur = cur->next;
          bidx = 0;
          if (cur == NULL)
            {
              return total_written;
            }
        }
    }

  return total_written;
}

i64
block_array_remove (struct block_array *r, const struct stride str, const u32 size,
                    void *_dest, error *e)
{
  u8 *dest = _dest;

  // Seek
  struct block *rcur = r->head; // Read block
  u32 rbidx = str.start * size; // Read idx

  while (true)
    {
      if (rcur == NULL)
        {
          return 0;
        }
      else if (rbidx >= rcur->len)
        {
          rbidx -= rcur->len;
          rcur = rcur->next;
        }
      else
        {
          break;
        }
    }

  struct block *wcur = rcur; // Write block
  u32 wbidx = rbidx;         // Wride idx

  struct stride_state state = {
    .next = size,
    .active = true,
  };
  u32 total_removed = 0;

  while (total_removed < str.nelems)
    {
      if (state.active)
        {
          const u32 next = MIN (state.next, rcur->len - rbidx);
          if (next > 0 && dest)
            {
              memcpy (dest, &rcur->data[rbidx], next);
              dest += next;
            }
          rbidx += next;
          state.next -= next;

          if (state.next == 0)
            {
              total_removed++;

              if (str.stride == 1)
                {
                  state = (struct stride_state){
                    .next = size,
                    .active = true,
                  };
                }
              else
                {
                  state = (struct stride_state){
                    .next = (str.stride - 1) * size,
                    .active = false,
                  };
                }
            }
        }
      else
        {
          u32 next = state.next;
          next = MIN (next,
                      r->cap_per_node - wbidx); // Writable
          next = MIN (next,
                      rcur->len - rbidx); // Readable

          if (next > 0)
            {
              memmove (&wcur->data[wbidx], &rcur->data[rbidx], next);
            }

          wbidx += next;
          rbidx += next;
          state.next -= next;

          // Write node is full
          if (wbidx == r->cap_per_node)
            {
              wcur->len = r->cap_per_node;
              wcur = wcur->next;
              ASSERT (wcur == rcur);
              wbidx = 0;
            }

          if (state.next == 0)
            {
              state = (struct stride_state){
                .next = size,
                .active = true,
              };
            }
        }

      if (rbidx == rcur->len)
        {
          struct block *next = rcur->next;

          if (rcur != wcur)
            {
              // Delete rcur
              if (rcur->prev)
                {
                  rcur->prev->next = rcur->next;
                }
              if (rcur->next)
                {
                  rcur->next->prev = rcur->prev;
                }

              slab_alloc_free (&r->block_alloc, rcur);
            }

          rcur = next;
          rbidx = 0;
          if (rcur == NULL)
            {
              break;
            }
        }
    }

  if (rcur != NULL)
    {
      while (true)
        {
          // Basically the same as inactive block
          // except without state
          u32 next = r->cap_per_node - wbidx; // Writable
          next = MIN (next,
                      rcur->len - rbidx); // Readable

          if (next > 0)
            {
              memmove (&wcur->data[wbidx], &rcur->data[rbidx], next);
            }

          wbidx += next;
          rbidx += next;

          // Write node is full
          if (wbidx == r->cap_per_node)
            {
              wcur->len = r->cap_per_node;
              wcur = wcur->next;
              ASSERT (wcur == rcur);
              wbidx = 0;
            }

          if (rbidx == rcur->len)
            {
              if (rcur != wcur)
                {
                  // Delete
                  // rcur
                  if (rcur->prev)
                    {
                      rcur->prev->next = rcur->next;
                    }
                  if (rcur->next)
                    {
                      rcur->next->prev = rcur->prev;
                    }

                  slab_alloc_free (&r->block_alloc, rcur);
                }
              break;
            }
        }
    }

  wcur->len = wbidx;

  return total_removed;
}

u64
block_array_getlen (const struct block_array *r)
{
  u64 len = 0;
  const struct block *cur = r->head;
  while (cur != NULL)
    {
      len += cur->len;
      cur = cur->next;
    }
  return len;
}

static err_t
block_array_insert_func (void *ctx, const u32 ofst, const void *src, const u32 slen,
                         error *e)
{
  struct block_array *arr = ctx;
  return block_array_insert (arr, ofst, src, slen, e);
}
static i64
block_array_read_func (void *ctx, const struct stride str, const u32 size, void *dest,
                       error *e)
{
  struct block_array *arr = ctx;
  return block_array_read (arr, str, size, dest);
}
static i64
block_array_write_func (void *ctx, const struct stride str, const u32 size,
                        const void *src, error *e)
{
  struct block_array *arr = ctx;
  return block_array_write (arr, str, size, src);
}
static i64
block_array_remove_func (void *ctx, const struct stride str, const u32 size, void *dest,
                         error *e)
{
  struct block_array *arr = ctx;
  return block_array_remove (arr, str, size, dest, e);
}
static i64
block_array_getlen_func (void *ctx, error *e)
{
  struct block_array *arr = ctx;
  return block_array_getlen (arr);
}

static const struct data_writer_functions funcs = {
  .insert = block_array_insert_func,
  .read = block_array_read_func,
  .write = block_array_write_func,
  .remove = block_array_remove_func,
  .getlen = block_array_getlen_func,
};

void
block_array_data_writer (struct data_writer *dest, struct block_array *arr)
{
  dest->functions = funcs;
  dest->ctx = arr;
}

#ifndef NTEST
TEST (TT_UNIT, block_insert_read)
{
  TEST_CASE ("basic")
  {
    error e = error_create ();
    struct block_array *b = block_array_create (2, &e);

    u32 src[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    block_array_insert (b, 0, src, sizeof (src), &e);

    u32 dest[arrlen (src)] = { 0 };
    block_array_read (b,
                      (struct stride){
                          .start = 1,
                          .stride = 2,
                          .nelems = 3,
                      },
                      sizeof (u32), dest);

    u32 expected[] = { 2, 4, 6 };

    test_assert_memequal (expected, dest, arrlen (dest));

    block_array_free (b);
  }

  // Read all elements sequentially (stride=1, start=0)
  TEST_CASE ("block_array_read_all")
  {
    error e = error_create ();
    struct block_array *b = block_array_create (2, &e);

    u32 src[] = { 10, 20, 30, 40, 50 };
    block_array_insert (b, 0, src, sizeof (src), &e);

    u32 dest[arrlen (src)] = { 0 };
    block_array_read (b,
                      (struct stride){
                          .start = 0,
                          .stride = 1,
                          .nelems = 5,
                      },
                      sizeof (u32), dest);

    test_assert_memequal (src, dest, arrlen (dest));
    block_array_free (b);
  }

  // Read a single element from the middle
  TEST_CASE ("block_array_read_single_middle")
  {
    error e = error_create ();
    struct block_array *b = block_array_create (4, &e);

    u32 src[] = { 100, 200, 300, 400 };
    block_array_insert (b, 0, src, sizeof (src), &e);

    u32 dest = 0;
    block_array_read (b,
                      (struct stride){
                          .start = 2,
                          .stride = 1,
                          .nelems = 1,
                      },
                      sizeof (u32), &dest);

    u32 expected = 300;
    test_assert_memequal (&expected, &dest, 1);
    block_array_free (b);
  }

  // Read every 3rd element
  TEST_CASE ("block_array_read_stride3")
  {
    error e = error_create ();
    struct block_array *b = block_array_create (2, &e);

    u32 src[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    block_array_insert (b, 0, src, sizeof (src), &e);

    u32 dest[3] = { 0 };
    block_array_read (b,
                      (struct stride){
                          .start = 0,
                          .stride = 3,
                          .nelems = 3,
                      },
                      sizeof (u32), dest);

    u32 expected[] = { 1, 4, 7 };
    test_assert_memequal (expected, dest, arrlen (dest));
    block_array_free (b);
  }

  // Insert two disjoint chunks then read across both
  TEST_CASE ("block_array_insert_two_chunks")
  {
    error e = error_create ();
    struct block_array *b = block_array_create (4, &e);

    u32 first[] = { 1, 2, 3 };
    u32 second[] = { 4, 5, 6 };
    block_array_insert (b, 0, first, sizeof (first), &e);
    block_array_insert (b, sizeof (first), second, sizeof (second), &e);

    u32 dest[6] = { 0 };
    block_array_read (b,
                      (struct stride){
                          .start = 0,
                          .stride = 1,
                          .nelems = 6,
                      },
                      sizeof (u32), dest);

    u32 expected[] = { 1, 2, 3, 4, 5, 6 };
    test_assert_memequal (expected, dest, arrlen (dest));
    block_array_free (b);
  }

  // Insert in the middle of existing data
  TEST_CASE ("block_array_insert_middle")
  {
    error e = error_create ();
    struct block_array *b = block_array_create (2, &e);

    u32 initial[] = { 1, 3, 4 };
    u32 inserted[] = { 2 };
    block_array_insert (b, 0, initial, sizeof (initial), &e);
    // Insert 2 after the first element (byte offset = sizeof u32)
    block_array_insert (b, sizeof (u32), inserted, sizeof (inserted), &e);

    u32 dest[4] = { 0 };
    block_array_read (b,
                      (struct stride){
                          .start = 0,
                          .stride = 1,
                          .nelems = 4,
                      },
                      sizeof (u32), dest);

    u32 expected[] = { 1, 2, 3, 4 };
    test_assert_memequal (expected, dest, arrlen (dest));
    block_array_free (b);
  }

  // cap_per_node larger than the entire payload — single block case
  TEST_CASE ("block_array_single_block")
  {
    error e = error_create ();
    struct block_array *b = block_array_create (256, &e);

    u32 src[] = { 7, 14, 21 };
    block_array_insert (b, 0, src, sizeof (src), &e);

    u32 dest[arrlen (src)] = { 0 };
    block_array_read (b,
                      (struct stride){
                          .start = 0,
                          .stride = 1,
                          .nelems = 3,
                      },
                      sizeof (u32), dest);

    test_assert_memequal (src, dest, arrlen (dest));
    block_array_free (b);
  }

  // Read past end returns fewer elements than requested
  TEST_CASE ("block_array_read_past_end")
  {
    error e = error_create ();
    struct block_array *b = block_array_create (2, &e);

    u32 src[] = { 1, 2, 3 };
    block_array_insert (b, 0, src, sizeof (src), &e);

    u32 dest[6] = { 0 };
    i64 nread = block_array_read (b,
                                  (struct stride){
                                      .start = 1,
                                      .stride = 1,
                                      .nelems = 6,
                                  },
                                  sizeof (u32), dest);

    // Only 2 elements remain after start=1
    test_assert (nread == 2);
    u32 expected[] = { 2, 3 };
    test_assert_memequal (expected, dest, 2);
    block_array_free (b);
  }

  // Randomised insert then full sequential read-back
  TEST_CASE ("block_array_random")
  {
    error e = error_create ();

    srand (12345);

    for (int trial = 0; trial < 64; trial++)
      {
        // cap_per_node: 1–15 bytes (forces many
        // different block-boundary patterns)
        u32 cap = (rand () % 15) + 1;
        struct block_array *b = block_array_create (cap, &e);

        // number of u32 elements: 1–127
        u32 nelems = (rand () % 127) + 1;
        u32 src[128];
        for (u32 i = 0; i < nelems; i++)
          {
            src[i] = (u32)rand ();
          }

        block_array_insert (b, 0, src, nelems * sizeof (u32), &e);

        u32 dest[128] = { 0 };
        i64 nread = block_array_read (b,
                                      (struct stride){
                                          .start = 0,
                                          .stride = 1,
                                          .nelems = nelems,
                                      },
                                      sizeof (u32), dest);

        test_assert (nread == (i64)nelems);
        test_assert_memequal (src, dest, nelems);

        block_array_free (b);
      }
  }
}

TEST (TT_UNIT, block_insert_remove_read)
{
  TEST_CASE ("remove_from_middle")
  {
    error e = error_create ();
    struct block_array *b = block_array_create (4, &e);

    const u32 src[] = { 1, 2, 3, 4, 5 };
    block_array_insert (b, 0, src, sizeof (src), &e);

    // Remove the middle element (index 2)
    u32 removed = 0;
    const i64 n = block_array_remove (
        b, (struct stride){ .start = 2, .stride = 1, .nelems = 1 },
        sizeof (u32), &removed, &e);

    test_assert (n == 1);
    u32 expected_removed = 3;
    test_assert_memequal (&expected_removed, &removed, 1);

    u32 dest[4] = { 0 };
    block_array_read (b,
                      (struct stride){ .start = 0, .stride = 1, .nelems = 4 },
                      sizeof (u32), dest);

    const u32 expected[] = { 1, 2, 4, 5 };
    test_assert_memequal (expected, dest, arrlen (expected));

    block_array_free (b);
  }

  TEST_CASE ("remove_first")
  {
    error e = error_create ();
    struct block_array *b = block_array_create (2, &e);

    const u32 src[] = { 10, 20, 30 };
    block_array_insert (b, 0, src, sizeof (src), &e);

    u32 removed = 0;
    const i64 n = block_array_remove (
        b, (struct stride){ .start = 0, .stride = 1, .nelems = 1 },
        sizeof (u32), &removed, &e);

    test_assert (n == 1);
    u32 expected_removed = 10;
    test_assert_memequal (&expected_removed, &removed, 1);

    u32 dest[2] = { 0 };
    block_array_read (b,
                      (struct stride){ .start = 0, .stride = 1, .nelems = 2 },
                      sizeof (u32), dest);

    const u32 expected[] = { 20, 30 };
    test_assert_memequal (expected, dest, arrlen (expected));

    block_array_free (b);
  }

  TEST_CASE ("remove_last")
  {
    error e = error_create ();
    struct block_array *b = block_array_create (2, &e);

    const u32 src[] = { 10, 20, 30 };
    block_array_insert (b, 0, src, sizeof (src), &e);

    u32 removed = 0;
    const i64 n = block_array_remove (
        b, (struct stride){ .start = 2, .stride = 1, .nelems = 1 },
        sizeof (u32), &removed, &e);

    test_assert (n == 1);
    u32 expected_removed = 30;
    test_assert_memequal (&expected_removed, &removed, 1);

    u32 dest[2] = { 0 };
    block_array_read (b,
                      (struct stride){ .start = 0, .stride = 1, .nelems = 2 },
                      sizeof (u32), dest);

    const u32 expected[] = { 10, 20 };
    test_assert_memequal (expected, dest, arrlen (expected));

    block_array_free (b);
  }

  TEST_CASE ("remove_strided")
  {
    error e = error_create ();
    struct block_array *b = block_array_create (2, &e);

    // Remove every other element: 1, 3, 5 — leaving 2, 4, 6
    const u32 src[] = { 1, 2, 3, 4, 5, 6 };
    block_array_insert (b, 0, src, sizeof (src), &e);

    u32 removed[3] = { 0 };
    const i64 n = block_array_remove (
        b, (struct stride){ .start = 0, .stride = 2, .nelems = 3 },
        sizeof (u32), removed, &e);

    test_assert (n == 3);
    const u32 expected_removed[] = { 1, 3, 5 };
    test_assert_memequal (expected_removed, removed,
                          arrlen (expected_removed));

    u32 dest[3] = { 0 };
    block_array_read (b,
                      (struct stride){ .start = 0, .stride = 1, .nelems = 3 },
                      sizeof (u32), dest);

    const u32 expected[] = { 2, 4, 6 };
    test_assert_memequal (expected, dest, arrlen (expected));

    block_array_free (b);
  }
}

TEST (TT_UNIT, block_insert_write_read)
{
  TEST_CASE ("overwrite_middle")
  {
    error e = error_create ();
    struct block_array *b = block_array_create (4, &e);

    const u32 src[] = { 1, 2, 3, 4, 5 };
    block_array_insert (b, 0, src, sizeof (src), &e);

    const u32 patch[] = { 99 };
    const i64 n = block_array_write (b,
                                     (struct stride){
                                         .start = 2,
                                         .stride = 1,
                                         .nelems = 1,
                                     },
                                     sizeof (u32), patch);

    test_assert (n == 1);

    u32 dest[5] = { 0 };
    block_array_read (b,
                      (struct stride){
                          .start = 0,
                          .stride = 1,
                          .nelems = 5,
                      },
                      sizeof (u32), dest);

    const u32 expected[] = { 1, 2, 99, 4, 5 };
    test_assert_memequal (expected, dest, arrlen (expected));

    block_array_free (b);
  }

  TEST_CASE ("overwrite_all")
  {
    error e = error_create ();
    struct block_array *b = block_array_create (2, &e);

    const u32 src[] = { 0, 0, 0, 0 };
    block_array_insert (b, 0, src, sizeof (src), &e);

    const u32 patch[] = { 10, 20, 30, 40 };
    const i64 n = block_array_write (b,
                                     (struct stride){
                                         .start = 0,
                                         .stride = 1,
                                         .nelems = 4,
                                     },
                                     sizeof (u32), patch);

    test_assert (n == 4);

    u32 dest[4] = { 0 };
    block_array_read (b,
                      (struct stride){
                          .start = 0,
                          .stride = 1,
                          .nelems = 4,
                      },
                      sizeof (u32), dest);

    test_assert_memequal (patch, dest, arrlen (patch));

    block_array_free (b);
  }

  TEST_CASE ("overwrite_strided")
  {
    error e = error_create ();
    struct block_array *b = block_array_create (2, &e);

    const u32 src[] = { 1, 2, 3, 4, 5, 6 };
    block_array_insert (b, 0, src, sizeof (src), &e);

    // Overwrite even indices (0, 2, 4) with 0
    const u32 patch[] = { 0, 0, 0 };
    const i64 n = block_array_write (b,
                                     (struct stride){
                                         .start = 0,
                                         .stride = 2,
                                         .nelems = 3,
                                     },
                                     sizeof (u32), patch);

    test_assert (n == 3);

    u32 dest[6] = { 0 };
    block_array_read (b,
                      (struct stride){
                          .start = 0,
                          .stride = 1,
                          .nelems = 6,
                      },
                      sizeof (u32), dest);

    const u32 expected[] = { 0, 2, 0, 4, 0, 6 };
    test_assert_memequal (expected, dest, arrlen (expected));

    block_array_free (b);
  }

  TEST_CASE ("overwrite_last")
  {
    error e = error_create ();
    struct block_array *b = block_array_create (2, &e);

    const u32 src[] = { 1, 2, 3 };
    block_array_insert (b, 0, src, sizeof (src), &e);

    const u32 patch[] = { 42 };
    const i64 n = block_array_write (b,
                                     (struct stride){
                                         .start = 2,
                                         .stride = 1,
                                         .nelems = 1,
                                     },
                                     sizeof (u32), patch);

    test_assert (n == 1);

    u32 dest[3] = { 0 };
    block_array_read (b,
                      (struct stride){
                          .start = 0,
                          .stride = 1,
                          .nelems = 3,
                      },
                      sizeof (u32), dest);

    const u32 expected[] = { 1, 2, 42 };
    test_assert_memequal (expected, dest, arrlen (expected));

    block_array_free (b);
  }
}

TEST (TT_UNIT, block_random)
{
  error e = error_create ();

  // Block sizes to test
  const u32 sizes[] = { 1, 2, 3, 4, 5, 10, 100, 500, 1000, 5000, 10000 };
  const u32 niters[]
      = { 100, 100, 100, 100, 100, 100, 1000, 1000, 1000, 1000, 10000 };

  for (u32 i = 0; i < arrlen (sizes); ++i)
    {
      i_log_info ("Block random test: %d\n", i);

      struct ext_array ext_arr = ext_array_create ();
      struct block_array *block_arr = block_array_create (sizes[i], &e);
      struct data_writer ref;
      struct data_writer sut;

      ext_array_data_writer (&ref, &ext_arr);
      block_array_data_writer (&sut, block_arr);

      struct dvalidtr d = {
        .sut = sut,
        .ref = ref,
        .isvalid = NULL,
      };

      dvalidtr_random_test (&d, 1, niters[i], 1000, &e);

      ext_array_free (&ext_arr);
      block_array_free (block_arr);
    }
}
#endif
