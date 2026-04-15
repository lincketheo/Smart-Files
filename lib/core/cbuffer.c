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

#include "core/cbuffer.h"

#include "core/error.h"
#include "test/testing.h"

#include <string.h>

#define MIN(a, b) ((a) < (b) ? (a) : (b))

struct cbuffer
cbuffer_create (void *data, const u32 cap)
{
  ASSERT (data);
  ASSERT (cap > 0);
  const struct cbuffer ret = (struct cbuffer){
    .head = 0,
    .tail = 0,
    .cap = cap,
    .data = data,
    .isfull = 0,
  };
  return ret;
}

struct cbuffer
cbuffer_create_with (void *data, const u32 cap, const u32 len)
{
  ASSERT (data);
  ASSERT (cap > 0);
  ASSERT (len <= cap);
  const struct cbuffer ret = (struct cbuffer){
    .head = len % cap,
    .tail = 0,
    .cap = cap,
    .data = data,
    .isfull = len == cap,
  };
  return ret;
}

////////////////////////////////////////////////////////////
// UTILS

#ifndef NTEST
TEST (TT_UNIT, cbuffer_isempty)
{
  u8 buf[1];
  struct cbuffer b = cbuffer_create (buf, 1);
  test_assert_int_equal (cbuffer_isempty (&b), 1);
  const u8 next = 0xFF;
  cbuffer_push_back (&next, 1, &b);
  test_assert_int_equal (cbuffer_isempty (&b), 0);
}

TEST (TT_UNIT, cbuffer_len)
{
  u8 buf[4];
  struct cbuffer b = cbuffer_create (buf, 4);
  test_assert_int_equal (cbuffer_len (&b), 0);
  cbuffer_pushb_back_expect (1, &b);
  cbuffer_pushb_back_expect (2, &b);
  test_assert_int_equal (cbuffer_len (&b), 2);
}

TEST (TT_UNIT, cbuffer_avail)
{
  u8 buf[4];
  struct cbuffer b = cbuffer_create (buf, 4);
  test_assert_int_equal (cbuffer_avail (&b), 4);
  cbuffer_pushb_back_expect (9, &b);
  test_assert_int_equal (cbuffer_avail (&b), 3);
}
#endif

void
cbuffer_discard_all (struct cbuffer *b)
{
  DBG_ASSERT (cbuffer, b);

  b->tail = 0;
  b->head = 0;
  b->isfull = 0;
}

struct bytes
cbuffer_get_next_data_bytes (const struct cbuffer *b)
{
  DBG_ASSERT (cbuffer, b);

  if (b->head == b->tail && !b->isfull)
    {
      return (struct bytes){
        .head = NULL,
        .len = 0,
      };
    }

  if (b->head > b->tail)
    {
      return (struct bytes){
        .head = &b->data[b->tail],
        .len = b->head - b->tail,
      };
    }
  else
    {
      return (struct bytes){
        .head = &b->data[b->tail],
        .len = b->cap - b->tail,
      };
    }
}

#ifndef NTEST
TEST (TT_UNIT, cbuffer_get_next_data_bytes)
{
  u8 raw[16];
  struct cbuffer b = {
    .data = raw,
    .cap = sizeof (raw),
    .head = 0,
    .tail = 0,
    .isfull = false,
  };

  {
    const struct bytes out = cbuffer_get_next_data_bytes (&b);
    test_assert_ptr_equal (out.head, NULL);
    test_assert_int_equal (out.len, 0);
  }

  TEST_CASE ("Right half")
  {
    b.head = 2;
    b.tail = 6;
    b.isfull = false;

    const struct bytes out = cbuffer_get_next_data_bytes (&b);
    test_assert_ptr_equal (out.head, &raw[6]);
    test_assert_int_equal (out.len, 16 - 6);
  }

  TEST_CASE ("Middle")
  {
    b.head = 12;
    b.tail = 4;
    b.isfull = false;

    const struct bytes out = cbuffer_get_next_data_bytes (&b);
    test_assert_ptr_equal (out.head, &raw[4]);
    test_assert_int_equal (out.len, 12 - 4);
  }
}
#endif

struct bytes
cbuffer_get_next_avail_bytes (const struct cbuffer *b)
{
  DBG_ASSERT (cbuffer, b);

  if (b->isfull)
    {
      return (struct bytes){
        .head = NULL,
        .len = 0,
      };
    }

  // [ _______ tail / head __________ ] (e.g. empty)
  // [ _______ tail Data Data head __ ]
  if (b->head >= b->tail)
    {
      return (struct bytes){
        .head = &b->data[b->head],
        .len = b->cap - b->head,
      };
    }
  else
    {
      return (struct bytes){
        .head = &b->data[b->head],
        .len = b->tail - b->head,
      };
    }
}

#ifndef NnEST
TEST (TT_UNIT, cbuffer_get_nbytes)
{
  u8 raw[16];
  struct cbuffer b = {
    .data = raw,
    .cap = sizeof (raw),
    .head = 0,
    .tail = 0,
    .isfull = false,
  };

  TEST_CASE ("Empty buffer")
  {
    const struct bytes out = cbuffer_get_next_avail_bytes (&b);
    test_assert_ptr_equal (out.head, &raw[0]);
    test_assert_int_equal (out.len, 16);
  }

  TEST_CASE ("Head < Tail, normal case")
  {
    b.head = 2;
    b.tail = 6;
    b.isfull = false;

    const struct bytes out = cbuffer_get_next_avail_bytes (&b);
    test_assert_ptr_equal (out.head, &raw[2]);
    test_assert_int_equal (out.len, 4); // 6 - 2
  }

  TEST_CASE ("Head > Tail, wraparound case")
  {
    b.head = 12;
    b.tail = 4;
    b.isfull = false;

    const struct bytes out = cbuffer_get_next_avail_bytes (&b);
    test_assert_ptr_equal (out.head, &raw[12]);
    test_assert_int_equal (out.len, 4); // cap - head = 16 - 12
  }

  TEST_CASE ("Full buffer")
  {
    b.head = 5;
    b.tail = 5;
    b.isfull = true;

    const struct bytes out = cbuffer_get_next_avail_bytes (&b);
    test_assert_ptr_equal (out.head, NULL);
    test_assert_int_equal (out.len, 0);
  }
}
#endif

void
cbuffer_fakewrite (struct cbuffer *b, const u32 nbytes)
{
  DBG_ASSERT (cbuffer, b);
  ASSERT (nbytes <= cbuffer_avail (b));

  b->head = (b->head + nbytes) % b->cap;
  if (nbytes > 0 && b->head == b->tail)
    {
      b->isfull = true;
    }
}

#ifndef NTEST
TEST (TT_UNIT, cbuffer_fakewrite)
{
  u8 raw[8];
  struct cbuffer b = cbuffer_create_from (raw);

  TEST_CASE ("empty buffer")
  {
    cbuffer_fakewrite (&b, 0);
    test_assert_int_equal (b.head, 0);
    test_assert_int_equal (b.tail, 0);
    test_assert (!b.isfull);
  }

  TEST_CASE ("normal")
  {
    b.head = 0;
    b.tail = 5;
    b.isfull = false;

    cbuffer_fakewrite (&b, 3);
    test_assert_int_equal (b.head, 3);
    test_assert_int_equal (b.tail, 5);
    test_assert (!b.isfull);
  }

  TEST_CASE ("wraparound")
  {
    b.head = 6;
    b.tail = 2;
    b.isfull = false;

    cbuffer_fakewrite (&b, 2);
    test_assert_int_equal (b.head, 0);
    test_assert_int_equal (b.tail, 2);
    test_assert (!b.isfull);
  }

  TEST_CASE ("reach tail")
  {
    b.head = 2;
    b.tail = 5;
    b.isfull = false;

    cbuffer_fakewrite (&b, 3);
    test_assert_int_equal (b.head, 5);
    test_assert (b.isfull);
  }

  TEST_CASE ("reach tail via wrap")
  {
    b.head = 6;
    b.tail = 2;
    b.isfull = false;

    cbuffer_fakewrite (&b, 4); // 6 + 4 == 10 % 8 == 2
    test_assert_int_equal (b.head, 2);
    test_assert (b.isfull);
  }

  TEST_CASE ("write 0 on full buffer does not change state")
  {
    b.head = 3;
    b.tail = 3;
    b.isfull = true;

    cbuffer_fakewrite (&b, 0);
    test_assert_int_equal (b.head, 3);
    test_assert_int_equal (b.tail, 3);
    test_assert (b.isfull);
  }
}
#endif

void
cbuffer_fakeread (struct cbuffer *b, const u32 nbytes)
{
  DBG_ASSERT (cbuffer, b);
  ASSERT (nbytes <= cbuffer_len (b));

  b->tail = (b->tail + nbytes) % b->cap;
  if (nbytes > 0)
    {
      b->isfull = false;
    }
}

#ifndef NTEST
TEST (TT_UNIT, cbuffer_fakeread)
{
  u8 raw[8];
  struct cbuffer b = cbuffer_create_from (raw);

  TEST_CASE ("empty buffer")
  {
    cbuffer_fakeread (&b, 0);
    test_assert_int_equal (b.head, 0);
    test_assert_int_equal (b.tail, 0);
    test_assert (!b.isfull);
  }

  TEST_CASE ("normal")
  {
    b.head = 0;
    b.tail = 5;
    b.isfull = false;

    cbuffer_fakeread (&b, 3);
    test_assert_int_equal (b.head, 0);
    test_assert_int_equal (b.tail, 0);
    test_assert (!b.isfull);
  }

  TEST_CASE ("wraparound")
  {
    b.head = 6;
    b.tail = 2;
    b.isfull = false;

    cbuffer_fakeread (&b, 2);
    test_assert_int_equal (b.head, 6);
    test_assert_int_equal (b.tail, 4);
    test_assert (!b.isfull);
  }

  {
    b.head = 2;
    b.tail = 5;
    b.isfull = false;

    cbuffer_fakeread (&b, 3);
    test_assert_int_equal (b.head, 2);
    test_assert_int_equal (b.tail, 0);
    test_assert (!b.isfull);
  }

  {
    b.head = 6;
    b.tail = 2;
    b.isfull = false;

    cbuffer_fakeread (&b, 4);
    test_assert_int_equal (b.head, 6);
    test_assert_int_equal (b.tail, 6);
    test_assert (!b.isfull);
  }

  {
    b.head = 3;
    b.tail = 3;
    b.isfull = true;

    cbuffer_fakeread (&b, 0);
    test_assert_int_equal (b.head, 3);
    test_assert_int_equal (b.tail, 3);
    test_assert (b.isfull);
  }
}
#endif

////////////////////////////////////////////////////////////
// RAW READ / WRITE

u32
cbuffer_read (void *dest, const u32 size, const u32 n, struct cbuffer *b)
{
  DBG_ASSERT (cbuffer, b);
  ASSERT (size > 0);
  ASSERT (n > 0);

  const u32 len = cbuffer_len (b) / size;
  const u32 ntoread = (n < len) ? n : len;
  const u32 btoread = ntoread * size;
  u32 bread = 0;

  u8 *output = NULL;
  if (dest)
    {
      output = (u8 *)dest;
    }

  while (bread < btoread)
    {
      u32 next;

      if (!b->isfull && b->head > b->tail)
        {
          next = MIN (b->head - b->tail, btoread - bread);
        }
      else
        {
          next = MIN (b->cap - b->tail, btoread - bread);
        }

      if (output)
        {
          memcpy (output + bread, b->data + b->tail, next);
        }
      b->tail = (b->tail + next) % b->cap;
      bread += next;
      b->isfull = 0;
    }

  ASSERT (ntoread * size == bread);

  return ntoread;
}

#ifndef NTEST
TEST (TT_UNIT, cbuffer_read)
{
  u8 buf[3];
  struct cbuffer b = cbuffer_create (buf, 3);
  u8 out[3];

  // read from empty: returns 0
  const u32 r1 = cbuffer_read (out, 1, 1, &b);
  test_assert_int_equal (r1, 0);

  // read after write
  const u8 src[3] = { 7, 8, 9 };
  cbuffer_write (src, 1, 3, &b);
  const u32 r2 = cbuffer_read (out, 1, 2, &b);
  test_assert_int_equal (r2, 2);
  test_assert_int_equal (out[0], 7);
  test_assert_int_equal (out[1], 8);
  test_assert_int_equal (cbuffer_len (&b), 1);
}
#endif

u32
cbuffer_copy (void *dest, const u32 size, const u32 n, const struct cbuffer *b)
{
  DBG_ASSERT (cbuffer, b);
  ASSERT (size > 0);
  ASSERT (n > 0);

  const u32 len = cbuffer_len (b) / size;
  const u32 ntoread = (n < len) ? n : len;
  const u32 btoread = ntoread * size;
  u32 bread = 0;

  u8 *output = NULL;
  if (dest)
    {
      output = (u8 *)dest;
    }

  // Literally the exact same as read but
  // use these temp variables instead
  // I was lazy
  u32 tail = b->tail;
  bool isfull = b->isfull;

  while (bread < btoread)
    {
      u32 next;

      if (!isfull && b->head > tail)
        {
          next = MIN (b->head - tail, btoread - bread);
        }
      else
        {
          next = MIN (b->cap - tail, btoread - bread);
        }

      if (output)
        {
          memcpy (output + bread, b->data + tail, next);
        }
      tail = (tail + next) % b->cap;
      bread += next;
      isfull = 0;
    }

  ASSERT (ntoread * size == bread);
  return ntoread;
}

#ifndef NTEST
TEST (TT_UNIT, cbuffer_copy)
{
  u8 buf[3];
  struct cbuffer b = cbuffer_create (buf, 3);
  u8 out[3];
  u32 r2;

  TEST_CASE ("read from empty: returns 0")
  {
    const u32 r1 = cbuffer_copy (out, 1, 1, &b);
    test_assert_int_equal (r1, 0);
  }

  TEST_CASE ("read after write")
  {
    const u8 src[3] = { 7, 8, 9 };
    cbuffer_write (src, 1, 3, &b);
    r2 = cbuffer_copy (out, 1, 2, &b);
    test_assert_int_equal (r2, 2);
    test_assert_int_equal (out[0], 7);
    test_assert_int_equal (out[1], 8);
    test_assert_int_equal (cbuffer_len (&b), 3);
  }

  TEST_CASE ("Do it again and get the same results")
  {
    r2 = cbuffer_copy (out, 1, 2, &b);
    test_assert_int_equal (r2, 2);
    test_assert_int_equal (out[0], 7);
    test_assert_int_equal (out[1], 8);
    test_assert_int_equal (cbuffer_len (&b), 3);
  }
}
#endif

u32
cbuffer_write (const void *src, const u32 size, const u32 n, struct cbuffer *b)
{
  DBG_ASSERT (cbuffer, b);
  ASSERT (src);
  ASSERT (size > 0);
  ASSERT (n > 0);

  ASSERT (b->cap >= cbuffer_len (b));
  const u32 avail = (b->cap - cbuffer_len (b)) / size;
  const u32 ntowrite = (n < avail) ? n : avail;
  const u32 btowrite = ntowrite * size;
  u32 bwrite = 0;

  const u8 *input = (u8 *)src;

  while (bwrite < btowrite)
    {
      u32 next;

      ASSERT (btowrite > bwrite);
      if (b->head >= b->tail)
        {
          ASSERT (b->cap > b->head);
          next = MIN (b->cap - b->head, btowrite - bwrite);

          if (next == 0)
            {
              b->head = 0;
              next = b->tail;
            }
        }
      else
        {
          ASSERT (b->tail > b->head);
          next = MIN (b->tail - b->head, btowrite - bwrite);
        }

      memcpy (b->data + b->head, input + bwrite, next);
      b->head = (b->head + next) % b->cap;
      bwrite += next;

      if (b->head == b->tail)
        {
          b->isfull = 1;
        }
    }

  ASSERT (ntowrite * size == bwrite);

  return ntowrite;
}

#ifndef NTEST
TEST (TT_UNIT, cbuffer_write)
{
  u8 buf[3];
  struct cbuffer b = cbuffer_create (buf, 3);
  const u8 src[4] = { 1, 2, 3, 4 };

  TEST_CASE ("size > capacity: writes nothing")
  {
    const u32 w1 = cbuffer_write (src, 4, 1, &b);
    test_assert_int_equal (w1, 0);
  }

  TEST_CASE ("element size 1, too many elements: only 3 fit")
  {
    const u32 w2 = cbuffer_write (src, 1, 4, &b);
    test_assert_int_equal (w2, 3);
    test_assert_int_equal (cbuffer_len (&b), 3);
  }
}
#endif

////////////////////////////////////////////////////////////
// CBUFFER READ / WRITE

u32
cbuffer_cbuffer_move (struct cbuffer *dest, const u32 sz, const u32 cnt,
                      struct cbuffer *src)
{
  DBG_ASSERT (cbuffer, dest);
  DBG_ASSERT (cbuffer, src);
  ASSERT (sz > 0 && cnt > 0);

  // calculate number of elements to move
  const u32 src_elems = cbuffer_len (src) / sz;
  const u32 dst_space = cbuffer_avail (dest) / sz;
  u32 n;
  if (cnt < src_elems)
    {
      n = cnt;
    }
  else
    {
      n = src_elems;
    }
  if (dst_space < n)
    {
      n = dst_space;
    }
  if (n == 0)
    {
      return 0;
    }

  // total nbytes to transfer
  u32 nbytes = n * sz;

  // first chunk: from tail up to end of buffer
  u32 first;
  if (src->isfull || src->head <= src->tail)
    {
      first = src->cap - src->tail;
    }
  else
    {
      first = src->head - src->tail;
    }
  if (first > nbytes)
    {
      first = nbytes;
    }

  // write first chunk into dest
  cbuffer_write (src->data + src->tail, 1, first, dest);

  // advance tail and clear full flag
  src->tail = (src->tail + first) % src->cap;
  src->isfull = false;
  nbytes -= first;

  // second chunk if any remains
  if (nbytes > 0)
    {
      cbuffer_write (src->data + src->tail, 1, nbytes, dest);
      src->tail = (src->tail + nbytes) % src->cap;
    }

  return n;
}

#ifndef NTEST
TEST (TT_UNIT, cbuffer_cbuffer_move)
{
  u8 buf_s[4];
  u8 buf_d[4];
  struct cbuffer src = cbuffer_create (buf_s, 4);
  struct cbuffer dst = cbuffer_create (buf_d, 4);
  u8 out[4];

  TEST_CASE ("empty source: should read 0 elements")
  {
    const u32 r0 = cbuffer_cbuffer_move (&dst, 1, 1, &src);
    test_assert_int_equal (r0, 0);
  }

  TEST_CASE ("fill source with 3 nbytes")
  {
    const u8 data2[3] = { 4, 5, 6 };
    cbuffer_write (data2, 1, 3, &src);
  }

  TEST_CASE ("read 2 elements into dst")
  {
    const u32 r1 = cbuffer_cbuffer_move (&dst, 1, 2, &src);
    test_assert_int_equal (r1, 2);
    test_assert_int_equal (cbuffer_len (&src), 1);
    test_assert_int_equal (cbuffer_len (&dst), 2);
  }

  TEST_CASE ("verify dst contents")
  {
    const u32 r2 = cbuffer_read (out, 1, 2, &dst);
    test_assert_int_equal (r2, 2);
    test_assert_int_equal (out[0], 4);
    test_assert_int_equal (out[1], 5);
  }
}
#endif

u32
cbuffer_cbuffer_copy (struct cbuffer *dest, const u32 sz, const u32 cnt,
                      const struct cbuffer *src)
{
  DBG_ASSERT (cbuffer, dest);
  DBG_ASSERT (cbuffer, src);
  ASSERT (sz > 0 && cnt > 0);

  // calculate number of elements to copy
  const u32 src_elems = cbuffer_len (src) / sz;
  const u32 dst_space = cbuffer_avail (dest) / sz;
  u32 n;
  if (cnt < src_elems)
    {
      n = cnt;
    }
  else
    {
      n = src_elems;
    }
  if (dst_space < n)
    {
      n = dst_space;
    }
  if (n == 0)
    {
      return 0;
    }

  // total nbytes to copy
  u32 nbytes = n * sz;

  // local cursor and copy of full flag
  u32 pos = src->tail;
  bool full = src->isfull;

  // first chunk: from pos up to end of buffer
  u32 first;
  if (full || src->head <= pos)
    {
      first = src->cap - pos;
    }
  else
    {
      first = src->head - pos;
    }
  if (first > nbytes)
    {
      first = nbytes;
    }

  // write first chunk into dest
  cbuffer_write (src->data + pos, 1, first, dest);

  // advance local cursor
  pos = (pos + first) % src->cap;
  nbytes -= first;

  // second chunk if any remains
  if (nbytes > 0)
    {
      cbuffer_write (src->data + pos, 1, nbytes, dest);
    }

  return n;
}
#ifndef NTEST
TEST (TT_UNIT, cbuffer_cbuffer_copy)
{
  u8 buf_s[4];
  u8 buf_d[4];
  struct cbuffer src = cbuffer_create (buf_s, 4);
  struct cbuffer dst = cbuffer_create (buf_d, 4);
  u8 out[4];

  TEST_CASE ("empty source: should copy 0 elements")
  {
    const u32 r0 = cbuffer_cbuffer_copy (&dst, 1, 1, &src);
    test_assert_int_equal (r0, 0);
  }

  TEST_CASE ("fill source with 3 nbytes")
  {
    const u8 data1[3] = { 1, 2, 3 };
    cbuffer_write (data1, 1, 3, &src);
  }

  TEST_CASE ("copy 2 elements into dst")
  {
    const u32 r1 = cbuffer_cbuffer_copy (&dst, 1, 2, &src);
    test_assert_int_equal (r1, 2);
    test_assert_int_equal (cbuffer_len (&src), 3);
    test_assert_int_equal (cbuffer_len (&dst), 2);
  }

  TEST_CASE ("verify dst contents")
  {
    const u32 r2 = cbuffer_read (out, 1, 2, &dst);
    test_assert_int_equal (r2, 2);
    test_assert_int_equal (out[0], 1);
    test_assert_int_equal (out[1], 2);
  }
}
#endif

////////////////////////////////////////////////////////////
// IO READ / WRITE

i32
cbuffer_read_from_file_1 (i_file *src, const struct cbuffer *b, const u32 len,
                          error *e)
{
  ASSERT (src);
  ASSERT (b);

  const u32 btowrite = MIN (cbuffer_avail (b), len);
  u32 bwrite = 0;
  struct bytes iov[2];
  int iovcnt = 0;
  u32 newhead = b->head;

  while (bwrite < btowrite)
    {
      u32 next;
      if (newhead >= b->tail)
        {
          ASSERT (b->cap > newhead);
          next = MIN (b->cap - newhead, btowrite - bwrite);
        }
      else
        {
          ASSERT (b->tail > newhead);
          next = MIN (b->tail - newhead, btowrite - bwrite);
        }

      ASSERT (iovcnt < 2);
      iov[iovcnt].head = b->data + newhead;
      iov[iovcnt].len = next;
      iovcnt++;

      newhead = (newhead + next) % b->cap;
      bwrite += next;
    }

  if (btowrite == 0)
    {
      return 0;
    }

  const i64 nread = i_readv_all (src, iov, iovcnt, e);
  if (nread < 0)
    {
      return error_trace (e);
    }

  return nread;
}

err_t
cbuffer_read_from_file_1_expect (i_file *src, const struct cbuffer *b, const u32 len,
                                 error *e)
{
  const i32 read = cbuffer_read_from_file_1 (src, b, len, e);
  if (read < 0)
    {
      return read;
    }
  ASSERT ((u32)read == len);
  return SUCCESS;
}

void
cbuffer_read_from_file_2 (struct cbuffer *b, const u32 nread)
{
  DBG_ASSERT (cbuffer, b);
  ASSERT (nread <= cbuffer_avail (b));

  b->head = (b->head + nread) % b->cap;

  if (b->head == b->tail && nread > 0)
    {
      b->isfull = 1;
    }
}

i32
cbuffer_read_from_file (i_file *src, struct cbuffer *b, const u32 len, error *e)
{
  DBG_ASSERT (cbuffer, b);
  ASSERT (src);

  const i32 nread = cbuffer_read_from_file_1 (src, b, len, e);
  if (nread < 0)
    {
      return nread;
    }

  cbuffer_read_from_file_2 (b, (u32)nread);

  return nread;
}

i32
cbuffer_write_to_file_1 (i_file *dest, const struct cbuffer *b, const u32 len,
                         error *e)
{
  ASSERT (dest);
  ASSERT (b);

  const u32 btoread = MIN (cbuffer_len (b), len);
  u32 bread = 0;
  struct bytes iov[2];
  int iovcnt = 0;
  u32 newtail = b->tail;

  while (bread < btoread)
    {
      u32 next;
      if (!b->isfull && b->head > newtail)
        {
          next = MIN (b->head - newtail, btoread - bread);
        }
      else
        {
          next = MIN (b->cap - newtail, btoread - bread);
        }

      ASSERT (iovcnt < 2);
      iov[iovcnt].head = b->data + newtail;
      iov[iovcnt].len = next;
      iovcnt++;

      newtail = (newtail + next) % b->cap;
      bread += next;
    }

  if (btoread == 0)
    {
      return 0;
    }

  const err_t err = i_writev_all (dest, iov, iovcnt, e);
  if (err != SUCCESS)
    {
      return error_trace (e);
    }

  return btoread;
}

err_t
cbuffer_write_to_file_1_expect (i_file *dest, const struct cbuffer *b, const u32 len,
                                error *e)
{
  const i32 written = cbuffer_write_to_file_1 (dest, b, len, e);
  if (written < 0)
    {
      return written;
    }
  ASSERT ((u32)written == len);
  return SUCCESS;
}

void
cbuffer_write_to_file_2 (struct cbuffer *b, const u32 nwritten)
{
  DBG_ASSERT (cbuffer, b);
  ASSERT (nwritten <= cbuffer_len (b));

  b->tail = (b->tail + nwritten) % b->cap;

  if (nwritten > 0)
    {
      b->isfull = 0;
    }
}

i32
cbuffer_write_to_file (i_file *dest, struct cbuffer *b, const u32 len, error *e)
{
  DBG_ASSERT (cbuffer, b);
  ASSERT (dest);

  const i32 result = cbuffer_write_to_file_1 (dest, b, len, e);
  if (result < 0)
    {
      return result;
    }

  cbuffer_write_to_file_2 (b, (u32)result);

  return result;
}

////////////////////////////////////////////////////////////
// WORKING WITH SINGLE ELEMENTS
static u8
cbuffer_get_no_check (void *dest, const u32 size, const u32 idx, const struct cbuffer *b)
{
  DBG_ASSERT (cbuffer, b);
  ASSERT (size > 0);
  ASSERT (idx * size < b->cap);

  const u32 len = cbuffer_len (b) / size;
  ASSERT (idx < len);

  if (dest)
    {
      const u32 offset = (b->tail + idx * size) % b->cap;

      if (offset + size <= b->cap)
        {
          memcpy (dest, b->data + offset, size);
        }
      else
        {
          const u32 first = b->cap - offset;
          memcpy (dest, b->data + offset, first);
          memcpy ((u8 *)dest + first, b->data, size - first);
        }
    }

  return 0;
}

#ifndef NTEST
TEST (TT_UNIT, cbuffer_get_no_check)
{
  TEST_CASE ("Normal behavior")
  {
    u8 buf[4];
    struct cbuffer b = cbuffer_create (buf, 4);
    u8 out[4];

    // write: [7, 8, 9]
    const u8 src[3] = { 7, 8, 9 };
    cbuffer_write (src, 1, 3, &b);

    // get(0) = 7
    cbuffer_get_no_check (out, 1, 0, &b);
    test_assert_int_equal (out[0], 7);

    // get(1) = 8
    cbuffer_get_no_check (out, 1, 1, &b);
    test_assert_int_equal (out[0], 8);

    // get(2) = 9
    cbuffer_get_no_check (out, 1, 2, &b);
    test_assert_int_equal (out[0], 9);

    // still full
    test_assert_int_equal (cbuffer_len (&b), 3);

    // test wraparound for size=2
    struct cbuffer b2 = cbuffer_create (buf, 4);
    const u8 src2[4] = { 0x11, 0x22, 0x33, 0x44 };
    cbuffer_write (src2, 2, 2, &b2); // two 2-byte elements

    u8 out2[2];
    cbuffer_get_no_check (out2, 2, 0, &b2);
    test_assert_int_equal (out2[0], 0x11);
    test_assert_int_equal (out2[1], 0x22);

    cbuffer_get_no_check (out2, 2, 1, &b2);
    test_assert_int_equal (out2[0], 0x33);
    test_assert_int_equal (out2[1], 0x44);

    test_assert_int_equal (cbuffer_len (&b2), 4);
  }

  TEST_CASE ("wraparound behavior")
  {
    u8 buf[4];
    struct cbuffer b = cbuffer_create (buf, 4);

    // Fill buffer completely
    const u8 src1[4] = { 0xAA, 0xBB, 0xCC, 0xDD };
    cbuffer_write (src1, 1, 4, &b); // buffer now full

    u8 temp[2];
    cbuffer_read (temp, 1, 2,
                  &b); // advance tail by 2 bytes // head = 0, tail = 2 now

    const u8 src2[2] = { 0xEE, 0xFF };
    cbuffer_write (src2, 1, 2,
                   &b); // write at head (0,1), causes wrap

    // buffer now has: [0xEE, 0xFF, 0xCC, 0xDD]
    // tail = 2, head = 2, full circle
    // logical order: [0xCC, 0xDD, 0xEE, 0xFF]

    u8 out;
    cbuffer_get_no_check (&out, 1, 0, &b); // tail+0
    test_assert_int_equal (out, 0xCC);

    cbuffer_get_no_check (&out, 1, 1, &b); // tail+1
    test_assert_int_equal (out, 0xDD);

    cbuffer_get_no_check (&out, 1, 2, &b); // wrap to buf[0]
    test_assert_int_equal (out, 0xEE);

    cbuffer_get_no_check (&out, 1, 3, &b); // buf[1]
    test_assert_int_equal (out, 0xFF);
  }
}
#endif

bool
cbuffer_get (void *dest, const u32 size, const u32 idx, const struct cbuffer *b)
{
  DBG_ASSERT (cbuffer, b);
  ASSERT (dest);

  if (!b->isfull && (u32)idx >= (b->head + b->cap - b->tail) % b->cap)
    {
      return false;
    }

  cbuffer_get_no_check (dest, size, idx, b);

  return true;
}

#ifndef NTEST
TEST (TT_UNIT, cbuffer_get)
{
  u8 buf[2];
  struct cbuffer b = cbuffer_create (buf, 2);
  const u8 x = 5;
  cbuffer_push_back (&x, 1, &b);

  u8 v = 0xFF;
  // in-range index
  test_assert_int_equal (cbuffer_get (&v, 1, 0, &b), 1);
  test_assert_int_equal (v, 5);
  // out-of-range index
  test_assert_int_equal (cbuffer_get (&v, 1, 1, &b), 0);
}
#endif

bool
cbuffer_peek_back (void *dest, const u32 size, const struct cbuffer *b)
{
  ASSERT (dest);
  DBG_ASSERT (cbuffer, b);

  if (!b->isfull && b->head == b->tail)
    {
      return false;
    }

  return cbuffer_get (dest, size, 0, b);
}

#ifndef NTEST
TEST (TT_UNIT, cbuffer_peek_back)
{
  u8 buf1[1];
  struct cbuffer b1 = cbuffer_create (buf1, 1);
  u8 v;

  // peek on empty buffer fails
  test_assert_int_equal (cbuffer_peek_back (&v, 1, &b1), 0);

  // push_back then peek
  u8 x = 0xAA;
  cbuffer_push_back (&x, 1, &b1);
  test_assert_int_equal (cbuffer_peek_back (&v, 1, &b1), 1);
  test_assert_int_equal (v, 0xAA);
  test_assert_int_equal (cbuffer_len (&b1), 1);

  // pop_back clears it
  cbuffer_pop_back (&v, 1, &b1);
  test_assert_int_equal (cbuffer_peek_back (&v, 1, &b1), 0);

  // wraparound
  u8 buf2[3];
  struct cbuffer b2 = cbuffer_create (buf2, 3);
  x = 5;
  cbuffer_push_back (&x, 1, &b2);
  x = 6;
  cbuffer_push_back (&x, 1, &b2);
  x = 7;
  cbuffer_push_back (&x, 1, &b2);

  test_assert_int_equal (cbuffer_pop_back (&v, 1, &b2), 1);
  test_assert_int_equal (cbuffer_pop_back (&v, 1, &b2), 1);

  x = 8;
  cbuffer_push_back (&x, 1, &b2);

  test_assert_int_equal (cbuffer_peek_back (&v, 1, &b2), 1);
  test_assert_int_equal (v, 7);
}
#endif

bool
cbuffer_peek_front (void *dest, const u32 size, const struct cbuffer *b)
{
  ASSERT (dest);
  DBG_ASSERT (cbuffer, b);

  if (!b->isfull && b->head == b->tail)
    {
      return false;
    }

  return cbuffer_get (dest, size, 0, b);
}

#ifndef NTEST
TEST (TT_UNIT, cbuffer_peek_front)
{
  u8 buf1[1];
  struct cbuffer b1 = cbuffer_create (buf1, 1);
  u8 v;

  // peek from empty buffer fails
  test_assert_int_equal (cbuffer_peek_front (&v, 1, &b1), 0);

  // push_front then peek_front
  u8 x = 0xAB;
  cbuffer_push_front (&x, 1, &b1);
  test_assert_int_equal (cbuffer_peek_front (&v, 1, &b1), 1);
  test_assert_int_equal (v, 0xAB);
  test_assert_int_equal (cbuffer_len (&b1), 1);

  // pop_front clears it
  cbuffer_pop_front (&v, 1, &b1);
  test_assert_int_equal (cbuffer_peek_front (&v, 1, &b1), 0);

  // wraparound
  u8 buf2[3];
  struct cbuffer b2 = cbuffer_create (buf2, 3);
  x = 1;
  cbuffer_push_front (&x, 1, &b2);
  x = 2;
  cbuffer_push_front (&x, 1, &b2);
  x = 3;
  cbuffer_push_front (&x, 1, &b2);

  test_assert_int_equal (cbuffer_pop_front (&v, 1, &b2), 1);
  test_assert_int_equal (cbuffer_pop_front (&v, 1, &b2), 1);

  x = 4;
  cbuffer_push_front (&x, 1, &b2);

  test_assert_int_equal (cbuffer_peek_front (&v, 1, &b2), 1);
  test_assert_int_equal (v, 4);
}
#endif

bool
cbuffer_push_back (const void *src, const u32 size, struct cbuffer *b)
{
  DBG_ASSERT (cbuffer, b);
  ASSERT (size > 0);

  if (cbuffer_avail (b) < size)
    {
      return false;
    }

  const u32 offset = b->head;
  if (offset + size <= b->cap)
    {
      memcpy (b->data + offset, src, size);
    }
  else
    {
      const u32 first = b->cap - offset;
      memcpy (b->data + offset, src, first);
      memcpy (b->data, (u8 *)src + first, size - first);
    }

  b->head = (b->head + size) % b->cap;
  if (b->head == b->tail)
    {
      b->isfull = true;
    }

  return true;
}

#ifndef NTEST
TEST (TT_UNIT, cbuffer_push_back)
{
  u8 buf[2];
  struct cbuffer b = cbuffer_create (buf, 2);
  u8 v;

  u8 x = 0x11;
  test_assert_int_equal (cbuffer_push_back (&x, 1, &b), 1);

  x = 0x22;
  test_assert_int_equal (cbuffer_push_back (&x, 1, &b), 1);

  x = 0x33;
  test_assert_int_equal (cbuffer_push_back (&x, 1, &b), 0);

  test_assert_int_equal (cbuffer_pop_back (&v, 1, &b), 1);
  test_assert_int_equal (v, 0x11);

  x = 0x33;
  test_assert_int_equal (cbuffer_push_back (&x, 1, &b), 1);

  x = 0x44;
  test_assert_int_equal (cbuffer_push_back (&x, 1, &b), 0);
}
#endif

bool
cbuffer_push_front (const void *src, const u32 size, struct cbuffer *b)
{
  DBG_ASSERT (cbuffer, b);
  ASSERT (size > 0);

  if (cbuffer_avail (b) < size)
    {
      return false;
    }

  // Compute new tail position before wrapping
  const u32 new_tail = (b->tail + b->cap - size) % b->cap;

  if (new_tail + size <= b->cap)
    {
      memcpy (b->data + new_tail, src, size);
    }
  else
    {
      const u32 first = b->cap - new_tail;
      memcpy (b->data + new_tail, src, first);
      memcpy (b->data, (u8 *)src + first, size - first);
    }

  b->tail = new_tail;
  if (b->head == b->tail)
    b->isfull = true;

  return true;
}

#ifndef NTEST
TEST (TT_UNIT, cbuffer_push_front)
{
  u8 buf[3];
  struct cbuffer b = cbuffer_create (buf, 3);
  u8 v;

  // Push front 0x44
  u8 x = 0x44;
  test_assert_int_equal (cbuffer_push_front (&x, 1, &b), 1);

  // Push front 0x33
  x = 0x33;
  test_assert_int_equal (cbuffer_push_front (&x, 1, &b), 1);

  // Push front 0x22
  x = 0x22;
  test_assert_int_equal (cbuffer_push_front (&x, 1, &b), 1);

  // Buffer full: next push fails
  x = 0x11;
  test_assert_int_equal (cbuffer_push_front (&x, 1, &b), 0);

  // Pop front returns 0x22
  test_assert_int_equal (cbuffer_pop_front (&v, 1, &b), 1);
  test_assert_int_equal (v, 0x22);

  // Push front 0x11 now succeeds (wrap-around)
  x = 0x11;
  test_assert_int_equal (cbuffer_push_front (&x, 1, &b), 1);

  // Pop all remaining values in order: 0x11, 0x33, 0x44
  test_assert_int_equal (cbuffer_pop_front (&v, 1, &b), 1);
  test_assert_int_equal (v, 0x11);
  test_assert_int_equal (cbuffer_pop_front (&v, 1, &b), 1);
  test_assert_int_equal (v, 0x33);
  test_assert_int_equal (cbuffer_pop_front (&v, 1, &b), 1);
  test_assert_int_equal (v, 0x44);
}
#endif

bool
cbuffer_pop_back (void *dest, const u32 size, struct cbuffer *b)
{
  ASSERT (dest);
  DBG_ASSERT (cbuffer, b);
  ASSERT (size > 0);

  if (!b->isfull && b->head == b->tail)
    {
      return false;
    }

  const u32 offset = b->tail;
  if (offset + size <= b->cap)
    {
      memcpy (dest, b->data + offset, size);
    }
  else
    {
      const u32 first = b->cap - offset;
      memcpy (dest, b->data + offset, first);
      memcpy ((u8 *)dest + first, b->data, size - first);
    }

  b->tail = (b->tail + size) % b->cap;
  b->isfull = false;

  return true;
}

#ifndef NTEST
TEST (TT_UNIT, cbuffer_pop_back)
{
  u8 buf1[1];
  struct cbuffer b1 = cbuffer_create (buf1, 1);
  u8 v;

  test_assert_int_equal (cbuffer_pop_back (&v, 1, &b1), 0);

  u8 x = 0x7F;
  cbuffer_push_back (&x, 1, &b1);
  test_assert_int_equal (cbuffer_pop_back (&v, 1, &b1), 1);
  test_assert_int_equal (v, 0x7F);

  test_assert_int_equal (cbuffer_pop_back (&v, 1, &b1), 0);

  u8 buf2[3];
  struct cbuffer b2 = cbuffer_create (buf2, 3);

  x = 1;
  cbuffer_push_back (&x, 1, &b2);
  x = 2;
  cbuffer_push_back (&x, 1, &b2);
  x = 3;
  cbuffer_push_back (&x, 1, &b2);

  test_assert_int_equal (cbuffer_pop_back (&v, 1, &b2), 1);
  test_assert_int_equal (v, 1);

  x = 4;
  cbuffer_push_back (&x, 1, &b2);

  test_assert_int_equal (cbuffer_pop_back (&v, 1, &b2), 1);
  test_assert_int_equal (v, 2);
  test_assert_int_equal (cbuffer_pop_back (&v, 1, &b2), 1);
  test_assert_int_equal (v, 3);
  test_assert_int_equal (cbuffer_pop_back (&v, 1, &b2), 1);
  test_assert_int_equal (v, 4);
}
#endif

bool
cbuffer_pop_front (void *dest, const u32 size, struct cbuffer *b)
{
  ASSERT (dest);
  DBG_ASSERT (cbuffer, b);
  ASSERT (size > 0);

  if (!b->isfull && b->head == b->tail)
    {
      return false;
    }

  const u32 offset = b->tail;
  if (offset + size <= b->cap)
    {
      memcpy (dest, b->data + offset, size);
    }
  else
    {
      const u32 first = b->cap - offset;
      memcpy (dest, b->data + offset, first);
      memcpy ((u8 *)dest + first, b->data, size - first);
    }

  b->tail = (b->tail + size) % b->cap;
  b->isfull = false;

  return true;
}

#ifndef NTEST
TEST (TT_UNIT, cbuffer_pop_front)
{
  u8 buf1[1];
  struct cbuffer b1 = cbuffer_create (buf1, 1);
  u8 v;

  // pop from empty fails
  test_assert_int_equal (cbuffer_pop_front (&v, 1, &b1), 0);

  // single push_front and pop_front
  u8 x = 0xAB;
  cbuffer_push_front (&x, 1, &b1);
  test_assert_int_equal (cbuffer_pop_front (&v, 1, &b1), 1);
  test_assert_int_equal (v, 0xAB);

  test_assert_int_equal (cbuffer_pop_front (&v, 1, &b1), 0);

  // fill buffer with push_front in reverse order: [3, 2, 1]
  u8 buf2[3];
  struct cbuffer b2 = cbuffer_create (buf2, 3);

  x = 1;
  cbuffer_push_front (&x, 1, &b2);
  x = 2;
  cbuffer_push_front (&x, 1, &b2);
  x = 3;
  cbuffer_push_front (&x, 1, &b2);

  test_assert_int_equal (cbuffer_pop_front (&v, 1, &b2), 1); // gets 3
  test_assert_int_equal (v, 3);
  test_assert_int_equal (cbuffer_pop_front (&v, 1, &b2), 1); // gets 2
  test_assert_int_equal (v, 2);
  test_assert_int_equal (cbuffer_pop_front (&v, 1, &b2), 1); // gets 1
  test_assert_int_equal (v, 1);
}
#endif
