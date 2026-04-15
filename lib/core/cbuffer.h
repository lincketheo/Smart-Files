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

#pragma once

#include "core/assert.h"
#include "core/bytes.h"
#include "core/signatures.h"

// [-------------++++++++++++++++------------]
// tail            head
struct cbuffer
{
  u8 *data;
  u32 cap;
  u32 head;
  u32 tail;
  bool isfull;
};

#define cbuffer_create_from(data) cbuffer_create (data, sizeof data)
#define cbuffer_create_full_from(data) \
  cbuffer_create_with (data, sizeof data, sizeof data)
#define cbuffer_create_from_cstr(cstr) \
  cbuffer_create_with (cstr, strlen (cstr), strlen (cstr))
struct cbuffer cbuffer_create (void *data, u32 cap);
struct cbuffer cbuffer_create_with (void *data, u32 cap, u32 len);

////////////////////////////////////////////////////////////
// Utils

HEADER_FUNC u32
cbuffer_len (const struct cbuffer *b)
{
  u32 len;
  if (b->isfull)
    {
      len = b->cap;
    }
  else if (b->head >= b->tail)
    {
      len = b->head - b->tail;
    }
  else
    {
      len = b->cap - (b->tail - b->head);
    }
  return len;
}

DEFINE_DBG_ASSERT (struct cbuffer, cbuffer, b, {
  ASSERT (b);
  ASSERT (b->cap > 0);
  ASSERT (b->data);
  if (b->isfull)
    {
      ASSERT (b->tail == b->head);
    }
  ASSERT (cbuffer_len (b) <= b->cap);
})

HEADER_FUNC bool
cbuffer_isempty (const struct cbuffer *b)
{
  DBG_ASSERT (cbuffer, b);
  return (!b->isfull && b->head == b->tail);
}

HEADER_FUNC u32
cbuffer_slen (const struct cbuffer *b, const u32 size)
{
  const u32 len = cbuffer_len (b);
  ASSERT (len % size == 0);
  return len / size;
}

HEADER_FUNC u32
cbuffer_avail (const struct cbuffer *b)
{
  DBG_ASSERT (cbuffer, b);
  const u32 len = cbuffer_len (b);
  ASSERT (b->cap >= len);
  return b->cap - len;
}

HEADER_FUNC u32
cbuffer_savail (const struct cbuffer *b, const u32 size)
{
  DBG_ASSERT (cbuffer, b);
  const u32 len = cbuffer_len (b);
  ASSERT (b->cap >= len);
  ASSERT (len % size == 0);
  return (b->cap - len) / size;
}

void cbuffer_discard_all (struct cbuffer *b); // resets buffer
struct bytes cbuffer_get_next_avail_bytes (
    const struct cbuffer *b); // Gets the next available contiguous open space
struct bytes cbuffer_get_next_data_bytes (
    const struct cbuffer *b); // Next available contiguous data
void cbuffer_fakeread (struct cbuffer *b,
                       u32 bytes); // Reads but not into any output
void
cbuffer_fakewrite (struct cbuffer *b,
                   u32 bytes); // Writes assumes you directly modified already

////////////////////////////////////////////////////////////
// Raw Read / Write
u32 cbuffer_read (void *dest, u32 size, u32 n, struct cbuffer *b);
u32 cbuffer_copy (void *dest, u32 size, u32 n, const struct cbuffer *b);
u32 cbuffer_write (const void *src, u32 size, u32 n, struct cbuffer *b);

#define cbuffer_read_expect(dest, size, n, b)       \
  do                                                \
    {                                               \
      u32 __read = cbuffer_read (dest, size, n, b); \
      ASSERT (__read == n);                         \
    }                                               \
  while (0)

#define cbuffer_write_expect(src, size, n, b)          \
  do                                                   \
    {                                                  \
      u32 __written = cbuffer_write (src, size, n, b); \
      ASSERT (__written == n);                         \
    }                                                  \
  while (0)

// /////////////////////// CBuffer Read / Write
u32 cbuffer_cbuffer_move (struct cbuffer *dest, u32 size, u32 n,
                          struct cbuffer *src);
u32 cbuffer_cbuffer_copy (struct cbuffer *dest, u32 size, u32 n,
                          const struct cbuffer *src);

#define cbuffer_cbuffer_move_max(dest, src) \
  cbuffer_cbuffer_move (dest, 1, cbuffer_len (src), src)
#define cbuffer_cbuffer_copy_max(dest, src) \
  cbuffer_cbuffer_copy (dest, 1, cbuffer_len (src), src)

////////////////////////////////////////////////////////////
// IO Read / Write

i32 cbuffer_write_to_file_1 (i_file *dest, const struct cbuffer *b, u32 len,
                             error *e);
err_t cbuffer_write_to_file_1_expect (i_file *dest, const struct cbuffer *b,
                                      u32 len, error *e);
void cbuffer_write_to_file_2 (struct cbuffer *b, u32 nwritten);
i32 cbuffer_write_to_file (i_file *dest, struct cbuffer *b, u32 len, error *e);
i32 cbuffer_read_from_file_1 (i_file *src, const struct cbuffer *b, u32 len,
                              error *e);
err_t cbuffer_read_from_file_1_expect (i_file *src, const struct cbuffer *b,
                                       u32 len, error *e);
void cbuffer_read_from_file_2 (struct cbuffer *b, u32 nread);
i32 cbuffer_read_from_file (i_file *src, struct cbuffer *b, u32 len, error *e);

////////////////////////////////////////////////////////////
// Single Element read / writes

bool cbuffer_get (void *dest, u32 size, u32 idx, const struct cbuffer *b);

// Push
bool cbuffer_push_back (const void *src, u32 size, struct cbuffer *b);
bool cbuffer_push_front (const void *src, u32 size, struct cbuffer *b);

// Pop
bool cbuffer_pop_back (void *dest, u32 size, struct cbuffer *b);
bool cbuffer_pop_front (void *dest, u32 size, struct cbuffer *b);

// Peek
bool cbuffer_peek_back (void *dest, u32 size, const struct cbuffer *b);
bool cbuffer_peek_front (void *dest, u32 size, const struct cbuffer *b);

// Push
#define cbuffer_push_back_expect(src, size, b)       \
  do                                                 \
    {                                                \
      bool __ret = cbuffer_push_back (src, size, b); \
      ASSERT (__ret);                                \
    }                                                \
  while (0)
#define cbuffer_push_front_expect(src, size, b)       \
  do                                                  \
    {                                                 \
      bool __ret = cbuffer_push_front (src, size, b); \
      ASSERT (__ret);                                 \
    }                                                 \
  while (0)

// Pop
#define cbuffer_pop_back_expect(dest, size, b)       \
  do                                                 \
    {                                                \
      bool __ret = cbuffer_pop_back (dest, size, b); \
      ASSERT (__ret);                                \
    }                                                \
  while (0)
#define cbuffer_pop_front_expect(dest, size, b)       \
  do                                                  \
    {                                                 \
      bool __ret = cbuffer_pop_front (dest, size, b); \
      ASSERT (__ret);                                 \
    }                                                 \
  while (0)

// Peek
#define cbuffer_peek_back_expect(dest, size, b)       \
  do                                                  \
    {                                                 \
      bool __ret = cbuffer_peek_back (dest, size, b); \
      ASSERT (__ret);                                 \
    }                                                 \
  while (0)
#define cbuffer_peek_front_expect(dest, size, b)       \
  do                                                   \
    {                                                  \
      bool __ret = cbuffer_peek_front (dest, size, b); \
      ASSERT (__ret);                                  \
    }                                                  \
  while (0)

// Push Byte
#define cbuffer_pushb_back_expect(src, b)           \
  do                                                \
    {                                               \
      u8 _src = src;                                \
      bool __ret = cbuffer_push_back (&_src, 1, b); \
      ASSERT (__ret);                               \
    }                                               \
  while (0)
#define cbuffer_pushb_front_expect(src, b)           \
  do                                                 \
    {                                                \
      u8 _src = src;                                 \
      bool __ret = cbuffer_push_front (&_src, 1, b); \
      ASSERT (__ret);                                \
    }                                                \
  while (0)
