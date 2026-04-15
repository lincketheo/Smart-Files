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

#include "core/dbl_buffer.h"

#include "core/assert.h"
#include "numstore/errors.h"
#include "test/testing.h"

#include <string.h>

DEFINE_DBG_ASSERT (struct dbl_buffer, dbl_buffer, d, {
  ASSERT (d);
  ASSERT (d->data);
  ASSERT (d->nelem <= d->nelem_cap);
})

err_t
dblb_create (struct dbl_buffer *dest, const u32 size, const u32 initial_cap, error *e)
{
  ASSERT (initial_cap > 0);
  ASSERT (size > 0);

  void *data = i_malloc (initial_cap, size, e);
  if (data == NULL)
    {
      return error_trace (e);
    }

  *dest = (struct dbl_buffer){
    .size = size,
    .nelem = 0,
    .nelem_cap = initial_cap,
    .data = data,
  };

  DBG_ASSERT (dbl_buffer, dest);

  return SUCCESS;
}

err_t
dblb_append (struct dbl_buffer *d, const void *data, const u32 nelem, error *e)
{
  DBG_ASSERT (dbl_buffer, d);

  void *head = dblb_append_alloc (d, nelem, e);
  if (head == NULL)
    {
      return error_trace (e);
    }

  memcpy (head, data, d->size * nelem);

  return SUCCESS;
}

err_t
dblb_ensure_space (struct dbl_buffer *d, const u32 nelem, error *e)
{
  if (nelem >= d->nelem_cap)
    {
      void *newdata
          = i_realloc_right (d->data, d->nelem_cap, 2 * nelem, d->size, e);
      if (newdata == NULL)
        {
          return error_trace (e);
        }
      d->data = newdata;
      d->nelem_cap = 2 * nelem;
    }
  return error_trace (e);
}

void *
dblb_append_alloc (struct dbl_buffer *d, const u32 nelem, error *e)
{
  DBG_ASSERT (dbl_buffer, d);

  const u32 newnelem_cap = d->nelem + nelem;

  if (dblb_ensure_space (d, newnelem_cap, e))
    {
      return NULL;
    }

  void *ret = (u8 *)d->data + d->nelem * d->size;
  d->nelem += nelem;

  return ret;
}

void
dblb_free (struct dbl_buffer *d)
{
  DBG_ASSERT (dbl_buffer, d);

  i_free (d->data);
  d->nelem_cap = 0;
  d->nelem = 0;
}

#ifndef NTEST
TEST (TT_UNIT, dblb_create_basic)
{
  struct dbl_buffer db;
  error e = error_create ();

  // Create buffer with size=4, initial_cap=2
  const err_t err = dblb_create (&db, 4, 2, &e);
  test_assert_int_equal (err, SUCCESS);
  test_assert_int_equal (db.size, 4);
  test_assert_int_equal (db.nelem, 0);
  test_assert_int_equal (db.nelem_cap, 2);
  test_assert (db.data != NULL);

  dblb_free (&db);
}

TEST (TT_UNIT, dblb_append_single)
{
  struct dbl_buffer db;
  error e = error_create ();

  dblb_create (&db, sizeof (u32), 4, &e);

  // Append single element
  u32 val = 0x12345678;
  const err_t err = dblb_append (&db, &val, 1, &e);
  test_assert_int_equal (err, SUCCESS);
  test_assert_int_equal (db.nelem, 1);
  test_assert_int_equal (db.nelem_cap, 4);

  // Verify data
  const u32 *data = (u32 *)db.data;
  test_assert_int_equal (data[0], 0x12345678);

  dblb_free (&db);
}

TEST (TT_UNIT, dblb_append_multiple)
{
  struct dbl_buffer db;
  error e = error_create ();

  dblb_create (&db, sizeof (u32), 2, &e);

  // Append multiple elements
  u32 vals[] = { 0x11, 0x22, 0x33 };
  const err_t err = dblb_append (&db, vals, 3, &e);
  test_assert_int_equal (err, SUCCESS);
  test_assert_int_equal (db.nelem, 3);

  // Verify data
  const u32 *data = (u32 *)db.data;
  test_assert_int_equal (data[0], 0x11);
  test_assert_int_equal (data[1], 0x22);
  test_assert_int_equal (data[2], 0x33);

  dblb_free (&db);
}

TEST (TT_UNIT, dblb_append_triggers_realloc)
{
  struct dbl_buffer db;
  error e = error_create ();

  // Start with capacity of 2
  dblb_create (&db, sizeof (u32), 2, &e);

  u32 val1 = 0xAA;
  dblb_append (&db, &val1, 1, &e);
  test_assert_int_equal (db.nelem, 1);
  test_assert_int_equal (db.nelem_cap, 2);

  u32 val2 = 0xBB;
  dblb_append (&db, &val2, 1, &e);
  test_assert_int_equal (db.nelem, 2);
  test_assert_int_equal (db.nelem_cap, 4);

  u32 val3 = 0xCC;
  dblb_append (&db, &val3, 1, &e);
  test_assert_int_equal (db.nelem, 3);
  test_assert_int_equal (db.nelem_cap, 4);

  u32 val4 = 0xDD;
  dblb_append (&db, &val4, 1, &e);
  test_assert_int_equal (db.nelem, 4);
  test_assert_int_equal (db.nelem_cap, 8);

  const u32 *data = (u32 *)db.data;
  test_assert_int_equal (data[0], 0xAA);
  test_assert_int_equal (data[1], 0xBB);
  test_assert_int_equal (data[2], 0xCC);
  test_assert_int_equal (data[3], 0xDD);

  dblb_free (&db);
}

TEST (TT_UNIT, dblb_append_alloc_basic)
{
  struct dbl_buffer db;
  error e = error_create ();

  dblb_create (&db, sizeof (u32), 4, &e);

  // Allocate space for 2 elements
  void *ptr = dblb_append_alloc (&db, 2, &e);
  test_assert (ptr != NULL);
  test_assert_int_equal (db.nelem, 2);

  // Write directly to allocated space
  u32 *data = (u32 *)ptr;
  data[0] = 0x1111;
  data[1] = 0x2222;

  // Verify
  const u32 *base = (u32 *)db.data;
  test_assert_int_equal (base[0], 0x1111);
  test_assert_int_equal (base[1], 0x2222);

  dblb_free (&db);
}

TEST (TT_UNIT, dblb_append_alloc_sequential)
{
  struct dbl_buffer db;
  error e = error_create ();

  dblb_create (&db, sizeof (u32), 8, &e);

  // First allocation
  u32 *ptr1 = (u32 *)dblb_append_alloc (&db, 2, &e);
  test_assert (ptr1 != NULL);
  ptr1[0] = 0xAA;
  ptr1[1] = 0xBB;

  // Second allocation - should be right after first
  u32 *ptr2 = (u32 *)dblb_append_alloc (&db, 2, &e);
  test_assert (ptr2 != NULL);
  test_assert_int_equal ((u8 *)ptr2 - (u8 *)ptr1, 2 * sizeof (u32));
  ptr2[0] = 0xCC;
  ptr2[1] = 0xDD;

  // Verify all data
  const u32 *base = (u32 *)db.data;
  test_assert_int_equal (base[0], 0xAA);
  test_assert_int_equal (base[1], 0xBB);
  test_assert_int_equal (base[2], 0xCC);
  test_assert_int_equal (base[3], 0xDD);
  test_assert_int_equal (db.nelem, 4);

  dblb_free (&db);
}

TEST (TT_UNIT, dblb_append_alloc_triggers_realloc)
{
  struct dbl_buffer db;
  error e = error_create ();

  dblb_create (&db, sizeof (u32), 2, &e);

  // Fill to capacity
  dblb_append_alloc (&db, 2, &e);
  test_assert_int_equal (db.nelem_cap, 4);

  // This should trigger realloc
  const void *ptr = dblb_append_alloc (&db, 1, &e);
  test_assert (ptr != NULL);
  test_assert_int_equal (db.nelem, 3);
  test_assert_int_equal (db.nelem_cap, 4);

  dblb_free (&db);
}

TEST (TT_UNIT, dblb_different_element_sizes)
{
  struct dbl_buffer db;
  error e = error_create ();

  // Test with u8
  dblb_create (&db, sizeof (u8), 4, &e);
  u8 byte = 0x42;
  dblb_append (&db, &byte, 1, &e);
  test_assert_int_equal (((u8 *)db.data)[0], 0x42);
  dblb_free (&db);

  // Test with u64
  dblb_create (&db, sizeof (u64), 4, &e);
  u64 large = 0x123456789ABCDEF0;
  dblb_append (&db, &large, 1, &e);
  test_assert_equal (((u64 *)db.data)[0], 0x123456789ABCDEF0);
  dblb_free (&db);
}

TEST (TT_UNIT, dblb_struct_elements)
{
  struct dbl_buffer db;
  error e = error_create ();

  struct test_struct
  {
    u32 id;
    u32 value;
  };

  dblb_create (&db, sizeof (struct test_struct), 2, &e);

  struct test_struct s1 = { .id = 1, .value = 100 };
  struct test_struct s2 = { .id = 2, .value = 200 };

  dblb_append (&db, &s1, 1, &e);
  dblb_append (&db, &s2, 1, &e);

  const struct test_struct *data = (struct test_struct *)db.data;
  test_assert_int_equal (data[0].id, 1);
  test_assert_int_equal (data[0].value, 100);
  test_assert_int_equal (data[1].id, 2);
  test_assert_int_equal (data[1].value, 200);

  dblb_free (&db);
}

TEST (TT_UNIT, dblb_free_resets)
{
  struct dbl_buffer db;
  error e = error_create ();

  dblb_create (&db, sizeof (u32), 4, &e);
  u32 val = 0x1234;
  dblb_append (&db, &val, 1, &e);

  dblb_free (&db);

  // Verify fields are reset
  test_assert_int_equal (db.nelem, 0);
  test_assert_int_equal (db.nelem_cap, 0);
}

TEST (TT_UNIT, dblb_large_append)
{
  struct dbl_buffer db;
  error e = error_create ();

  dblb_create (&db, sizeof (u32), 2, &e);

  // Append many elements at once
  u32 vals[100];
  for (u32 i = 0; i < 100; i++)
    {
      vals[i] = i;
    }

  dblb_append (&db, vals, 100, &e);
  test_assert_int_equal (db.nelem, 100);

  // Verify data
  const u32 *data = (u32 *)db.data;
  for (u32 i = 0; i < 100; i++)
    {
      test_assert_int_equal (data[i], i);
    }

  dblb_free (&db);
}
#endif
