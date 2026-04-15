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

#include "paging/dpgt/dirty_page_table.h"
#include "test/testing.h"

#ifndef NTEST
TEST (TT_UNIT, dpgt_open)
{
  TEST_CASE ("basic")
  {
    error e = error_create ();
    struct dpg_table *t = dpgt_open (&e);
    dpgt_close (t);
  }

  TEST_CASE ("open multiple")
  {
    error e = error_create ();
    for (int i = 0; i < 4; ++i)
      {
        struct dpg_table *t = dpgt_open (&e);
        dpgt_close (t);
      }
  }
}

TEST (TT_UNIT, dpgt_merge_into)
{
  TEST_CASE ("empty to empty")
  {
    error e = error_create ();
    struct dpg_table *src = dpgt_open (&e);
    struct dpg_table *dest = dpgt_open (&e);
    const err_t result = dpgt_merge_into (dest, src, &e);
    test_assert (result == SUCCESS);

    dpgt_close (dest);
    dpgt_close (src);
  }

  TEST_CASE ("data")
  {
    error e = error_create ();
    struct dpg_table *dest = dpgt_open (&e);
    struct dpg_table *src = dpgt_open (&e);
    // Add to dest (pages 1-5)
    for (pgno pg = 1; pg <= 5; pg++)
      {
        dpgt_add (dest, pg, pg * 10, &e);
      }

    // Add to src (pages 6-10)
    for (pgno pg = 6; pg <= 10; pg++)
      {
        dpgt_add (src, pg, pg * 10, &e);
      }

    const err_t result = dpgt_merge_into (dest, src, &e);
    test_assert (result == SUCCESS);

    // Verify all pages exist in dest
    for (pgno pg = 1; pg <= 10; pg++)
      {
        test_assert (dpgt_exists (dest, pg));
      }

    dpgt_close (dest);
    dpgt_close (src);
  }

  TEST_CASE ("dest gets new rec_lsn on collision")
  {
    error e = error_create ();
    struct dpg_table *dest = dpgt_open (&e);
    struct dpg_table *src = dpgt_open (&e);
    // Same page in both with different rec_lsn
    dpgt_add (dest, 42, 100, &e);
    dpgt_add (src, 42, 200, &e);

    dpgt_merge_into (dest, src, &e);

    lsn rec_lsn;
    bool found = dpgt_get (&rec_lsn, dest, 42);
    test_assert (found);
    test_assert_int_equal (rec_lsn, 200);

    dpgt_close (dest);
    dpgt_close (src);
  }
}

TEST (TT_UNIT, dpgt_min_rec_lsn)
{
  TEST_CASE ("single entry")
  {
    error e = error_create ();
    struct dpg_table *t = dpgt_open (&e);
    dpgt_add (t, 1, 50, &e);

    const lsn min = dpgt_min_rec_lsn (t);
    test_assert_int_equal (min, 50);

    dpgt_close (t);
  }

  TEST_CASE ("multiple entries")
  {
    error e = error_create ();
    struct dpg_table *t = dpgt_open (&e);
    dpgt_add (t, 1, 100, &e);
    dpgt_add (t, 2, 25, &e);
    dpgt_add (t, 3, 75, &e);
    dpgt_add (t, 4, 50, &e);

    const lsn min = dpgt_min_rec_lsn (t);
    test_assert (min == 25);

    dpgt_close (t);
  }
}

TEST (TT_UNIT, dpgt_exists)
{
  TEST_CASE ("nonexistent returns false")
  {
    error e = error_create ();
    struct dpg_table *t = dpgt_open (&e);
    test_assert (!dpgt_exists (t, 9999));

    dpgt_close (t);
  }

  TEST_CASE ("exists after add")
  {
    error e = error_create ();
    struct dpg_table *t = dpgt_open (&e);
    test_assert (!dpgt_exists (t, 1100));
    dpgt_add (t, 1100, 500, &e);
    test_assert (dpgt_exists (t, 1100));

    dpgt_close (t);
  }
}

TEST (TT_UNIT, dpgt_add)
{
  TEST_CASE ("new entry")
  {
    error e = error_create ();
    struct dpg_table *t = dpgt_open (&e);
    dpgt_add (t, 900, 100, &e);

    lsn rec_lsn;
    bool found = dpgt_get (&rec_lsn, t, 900);
    test_assert (found);
    test_assert_int_equal (rec_lsn, 100);

    dpgt_close (t);
  }

  TEST_CASE ("multiple entries different pages")
  {
    error e = error_create ();
    struct dpg_table *t = dpgt_open (&e);
    for (pgno pg = 1; pg <= 5; pg++)
      {
        dpgt_add (t, pg, pg * 10, &e);
      }

    for (pgno pg = 1; pg <= 5; pg++)
      {
        lsn rec_lsn;
        bool found = dpgt_get (&rec_lsn, t, pg);
        test_assert (found);
        test_assert_int_equal (rec_lsn, pg * 10);
      }

    dpgt_close (t);
  }
}

TEST (TT_UNIT, dpgt_get)
{
  TEST_CASE ("nonexistent returns false")
  {
    error e = error_create ();
    struct dpg_table *t = dpgt_open (&e);
    lsn rec_lsn;
    bool found = dpgt_get (&rec_lsn, t, 9999);
    test_assert (!found);

    dpgt_close (t);
  }

  TEST_CASE ("get rec_lsn")
  {
    error e = error_create ();
    struct dpg_table *t = dpgt_open (&e);
    dpgt_add (t, 100, 50, &e);

    lsn rec_lsn;
    bool found = dpgt_get (&rec_lsn, t, 100);
    test_assert (found);
    test_assert_int_equal (rec_lsn, 50);

    dpgt_close (t);
  }

  TEST_CASE ("update rec_lsn")
  {
    error e = error_create ();
    struct dpg_table *t = dpgt_open (&e);
    dpgt_add (t, 600, 100, &e);

    dpgt_update (t, 600, 200);

    lsn rec_lsn;
    bool found = dpgt_get (&rec_lsn, t, 600);
    test_assert (found);
    test_assert_int_equal (rec_lsn, 200);

    dpgt_close (t);
  }

  TEST_CASE ("multiple pages independent")
  {
    error e = error_create ();
    struct dpg_table *t = dpgt_open (&e);
    dpgt_add (t, 1, 10, &e);
    dpgt_add (t, 2, 20, &e);
    dpgt_add (t, 3, 300, &e);

    lsn rec_lsn;

    dpgt_get (&rec_lsn, t, 1);
    test_assert_int_equal (rec_lsn, 10);

    dpgt_get (&rec_lsn, t, 2);
    test_assert_int_equal (rec_lsn, 20);

    dpgt_get (&rec_lsn, t, 3);
    test_assert_int_equal (rec_lsn, 300);

    dpgt_close (t);
  }
}

TEST (TT_UNIT, dpgt_remove)
{
  TEST_CASE ("remove existing")
  {
    error e = error_create ();
    struct dpg_table *t = dpgt_open (&e);
    dpgt_add (t, 400, 100, &e);

    bool removed;
    dpgt_remove (&removed, t, 400);
    test_assert (removed);

    lsn rec_lsn;
    bool found = dpgt_get (&rec_lsn, t, 400);
    test_assert (!found);

    dpgt_close (t);
  }

  TEST_CASE ("remove nonexistent")
  {
    error e = error_create ();
    struct dpg_table *t = dpgt_open (&e);
    bool removed;
    dpgt_remove (&removed, t, 500);
    test_assert (!removed);

    dpgt_close (t);
  }

  TEST_CASE ("double remove")
  {
    error e = error_create ();
    struct dpg_table *t = dpgt_open (&e);
    dpgt_add (t, 100, 50, &e);

    bool removed;
    dpgt_remove (&removed, t, 100);
    test_assert (removed);

    dpgt_remove (&removed, t, 100);
    test_assert (!removed);

    dpgt_close (t);
  }

  TEST_CASE ("get fails after remove")
  {
    error e = error_create ();
    struct dpg_table *t = dpgt_open (&e);
    dpgt_add (t, 200, 50, &e);

    bool removed;
    dpgt_remove (&removed, t, 200);
    test_assert (removed);

    lsn rec_lsn;
    bool found = dpgt_get (&rec_lsn, t, 200);
    test_assert (!found);

    dpgt_close (t);
  }
}

TEST (TT_UNIT, dpgt_serialize)
{
  TEST_CASE ("serialize deserialize empty")
  {
    error e = error_create ();
    struct dpg_table *t = dpgt_open (&e);
    u8 buffer[4096];
    const u32 size = dpgt_serialize (buffer, sizeof (buffer), t);
    test_assert (size == 0);

    struct dpg_table *t2 = dpgt_deserialize (buffer, size, &e);
    test_assert (dpgt_equal (t, t2));

    dpgt_close (t);
    dpgt_close (t2);
  }

  TEST_CASE ("serialize deserialize with data")
  {
    error e = error_create ();
    struct dpg_table *t = dpgt_open (&e);
    for (pgno pg = 1; pg <= 10; pg++)
      {
        dpgt_add (t, pg, pg * 100, &e);
      }

    u8 buffer[4096];
    const u32 size = dpgt_serialize (buffer, sizeof (buffer), t);
    test_assert (size > 0);

    struct dpg_table *t2 = dpgt_deserialize (buffer, size, &e);
    for (pgno pg = 1; pg <= 10; pg++)
      {
        lsn rec_lsn;
        bool found = dpgt_get (&rec_lsn, t2, pg);
        test_assert (found);
        test_assert_int_equal (rec_lsn, pg * 100);
      }

    dpgt_close (t);
    dpgt_close (t2);
  }

  TEST_CASE ("round trip preserves size")
  {
    error e = error_create ();
    struct dpg_table *t = dpgt_open (&e);
    for (pgno pg = 0; pg < 50; pg++)
      {
        dpgt_add (t, pg, pg * 7, &e);
      }

    u8 buffer[8192];
    const u32 size = dpgt_serialize (buffer, sizeof (buffer), t);

    struct dpg_table *t2 = dpgt_deserialize (buffer, size, &e);
    test_assert_int_equal (dpgt_get_size (t), dpgt_get_size (t2));
    test_assert (dpgt_equal (t, t2));

    dpgt_close (t);
    dpgt_close (t2);
  }
}

TEST (TT_UNIT, dpgt_equal)
{
  TEST_CASE ("empty tables")
  {
    error e = error_create ();
    struct dpg_table *t1 = dpgt_open (&e);
    struct dpg_table *t2 = dpgt_open (&e);
    test_assert (dpgt_equal (t1, t2));

    dpgt_close (t1);
    dpgt_close (t2);
  }

  TEST_CASE ("same content")
  {
    error e = error_create ();
    struct dpg_table *t1 = dpgt_open (&e);
    struct dpg_table *t2 = dpgt_open (&e);
    for (pgno pg = 1; pg <= 5; pg++)
      {
        dpgt_add (t1, pg, pg * 10, &e);
        dpgt_add (t2, pg, pg * 10, &e);
      }

    test_assert (dpgt_equal (t1, t2));

    dpgt_close (t1);
    dpgt_close (t2);
  }

  TEST_CASE ("different rec_lsn")
  {
    error e = error_create ();
    struct dpg_table *t1 = dpgt_open (&e);
    struct dpg_table *t2 = dpgt_open (&e);
    dpgt_add (t1, 1, 10, &e);
    dpgt_add (t2, 1, 20, &e);

    test_assert (!dpgt_equal (t1, t2));

    dpgt_close (t1);
    dpgt_close (t2);
  }

  TEST_CASE ("different sizes")
  {
    error e = error_create ();
    struct dpg_table *t1 = dpgt_open (&e);
    struct dpg_table *t2 = dpgt_open (&e);
    dpgt_add (t1, 1, 10, &e);
    dpgt_add (t1, 2, 20, &e);
    dpgt_add (t2, 1, 10, &e);

    test_assert (!dpgt_equal (t1, t2));

    dpgt_close (t1);
    dpgt_close (t2);
  }

  TEST_CASE ("different pages same rec_lsn")
  {
    error e = error_create ();
    struct dpg_table *t1 = dpgt_open (&e);
    struct dpg_table *t2 = dpgt_open (&e);
    dpgt_add (t1, 1, 10, &e);
    dpgt_add (t2, 2, 10, &e);

    test_assert (!dpgt_equal (t1, t2));

    dpgt_close (t1);
    dpgt_close (t2);
  }
}
#endif
