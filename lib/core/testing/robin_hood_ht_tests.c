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

#include "numstore/compile_config.h"
#include "test/testing.h"

#define VTYPE u32
#define KTYPE pgno
#define SUFFIX idx
#include "core/robin_hood_ht.h"
#undef VTYPE
#undef KTYPE
#undef SUFFIX

#ifndef NTEST
TEST (TT_UNIT, ht_insert_idx_regression_trigger_swap)
{
  // Although this used to work, it would skip over
  // the skipped value
  hash_table_idx ht;
  hentry_idx entries[MEMORY_PAGE_LEN];
  ht_init_idx (&ht, entries, MEMORY_PAGE_LEN);

  for (u32 i = 0; i < MEMORY_PAGE_LEN; ++i)
    {
      test_assert_int_equal (
          ht_insert_idx (&ht, (hdata_idx){ .key = i, .value = i }),
          HTIR_SUCCESS);
    }

  test_assert_int_equal (ht_delete_idx (&ht, NULL, 0), HTAR_SUCCESS);
  test_assert_int_equal (ht_delete_idx (&ht, NULL, 1), HTAR_SUCCESS);
  test_assert_int_equal (ht_delete_idx (&ht, NULL, 2), HTAR_SUCCESS);

  test_assert_int_equal (
      ht_insert_idx (&ht, (hdata_idx){ .key = MEMORY_PAGE_LEN, .value = 0 }),
      HTIR_SUCCESS);
  test_assert_int_equal (
      ht_insert_idx (&ht,
                     (hdata_idx){ .key = MEMORY_PAGE_LEN + 1, .value = 1 }),
      HTIR_SUCCESS);

  test_assert_int_equal (
      ht_insert_idx (&ht, (hdata_idx){ .key = 0, .value = 0 }), HTIR_SUCCESS);

  bool has_bug = false;

  if (has_bug)
    {
      test_assert_int_equal (ht.elems[0].data.key, MEMORY_PAGE_LEN);
      test_assert_int_equal (ht.elems[0].dib, 0);
      test_assert_int_equal (ht.elems[0].present, 1);

      test_assert_int_equal (ht.elems[1].data.key, 0);
      test_assert_int_equal (ht.elems[1].dib, 1);
      test_assert_int_equal (ht.elems[1].present, 1);

      test_assert_int_equal (ht.elems[3].data.key, MEMORY_PAGE_LEN + 1);
      test_assert_int_equal (ht.elems[3].dib, 1);
      test_assert_int_equal (ht.elems[3].present, 1);

      // Everything else is really unpredictable because the
      // +i just screws up everything
    }
  else
    {
      test_assert_int_equal (ht.elems[0].data.key, MEMORY_PAGE_LEN);
      test_assert_int_equal (ht.elems[0].dib, 0);
      test_assert_int_equal (ht.elems[0].present, 1);

      test_assert_int_equal (ht.elems[1].data.key, 0);
      test_assert_int_equal (ht.elems[1].dib, 1);
      test_assert_int_equal (ht.elems[1].present, 1);

      test_assert_int_equal (ht.elems[2].data.key, MEMORY_PAGE_LEN + 1);
      test_assert_int_equal (ht.elems[2].dib, 1);
      test_assert_int_equal (ht.elems[2].present, 1);

      for (u32 i = 3; i < MEMORY_PAGE_LEN; ++i)
        {
          test_assert_int_equal (ht.elems[i].data.key, i);
          test_assert_int_equal (ht.elems[i].dib, 0);
          test_assert_int_equal (ht.elems[i].present, 1);
        }
    }
}
#endif

#ifndef NTEST
TEST (TT_UNIT, robin_hood_ht)
{
  hash_table_idx ht;
  hentry_idx entries[MEMORY_PAGE_LEN];
  ht_init_idx (&ht, entries, MEMORY_PAGE_LEN);
  hdata_idx out;

  test_assert_int_equal (
      ht_insert_idx (&ht, (hdata_idx){ .key = 10u, .value = 5 }),
      HTIR_SUCCESS);

  TEST_CASE ("Duplicate insert")
  {
    test_assert_int_equal (
        ht_insert_idx (&ht, (hdata_idx){ .key = 10u, .value = 99 }),
        HTIR_EXISTS);
  }

  TEST_CASE ("Get normal")
  {
    test_assert_int_equal (ht_get_idx (&ht, &out, 10u), HTAR_SUCCESS);
    test_assert_int_equal (out.value, 5);
  }

  TEST_CASE ("Miss look up")
  {
    test_assert_int_equal (ht_get_idx (&ht, &out, 123u), HTAR_DOESNT_EXIST);
  }

  TEST_CASE ("Delete normal")
  {
    test_assert_int_equal (ht_delete_idx (&ht, NULL, 10u), HTAR_SUCCESS);
    test_assert_int_equal (ht_get_idx (&ht, &out, 10u), HTAR_DOESNT_EXIST);
  }

  TEST_CASE ("Delete doesn't exist")
  {
    test_assert_int_equal (ht_delete_idx (&ht, NULL, 10u), HTAR_DOESNT_EXIST);
  }

  TEST_CASE ("Linear probing")
  {
    test_assert_int_equal (
        ht_insert_idx (&ht, (hdata_idx){ .key = 10u, .value = 10 }),
        HTIR_SUCCESS);
    test_assert_int_equal (
        ht_insert_idx (&ht, (hdata_idx){ .key = 110u, .value = 11 }),
        HTIR_SUCCESS);
    test_assert_int_equal (
        ht_insert_idx (&ht, (hdata_idx){ .key = 210u, .value = 21 }),
        HTIR_SUCCESS);
  }

  TEST_CASE ("Delete and create a hole")
  {
    test_assert_int_equal (ht_delete_idx (&ht, NULL, 10u), HTAR_SUCCESS);
  }

  TEST_CASE ("Fetch the rest")
  {
    test_assert_int_equal (ht_get_idx (&ht, &out, 110u), HTAR_SUCCESS);
    test_assert_int_equal (out.value, 11);
    test_assert_int_equal (ht_get_idx (&ht, &out, 210u), HTAR_SUCCESS);
    test_assert_int_equal (out.value, 21);
  }

  TEST_CASE ("Optional dest")
  {
    test_assert_int_equal (ht_get_idx (&ht, NULL, 110u), HTAR_SUCCESS);
  }

  TEST_CASE ("Full table")
  {
    hash_table_idx tiny;
    hentry_idx _tiny[4];
    ht_init_idx (&tiny, _tiny, 4);

    for (u32 k = 0; k < 4; ++k)
      {
        test_assert_int_equal (
            ht_insert_idx (&tiny, (hdata_idx){ .key = k, .value = k }),
            HTIR_SUCCESS);
      }

    test_assert_int_equal (
        ht_insert_idx (&tiny, (hdata_idx){ .key = 99u, .value = 99 }),
        HTIR_FULL);
  }
}
#endif
