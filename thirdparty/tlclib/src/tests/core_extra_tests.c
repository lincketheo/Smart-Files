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

#include "tlclib/core/checksums.h"
#include "tlclib/core/macros.h"
#include "tlclib/core/math.h"
#include "tlclib/core/numbers.h"
#include "tlclib/dev/error.h"
#include "tlclib/dev/stdtypes.h"
#include "tlclib/dev/testing.h"
#include "tlclib/ds/cbuffer.h"
#include "tlclib/ds/ext_array.h"
#include "tlclib/ds/llist.h"
#include "tlclib/ds/stride.h"
#include "tlclib/ds/string.h"
#include "tlclib/memory/serializer.h"

#include <math.h>
#include <string.h>

#ifndef NTEST

// Parameterised table for paths the current code handles CORRECTLY:
// zero, negative-zero, normals, infinity, and NaN.
static const struct
{
  const char *name;
  u16 h16;
  u32 expected_f32_bits;
} f16_f32_cases[] = {
  // Positive zero: sign=0, exp=0, mant=0
  { "positive zero", 0x0000u, 0x00000000u },
  // Negative zero: sign=1, exp=0, mant=0
  { "negative zero", 0x8000u, 0x80000000u },
  // 1.0:  sign=0, exp=15, mant=0  → f32 exp = 15+112 = 127
  { "1.0", 0x3C00u, 0x3F800000u },
  // -1.0: sign=1, exp=15, mant=0
  { "-1.0", 0xBC00u, 0xBF800000u },
  // 2.0:  sign=0, exp=16, mant=0  → f32 exp = 128
  { "2.0", 0x4000u, 0x40000000u },
  // +infinity: sign=0, exp=31, mant=0
  { "+infinity", 0x7C00u, 0x7F800000u },
  // -infinity: sign=1, exp=31, mant=0
  { "-infinity", 0xFC00u, 0xFF800000u },
  // Largest normal F16 (65504): exp=30, mant=0x3FF
  //   f32 exp = 30+112 = 142,  mant_f32 = 0x3FF<<13 = 0x7FE000
  { "largest normal (65504)", 0x7BFFu, 0x477FE000u },
  // Smallest normal F16 (2^-14): exp=1, mant=0
  //   f32 exp = 1+112 = 113
  { "smallest normal (2^-14)", 0x0400u, 0x38800000u },
};

TEST (f16_to_f32_normals_and_specials)
{
  for (u32 i = 0; i < arrlen (f16_f32_cases); ++i)
    {
      TEST_CASE ("%s", f16_f32_cases[i].name)
      {
        float result = f16_to_f32 (f16_f32_cases[i].h16);
        u32 bits;
        memcpy (&bits, &result, sizeof bits);
        test_assert_type_equal (bits, f16_f32_cases[i].expected_f32_bits, u32,
                                PRIu32);
      }
    }
}

TEST (f16_to_f32_nan_is_nan)
{
  // sign=0, exp=31, mant=0x200 — arbitrary non-zero mantissa → NaN
  const float result = f16_to_f32 (0x7E00u);
  test_assert (isnan (result));
}

TEST (f16_to_f32_smallest_subnormal_correct_value)
{
  const float result = f16_to_f32 (0x0001u);
  const u32 expected_bits = 0x33800000u;
  u32 result_bits;
  memcpy (&result_bits, &result, sizeof result_bits);
  test_assert_type_equal (result_bits, expected_bits, u32, PRIu32);
}

static const struct
{
  const char *name;
  const char *input;
  u32 ilen;
  i32 expected_val;
  err_t expected_ret;
} parse_i32_cases[] = {
  // Trivial cases
  { "zero", "0", 1, 0, SUCCESS },
  { "negative zero", "-0", 2, 0, SUCCESS },
  { "explicit plus sign", "+42", 3, 42, SUCCESS },
  // Boundaries
  // I32_MAX = 2147483647
  { "I32_MAX", "2147483647", 10, I32_MAX, SUCCESS },
  // I32_MIN = -2147483648
  { "I32_MIN", "-2147483648", 11, I32_MIN, SUCCESS },
  // Overflow: one past I32_MAX
  { "I32_MAX+1 overflows", "2147483648", 10, 0, ERR_ARITH },
  // Overflow: one past I32_MIN
  { "I32_MIN-1 overflows", "-2147483649", 11, 0, ERR_ARITH },
};

TEST (parse_i32_boundary_values)
{
  error e = error_create ();
  i32 out = 0;

  for (u32 i = 0; i < arrlen (parse_i32_cases); ++i)
    {
      TEST_CASE ("%s", parse_i32_cases[i].name)
      {
        const err_t ret = parse_i32_expect (&out, parse_i32_cases[i].input,
                                            parse_i32_cases[i].ilen, &e);
        test_assert_int_equal (ret, parse_i32_cases[i].expected_ret);
        if (ret == SUCCESS)
          {
            test_assert_type_equal (out, parse_i32_cases[i].expected_val, i32,
                                    PRId32);
          }
        else
          {
            e.cause_code = SUCCESS; // reset for next iteration
          }
      }
    }
}

static const struct
{
  const char *name;
  const char *input;
  u32 ilen;
  i64 expected_val;
  err_t expected_ret;
} parse_i64_cases[] = {
  { "zero", "0", 1, 0, SUCCESS },
  { "negative zero", "-0", 2, 0, SUCCESS },
  { "small positive", "99", 2, 99, SUCCESS },
  // I64_MAX = 9223372036854775807
  { "I64_MAX", "9223372036854775807", 19, I64_MAX, SUCCESS },
  // I64_MIN = -9223372036854775808
  { "I64_MIN", "-9223372036854775808", 20, I64_MIN, SUCCESS },
  // Overflow one past I64_MAX
  { "I64_MAX+1 overflows", "9223372036854775808", 19, 0, ERR_ARITH },
  // Overflow one past I64_MIN
  { "I64_MIN-1 overflows", "-9223372036854775809", 20, 0, ERR_ARITH },
};

TEST (parse_i64_boundary_values)
{
  error e = error_create ();
  i64 out = 0;

  for (u32 i = 0; i < arrlen (parse_i64_cases); ++i)
    {
      TEST_CASE ("%s", parse_i64_cases[i].name)
      {
        const err_t ret = parse_i64_expect (&out, parse_i64_cases[i].input,
                                            parse_i64_cases[i].ilen, &e);
        test_assert_int_equal (ret, parse_i64_cases[i].expected_ret);
        if (ret == SUCCESS)
          {
            test_assert_type_equal (out, parse_i64_cases[i].expected_val, i64,
                                    PRId64);
          }
        else
          {
            e.cause_code = SUCCESS;
          }
      }
    }
}

// Verify that inserting past the current cap triggers a realloc and that
// capacity at least doubles (catching regressions to the doubling formula).
TEST (ext_array_capacity_doubles_on_growth)
{
  error e = error_create ();
  struct ext_array a = ext_array_create ();

  // Seed with one byte to force the first allocation (cap → 2).
  const u8 seed = 0x01;
  ext_array_insert (&a, 0, &seed, 1, &e);
  test_assert_int_equal (e.cause_code, SUCCESS);
  test_assert (a.cap >= 2u);

  // Fill to capacity.
  const u32 orig_cap = a.cap;
  for (u32 i = a.len; i < orig_cap; ++i)
    {
      u8 b = (u8)(i & 0xFF);
      ext_array_insert (&a, a.len, &b, 1, &e);
      test_assert_int_equal (e.cause_code, SUCCESS);
    }
  test_assert_type_equal (a.len, orig_cap, u32, PRIu32);

  // One more byte must trigger realloc; new cap must be >= 2×old.
  const u8 extra = 0xFF;
  ext_array_insert (&a, a.len, &extra, 1, &e);
  test_assert_int_equal (e.cause_code, SUCCESS);
  test_assert (a.cap >= orig_cap * 2u);

  ext_array_free (&a);
}

// Removing every element via stride must leave an empty array.
TEST (ext_array_remove_all_produces_empty)
{
  error e = error_create ();
  struct ext_array a = ext_array_create ();

  const u8 src[5] = { 10, 20, 30, 40, 50 };
  ext_array_insert (&a, 0, src, sizeof src, &e);
  test_assert_int_equal (e.cause_code, SUCCESS);

  u8 out[5] = { 0 };
  const i64 n = ext_array_remove (
      &a, (struct stride){ .start = 0, .stride = 1, .nelems = 5 }, 1, out, &e);

  test_assert_type_equal (n, (i64)5, i64, PRId64);
  test_assert_type_equal (ext_array_get_len (&a), (u64)0, u64, PRIu64);
  test_assert_memequal (src, out, 5);

  ext_array_free (&a);
}

struct llt_node
{
  int value;
  struct llnode link;
};

static bool
llt_eq (const struct llnode *a, const struct llnode *b)
{
  return container_of (a, struct llt_node, link)->value == container_of (b, struct llt_node, link)->value;
}

// list_append must produce FIFO order (oldest-first on pop).
TEST (llist_append_maintaififo_order)
{
  struct llt_node nodes[5];
  struct llnode *head = NULL;

  for (int i = 0; i < 5; ++i)
    {
      nodes[i].value = i;
      llnode_init (&nodes[i].link);
      list_append (&head, &nodes[i].link);
    }

  test_assert_int_equal ((int)list_length (head), 5);

  for (int i = 0; i < 5; ++i)
    {
      struct llnode *n = list_pop (&head);
      test_assert (n != NULL);
      test_assert_int_equal (container_of (n, struct llt_node, link)->value, i);
    }
  test_assert_equal (head, NULL);
}

// list_find must return the correct node and its 0-based index.
TEST (llist_find_returnode_and_index)
{
  struct llt_node nodes[3];
  struct llnode *head = NULL;

  for (int i = 0; i < 3; ++i)
    {
      nodes[i].value = i * 10; // 0, 10, 20
      llnode_init (&nodes[i].link);
      list_append (&head, &nodes[i].link);
    }

  TEST_CASE ("find middle element (value 10, index 1)")
  {
    struct llt_node key = { .value = 10 };
    llnode_init (&key.link);
    u32 idx = 0;
    const struct llnode *found = list_find (&idx, head, &key.link, llt_eq);
    test_assert (found != NULL);
    test_assert_int_equal ((int)idx, 1);
    test_assert_equal (found, &nodes[1].link);
  }

  TEST_CASE ("find absent element returns NULL")
  {
    struct llt_node key = { .value = 99 };
    llnode_init (&key.link);
    u32 idx = 0;
    const struct llnode *found = list_find (&idx, head, &key.link, llt_eq);
    test_assert_equal (found, NULL);
  }
}

// list_remove must correctly excise nodes from head, middle, and tail.
TEST (llist_remove_from_head_middle_tail)
{
  struct llt_node nodes[4];
  struct llnode *head = NULL;

  for (int i = 0; i < 4; ++i)
    {
      nodes[i].value = i;
      llnode_init (&nodes[i].link);
      list_append (&head, &nodes[i].link);
    }
  // list: 0 -> 1 -> 2 -> 3

  TEST_CASE ("remove middle (value=1)")
  {
    list_remove (&head, &nodes[1].link);
    test_assert_int_equal ((int)list_length (head), 3);
    test_assert_equal (nodes[1].link.next, NULL);
  }

  TEST_CASE ("remove head (value=0)")
  {
    list_remove (&head, &nodes[0].link);
    test_assert_int_equal ((int)list_length (head), 2);
    test_assert_equal (head, &nodes[2].link); // new head
  }

  TEST_CASE ("remove tail (value=3)")
  {
    list_remove (&head, &nodes[3].link);
    test_assert_int_equal ((int)list_length (head), 1);
    test_assert_equal (head, &nodes[2].link);
    test_assert_equal (nodes[2].link.next, NULL);
  }
}

// Removing a node that was never inserted must leave the list unchanged.
TEST (llist_remove_absent_node_is_noop)
{
  struct llt_node a = { .value = 1 };
  struct llt_node b = { .value = 2 };
  struct llt_node outsider = { .value = 99 };
  llnode_init (&a.link);
  llnode_init (&b.link);
  llnode_init (&outsider.link);

  struct llnode *head = NULL;
  list_append (&head, &a.link);
  list_append (&head, &b.link);

  list_remove (&head, &outsider.link);

  test_assert_int_equal ((int)list_length (head), 2);
  test_assert_equal (head, &a.link);
}

// Well-known CRC32C (Castagnoli) check value for the ASCII string "123456789"
// is 0xE3069283. A wrong polynomial or table would produce a different value,
// but would still pass the existing "non-zero" and "deterministic" tests.
TEST (checksum_known_crc32c_vector)
{
  const u8 data[] = { '1', '2', '3', '4', '5', '6', '7', '8', '9' };
  u32 state = checksum_init ();
  checksum_execute (&state, data, sizeof data);
  test_assert_type_equal (state, 0xE3069283u, u32, PRIu32);
}

// Two distinct single-byte inputs must produce different checksums.
TEST (checksum_distinct_bytes_differ)
{
  const u8 a[] = { 0x00 };
  const u8 b[] = { 0x01 };
  u32 sa = checksum_init ();
  u32 sb = checksum_init ();
  checksum_execute (&sa, a, 1);
  checksum_execute (&sb, b, 1);
  test_assert (sa != sb);
}

// Writing exactly dcap bytes must succeed; one additional byte must return
// false.
TEST (serializer_write_at_capacity_then_overflow)
{
  u8 buf[8];
  struct serializer s = srlizr_create (buf, sizeof buf);

  const u8 full[8] = { 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88 };
  test_assert (srlizr_write (&s, full, sizeof full));
  test_assert_int_equal (s.dlen, 8);

  // One more byte must be rejected.
  const u8 extra = 0xFF;
  test_assert_int_equal (srlizr_write (&s, &extra, 1), false);

  // Buffer contents and length must be unchanged after the failed write.
  test_assert_int_equal (s.dlen, 8);
  test_assert_memequal (buf, full, 8);
}

// Multiple partial writes that together exceed dcap: last write must fail.
TEST (serializer_incremental_write_overflow)
{
  u8 buf[6];
  struct serializer s = srlizr_create (buf, sizeof buf);

  const u8 part_a[3] = { 0xAA, 0xBB, 0xCC };
  const u8 part_b[3] = { 0xDD, 0xEE, 0xFF };

  test_assert (srlizr_write (&s, part_a, 3));
  test_assert (srlizr_write (&s, part_b, 3));

  // Capacity exhausted — any further write must return false.
  const u8 c = 0x00;
  test_assert_int_equal (srlizr_write (&s, &c, 1), false);

  const u8 expected[6] = { 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF };
  test_assert_memequal (buf, expected, 6);
}

TEST (stride_constructors_resolve_correctly)
{
  struct stride r;
  error e = error_create ();
  const u64 N = 10;

  TEST_CASE ("ustride() = [::] — all elements")
  {
    stride_resolve (&r, ustride (), N, &e);
    test_assert_int_equal (r.start, 0);
    test_assert_int_equal (r.stride, 1);
    test_assert_int_equal (r.nelems, 10);
  }

  TEST_CASE ("ustride0(3) = [3:] — from index 3 to end")
  {
    stride_resolve (&r, ustride0 (3), N, &e);
    test_assert_int_equal (r.start, 3);
    test_assert_int_equal (r.stride, 1);
    test_assert_int_equal (r.nelems, 7);
  }

  TEST_CASE ("ustride2(6) = [:6] — first 6 elements")
  {
    stride_resolve (&r, ustride2 (6), N, &e);
    test_assert_int_equal (r.start, 0);
    test_assert_int_equal (r.stride, 1);
    test_assert_int_equal (r.nelems, 6);
  }

  TEST_CASE ("ustride1(3) = [::3] — every 3rd, indices 0,3,6,9")
  {
    stride_resolve (&r, ustride1 (3), N, &e);
    test_assert_int_equal (r.start, 0);
    test_assert_int_equal (r.stride, 3);
    test_assert_int_equal (r.nelems, 4);
  }

  TEST_CASE ("ustride01(2,3) = [2::3] — from 2, step 3, indices 2,5,8")
  {
    stride_resolve (&r, ustride01 (2, 3), N, &e);
    test_assert_int_equal (r.start, 2);
    test_assert_int_equal (r.stride, 3);
    test_assert_int_equal (r.nelems, 3);
  }

  TEST_CASE ("ustride12(2,8) = [::2] stop 8 — indices 0,2,4,6")
  {
    stride_resolve (&r, ustride12 (2, 8), N, &e);
    test_assert_int_equal (r.start, 0);
    test_assert_int_equal (r.stride, 2);
    test_assert_int_equal (r.nelems, 4);
  }

  TEST_CASE ("ustride012(1,2,7) = [1:7:2] — indices 1,3,5")
  {
    stride_resolve (&r, ustride012 (1, 2, 7), N, &e);
    test_assert_int_equal (r.start, 1);
    test_assert_int_equal (r.stride, 2);
    test_assert_int_equal (r.nelems, 3);
  }

  TEST_CASE ("usfrms round-trip: stride → user_stride → stride is identity")
  {
    // Build a concrete stride and convert it back; resolution must agree.
    const struct stride orig = { .start = 2, .stride = 3, .nelems = 3 };
    const struct user_stride us = usfrms (orig);
    struct stride got;
    stride_resolve (&got, us, N, &e);
    test_assert_type_equal (got.start, orig.start, u64, PRIu64);
    test_assert_type_equal (got.stride, orig.stride, u32, PRIu32);
    test_assert_type_equal (got.nelems, orig.nelems, u64, PRIu64);
  }
}

TEST (string_ordering_operators)
{
  const struct string abc = { .data = "abc", .len = 3 };
  const struct string abd = { .data = "abd", .len = 3 };
  const struct string ab = { .data = "ab", .len = 2 };
  const struct string xyz1 = { .data = "xyz", .len = 3 };
  const struct string xyz2 = { .data = "xyz", .len = 3 };

  TEST_CASE ("'abc' < 'abd': less, not greater")
  {
    test_assert (string_less_string (abc, abd));
    test_assert (!string_greater_string (abc, abd));
  }

  TEST_CASE ("shorter prefix is less than its extension")
  {
    test_assert (string_less_string (ab, abc));
    test_assert (string_greater_string (abc, ab));
  }

  TEST_CASE ("equal strings: not less, not greater")
  {
    test_assert (!string_less_string (xyz1, xyz2));
    test_assert (!string_greater_string (xyz1, xyz2));
  }

  TEST_CASE ("less_equal and greater_equal hold for equal pair")
  {
    test_assert (string_less_equal_string (xyz1, xyz2));
    test_assert (string_greater_equal_string (xyz1, xyz2));
  }

  TEST_CASE ("less_equal / greater_equal are consistent with strict versions")
  {
    test_assert (string_less_equal_string (abc, abd));
    test_assert (!string_greater_equal_string (abc, abd));
    test_assert (string_greater_equal_string (abd, abc));
    test_assert (!string_less_equal_string (abd, abc));
  }
}

TEST (line_length_newline_found)
{
  TEST_CASE ("newline at position 0")
  {
    const char buf[] = "\nhello";
    test_assert_type_equal (line_length (buf, sizeof (buf) - 1), (u64)0, u64,
                            PRIu64);
  }

  TEST_CASE ("newline in the middle")
  {
    const char buf[] = "hello\nworld";
    test_assert_type_equal (line_length (buf, sizeof (buf) - 1), (u64)5, u64,
                            PRIu64);
  }

  TEST_CASE ("newline at the last allowed position")
  {
    const char buf[] = "abc\n";
    // max = 4, newline is at index 3
    test_assert_type_equal (line_length (buf, 4), (u64)3, u64, PRIu64);
  }

  TEST_CASE ("no newline within max: returns max")
  {
    const char buf[] = "hello";
    test_assert_type_equal (line_length (buf, 5), (u64)5, u64, PRIu64);
  }

  TEST_CASE ("max=1 and the single byte is not a newline: returns 1")
  {
    const char buf[] = "x";
    test_assert_type_equal (line_length (buf, 1), (u64)1, u64, PRIu64);
  }

  TEST_CASE ("max=1 and the single byte is a newline: returns 0")
  {
    const char buf[] = "\n";
    test_assert_type_equal (line_length (buf, 1), (u64)0, u64, PRIu64);
  }

  TEST_CASE ("newline beyond max is invisible")
  {
    // The buffer is "hello\n" but we only scan the first 5 bytes.
    const char buf[] = "hello\n";
    test_assert_type_equal (line_length (buf, 5), (u64)5, u64, PRIu64);
  }
}

TEST (string_equal_cases)
{
  TEST_CASE ("identical strings are equal")
  {
    const struct string a = { .data = "abc", .len = 3 };
    const struct string b = { .data = "abc", .len = 3 };
    test_assert (string_equal (a, b));
  }

  TEST_CASE ("same content different length: not equal")
  {
    const struct string a = { .data = "abc", .len = 3 };
    const struct string b = { .data = "abcd", .len = 4 };
    test_assert (!string_equal (a, b));
  }

  TEST_CASE ("same length different content: not equal")
  {
    const struct string a = { .data = "abc", .len = 3 };
    const struct string b = { .data = "abd", .len = 3 };
    test_assert (!string_equal (a, b));
  }

  TEST_CASE ("single-char strings: equal")
  {
    const struct string a = { .data = "z", .len = 1 };
    const struct string b = { .data = "z", .len = 1 };
    test_assert (string_equal (a, b));
  }

  TEST_CASE ("single-char strings: not equal")
  {
    const struct string a = { .data = "a", .len = 1 };
    const struct string b = { .data = "b", .len = 1 };
    test_assert (!string_equal (a, b));
  }
}

TEST (strings_are_disjoint_cases)
{
  TEST_CASE ("both arrays empty: disjoint (returns NULL)")
  {
    test_assert (strings_are_disjoint (NULL, 0, NULL, 0) == NULL);
  }

  TEST_CASE ("left empty: disjoint")
  {
    char d[] = "a";
    const struct string right[] = { { .len = 1, .data = d } };
    test_assert (strings_are_disjoint (NULL, 0, right, 1) == NULL);
  }

  TEST_CASE ("no shared element: returns NULL")
  {
    char da[] = "foo";
    char db[] = "bar";
    const struct string left[] = { { .len = 3, .data = da } };
    const struct string right[] = { { .len = 3, .data = db } };
    test_assert (strings_are_disjoint (left, 1, right, 1) == NULL);
  }

  TEST_CASE ("shared element: returns pointer into left")
  {
    char da[] = "foo";
    char db[] = "baz";
    char dc[] = "foo"; // duplicate of da
    const struct string left[] = { { .len = 3, .data = da },
                                   { .len = 3, .data = db } };
    const struct string right[] = { { .len = 3, .data = dc } };
    const struct string *hit = strings_are_disjoint (left, 2, right, 1);
    test_assert (hit != NULL);
    test_assert (string_equal (*hit, (struct string){ .len = 3, .data = "foo" }));
  }

  TEST_CASE ("shared element found in second left position")
  {
    char da[] = "alpha";
    char db[] = "beta";
    char dc[] = "beta";
    const struct string left[] = { { .len = 5, .data = da },
                                   { .len = 4, .data = db } };
    const struct string right[] = { { .len = 4, .data = dc } };
    const struct string *hit = strings_are_disjoint (left, 2, right, 1);
    test_assert (hit != NULL);
    test_assert (string_equal (*hit, (struct string){ .len = 4, .data = "beta" }));
  }

  TEST_CASE ("multiple right candidates, none match: returns NULL")
  {
    char da[] = "x";
    char db[] = "y";
    char dc[] = "z";
    const struct string left[] = { { .len = 1, .data = da } };
    const struct string right[] = { { .len = 1, .data = db },
                                    { .len = 1, .data = dc } };
    test_assert (strings_are_disjoint (left, 1, right, 2) == NULL);
  }
}

TEST (string_plus_concatenates)
{
  TEST_CASE ("basic concatenation")
  {
    u8 arena[64];
    struct lalloc alloc = lalloc_create_from (arena);
    error e = error_create ();

    const struct string left = strfcstr ("Hello");
    const struct string right = strfcstr (" world");
    const struct string result = string_plus (left, right, &alloc, &e);

    test_assert (result.data != NULL);
    test_assert_type_equal (result.len, (u32)(5 + 6), u32, PRIu32);
    test_assert (memcmp (result.data, "Hello world", 11) == 0);
  }

  TEST_CASE ("concatenate two single-char strings")
  {
    u8 arena[16];
    struct lalloc alloc = lalloc_create_from (arena);
    error e = error_create ();

    const struct string left = strfcstr ("A");
    const struct string right = strfcstr ("B");
    const struct string result = string_plus (left, right, &alloc, &e);

    test_assert (result.data != NULL);
    test_assert_type_equal (result.len, (u32)2, u32, PRIu32);
    test_assert (result.data[0] == 'A');
    test_assert (result.data[1] == 'B');
  }

  TEST_CASE ("concatenate three strings via two calls")
  {
    u8 arena[128];
    struct lalloc alloc = lalloc_create_from (arena);
    error e = error_create ();

    const struct string a = strfcstr ("foo");
    const struct string b = strfcstr ("/");
    const struct string c = strfcstr ("bar");
    const struct string ab = string_plus (a, b, &alloc, &e);
    const struct string abc = string_plus (ab, c, &alloc, &e);

    test_assert (abc.data != NULL);
    test_assert_type_equal (abc.len, (u32)7, u32, PRIu32);
    test_assert (memcmp (abc.data, "foo/bar", 7) == 0);
  }
}

TEST (cbuffer_discard_all_resets_state)
{
  TEST_CASE ("discard non-empty buffer produces empty buffer at position zero")
  {
    u8 buf[4];
    struct cbuffer b = cbuffer_create (buf, 4);

    const u8 vals[3] = { 1, 2, 3 };
    cbuffer_write (vals, 1, 3, &b);
    test_assert_type_equal (cbuffer_len (&b), (u32)3, u32, PRIu32);

    cbuffer_discard_all (&b);

    test_assert (cbuffer_isempty (&b));
    test_assert_type_equal (b.head, (u32)0, u32, PRIu32);
    test_assert_type_equal (b.tail, (u32)0, u32, PRIu32);
    test_assert_int_equal ((int)b.isfull, (int)false);
  }

  TEST_CASE ("discard full buffer also clears isfull flag")
  {
    u8 buf[2];
    struct cbuffer b = cbuffer_create (buf, 2);

    u8 x = 0xAA;
    cbuffer_push_back (&x, 1, &b);
    x = 0xBB;
    cbuffer_push_back (&x, 1, &b);
    test_assert (b.isfull);

    cbuffer_discard_all (&b);

    test_assert (!b.isfull);
    test_assert (cbuffer_isempty (&b));
  }
}

TEST (cbuffer_read_write_wraparound)
{
  // Scenario: fill buffer, partially drain it, then write again so that
  // the data physically wraps around the end of the backing array.  Read
  // must reassemble the correct byte sequence regardless.
  TEST_CASE ("write wraps around, read yields original bytes in order")
  {
    u8 buf[4];
    struct cbuffer b = cbuffer_create (buf, 4);

    // Write 3 bytes: [A B C _]  head=3, tail=0
    const u8 src[4] = { 0xAA, 0xBB, 0xCC, 0xDD };
    cbuffer_write (src, 1, 3, &b);
    test_assert_type_equal (cbuffer_len (&b), (u32)3, u32, PRIu32);

    // Read 2 bytes: tail advances to 2, buffer now holds [_ _ C _]
    u8 dst[4] = { 0 };
    u32 n = cbuffer_read (dst, 1, 2, &b);
    test_assert_int_equal ((int)n, 2);
    test_assert_int_equal (dst[0], 0xAA);
    test_assert_int_equal (dst[1], 0xBB);

    // Write 3 more bytes: head was 3, wraps → [EE FF _ CC]
    const u8 src2[3] = { 0xDD, 0xEE, 0xFF };
    n = cbuffer_write (src2, 1, 3, &b);
    test_assert_int_equal ((int)n, 3);
    test_assert_type_equal (cbuffer_len (&b), (u32)4, u32, PRIu32);

    // Read all 4 bytes — must be CC DD EE FF in order
    u8 result[4] = { 0 };
    n = cbuffer_read (result, 1, 4, &b);
    test_assert_int_equal ((int)n, 4);
    test_assert_int_equal (result[0], 0xCC);
    test_assert_int_equal (result[1], 0xDD);
    test_assert_int_equal (result[2], 0xEE);
    test_assert_int_equal (result[3], 0xFF);
  }

  TEST_CASE ("copy does not advance tail")
  {
    u8 buf[4];
    struct cbuffer b = cbuffer_create (buf, 4);

    const u8 src[4] = { 10, 20, 30, 40 };
    cbuffer_write (src, 1, 4, &b);

    u8 out1[4] = { 0 };
    u8 out2[4] = { 0 };
    const u32 c1 = cbuffer_copy (out1, 1, 4, &b);
    const u32 c2 = cbuffer_copy (out2, 1, 4, &b);

    test_assert_int_equal ((int)c1, 4);
    test_assert_int_equal ((int)c2, 4);
    test_assert (memcmp (out1, out2, 4) == 0);
    // Buffer not consumed: still 4 bytes
    test_assert_type_equal (cbuffer_len (&b), (u32)4, u32, PRIu32);
  }
}

TEST (cbuffer_cbuffer_move_transfers_bytes)
{
  TEST_CASE ("move all bytes from src to dst")
  {
    u8 sbuf[4], dbuf[8];
    struct cbuffer src = cbuffer_create (sbuf, 4);
    struct cbuffer dst = cbuffer_create (dbuf, 8);

    const u8 data[4] = { 1, 2, 3, 4 };
    cbuffer_write (data, 1, 4, &src);

    const u32 moved = cbuffer_cbuffer_move (&dst, 1, 4, &src);
    test_assert_int_equal ((int)moved, 4);
    test_assert (cbuffer_isempty (&src));
    test_assert_type_equal (cbuffer_len (&dst), (u32)4, u32, PRIu32);

    u8 out[4] = { 0 };
    cbuffer_read (out, 1, 4, &dst);
    test_assert (memcmp (out, data, 4) == 0);
  }

  TEST_CASE ("move is limited by available src bytes")
  {
    u8 sbuf[4], dbuf[8];
    struct cbuffer src = cbuffer_create (sbuf, 4);
    struct cbuffer dst = cbuffer_create (dbuf, 8);

    const u8 data[2] = { 0xAB, 0xCD };
    cbuffer_write (data, 1, 2, &src);

    // Request 4 but only 2 available
    const u32 moved = cbuffer_cbuffer_move (&dst, 1, 4, &src);
    test_assert_int_equal ((int)moved, 2);
  }
}

#endif // NTEST
