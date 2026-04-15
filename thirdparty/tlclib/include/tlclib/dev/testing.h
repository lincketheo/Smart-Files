/// Copyright [yyyy] [name of copyright owner]
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

#ifndef NTEST

#include "assert.h"
#include "signatures.h"
#include "tlclib/core/filenames.h"
#include "tlclib/intf/logging.h"

#define FPREFIX_STR "%s:%d (%s): \n            "

typedef bool (*test_func) (void);
typedef struct
{
  test_func test;
  char *test_name;
} test;

extern int test_ret; // Global (thread unsafe) return code for tests

// Defines a named test suite with a fixed-capacity array.
// Use an enum for the max so it's a compile-time constant usable in the
// array size and the assert below.
//
// Usage (file scope):
//   TEST_SUITE (core, 64);
//
// Then in your init function — the ONE place to add/remove tests:
//   REGISTER (core, foo);
//   REGISTER (core, bar);
#define TEST_SUITE(name, max)    \
  enum                           \
  {                              \
    name##_max = (max)           \
  };                             \
  test name##_tests[name##_max]; \
  int name##_count = 0

// Appends a test to a suite. Pulls in the extern declarations itself so
// there is no separate declare step.
#define REGISTER(suite, name)                  \
  do                                           \
    {                                          \
      extern bool wrapper_test_##name (void);  \
      ASSERT (suite##_count < suite##_max);    \
      suite##_tests[suite##_count++] = (test){ \
        .test = wrapper_test_##name,           \
        .test_name = #name,                    \
      };                                       \
    }                                          \
  while (0)

// clang-format off
// TEST(foobar) { ... } expands to a wrapper + the static body.
// See the long comment in the original header for the full expansion.
// clang-format on
#define TEST(name)                                                   \
  static void test_##name (void);                                    \
  bool wrapper_test_##name (void);                                   \
  bool wrapper_test_##name (void)                                    \
  {                                                                  \
    i_log_info ("========================= TEST CASE: %s\n", #name); \
    int prev = test_ret;                                             \
    test_ret = 0;                                                    \
    test_##name ();                                                  \
    if (!test_ret)                                                   \
      {                                                              \
        i_log_passed ("%s\n", #name);                                \
        test_ret = prev;                                             \
        return true;                                                 \
      }                                                              \
    return false;                                                    \
  }                                                                  \
  static void test_##name (void)

#define TEST_CASE(fmt, ...)                                           \
  for (int _tc_once                                                   \
       = (i_log_info ("------ CASE: " fmt "\n", ##__VA_ARGS__), 1),   \
       _tc_prev = test_ret;                                           \
       _tc_once; _tc_once = 0,                                        \
       (test_ret == _tc_prev                                          \
            ? (i_log_passed ("------ : " fmt "\n", ##__VA_ARGS__), 0) \
            : (i_log_failure ("------ : " fmt "\n", ##__VA_ARGS__), 0)))

#define fail_test(fmt, ...)                                       \
  do                                                              \
    {                                                             \
      i_log_failure (FPREFIX_STR fmt, FPREFIX_ARGS, __VA_ARGS__); \
      test_ret = -1;                                              \
      return;                                                     \
    }                                                             \
  while (0)

#define test_assert_equal(left, right)             \
  do                                               \
    {                                              \
      if ((left) != (right))                       \
        {                                          \
          fail_test ("%s != %s\n", #left, #right); \
        }                                          \
    }                                              \
  while (0)

#define test_assert_int_equal(left, right) \
  test_assert_type_equal (left, right, i32, PRId32)

#define test_assert_type_equal(left, right, type, fmt) \
  do                                                   \
    {                                                  \
      type _left = left;                               \
      type _right = right;                             \
      if ((_left) != (_right))                         \
        {                                              \
          fail_test ("Expression: %s != %s values: "   \
                     "(%" fmt ") != (%" fmt ")\n",     \
                     #left, #right, _left, _right);    \
        }                                              \
    }                                                  \
  while (0)

#define test_assert_ptr_equal(left, right) \
  test_assert_equal ((void *)left, (void *)right)

#define test_assert(expr)                      \
  do                                           \
    {                                          \
      if (!(expr))                             \
        {                                      \
          fail_test ("Expected: %s\n", #expr); \
        }                                      \
    }                                          \
  while (0)

#define test_fail_if(expr)                       \
  do                                             \
    {                                            \
      if (expr)                                  \
        {                                        \
          fail_test ("Unexpected: %s\n", #expr); \
        }                                        \
    }                                            \
  while (0)

#define test_err_t_check(expr, exp, ename) \
  do                                       \
    {                                      \
      err_t __ret = (err_t)expr;           \
      test_assert_int_equal (__ret, exp);  \
      (ename)->cause_code = SUCCESS;       \
    }                                      \
  while (0)

#define test_assert_memequal(a, b, size) \
  test_assert_int_equal (memcmp (a, b, size), 0)

#else // NTEST

#define TEST_SUITE(name, max) ((void)0)
#define REGISTER(suite, name) ((void)0)
#define TEST(type, name)                              \
  static inline void test_##name (void) MAYBE_UNUSED; \
  static inline void test_##name (void)
#define TEST_CASE(fmt, ...) if (0)
#define TEST_MARKER(_label) ((void)0)
#define CONDITIONAL_TEST_MARKER(condition, _label) ((void)0)
#define test_assert(expr) ((void)0)
#define test_assert_equal(left, right) ((void)0)
#define test_assert_int_equal(left, right) ((void)0)
#define test_assert_type_equal(left, right, t, fmt) ((void)0)
#define test_assert_ptr_equal(left, right) ((void)0)
#define test_assert_memequal(a, b, size) ((void)0)
#define test_fail_if(expr) ((void)0)
#define test_err_t_wrap(expr, ename) ((void)(expr))
#define test_err_t_check(expr, exp, ename) ((void)(expr))
#define fail_test(fmt, ...) ((void)0)

#endif // NTEST
