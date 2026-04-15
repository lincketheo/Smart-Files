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

#include "core/string.h"

#include "core/assert.h"
#include "numstore/types.h"
#include "test/testing.h"

#include <string.h>

DEFINE_DBG_ASSERT (struct string, string, s, {
  ASSERT (s);
  ASSERT (s->data);
  ASSERT (s->len > 0);
})

DEFINE_DBG_ASSERT (struct string, cstring, s, {
  DBG_ASSERT (string, s);
  ASSERT (s->data[s->len] == 0);
})

struct string
strfcstr (const char *cstr)
{
  return (struct string){ .data = cstr, .len = strlen (cstr) };
}

u64
line_length (const char *buf, const u64 max)
{
  ASSERT (buf);
  ASSERT (max > 0);

  const char *nl = memchr (buf, '\n', max);
  u64 ret;

  if (nl != NULL)
    {
      ret = (u64)(nl - buf);
    }
  else
    {
      ret = max;
    }

  return ret;
}

int
strings_all_unique (const struct string *strs, const u32 count)
{
  for (u32 i = 0; i < count; ++i)
    {
      for (u32 j = i + 1; j < count; ++j)
        {
          if (strs[i].len != strs[j].len)
            {
              continue;
            }
          if (memcmp (strs[i].data, strs[j].data, strs[i].len) == 0)
            {
              return 0;
            }
        }
    }
  return 1;
}

#ifndef NTEST
TEST (TT_UNIT, strings_all_unique)
{
  TEST_CASE ("empty array: trivially unique")
  {
    test_assert_int_equal (strings_all_unique (NULL, 0), 1);
  }

  TEST_CASE ("one string: unique")
  {
    char data1[] = "hello";
    const struct string s1[] = { { 5, data1 } };
    test_assert_int_equal (strings_all_unique (s1, 1), 1);
  }

  TEST_CASE ("two different strings, different lengths")
  {
    char d1[] = "a";
    char d2[] = "ab";
    const struct string s2[] = { { 1, d1 }, { 2, d2 } };
    test_assert_int_equal (strings_all_unique (s2, 2), 1);
  }

  TEST_CASE ("two different strings, same length")
  {
    char e1[] = "ab";
    char e2[] = "cd";
    const struct string s3[] = { { 2, e1 }, { 2, e2 } };
    test_assert_int_equal (strings_all_unique (s3, 2), 1);
  }

  TEST_CASE ("duplicate strings")
  {
    char f1[] = "dup";
    char f2[] = "dup";
    const struct string s4[] = { { 3, f1 }, { 3, f2 } };
    test_assert_int_equal (strings_all_unique (s4, 2), 0);
  }

  TEST_CASE ("multiple strings with one duplicate in the middle")
  {
    char g1[] = "one";
    char g2[] = "two";
    char g3[] = "one";
    char g4[] = "four";
    const struct string s5[] = { { 3, g1 }, { 3, g2 }, { 3, g3 }, { 4, g4 } };
    test_assert_int_equal (strings_all_unique (s5, 4), 0);
  }

  TEST_CASE ("all unique in larger set")
  {
    char h1[] = "aa";
    char h2[] = "bb";
    char h3[] = "cc";
    char h4[] = "dd";
    const struct string s6[] = { { 2, h1 }, { 2, h2 }, { 2, h3 }, { 2, h4 } };
    test_assert_int_equal (strings_all_unique (s6, 4), 1);
  }
}
#endif

bool
string_equal (const struct string s1, const struct string s2)
{
  if (s1.len != s2.len)
    {
      return false;
    }
  return strncmp (s1.data, s2.data, s1.len) == 0;
}

struct string
string_plus (const struct string left, const struct string right,
             struct lalloc *alloc, error *e)
{
  const u32 len = left.len + right.len;
  ASSERT (len > 0);

  char *data = lmalloc (alloc, len, 1, e);

  if (data == NULL)
    {
      return (struct string){
        .data = NULL,
        .len = 0,
      };
    }

  memcpy (data, left.data, left.len);
  memcpy (data + left.len, right.data, right.len);

  return (struct string){
    .data = data,
    .len = len,
  };
}

const struct string *
strings_are_disjoint (const struct string *left, const u32 llen,
                      const struct string *right, const u32 rlen)
{
  for (u32 i = 0; i < llen; ++i)
    {
      for (u32 j = 0; j < rlen; ++j)
        {
          if (string_equal (left[i], right[j]))
            {
              return &left[i];
            }
        }
    }

  return NULL;
}

bool
string_contains (const struct string superset, const struct string subset)
{
  if (superset.len == 0 && subset.len == 0)
    {
      return true;
    }

  for (u32 i = 0; i < superset.len; ++i)
    {
      const u32 len = superset.len - i;
      if (len < subset.len)
        {
          return false;
        }

      const struct string _superset = {
        .data = &superset.data[i],
        .len = subset.len,
      };
      if (string_equal (_superset, subset))
        {
          return true;
        }
    }

  return false;
}

#ifndef NTEST
TEST (TT_UNIT, string_contains)
{
  test_assert (!string_contains (strfcstr ("foo"), strfcstr ("foobar")));
  test_assert (string_contains (strfcstr ("foobar"), strfcstr ("foo")));
  test_assert (!string_contains (strfcstr ("fobar"), strfcstr ("foo")));
  test_assert (string_contains (strfcstr ("barfoo"), strfcstr ("foo")));
  test_assert (!string_contains (strfcstr ("barfo"), strfcstr ("foo")));
  test_assert (string_contains (strfcstr ("foo"), strfcstr ("")));
  test_assert (string_contains (strfcstr (""), strfcstr ("")));
}
#endif

bool
string_less_string (const struct string left, const struct string right)
{
  const u32 min_len = (left.len < right.len) ? left.len : right.len;
  const int cmp = memcmp (left.data, right.data, min_len);
  if (cmp < 0)
    {
      return true;
    }
  if (cmp > 0)
    {
      return false;
    }
  return left.len < right.len;
}

bool
string_greater_string (const struct string left, const struct string right)
{
  const u32 min_len = (left.len < right.len) ? left.len : right.len;
  const int cmp = memcmp (left.data, right.data, min_len);
  if (cmp > 0)
    {
      return true;
    }
  if (cmp < 0)
    {
      return false;
    }
  return left.len > right.len;
}

bool
string_less_equal_string (const struct string left, const struct string right)
{
  return !string_greater_string (left, right);
}

bool
string_greater_equal_string (const struct string left,
                             const struct string right)
{
  return !string_less_string (left, right);
}
