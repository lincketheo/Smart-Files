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

#include "tlclib/core/numbers.h"

#include "tlclib/core/macros.h"
#include "tlclib/dev/assert.h"
#include "tlclib/dev/bounds.h"
#include "tlclib/dev/testing.h"

#include <string.h>

err_t
parse_i64_expect (i64 *dest, const char *data, const u32 len, error *e)
{
  ASSERT (data);
  ASSERT (len > 0);
  ASSERT (dest);

  u32 i = 0;
  bool neg = false;

  if (data[i] == '+' || data[i] == '-')
    {
      neg = (data[i] == '-');
      i++;
      ASSERT (i < len);
    }

  i64 acc = 0;

  for (; i < len; i++)
    {
      const char c = data[i];
      ASSERT (is_num (c));

      const i64 digit = c - '0';

      if (!safe_mul_i64 (&acc, acc, 10L))
        {
          goto failed;
        }

      if (!safe_sub_i64 (&acc, acc, digit))
        {
          goto failed;
        }
    }

  if (!neg)
    {
      if (acc == I64_MIN)
        {
          goto failed;
        }
      acc = -acc;
    }

  *dest = acc;
  return SUCCESS;

failed:
  return error_causef (e, ERR_ARITH, "i64 overflow");
}

err_t
parse_i32_expect (i32 *dest, const char *data, const u32 len, error *e)
{
  ASSERT (data);
  ASSERT (len > 0);
  ASSERT (dest);

  u32 i = 0;
  bool neg = false;

  if (data[i] == '+' || data[i] == '-')
    {
      neg = (data[i] == '-');
      i++;
      ASSERT (i < len); // We expect string to be valid
    }

  i32 acc = 0;

  for (; i < len; i++)
    {
      const char c = data[i];
      ASSERT (is_num (c));

      const i32 digit = c - '0';

      if (!safe_mul_i32 (&acc, acc, 10))
        {
          goto failed;
        }

      if (!safe_sub_i32 (&acc, acc, digit))
        {
          goto failed;
        }
    }

  if (!neg)
    {
      if (acc == I32_MIN)
        {
          goto failed;
        }
      acc = -acc;
    }

  *dest = acc;
  return SUCCESS;

failed:
  return error_causef (e, ERR_ARITH, "i32 overflow");
}

#ifndef NTEST
TEST (parse_i32_expect)
{
  i32 out = -1;
  error e = error_create ();

  test_assert_int_equal (parse_i32_expect (&out, "1234", 4, &e), SUCCESS);
  test_assert_int_equal (out, 1234);

  test_assert_int_equal (parse_i32_expect (&out, "-56", 3, &e), SUCCESS);
  test_assert_int_equal (out, -56);

  const char *big = "999999999999999999999999999999999999999999";
  test_assert_int_equal (parse_i32_expect (&out, big, strlen (big), &e),
                         ERR_ARITH);
}
#endif

err_t
parse_f32_expect (f32 *dest, const char *s, const u32 len, error *e)
{
  ASSERT (s);
  ASSERT (dest);
  ASSERT (len > 0);

  u32 i = 0;
  bool neg = false;

  if (s[i] == '+' || s[i] == '-')
    {
      neg = (s[i] == '-');
      i++;
      ASSERT (i < len);
    }

  // Integer part
  f32 acc = 0.0f;
  bool saw_digit = false;
  while (i < len && s[i] >= '0' && s[i] <= '9')
    {
      const f32 d = (f32)(s[i] - '0');
      if (!safe_mul_f32 (&acc, acc, 10.0f))
        {
          goto failed;
        }
      if (!safe_add_f32 (&acc, acc, d))
        {
          goto failed;
        }
      i++;
      saw_digit = true;
    }

  // Fractional part
  if (i < len && s[i] == '.')
    {
      i++;
      ASSERT (i < len); // cannot end with '.'
      f32 frac = 0.0f, scale = 1.0f;
      while (i < len && s[i] >= '0' && s[i] <= '9')
        {
          const f32 d = (f32)(s[i] - '0');
          if (!safe_mul_f32 (&frac, frac, 10.0f))
            {
              goto failed;
            }
          if (!safe_add_f32 (&frac, frac, d))
            {
              goto failed;
            }
          if (!safe_mul_f32 (&scale, scale, 10.0f))
            {
              goto failed;
            }
          i++;
          saw_digit = true;
        }
      f32 tmp;
      if (!safe_div_f32 (&tmp, frac, scale))
        {
          goto failed;
        }
      if (!safe_add_f32 (&acc, acc, tmp))
        {
          goto failed;
        }
    }

  ASSERT (saw_digit);

  // Exponent part
  if (i < len && (s[i] == 'e' || s[i] == 'E'))
    {
      i++;
      ASSERT (i < len); // must have exponent digits
      bool exp_neg = false;
      if (s[i] == '+' || s[i] == '-')
        {
          exp_neg = (s[i] == '-');
          i++;
          ASSERT (i < len);
        }
      u32 exp = 0;
      bool saw_exp = false;
      while (i < len && s[i] >= '0' && s[i] <= '9')
        {
          const u32 d = (u32)(s[i] - '0');
          ASSERT (exp <= (UINT32_MAX - d) / 10);
          exp = exp * 10 + d;
          i++;
          saw_exp = true;
        }
      ASSERT (saw_exp);

      // Apply exponent
      for (u32 k = 0; k < exp; k++)
        {
          if (exp_neg)
            {
              if (!safe_div_f32 (&acc, acc, 10.0f))
                {
                  goto failed;
                }
            }
          else
            {
              if (!safe_mul_f32 (&acc, acc, 10.0f))
                {
                  goto failed;
                }
            }
        }
    }

  ASSERT (i == len); // no extra characters

  if (neg)
    acc = -acc;
  *dest = acc;
  return SUCCESS;

failed:
  return error_causef (e, ERR_ARITH, "f32 overflow");
}

#define EPSILON 1e-6f

#ifndef NTEST
TEST (parse_f32_expect)
{
  f32 out = NAN;
  error e = error_create ();

  test_assert_int_equal (parse_f32_expect (&out, "3.14", 4, &e), SUCCESS);
  test_assert_int_equal (fabsf (out - 3.14f) < EPSILON, true);

  test_assert_int_equal (parse_f32_expect (&out, "-0.5", 4, &e), SUCCESS);
  test_assert_int_equal (fabsf (out + 0.5f) < EPSILON, true);

  test_assert_int_equal (parse_f32_expect (&out, "1.23e3", 6, &e), SUCCESS);
  test_assert_int_equal (fabsf (out - 1230.0f) < EPSILON, true);

  test_assert_int_equal (parse_f32_expect (&out, ".25", 3, &e), SUCCESS);
  test_assert_int_equal (fabsf (out - 0.25f) < EPSILON, true);

  test_assert_int_equal (parse_f32_expect (&out, "1e40", 4, &e), ERR_ARITH);
}
#endif

float
py_mod_f32 (const float num, const float denom)
{
  if (denom == 0.0f)
    {
      return INFINITY;
    }

  float rem = num - denom * (int)(num / denom);

  if ((rem < 0.0f && denom > 0.0f) || (rem > 0.0f && denom < 0.0f))
    {
      rem += denom;
    }

  return rem;
}

#ifndef NTEST
TEST (py_mod_f32)
{
  // +num , +denom  (generic)
  test_assert (py_mod_f32 (5.5f, 2.0f) == 1.5f);

  // –num , +denom  (Python keeps denom’s sign)
  test_assert (py_mod_f32 (-5.5f, 2.0f) == 0.5f);

  // +num , –denom
  test_assert (py_mod_f32 (5.5f, -2.0f) == -0.5f);

  // –num , –denom
  test_assert (py_mod_f32 (-5.5f, -2.0f) == -1.5f);

  // exact multiple (remainder 0)
  test_assert (py_mod_f32 (4.0f, 2.0f) == 0.0f);

  // zero numerator
  test_assert (py_mod_f32 (0.0f, 3.3f) == 0.0f);

  // denominator 0 ⇒ +INF  (sentinel-style)
  test_assert (isinf (py_mod_f32 (7.0f, 0.0f)));
}
#endif

i32
py_mod_i32 (const i32 num, const i32 denom)
{
  i32 r = num % denom;
  if ((r != 0) && ((r < 0) != (denom < 0)))
    {
      r += denom;
    }
  return r;
}

#ifndef NTEST
TEST (py_mod_i32)
{
  // +num , +denom
  test_assert (py_mod_i32 (5, 3) == 2);

  // –num , +denom
  test_assert (py_mod_i32 (-5, 3) == 1);

  // +num , –denom
  test_assert (py_mod_i32 (5, -3) == -1);

  // –num , –denom
  test_assert (py_mod_i32 (-5, -3) == -2);

  // exact multiple (remainder 0)
  test_assert (py_mod_i32 (9, 3) == 0);

  // zero numerator
  test_assert (py_mod_i32 (0, 7) == 0);
}
#endif
