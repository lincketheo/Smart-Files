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

#include "core/numbers.h"
#include "core/random.h"

#include <complex.h>
#include <math.h>

#define i_creal_64(f) (creal (f))
#define i_cimag_64(f) (cimag (f))

#define i_cabs_sqrd_64(f) ((creal (f) * creal (f)) + ((cimag (f) * cimag (f))))
#define i_cabs_64(f) cabsf (f)
#define i_fabs_32(f) fabsf (f)

#define arr_range(arr)                       \
  do                                         \
    {                                        \
      for (u32 i = 0; i < arrlen (arr); ++i) \
        {                                    \
          arr[i] = i;                        \
        }                                    \
    }                                        \
  while (0)

#define ptr_range(arr, size)            \
  do                                    \
    {                                   \
      for (u32 _i = 0; _i < size; ++_i) \
        {                               \
          arr[_i] = _i;                 \
        }                               \
    }                                   \
  while (0)

#define u32_arr_rand(arr)                    \
  do                                         \
    {                                        \
      for (u32 i = 0; i < arrlen (arr); ++i) \
        {                                    \
          arr[i] = randu32 ();               \
        }                                    \
    }                                        \
  while (0)

#define arr_contains(arr, len, val, ret)     \
  do                                         \
    {                                        \
      ret = false;                           \
      for (u32 ___i = 0; ___i < len; ++___i) \
        {                                    \
          if (arr[___i] == val)              \
            {                                \
              ret = arr[___i];               \
              ret = true;                    \
              break;                         \
            }                                \
        }                                    \
    }                                        \
  while (0)

float f16_to_f32 (u16 h);
