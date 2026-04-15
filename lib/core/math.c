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

#include "core/math.h"

#include <string.h>

float
f16_to_f32 (const u16 h)
{
  const u32 sign = (u32)(h >> 15) & 1u;
  const u32 exp = (u32)(h >> 10) & 0x1Fu;
  const u32 mant = (u32)(h) & 0x3FFu;
  u32 f;

  if (exp == 0)
    f = (sign << 31) | (mant << 13);
  else if (exp == 31)
    f = (sign << 31) | 0x7F800000u | (mant << 13);
  else
    f = (sign << 31) | ((exp + 112) << 23) | (mant << 13);

  float result;
  memcpy (&result, &f, 4);
  return result;
}
