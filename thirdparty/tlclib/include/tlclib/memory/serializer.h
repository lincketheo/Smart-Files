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

#include "tlclib/concurrency/latch.h"

struct serializer
{
  latch latch;
  u8 *data;
  u32 dlen;
  const u32 dcap;
};

struct serializer srlizr_create (u8 *data, u32 dcap);

bool srlizr_write (struct serializer *dest, const void *src, u32 len);
#define srlizr_write_expect(dest, src, len)     \
  do                                            \
    {                                           \
      bool ret = srlizr_write (dest, src, len); \
      ASSERT (ret);                             \
    }                                           \
  while (0)
