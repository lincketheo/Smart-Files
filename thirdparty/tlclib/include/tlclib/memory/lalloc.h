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
#include "tlclib/dev/assert.h"
#include "tlclib/dev/error.h"
#include "tlclib/dev/signatures.h"

struct lalloc
{
  latch latch;
  u32 used;
  u32 limit;
  u8 *data;
};

#define lalloc_create_from(buf) lalloc_create ((u8 *)buf, sizeof (buf))

struct lalloc lalloc_create (u8 *data, u32 limit);
u32 lalloc_get_state (struct lalloc *l);
void lalloc_reset_to_state (struct lalloc *l, u32 state);
void *lmalloc (struct lalloc *a, u32 req, u32 size, error *e);
void *lcalloc (struct lalloc *a, u32 req, u32 size, error *e);
void lalloc_reset (struct lalloc *a);

HEADER_FUNC void *
lmalloc_expect (struct lalloc *a, const u32 req, const u32 size)
{
  void *ret = lmalloc (a, req, size, NULL);
  ASSERT (ret);
  return ret;
}
