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

#include "tlclib/memory/chunk_alloc.h"
#include "tlclib/memory/lalloc.h"

struct alloc
{
  enum
  {
    AT_LALLOC,
    AT_CHNK_ALLOC,
    AT_MALLOC,
  } type;

  union
  {
    struct chunk_alloc _calloc;
    struct lalloc _lalloc;
  };
};

void *alloc_alloc (struct alloc *a, u32 nelem, u32 size, error *e);
void *alloc_calloc (struct alloc *a, u32 nelem, u32 size, error *e);
void alloc_free (const struct alloc *a, void *data);
