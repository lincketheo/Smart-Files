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

#include "core/assert.h"
#include "core/signatures.h"
#include "numstore/errors.h"
#include "numstore/stdtypes.h"

struct malloc_plan
{
  u32 size;
  u32 blen;
  void *buffer;

  enum
  {
    MP_PLANNING,
    MP_ALLOCING
  } mode;
};

HEADER_FUNC struct malloc_plan
malloc_plan_create (void)
{
  return (struct malloc_plan){
    .size = 0,
    .blen = 0,
    .buffer = NULL,
    .mode = MP_PLANNING,
  };
}

HEADER_FUNC void *
malloc_plan_head (const struct malloc_plan *plan)
{
  switch (plan->mode)
    {
    case MP_PLANNING:
      {
        return NULL;
      }
    case MP_ALLOCING:
      {
        return (u8 *)plan->buffer + plan->blen;
      }
    }
  UNREACHABLE ();
}

// Allocate memory
void *malloc_plan_memcpy (struct malloc_plan *plan, const void *data, u32 len);

// Do the planning -> alloc swap
err_t malloc_plan_alloc (struct malloc_plan *plan, error *e);
