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

#include "core/malloc_plan.h"

#include "core/assert.h"
#include "intf/os/memory.h"

#include <string.h>

void *
malloc_plan_memcpy (struct malloc_plan *plan, const void *data, const u32 len)
{
  switch (plan->mode)
    {
    case MP_PLANNING:
      {
        plan->size += len;
        return NULL;
      }
    case MP_ALLOCING:
      {
        ASSERT (plan->blen + len <= plan->size);
        void *ret = malloc_plan_head (plan);
        memcpy ((u8 *)plan->buffer + plan->blen, data, len);
        plan->blen += len;
        return ret;
      }
    }
  UNREACHABLE ();
}

err_t
malloc_plan_alloc (struct malloc_plan *plan, error *e)
{
  ASSERT (plan->mode == MP_PLANNING);
  plan->buffer = i_malloc (plan->size, 1, e);
  if (plan->buffer == NULL)
    {
      return e->cause_code;
    }
  plan->mode = MP_ALLOCING;

  return SUCCESS;
}
