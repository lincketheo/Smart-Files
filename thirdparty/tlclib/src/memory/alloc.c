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

#include "tlclib/memory/alloc.h"

#include "tlclib/dev/assert.h"
#include "tlclib/memory/chunk_alloc.h"
#include "tlclib/memory/lalloc.h"

void *
alloc_alloc (struct alloc *a, const u32 nelem, const u32 size, error *e)
{
  switch (a->type)
    {
    case AT_LALLOC:
      {
        return lmalloc (&a->_lalloc, nelem, size, e);
      }
    case AT_CHNK_ALLOC:
      {
        return chunk_malloc (&a->_calloc, nelem, size, e);
      }
    case AT_MALLOC:
      {
        return i_malloc (nelem, size, e);
      }
    }
  UNREACHABLE ();
}

void *
alloc_calloc (struct alloc *a, const u32 size, const u32 nelem, error *e)
{
  switch (a->type)
    {
    case AT_LALLOC:
      {
        return lcalloc (&a->_lalloc, nelem, size, e);
      }
    case AT_CHNK_ALLOC:
      {
        return chunk_calloc (&a->_calloc, nelem, size, e);
      }
    case AT_MALLOC:
      {
        return i_calloc (nelem, size, e);
      }
    }
  UNREACHABLE ();
}

void
alloc_free (const struct alloc *a, void *data)
{
  switch (a->type)
    {
    case AT_LALLOC:
      {
        UNREACHABLE ();
      }
    case AT_CHNK_ALLOC:
      {
        UNREACHABLE ();
      }
    case AT_MALLOC:
      {
        i_free (data);
      }
    }
  UNREACHABLE ();
}
