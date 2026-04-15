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

#include "paging/pager.h"

/*
 * Write all non-exclusively-held dirty pages to disk.
 *
 * Iterates every pool slot; for each frame that is present but not currently
 * locked for writing (PW_X clear), pgr_flush() is called.  Errors from
 * individual flushes are ignored — this is a best-effort sweep.
 *
 * Write-locked frames (PW_X set) are skipped because they have an in-
 * progress write frame; flushing them here would race with the transaction
 * that holds them.
 */
err_t
pgr_checkpoint (struct pager *p, error *e)
{
  ASSERT (p->ww);

  // Flush all pages
  for (u32 i = 0; i < MEMORY_PAGE_LEN; ++i)
    {
      struct page_frame *mp = &p->pages[i];
      if (mp->flags & PW_PRESENT && !(mp->flags & PW_X))
        {
          pgr_flush (p, mp, e); // Ignore error
        }
    }

  return error_trace (e);
}
