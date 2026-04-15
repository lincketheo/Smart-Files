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
 * Evict a page frame from the buffer pool.
 *
 * Flushes the frame to disk (honoring WAL-before-page), removes it from the
 * pgno→slot hash table, and clears the frame's flags so the slot can be
 * reused.  The frame must be unpinned (pin == 0) and must not be a write
 * frame (PW_X).
 */
err_t
pgr_evict (struct pager *p, struct page_frame *mp, error *e)
{
  DBG_ASSERT (pager, p);

  ASSERT ((mp->flags & PW_PRESENT));
  ASSERT (!(mp->flags & PW_X));
  ASSERT (mp->pin == 0);

  // Caller holds mp->latch, so use the unsafe (no-latch) flush variant
  if (pgr_flush (p, mp, e))
    {
      goto failed;
    }

  ht_delete_expect_idx (&p->pgno_to_value, NULL, mp->page.pg);
  mp->flags = 0;

  return SUCCESS;

failed:
  return error_trace (e);
}
