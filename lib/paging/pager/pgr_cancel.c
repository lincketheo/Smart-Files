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
 * Discard an in-flight page handle without writing changes to disk.
 *
 * For a write handle (PHM_X), the write frame is returned to the pool
 * without copying its contents to the read frame: the mutation is thrown
 * away.  The read frame's wsibling link is cleared so the pool no longer
 * knows a write twin existed.
 *
 * For either handle mode, the read frame's pin is decremented and the
 * handle is reset to PHM_NONE.  The read frame remains in the pool
 * (pinned by other holders or eligible for clock eviction).
 *
 * Used on all error paths to release pages without committing partial
 * changes — the WAL-before-page invariant is never violated because
 * no WAL record is written for cancelled mutations.
 */
void
pgr_cancel (const struct pager *p, page_h *h)
{
  DBG_ASSERT (pager, p);

  ASSERT (h->mode == PHM_X || h->mode == PHM_S);
  ASSERT (h->pgr->flags & PW_PRESENT);

  // Cancel write page
  if (h->mode == PHM_X)
    {
      h->pgw->flags = 0;
      h->pgr->wsibling = -1;
      h->pgw = NULL;
      h->mode = PHM_S;
    }

  // Decrement pin
  h->pgr->pin--;
  h->pgr = NULL;
  h->mode = PHM_NONE;
}

void
pgr_cancel_if_exists (struct pager *p, page_h *h)
{
  if (h->mode == PHM_NONE)
    {
      return;
    }

  pgr_cancel (p, h);
}
