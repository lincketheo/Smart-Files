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

#include "pager.h"

err_t
pgr_make_writable (struct pager *p, struct txn *tx, page_h *h, error *e)
{
  DBG_ASSERT (pager, p);
  ASSERT (h->mode == PHM_S);
  ASSERT (h->pgr->wsibling == -1);

  const i32 clock = pgr_reserve_at_clock_thread_unsafe (p, e);
  if (clock < 0)
    {
      goto theend;
    }

  struct page_frame *pgw = &p->pages[clock];

  // Initialize pgw
  pgw->pin = 1;
  pgw->wsibling = -1;
  pgw->flags = PW_PRESENT | PW_X;
  memcpy (pgw->page.raw, h->pgr->page.raw, PAGE_SIZE);
  pgw->page.pg = h->pgr->page.pg;

  // Modify pgr
  h->pgr->wsibling = clock;

  // Set h
  h->pgw = pgw;
  h->mode = PHM_X;
  h->tx = tx;

theend:
  return error_trace (e);
}

err_t
pgr_maybe_make_writable (struct pager *p, struct txn *tx, page_h *cur,
                         error *e)
{
  if (cur->mode == PHM_S)
    {
      return pgr_make_writable (p, tx, cur, e);
    }
  return SUCCESS;
}
