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

/*
 * Find the next available buffer pool slot using the clock (second-chance)
 * algorithm, evicting a victim if necessary.
 *
 * The hand sweeps forward through the pool.  At each frame:
 *   - If the frame is empty (not PW_PRESENT), use it immediately.
 *   - If the frame is pinned (pin > 0), skip it without affecting its access
 *     bit — it is currently in use and cannot be evicted.
 *   - If the access bit (PW_ACCESS) is set, clear it and move on.  This
 *     gives the page a second chance before it is evicted on the next pass.
 *   - Otherwise evict the frame: flush it to disk and return its slot.
 *
 * The loop runs for at most 2*MEMORY_PAGE_LEN iterations so that each frame
 * has at most one chance to have its access bit cleared.  If no slot can be
 * found (all frames pinned), ERR_PAGER_FULL is returned.
 *
 * This function is intentionally NOT thread-safe; callers are expected to
 * hold the pager lock before calling it.
 */
i32
pgr_reserve_at_clock_thread_unsafe (struct pager *p, error *e)
{
  DBG_ASSERT (pager, p);

  i_log_trace ("reserving buffer pool slot\n");

  // 2 times so that we might clear an access bit
  struct page_frame *mp;
  for (u32 i = 0; i < 2 * MEMORY_PAGE_LEN; ++i)
    {
      i_log_trace ("clock=%u\n", p->clock);

      mp = &p->pages[p->clock];

      // Found an empty spot
      if (!(mp->flags & PW_PRESENT))
        {
          i_log_trace ("page %u: empty, using\n", p->clock);
          goto found_spot;
        }

      // Pinned, skip it
      if (mp->pin > 0)
        {
          i_log_trace ("page %u: pinned "
                       "(pin=%u), skipping\n",
                       p->clock, mp->pin);
          p->clock = (p->clock + 1) % MEMORY_PAGE_LEN;
          continue;
        }

      // Access bit is on - set off and continue
      if (mp->flags & PW_ACCESS)
        {
          i_log_trace ("page %u: access bit "
                       "set, clearing\n",
                       p->clock);
          mp->flags &= ~PW_ACCESS;
          p->clock = (p->clock + 1) % MEMORY_PAGE_LEN;
          continue;
        }

      // EVICT
      i_log_trace ("page %u: evicting\n", p->clock);
      WRAP (pgr_evict (p, mp, e));
      goto found_spot;
    }

  return error_causef (e, ERR_PAGER_FULL, "buffer pool full");

found_spot:
  ASSERT (!(p->pages[p->clock].flags & PW_PRESENT));
  const u32 ret = p->clock;
  p->clock = (p->clock + 1) % MEMORY_PAGE_LEN;

  return ret;
}
