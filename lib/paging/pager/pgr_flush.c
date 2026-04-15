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
#include "paging/pages/page.h"

err_t
pgr_flush (const struct pager *p, struct page_frame *mp, error *e)
{
  DBG_ASSERT (pager, p);

  ASSERT (mp->flags & PW_PRESENT);
  ASSERT (!(mp->flags & PW_X));
  // ASSERT (mp->flags & PW_DIRTY);

  // Only need to write it out if it's dirty
  if (mp->flags & PW_DIRTY)
    {
      // WAL-before-page: flush WAL up to this page's LSN
      // before writing the page
      if (!(p->flags & PGR_ISRESTARTING) && p->ww)
        {
          const lsn plsn = page_get_page_lsn (&mp->page);
          if (oswal_flush_to (p->ww, plsn, e))
            {
              goto failed;
            }
        }

      // Set page checksum before flushing
      page_set_checksum (&mp->page, page_compute_checksum (&mp->page));

      if (ospgr_write (p->fp, mp->page.raw, mp->page.pg, e))
        {
          goto failed;
        }

      mp->flags &= ~PW_DIRTY;

      dpgt_remove_expect (p->dpt, mp->page.pg);
    }

  return SUCCESS;

failed:
  return error_trace (e);
}
