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
#include "paging/pages/root_node.h"

err_t
pgr_fuzzy_checkpoint (struct pager *p, error *e)
{
  page_h root = page_h_create ();
  slsn mlsn = 0;

  // BEGIN CHECKPOINT
  mlsn = oswal_append_ckpt_begin (p->ww, e);
  if (mlsn < 0)
    {
      goto failed;
    }

  // Flush all pages
  for (u32 i = 0; i < MEMORY_PAGE_LEN; ++i)
    {
      struct page_frame *mp = &p->pages[i];

      if (mp->flags & PW_PRESENT && !(mp->flags & PW_X)
          && mp->flags & PW_DIRTY)
        {
          // Crab the locks here

          // WAL-before-page: flush WAL up to
          // this page's LSN before writing the
          // page
          if (!(p->flags & PGR_ISRESTARTING) && p->ww)
            {
              const lsn plsn = page_get_page_lsn (&mp->page);
              if (oswal_flush_to (p->ww, plsn, e))
                {
                  continue; // We'll handle errors at the end
                }
            }

          // Write the page to the database file
          ASSERT (mp->page.pg < ospgr_get_npages (p->fp));

          if (ospgr_write (p->fp, mp->page.raw, mp->page.pg, e))
            {
              continue; // We'll
                        // handle
                        // errors at
                        // the end
            }

          mp->flags &= ~PW_DIRTY;

          dpgt_remove_expect (p->dpt, mp->page.pg);
        }
    }

  // Handle errors from flush loop
  if (e->cause_code)
    {
      goto failed;
    }

  const slsn end_lsn = oswal_append_ckpt_end (p->ww, p->tnxt, p->dpt, e);
  if (end_lsn < 0)
    {
      goto failed;
    }

  // Flush the wal so that master lsn is accurate
  if (oswal_flush_to (p->ww, end_lsn, e))
    {
      goto failed;
    }

  // Get the root page - TODO - (21) I'm not attaching this to a
  // transaction. Is that ok?
  if (pgr_get_writable (&root, NULL, PG_ROOT_NODE, ROOT_PGNO, p, e))
    {
      goto failed;
    }

  rn_set_master_lsn (page_h_w (&root), mlsn);

  if (pgr_release (p, &root, PG_ROOT_NODE, e))
    {
      goto failed;
    }

  if (pgr_get (&root, PG_ROOT_NODE, ROOT_PGNO, p, e))
    {
      goto failed;
    }

  if (pgr_flush (p, root.pgr, e))
    {
      goto failed;
    }

  return SUCCESS;

failed:
  pgr_cancel_if_exists (p, &root);
  return error_trace (e);
}
