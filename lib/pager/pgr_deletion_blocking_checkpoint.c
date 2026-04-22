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

#include "c_specx.h"
#include "c_specx/concurrency/gr_lock.h"
#include "c_specx/dev/error.h"
#include "lockt/lock_table.h"
#include "lockt/lt_lock.h"
#include "pager.h"
#include "pager/page_h.h"

err_t
pgr_deletion_blocking_checkpoint (struct pager *p, error *e)
{
  ASSERT (p->ww);

  if (lockt_lock (p->lt, lock_db (), LM_X, NULL, e))
    {
      return error_trace (e);
    }

  // Flush all pages
  for (u32 i = 0; i < MEMORY_PAGE_LEN; ++i)
    {
      struct page_frame *mp = &p->pages[i];
      /**
       * Because we have a global database lock,
       * there should be no running transactions
       * so all pages should be finalized
       */
      ASSERT (!(mp->flags & PW_X));

      if (mp->flags & PW_PRESENT)
        {
          pgr_flush (p, mp, e); // Ignore error
        }
    }

  if (e->cause_code)
    {
      goto failed;
    }

  // Delete the WAL and replace it with a fresh one
  {
    struct os_wal *new_ww = oswal_delete_and_reopen (p->ww, e);
    if (new_ww == NULL)
      {
        goto failed;
      }

    p->ww = new_ww;
  }

  if (lockt_unlock (p->lt, lock_db (), LM_X, e))
    {
      goto failed;
    }

  return SUCCESS;

failed:

  if (lockt_unlock (p->lt, lock_db (), LM_X, e))
    {
      goto failed;
    }

  return error_trace (e);
}
