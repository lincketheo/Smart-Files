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

#include "aries.h"
#include "c_specx.h"
#include "c_specx/dev/error.h"
#include "dpgt/dirty_page_table.h"
#include "errors.h"
#include "pager.h"
#include "pages/page.h"
#include "wal/wal_rec_hdr.h"

////////////////////////////////////////////////////////////
// REDO (FIGURE 11)

err_t
pgr_restart_redo (struct pager *p, struct aries_ctx *ctx, error *e)
{
  i_log_info ("Starting Redo phase\n");

  lsn read_lsn = ctx->redo_lsn;

  // Read the redo lsn log
  struct wal_rec_hdr_read *log_rec = oswal_read_entry (p->ww, read_lsn, e);
  if (log_rec == NULL)
    {
      goto failed;
    }

  u32 nredone = 0;

  while (log_rec->type != WL_EOF)
    {
      switch (log_rec->type)
        {
        case WL_UPDATE:
        case WL_CLR:
          {
            if (wrh_is_redoable (log_rec))
              {
                lsn rec_lsn;
                pgno pg = wrh_get_affected_pg (log_rec);

                if (!dpgt_get (&rec_lsn, ctx->dpt, pg))
                  {
                    break;
                  }

                if (read_lsn < rec_lsn)
                  {
                    break;
                  }

                page_h ph = page_h_create ();
                if (pgr_get_writable (&ph, NULL, PG_PERMISSIVE, pg, p, e))
                  {
                    goto failed;
                  }

                pgno page_lsn = page_get_page_lsn (page_h_ro (&ph));
                if (page_lsn < read_lsn)
                  {
                    wrh_redo (log_rec, &ph);
                    nredone++;
                    page_set_page_lsn (page_h_w (&ph), read_lsn);
                  }
                else
                  {
                    dpgt_update (ctx->dpt, pg, page_lsn + 1);
                  }

                pgr_unfix (p, &ph, PG_PERMISSIVE);
              }
            break;
          }
        default:
          {
            // Do nothing
            break;
          }
        }

      // Read next log record
      log_rec = oswal_read_next (p->ww, &read_lsn, e);
      if (log_rec == NULL)
        {
          goto failed;
        }
    }

  i_log_info ("Redo phase done. Total redos: %d\n", nredone);

  return SUCCESS;

failed:
  return error_trace (e);
}
