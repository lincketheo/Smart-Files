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
#include "c_specx/dev/assert.h"
#include "pager.h"
#include "pages/fsm_page.h"
#include "txns/txn_table.h"
#include "wal/wal_rec_hdr.h"

////////////////////////////////////////////////////////////
// UNDO (Figure 12)

err_t
pgr_restart_undo (struct pager *p, struct aries_ctx *ctx, error *e)
{
  i_log_info ("Starting Undo phase.\n");

  while (true)
    {
      slsn undo_lsn = txnt_max_u_undo_lsn (ctx->txt);
      if (undo_lsn < 0)
        {
          break;
        }

      struct wal_rec_hdr_read *log_rec = oswal_read_entry (p->ww, undo_lsn, e);
      if (log_rec == NULL)
        {
          goto failed;
        }

      switch (log_rec->type)
        {
        case WL_UPDATE:
          {
            struct txn *tx;
            txnt_get_expect (&tx, ctx->txt, log_rec->update.tid);

            if (wrh_is_undoable (log_rec))
              {
                page_h ph = page_h_create ();
                if (pgr_get_writable (&ph, NULL, PG_PERMISSIVE, log_rec->update.phys.pg, p, e))
                  {
                    goto failed;
                  }

                wrh_undo (log_rec, &ph);

                // Append a clr log
                slsn l = oswal_append_clr_log (
                    p->ww,
                    (struct wal_clr_write){
                        .type = WCLR_PHYSICAL,
                        .tid = log_rec->update.tid,
                        .prev = tx->data.last_lsn,
                        .undo_next = log_rec->update.prev,
                        .phys = {
                            .pg = log_rec->update.phys.pg,
                            .redo = log_rec->update.phys.undo,
                        },
                    },
                    e);
                if (l < 0)
                  {
                    goto failed;
                  }

                // Set the page lsn
                page_set_page_lsn (page_h_w (&ph), l);

                // Update the last lsn of the transaction
                tx->data.last_lsn = l;

                // Release this page
                pgr_unfix (p, &ph, PG_PERMISSIVE);
              }

            // Update undo next page
            tx->data.undo_next_lsn = log_rec->update.prev;

            if (log_rec->update.prev == 0)
              {
                slsn l = oswal_append_end_log (p->ww, tx->tid, tx->data.last_lsn, e);
                if (l < 0)
                  {
                    goto failed;
                  }
                txnt_remove_txn_expect (ctx->txt, tx);
                txn_update_state (tx, TX_DONE);
              }
            break;
          }

        case WL_CLR:
          {
            struct txn *tx;
            txnt_get_expect (&tx, ctx->txt, log_rec->clr.tid);
            tx->data.undo_next_lsn = log_rec->clr.undo_next;
            break;
          }

        case WL_BEGIN:
          {
            struct txn *tx;
            txnt_get_expect (&tx, ctx->txt, log_rec->begin.tid);

            slsn l = oswal_append_end_log (p->ww, tx->tid, tx->data.last_lsn, e);
            if (l < 0)
              {
                goto failed;
              }
            txnt_remove_txn_expect (ctx->txt, tx);
            txn_update_state (tx, TX_DONE);
            break;
          }
        case WL_COMMIT:
        case WL_EOF:
        case WL_END:
          {
            UNREACHABLE ();
          }
        }
    }

  i_log_info ("Undo phase done.\n");

  return SUCCESS;

failed:
  return error_trace (e);
}
