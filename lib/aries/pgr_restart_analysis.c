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

#include "aries/aries.h"
#include "c_specx.h"
#include "c_specx/dev/error.h"
#include "c_specx/intf/logging.h"
#include "dpgt/dirty_page_table.h"
#include "pager.h"
#include "pages/fsm_page.h"
#include "txns/txn.h"
#include "txns/txn_table.h"
#include "wal/wal_rec_hdr.h"

////////////////////////////////////////////////////////////
// ANALYSIS (Figure 10)

err_t
pgr_restart_analysis (struct pager *p, struct aries_ctx *ctx, error *e)
{
  i_log_info ("Starting Analysis phase\n");

  lsn read_lsn = 0;
  struct wal_rec_hdr_read *log_rec = oswal_read_next (p->ww, &read_lsn, e);
  if (log_rec == NULL)
    {
      goto failed;
    }

  // Keep track of this as we go
  ctx->redo_lsn = 0;

  while (log_rec->type != WL_EOF)
    {
      stxid tid = wrh_get_tid (log_rec);
      struct txn *tx = NULL;

      if (tid >= 0)
        {
          if (tid > (stxid)ctx->max_tid)
            {
              ctx->max_tid = tid;
            }

          slsn prev_lsn = wrh_get_prev_lsn (log_rec);
          ASSERT (prev_lsn >= 0);

          // Get or create the transaction associated with this log record
          if (!txnt_get (&tx, ctx->txt, tid))
            {
              // Allocate
              tx = aries_ctx_txn_alloc (ctx, e);
              if (tx == NULL)
                {
                  goto failed;
                }

              txn_init (tx, tid,
                        (struct txn_data){
                            .state = TX_CANDIDATE_FOR_UNDO,
                            .last_lsn = read_lsn,
                            .undo_next_lsn = prev_lsn,
                        });

              // Insert this transaction
              txnt_insert_txn_if_not_exists (ctx->txt, tx);
            }
          else
            {
              txn_update (tx, TX_CANDIDATE_FOR_UNDO, read_lsn, prev_lsn);
            }
        }

      switch (log_rec->type)
        {
        case WL_UPDATE:
        case WL_CLR:
          {
            tx->data.last_lsn = read_lsn;

            if (log_rec->type == WL_UPDATE)
              {
                if (wrh_is_undoable (log_rec))
                  {
                    tx->data.undo_next_lsn = read_lsn;
                  }
              }
            else
              {
                tx->data.undo_next_lsn = log_rec->clr.undo_next;
              }

            if (wrh_is_redoable (log_rec))
              {
                if (dpgt_add_if_ne (ctx->dpt, wrh_get_affected_pg (log_rec), read_lsn, e))
                  {
                    goto failed;
                  }
              }

            break;
          }
        case WL_COMMIT:
          {
            tx->data.last_lsn = read_lsn;
            tx->data.state = TX_COMMITTED;
            break;
          }
        case WL_BEGIN:
          {
            break;
          }
        case WL_END:
          {
            txnt_remove_txn_expect (ctx->txt, tx);
            break;
          }
        case WL_EOF:
          {
            UNREACHABLE ();
          }
        }

      log_rec = oswal_read_next (p->ww, &read_lsn, e);
      if (log_rec == NULL)
        {
          goto failed;
        }
    }

  u32 before = txnt_get_size (ctx->txt);
  i_log_info ("Analysis phase, txns in table: %d\n", before);

  // Append end logs and remove rolled back and committed txns
  for (u32 i = 0; i < ctx->txn_ptrs.nelem; ++i)
    {
      struct txn *tx = ((struct txn **)ctx->txn_ptrs.data)[i];

      bool nothing_to_do = tx->data.state == TX_CANDIDATE_FOR_UNDO && tx->data.undo_next_lsn == 0;
      bool committed = tx->data.state == TX_COMMITTED;

      if (nothing_to_do || committed)
        {
          // Append an end log
          const slsn l = oswal_append_end_log (p->ww, tx->tid, tx->data.last_lsn, e);
          if (l < 0)
            {
              goto failed;
            }
          txnt_remove_txn_expect (ctx->txt, tx);
          txn_update_state (tx, TX_DONE);
        }
    }

  ctx->redo_lsn = dpgt_get_size (ctx->dpt) > 0 ? dpgt_min_rec_lsn (ctx->dpt) : 0;

  i_log_info ("Analysis phase: %d txns were removed\n", before - txnt_get_size (ctx->txt));
  i_log_info ("Done with Analysis. RedoLSN = %" PRlsn "\n", ctx->redo_lsn);

  return SUCCESS;

failed:
  return error_trace (e);
}
