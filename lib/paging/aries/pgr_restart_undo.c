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
#include "core/assert.h"
#include "paging/pager.h"
#include "paging/pages/fsm_page.h"

////////////////////////////////////////////////////////////
// UNDO (Figure 12)

/*
 * ARIES undo phase.
 *
 * Iterates until the ATT contains no TX_CANDIDATE_FOR_UNDO transactions.
 * Each iteration picks the transaction with the highest undo_next_lsn (to
 * process the most recent update first) and reads that log record:
 *
 *   WL_UPDATE — the update is physically undone (before-image applied) via
 *   pgr_apply_undo_update(), which writes a CLR record and advances
 *   undo_next_lsn to the update's prev_lsn.
 *
 *   WL_CLR — the CLR's undo_next field is used to skip over the section of
 *   the log that was already undone in a prior recovery or partial rollback.
 *
 *   WL_BEGIN — the transaction's entire update chain has been undone.  An
 *   END record is appended and the transaction is removed from the ATT.
 *
 * Because CLR records are written during both this phase and pgr_rollback(),
 * a second crash during undo will find the CLRs in the log and redo will
 * replay them, ensuring no update is undone twice.
 */
err_t
pgr_restart_undo (struct pager *p, struct aries_ctx *ctx, error *e)
{
  i_log_info ("restart undo\n");

  // WHILE EXISTS Trans_Table entry with Status=U DO
  while (true)
    {
      // UndoLsn = maximum(UndoNxtLSN) from Trans_Table
      // entries with State = 'U'
      const slsn undo_lsn = txnt_max_u_undo_lsn (ctx->txt);

      // !EXISTS Trans_Table entry with Status=U
      if (undo_lsn < 0)
        {
          break;
        }

      // LogRec = LogRead(UndoNxtLSN)
      struct wal_rec_hdr_read *log_rec = oswal_read_entry (p->ww, undo_lsn, e);
      if (log_rec == NULL)
        {
          return error_trace (e);
        }

      switch (log_rec->type)
        {
        case WL_UPDATE:
          {
            pgr_apply_undo_update (p, log_rec, ctx, e);
            break;
          }

        case WL_CLR:
          {
            struct txn *tx;
            txnt_get_expect (&tx, ctx->txt, log_rec->clr.tid);
            txn_update_undo_next (tx, log_rec->clr.undo_next);
            break;
          }

          // If LogRec.PrevLSN == 0 THEN
        case WL_BEGIN:
          {
            // Log_Write('end',
            // LogRec.TransID,
            // Trans_Table[LogRec[LogRec.TransId].LastLSN,
            // ...);
            WRAP (oswal_append_end_log (p->ww, log_rec->begin.tid, undo_lsn, e));

            // delete Trans_Table entry
            // where TransID =
            // LogRec.TransID
            struct txn *tx;
            txnt_get_expect (&tx, ctx->txt, log_rec->begin.tid);
            txnt_remove_txn_expect (ctx->txt, tx);
            break;
          }
        case WL_COMMIT:
        case WL_CKPT_BEGIN:
        case WL_CKPT_END:
        case WL_EOF:
        case WL_END:
          {
            UNREACHABLE ();
          }
        }
    }

  return SUCCESS;
}
