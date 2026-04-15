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
#include "core/max_capture.h"
#include "paging/pager.h"

////////////////////////////////////////////////////////////
// REDO (FIGURE 11)

/*
 * ARIES redo phase.
 *
 * Scans the WAL forward from redo_lsn (the minimum RecLSN in the DPT) to
 * EOF, replaying every UPDATE and CLR record via pgr_apply_redo().  Records
 * for other types (BEGIN, COMMIT, END, CKPT_BEGIN) are skipped; CKPT_END
 * records have their ATT/DPT copies freed since they were already consumed by
 * the analysis phase.
 *
 * Redo is intentionally repeating history: it applies every update whose
 * effect may not have reached disk, including updates from transactions that
 * will subsequently be rolled back in the undo phase.  This restores the
 * exact pre-crash state so that undo produces correct before-images.
 */
err_t
pgr_restart_redo (struct pager *p, struct aries_ctx *ctx, error *e)
{
  i_log_info ("restart redo\n");

  // Open_Log_Scan(RedoLSN)
  // LogRec = Next_Log()
  struct wal_rec_hdr_read *log_rec = oswal_read_entry (p->ww, ctx->redo_lsn, e);
  if (log_rec == NULL)
    {
      return error_trace (e);
    }

  // While NOT(End_Of_Log) DO;
  while (log_rec->type != WL_EOF)
    {
      update_max_txid (&ctx->max_tid, wrh_get_tid (log_rec));

      switch (log_rec->type)
        {
        case WL_UPDATE:
        case WL_CLR:
          {
            if (pgr_apply_redo (p, log_rec, ctx, e))
              {
                return error_trace (e);
              }
            break;
          }
        case WL_CKPT_END:
          {
            // Checkpoint end records
            // during redo need their
            // ATT/DPT freed since we don't
            // use them in redo phase (only
            // in analysis)
            txnt_close (log_rec->ckpt_end.att);
            dpgt_close (log_rec->ckpt_end.dpt);
            break;
          }
        default:
          {
            // Do nothing
            break;
          }
        }

      // Read next log record
      log_rec = oswal_read_next (p->ww, &ctx->redo_lsn, e);
      if (log_rec == NULL)
        {
          return error_trace (e);
        }
    }

  // Switch back to write mode
  return SUCCESS;
}
