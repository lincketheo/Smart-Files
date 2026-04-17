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
#include "pager.h"
#include "pages/fsm_page.h"
#include "c_specx.h"

////////////////////////////////////////////////////////////
// ANALYSIS (Figure 10)

static void
update_max_txid (txid *cur, const stxid candidate)
{
  if (candidate >= 0 && candidate > (stxid)*cur)
    {
      *cur = candidate;
    }
}

struct wal_txnt_error
{
  const struct txn_table *t;
  struct os_wal *w;
  error *e;
};

/*
 * Append END records to transactions that finished during a prior recovery
 * but whose END records were never written.
 *
 * Two cases require this:
 *   - TX_COMMITTED: the COMMIT record is in the log but the process crashed
 *     before writing END.
 *   - TX_CANDIDATE_FOR_UNDO with undo_next_lsn == 0: undo reached the BEGIN
 *     record but END was not written (crashed during undo phase).
 *
 * After this call, all completed transactions are removed from the ATT so
 * the undo phase only sees transactions that genuinely need rolling back.
 */
static void
aries_append_end_records (struct txn *tx, void *ctx)
{
  const struct wal_txnt_error *_ctx = ctx;
  if (_ctx->e->cause_code)
    {
      return;
    }

  // Reached the bottom of the chain
  bool base = (((tx->data.state == TX_CANDIDATE_FOR_UNDO)
                && tx->data.undo_next_lsn == 0));

  // Finished commit but no End Record
  bool committed = tx->data.state == TX_COMMITTED;

  if (base || committed)
    {
      // Append an end log
      const slsn l = oswal_append_end_log (_ctx->w, tx->tid, tx->data.last_lsn, _ctx->e);
      if (l < 0)
        {
          return;
        }

      // State -> Done
      txn_update_state (tx, TX_DONE);
    }
}

static err_t
pgr_analysis_finish_some_open_txn_ptrs (
    const struct pager *p,
    const struct aries_ctx *ctx,
    error *e)
{
  // Loop through and append end records
  struct wal_txnt_error _ctx = {
    .t = ctx->txt,
    .w = p->ww,
    .e = e,
  };

  txnt_foreach (ctx->txt, aries_append_end_records, &_ctx);

  if (e->cause_code)
    {
      return error_trace (e);
    }

  // Remove them all from the table
  struct txn **txn_ptrs = ctx->txn_ptrs.data;
  for (u32 i = 0; i < ctx->txn_ptrs.nelem; ++i)
    {
      if (txn_ptrs[i]->data.state == TX_DONE)
        {
          txnt_remove_txn_expect (ctx->txt, txn_ptrs[i]);
        }
    }

  return SUCCESS;
}

/*
 * Find or create the ATT entry for the transaction referenced by a log record.
 *
 * If the transaction is already in the ATT, its last_lsn and undo_next_lsn
 * are advanced to reflect the new record.  If it is not, a new txn is
 * allocated from the slab, appended to txn_ptrs (so it can be freed later),
 * initialized as TX_CANDIDATE_FOR_UNDO, and inserted into the ATT.
 *
 * Setting undo_next_lsn to prev_lsn here is the standard ARIES bookkeeping:
 * after analysis completes, following undo_next_lsn backwards through the
 * chain replays all updates in reverse order.
 */
static struct txn *
pgr_analysis_get_associated_txn (
    struct pager *p,
    struct aries_ctx *ctx,
    const lsn read_lsn,
    const struct wal_rec_hdr_read *log_rec,
    error *e)
{
  struct txn *tx = NULL;
  const stxid tid = wrh_get_tid (log_rec);
  const slsn prev_lsn = wrh_get_prev_lsn (log_rec);

  // IF trans related record & LogRec.TransID NOT in TRANS_TABLE THEN
  // insert(LOGRec.TransID, 'U', LogRec.LSN, LogRec.PrevLSN)
  if (!txnt_get (&tx, ctx->txt, tid))
    {
      // Allocate
      tx = slab_alloc_alloc (&ctx->alloc, e);
      if (tx == NULL)
        {
          return NULL;
        }

      if (dblb_append (&ctx->txn_ptrs, &tx, 1, e))
        {
          return NULL;
        }

      // Fetch the previous lsn
      ASSERT (prev_lsn >= 0);

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

  return tx;
}

err_t
pgr_restart_analysis (struct pager *p, struct aries_ctx *ctx, error *e)
{
  i_log_info ("restart analysis\n");

  lsn read_lsn = 0;
  struct wal_rec_hdr_read *log_rec = NULL;

  // If we have a master record (checkpoint), start from there
  if (ctx->master_lsn > 0)
    {
      const struct wal_rec_hdr_read *master_rec = oswal_read_entry (p->ww, ctx->master_lsn, e);
      if (master_rec == NULL)
        {
          goto theend;
        }
      if (master_rec->type != WL_CKPT_BEGIN)
        {
          i_log_warn ("master LSN does not point to a checkpoint begin record");
          ctx->master_lsn = 0;
        }
      else
        {
          // Read log record following
          // Begin_Chkpt
          log_rec = oswal_read_next (p->ww, &read_lsn, e);
          if (log_rec == NULL)
            {
              goto theend;
            }
        }
    }

  // If no checkpoint, start from beginning
  if (ctx->master_lsn == 0)
    {
      log_rec = oswal_read_next (p->ww, &read_lsn, e);
      if (log_rec == NULL)
        {
          return error_trace (e);
        }
    }

  // Keep track of this as we go
  ctx->redo_lsn = 0;

  while (log_rec->type != WL_EOF)
    {
      update_max_txid (&ctx->max_tid, wrh_get_tid (log_rec));

      // Get or create the transaction associated with this log record
      struct txn *tx = pgr_analysis_get_associated_txn (p, ctx, read_lsn, log_rec, e);
      if (e->cause_code)
        {
          return error_trace (e);
        }

      switch (log_rec->type)
        {
        case WL_UPDATE:
          {
            // Trans_Table[LogRec.TransID].LastLSN
            // := LogRec.LSN
            // Trans_Table[LogRec.TransID].UndoNxtLSN
            // := LogRec.LSN
            txn_update_last_undo (tx, read_lsn, read_lsn);

            // IF LogRec.PageID not in
            // Dirty_Page_Table THEN
            //   Dirty_Page_Table[LogRec.PageID].RecLSN
            //   := LogRec.LSN
            switch (log_rec->update.type)
              {
              case WUP_PHYSICAL:
                {
                  const pgno pg = log_rec->update.phys.pg;
                  if (!dpgt_exists (ctx->dpt, pg))
                    {
                      if (dpgt_add (ctx->dpt, pg, read_lsn, e))
                        {
                          goto theend;
                        }
                    }
                  break;
                }
              case WUP_FSM:
                {
                  const pgno pg
                      = pgtofsm (log_rec->update.fsm.pg); // The affected page
                  if (!dpgt_exists (ctx->dpt, pg))
                    {
                      if (dpgt_add (ctx->dpt, pg, read_lsn, e))
                        {
                          goto theend;
                        }
                    }
                  break;
                }
              case WUP_FEXT:
                {
                  // Nothing
                  // to
                  // do
                  break;
                }
              }

            break;
          }
        case WL_CLR:
          {
            // Trans_Table[LogRec.TransID].LastLSN
            // := LogRec.LSN
            // Trans_Table[LogRec.TransID].UndoNxtLSN
            // := LogRec.UndoNxtLSN
            txn_update_last_undo (tx, read_lsn, log_rec->clr.undo_next);

            break;
          }
        case WL_CKPT_BEGIN:
          {
            // Skip this pointless
            // checkpoint
            break;
          }
        case WL_CKPT_END:
          {
            // FOR each entry in
            // LogRec.Tran_Table
            if (txnt_merge_into (ctx->txt, log_rec->ckpt_end.att,
                                 &ctx->txn_ptrs, &ctx->alloc, e))
              {
                goto theend;
              }

            // FOR each entry in
            // LogRec.Dirty_PageLst
            u32 ckpt_dpt_count = dpgt_get_size (log_rec->ckpt_end.dpt);

            if (dpgt_merge_into (ctx->dpt, log_rec->ckpt_end.dpt, e))
              {
                goto theend;
              }

            dpgt_close (log_rec->ckpt_end.dpt);
            txnt_close (log_rec->ckpt_end.att);
            if (log_rec->ckpt_end.txn_bank)
              {
                i_free (log_rec->ckpt_end.txn_bank);
              }

            break;
          }
        case WL_COMMIT:
          {
            // Update transaction state and
            // last LSN in the ATT
            txn_update_last_state (tx, read_lsn, TX_COMMITTED);
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
          return error_trace (e);
        }
    }

theend:
  // FOR EACH Trans_Table entry with (State == 'U') & (UndoNxtLSN = 0) DO
  //    write end record and remove entry from Trans_Table
  WRAP (pgr_analysis_finish_some_open_txn_ptrs (p, ctx, e));

  ctx->redo_lsn = dpgt_min_rec_lsn (ctx->dpt);

  return error_trace (e);
}
