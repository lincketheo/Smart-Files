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
#include "tlclib/dev/assert.h"
#include "tlclib/dev/error.h"
#include "paging/pager.h"
#include "paging/pages/fsm_page.h"

static err_t
pgr_apply_physical_undo_update (
    struct pager *p,
    struct wal_rec_hdr_read *log_rec,
    const struct aries_ctx *ctx,
    error *e)
{
  struct txn *tx;
  const txid tid = log_rec->update.tid;
  const lsn prev = log_rec->update.prev;

  page_h ph = page_h_create ();

  // Fetch the physical page number from disk
  if (pgr_get_writable (&ph, NULL, PG_PERMISSIVE, log_rec->update.phys.pg, p, e))
    {
      goto failed;
    }

  // Copy the undo data to that page
  memcpy (page_h_w (&ph)->raw, log_rec->update.phys.undo, PAGE_SIZE);

  // Get the transaction for this tid
  // It should be in the table - from earlier
  txnt_get_expect (&tx, ctx->txt, tid);

  // Append a clr log
  const slsn l = oswal_append_clr_log (
      p->ww,
      (struct wal_clr_write){
          .type = WCLR_PHYSICAL,
          .tid = tid,
          .prev = tx->data.last_lsn,
          .undo_next = prev,
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

  // Flush up to that lsn
  if (oswal_flush_to (p->ww, l, e))
    {
      goto failed;
    }

  // Set the page lsn
  page_set_page_lsn (page_h_w (&ph), l);

  // Update the last lsn of the transaction
  txn_update_last (tx, l);

  // Release this page
  if (pgr_release (p, &ph, PG_PERMISSIVE, e))
    {
      goto failed;
    }

  // Update undo next page
  txn_update_undo_next (tx, prev);

  return SUCCESS;

failed:
  pgr_cancel_if_exists (p, &ph);
  return e->cause_code;
}

static err_t
pgr_apply_fsm_undo_update (
    struct pager *p,
    const struct wal_rec_hdr_read *log_rec,
    const struct aries_ctx *ctx,
    error *e)
{
  struct txn *tx;
  const txid tid = log_rec->update.tid;
  const lsn prev = log_rec->update.prev;
  const pgno fsm_pg = pgtofsm (log_rec->update.fsm.pg);

  page_h ph = page_h_create ();
  WRAP (pgr_get_writable (&ph, NULL, PG_FREE_SPACE_MAP, fsm_pg, p, e));

  if (log_rec->update.fsm.undo)
    {
      fsm_set_bit (page_h_w (&ph), pgtoidx (log_rec->update.fsm.pg));
    }
  else
    {
      fsm_clr_bit (page_h_w (&ph), pgtoidx (log_rec->update.fsm.pg));
    }

  txnt_get_expect (&tx, ctx->txt, tid);

  const slsn l = oswal_append_clr_log (p->ww,
                                       (struct wal_clr_write){
                                           .type = WCLR_FSM,
                                           .tid = tid,
                                           .prev = tx->data.last_lsn,
                                           .undo_next = prev,
                                           .fsm = {
                                               .pg = log_rec->update.fsm.pg,
                                               .redo = log_rec->update.fsm.undo,
                                           },
                                       },
                                       e);
  WRAP (l);
  WRAP (oswal_flush_to (p->ww, l, e));

  page_set_page_lsn (page_h_w (&ph), l);
  txn_update_last (tx, l);

  WRAP (pgr_release (p, &ph, PG_PERMISSIVE, e));

  txn_update_undo_next (tx, prev);

  return SUCCESS;
}

static err_t
pgr_apply_file_extend_undo_update (struct pager *p,
                                   const struct wal_rec_hdr_read *log_rec,
                                   const struct aries_ctx *ctx, error *e)
{
  struct txn *tx;
  const txid tid = log_rec->update.tid;
  const lsn prev = log_rec->update.prev;

  txnt_get_expect (&tx, ctx->txt, tid);
  txn_update_undo_next (tx, prev);

  return SUCCESS;
}

err_t
pgr_apply_undo_update (
    struct pager *p,
    struct wal_rec_hdr_read *log_rec,
    struct aries_ctx *ctx,
    error *e)
{
  ASSERT (log_rec->type == WL_UPDATE);
  switch (log_rec->update.type)
    {
    case WUP_PHYSICAL:
      {
        return pgr_apply_physical_undo_update (p, log_rec, ctx, e);
      }
    case WUP_FSM:
      {
        return pgr_apply_fsm_undo_update (p, log_rec, ctx, e);
      }
    case WUP_FEXT:
      {
        return pgr_apply_file_extend_undo_update (p, log_rec, ctx, e);
      }
    }
  UNREACHABLE ();
}
