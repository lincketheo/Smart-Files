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
#include "paging/pages/page.h"

static err_t
pgr_handle_physical_rollback_update (
    struct pager *p,
    struct wal_rec_hdr_read *log_rec,
    struct txn *tx,
    page_h *ph,
    lsn *undo_nxt_lsn,
    error *e)
{
  const pgno pg = log_rec->update.phys.pg;
  const lsn prev_lsn = log_rec->update.prev;

  // Fetch the target page
  WRAP (pgr_get_writable (ph, NULL, PG_PERMISSIVE, pg, p, e));

  // Do the update
  memcpy (ph->pgw->page.raw, log_rec->update.phys.undo, PAGE_SIZE);

  // Append a clr log
  const slsn clr_lsn = oswal_append_clr_log (
      p->ww,
      (struct wal_clr_write){
          .type = WCLR_PHYSICAL,
          .tid = log_rec->update.tid,
          .prev = tx->data.last_lsn,
          .undo_next = prev_lsn,
          .phys = {
              .pg = pg,
              .redo = log_rec->update.phys.undo,
          },
      },
      e);
  if (clr_lsn < 0)
    {
      pgr_release (p, ph, PG_PERMISSIVE, e);
      return error_trace (e);
    }

  // Flush the wal
  WRAP (oswal_flush_to (p->ww, clr_lsn, e));

  // Set the last page lsn
  page_set_page_lsn (page_h_w (ph), clr_lsn);

  // Set the update lsn
  txn_update_last (tx, clr_lsn);

  // Release the page
  WRAP (pgr_release (p, ph, PG_PERMISSIVE, e));

  // Set the undo next lsn
  *undo_nxt_lsn = prev_lsn;

  return SUCCESS;
}

static err_t
pgr_handle_fsm_rollback_update (
    struct pager *p,
    const struct wal_rec_hdr_read *log_rec,
    struct txn *tx,
    page_h *ph,
    lsn *undo_nxt_lsn,
    error *e)
{
  const pgno data_pg = log_rec->update.fsm.pg;
  const pgno fsm_pg = pgtofsm (data_pg);
  const lsn prev_lsn = log_rec->update.prev;

  // Fetch the target page
  WRAP (pgr_get_writable (ph, NULL, PG_PERMISSIVE, fsm_pg, p, e));

  // Do the main undo action
  if (log_rec->update.fsm.undo)
    {
      fsm_set_bit (page_h_w (ph), pgtoidx (data_pg));
    }
  else
    {
      fsm_clr_bit (page_h_w (ph), pgtoidx (data_pg));
    }

  // Append a clr
  const slsn clr_lsn = oswal_append_clr_log (
      p->ww,
      (struct wal_clr_write){
          .type = WCLR_FSM,
          .tid = log_rec->update.tid,
          .prev = tx->data.last_lsn,
          .undo_next = prev_lsn,
          .fsm = {
              .pg = data_pg,
              .redo = log_rec->update.fsm.undo,
          },
      },
      e);
  if (clr_lsn < 0)
    {
      pgr_release (p, ph, PG_PERMISSIVE, e);
      return error_trace (e);
    }

  // Flush the wal
  WRAP (oswal_flush_to (p->ww, clr_lsn, e));

  // Set the page lsn
  page_set_page_lsn (page_h_w (ph), clr_lsn);

  // Update the last_lsn of the tx
  txn_update_last (tx, clr_lsn);

  // Release this page
  WRAP (pgr_release (p, ph, PG_PERMISSIVE, e));

  // Set the undo_nxt_lsn to prev
  *undo_nxt_lsn = prev_lsn;

  return SUCCESS;
}

err_t
pgr_handle_rollback_update (
    struct pager *p,
    struct wal_rec_hdr_read *log_rec,
    struct txn *tx,
    page_h *ph,
    lsn *undo_nxt_lsn,
    error *e)
{
  ASSERT (log_rec->type == WL_UPDATE);
  switch (log_rec->update.type)
    {
    case WUP_PHYSICAL:
      {
        return pgr_handle_physical_rollback_update (p, log_rec, tx, ph, undo_nxt_lsn, e);
      }
    case WUP_FSM:
      {
        return pgr_handle_fsm_rollback_update (p, log_rec, tx, ph, undo_nxt_lsn, e);
      }
    case WUP_FEXT:
      {
        // File extension is a nested top action - it
        // cannot be rolled back and there shouldn't be
        // any logs that point to a file extension.
        // This is corruption
        UNREACHABLE ();
      }
    }
  UNREACHABLE ();
}
