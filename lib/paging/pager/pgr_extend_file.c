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

#include "core/error.h"
#include "paging/pager.h"

/*
 * Extend the database file by npages and record it as a Nested Top Action.
 *
 * File extension is a physical operation (adding pages to the file) that
 * must be undoable if the transaction rolls back, but must NOT be re-undone
 * during a subsequent crash if it was already undone once.  ARIES models
 * this as a Nested Top Action (NTA):
 *
 *   1. Save the current tx's last_lsn as undo_next (the LSN to jump back
 *      to during undo, skipping the extension entirely).
 *
 *   2. Write a WUP_FEXT update record with undo=current_npages and
 *      redo=npages_after_extension.  This record lets redo replay the
 *      extension and undo shrink the file.
 *
 *   3. Write a WCLR_DUMMY CLR whose undo_next points to the LSN saved in
 *      step 1.  During rollback, when the undo cursor hits this CLR it
 *      jumps directly to undo_next, skipping the WUP_FEXT record and
 *      leaving the file extension undone already.  After a crash-during-undo
 *      the CLR is replayed by redo but has no physical effect, guaranteeing
 *      idempotency.
 *
 *   4. Advance tx's last_lsn and undo_next_lsn to the CLR's LSN so the
 *      undo chain is correctly anchored.
 *
 *   5. Actually extend the file on disk.
 */
err_t
pgr_extend_file (const struct pager *p, const pgno npages, struct txn *tx, error *e)
{
  // Do a Nested Top Action

  // 1. Ascertain the position of the current tx's last log record
  const lsn undo_next = tx->data.last_lsn;

  // 2. Logging the redo and undp information
  slsn top_lsn = oswal_append_update_log (p->ww,
                                          (struct wal_update_write){
                                              .type = WUP_FEXT,
                                              .tid = tx->tid,
                                              .prev = tx->data.last_lsn,
                                              .fext = {
                                                  .undo = ospgr_get_npages (p->fp),
                                                  .redo = npages,
                                              },
                                          },
                                          e);

  // 3. Writing a dummy CLR whose UNL points to the log record whose
  // position was remembered in 1
  top_lsn = oswal_append_clr_log (p->ww,
                                  (struct wal_clr_write){
                                      .type = WCLR_DUMMY,
                                      .tid = tx->tid,
                                      .prev = tx->data.last_lsn,
                                      .undo_next = undo_next,
                                  },
                                  e);

  // 4. Anchor both LSN fields to the CLR so the undo chain jumps over the
  // FEXT
  tx->data.last_lsn = top_lsn;
  tx->data.undo_next_lsn = top_lsn;

  // 5. Physically extend the file; if this fails the WAL records are already
  // durable
  if (ospgr_extend (p->fp, npages, e))
    {
      return error_trace (e);
    }

  return error_trace (e);
}
