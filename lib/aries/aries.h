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

#pragma once

#include "dpgt/dirty_page_table.h"
#include "pager.h"
#include "c_specx.h"
#include "txns/txn_table.h"
#include "wal/wal_rec_hdr.h"

struct aries_ctx
{
  /**
   * This is the lsn of the most
   * recently completed checkpoint
   */
  lsn master_lsn;

  /**
   * At the end of the analysis phase,
   * this is the minimum recovery lsn
   *
   * It's the minimum page we need to read first in
   * the restart phase on recovery
   */
  lsn redo_lsn;

  /**
   * We keep track of the maximum transaction id that
   * we see in the database in order to pick up where we left
   * off
   */
  txid max_tid;

  /**
   * These are the reconstruction of the active
   * transaction table and the dirty page table
   * while we run recovery.
   *
   * They are ephemral and will be destroyed at the
   * end of recovery. Then the pager will create
   * them again because we're in a clean state
   */
  struct txn_table *txt;
  struct dpg_table *dpt;

  /**
   * While we scan through the log, we'll
   * be adding transactions to the transaction table
   * and we need a place to allocate / put those transactions
   * (normally we do it on the stack)
   */
  struct dbl_buffer txn_ptrs;
  struct slab_alloc alloc;
};

err_t aries_ctx_create (struct aries_ctx *dest, lsn master_lsn, error *e);
void aries_ctx_free (struct aries_ctx *ctx);

/**
 * @brief Top-level recovery entry point
 *
 * Reads the master record to locate the last checkpoint LSN, then drives the
 * three ARIES recovery phases — analysis, redo, undo — in order. The master
 * record also provides the LSN of the begin-checkpoint log record so that
 * analysis can reconstruct the transaction table and dirty page table from a
 * known-good starting point rather than replaying the entire WAL from the
 * beginning. On return the database is in a consistent, transaction-complete
 * state and normal operation may resume.
 *
 * @param p   The pager to recover
 * @param ctx The ARIES recovery context, populated and consumed across all
 *            three phases
 * @param e   The error object
 */
err_t pgr_restart (struct pager *p, struct aries_ctx *ctx, error *e);

/**
 * @brief ARIES analysis pass
 *
 * Scans the WAL forward from the last checkpoint log record. Initialises the
 * transaction table and dirty page table from the checkpoint's snapshot, then
 * updates them as each subsequent log record is examined:
 *
 *   - UPDATE records add the affected page to the dirty page table (if absent,
 *     with recLSN = the record's LSN) and update the transaction's lastLSN in
 *     the transaction table.
 *   - CLR records update lastLSN and set the transaction's undoNextLSN to the
 *     CLR's undoNextLSN, so that undo can resume from the correct point if a
 *     second crash occurs mid-undo.
 *   - COMMIT / END records remove the transaction from the transaction table or
 *     mark it committed so that undo ignores it.
 *
 * After the scan, redoLSN is set to the minimum recLSN across all dirty page
 * table entries — the earliest WAL position that redo must revisit. Any
 * transaction still present and uncommitted in the table at the end of the scan
 * is a loser transaction that undo must roll back.
 *
 * @param p   The pager to recover
 * @param ctx The ARIES recovery context; on return ctx->tt holds the
 *            transaction table, ctx->dpt holds the dirty page table, and
 *            ctx->redo_lsn holds the computed redoLSN
 * @param e   The error object
 */
err_t pgr_restart_analysis (struct pager *p, struct aries_ctx *ctx, error *e);

/**
 * @brief ARIES redo pass
 *
 * Scans the WAL forward from redoLSN and re-applies every update that may not
 * have reached durable storage before the crash. For each log record examined:
 *
 *   - If the affected page is not in the dirty page table, the update was
 *     already flushed before the crash and is skipped.
 *   - If the record's LSN < the page's recLSN in the dirty page table, an
 *     even later update already covers this change and it is skipped.
 *   - Otherwise the page is fetched and its pageLSN is checked. If pageLSN >=
 *     the record's LSN the update is already on disk and is skipped. If
 *     pageLSN < the record's LSN the update is re-applied and pageLSN is set
 *     to the record's LSN.
 *
 * CLRs are treated as ordinary redo records — their redo action is applied
 * under the same conditions. This ensures that any undo work partially
 * completed before a prior crash is also re-applied. After this pass the
 * on-disk database state is identical to the state at the instant of the crash,
 * including all uncommitted changes made by loser transactions.
 *
 * @param p   The pager to recover
 * @param ctx The ARIES recovery context; ctx->redo_lsn and ctx->dpt must be
 *            populated by the analysis pass before calling this
 * @param e   The error object
 */
err_t pgr_restart_redo (struct pager *p, struct aries_ctx *ctx, error *e);

/**
 * @brief ARIES undo pass
 *
 * Rolls back all loser transactions identified by the analysis pass, processing
 * them in strict reverse LSN order to preserve the before-image chain. For
 * each loser, follows the lastLSN → prevLSN chain backwards through the WAL:
 *
 *   - For each UPDATE record encountered, the before-image is restored to the
 *     page and a CLR is written to the WAL. The CLR's undoNextLSN is set to
 *     the update's prevLSN so that a crash during undo can re-enter the undo
 *     chain at exactly the right record without re-undoing anything already
 *     covered by a CLR.
 *   - CLRs encountered during the backward scan are not re-undone; instead
 *     their undoNextLSN is followed directly, skipping over log records whose
 *     undo was already written in a prior recovery attempt.
 *   - Nested top actions (such as file extension) are skipped entirely, as
 *     they are non-undoable by design and their effects are intentionally
 *     durable regardless of transaction outcome.
 *
 * The pass completes when the transaction table is empty (every loser has been
 * fully rolled back and had an END record written). At that point the database
 * contains only the effects of committed transactions and is fully consistent.
 *
 * @param p   The pager to recover
 * @param ctx The ARIES recovery context; ctx->tt must be populated by the
 *            analysis pass before calling this
 * @param e   The error object
 */
err_t pgr_restart_undo (struct pager *p, struct aries_ctx *ctx, error *e);
/**
 * Depends on the update type,
 * but generally speaking:
 *
 * 1. If the affected page lsn is not in the dirty page table   -> skip
 *      - This page
 * 2. If affected page lsb >= redo_lsn                          -> skip
 * 3. If affected page rec_lsn > redo_lsn                       -> skip
 */
err_t pgr_apply_redo (
    struct pager *p,
    struct wal_rec_hdr_read *log_rec,
    struct aries_ctx *ctx,
    error *e);

/// Applies a CLR log to match a cooresponding undo log. We just read in [log_rec]
/// from the wal which is an update log. This method appends a CLR to the end of the
/// WAL and updates the affected page from [log_rec] to take back it's original data
err_t pgr_apply_undo_update (
    struct pager *p,
    struct wal_rec_hdr_read *log_rec,
    struct aries_ctx *ctx,
    error *e);

/// Different from apply_undo_update in that it is not attached to a recovery context
/// just a plain old rollback. We just read an update log record
err_t pgr_handle_rollback_update (
    struct pager *p,
    struct wal_rec_hdr_read *log_rec,
    struct txn *tx,
    page_h *ph,
    lsn *undo_nxt_lsn,
    error *e);
