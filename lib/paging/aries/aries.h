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

#include "core/slab_alloc.h"
#include "paging/dpgt/dirty_page_table.h"
#include "paging/pager.h"
#include "paging/txns/txn_table.h"
#include "paging/wal/wal_rec_hdr.h"

/*
 * ARIES Recovery
 *
 * ARIES (Algorithm for Recovery and Isolation Exploiting Semantics) provides
 * crash recovery for the pager.  On restart, the pager invokes pgr_restart()
 * which runs three sequential phases:
 *
 *   ANALYSIS — Scans the WAL forward from the last checkpoint (master_lsn)
 *   to EOF.  Reconstructs two in-memory tables: the Active Transaction Table
 *   (ATT) and the Dirty Page Table (DPT).  The ATT records every transaction
 *   that had not committed before the crash; the DPT records every page that
 *   had been modified but not flushed.  The minimum RecLSN across all DPT
 *   entries becomes redo_lsn — the earliest point from which redo must start.
 *   Transactions that already committed (TX_COMMITTED) or that completed
 *   their undo chain (TX_CANDIDATE_FOR_UNDO with undo_next_lsn == 0) have
 *   their END records appended here so the WAL is complete going forward.
 *
 *   REDO — Scans forward from redo_lsn, replaying every UPDATE and CLR
 *   record whose page is in the DPT and whose pageLSN is older than the
 *   record's LSN.  This restores the database to the state it was in at
 *   crash time, including the partial effects of uncommitted transactions.
 *
 *   UNDO — Iterates over the ATT in reverse LSN order, reading each
 *   transaction's update chain backwards and writing CLR (Compensation Log
 *   Record) records.  CLRs are idempotent: if the process crashes again
 *   during undo, redo will replay the CLRs rather than the original updates,
 *   and undo will skip past them via undo_next.  Each transaction is removed
 *   from the ATT once its BEGIN record is reached.
 *
 * The aries_ctx struct carries the ephemeral state across all three phases.
 * It is freed (including the temporary ATT and DPT) at the end of
 * pgr_restart().
 */
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

// Main functions
err_t pgr_restart (struct pager *p, struct aries_ctx *ctx, error *e);
err_t pgr_restart_analysis (struct pager *p, struct aries_ctx *ctx, error *e);
err_t pgr_restart_redo (struct pager *p, struct aries_ctx *ctx, error *e);
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
err_t pgr_apply_redo (struct pager *p, struct wal_rec_hdr_read *log_rec,
                      struct aries_ctx *ctx, error *e);

err_t pgr_apply_undo_update (struct pager *p, struct wal_rec_hdr_read *log_rec,
                             struct aries_ctx *ctx, error *e);

err_t pgr_handle_rollback_update (struct pager *p,
                                  struct wal_rec_hdr_read *log_rec,
                                  struct txn *tx, page_h *ph,
                                  lsn *undo_nxt_lsn, error *e);
