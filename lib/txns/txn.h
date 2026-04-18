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

#include "c_specx.h"
#include "lockt/lt_lock.h"
#include "nstypes.h"

/*
 * Transaction State
 *
 * A transaction goes through the following states during normal operation:
 *
 *   TX_RUNNING          — active; has been started but not yet committed or
 *                         aborted.
 *   TX_DONE             — committed and END record written; removed from ATT.
 *
 * During ARIES recovery two additional states arise:
 *
 *   TX_CANDIDATE_FOR_UNDO — transaction was active at crash time (no COMMIT
 *                           seen during analysis); must be rolled back in the
 *                           undo phase.  Corresponds to state 'U' (unprepared)
 *                           in the original ARIES paper.
 *   TX_COMMITTED          — COMMIT record seen but no END record; the undo
 *                           phase will skip this transaction; the analysis
 *                           phase will append its END record.  Corresponds to
 *                           state 'P' (prepared/committed) in the paper.
 *
 * The three LSN fields in txn_data serve distinct roles:
 *
 *   min_lsn         — LSN of the BEGIN record; used to bound how far back a
 *                     checkpoint must retain WAL records.
 *   last_lsn        — LSN of the most recent log record written by this
 *                     transaction; updated on every pgr_release() and
 *                     appended to COMMIT/END records so the next record in
 *                     this transaction's chain can be found.
 *   undo_next_lsn   — during normal operation equals last_lsn; during undo
 *                     decrements as each update is reversed, pointing to the
 *                     predecessor update that still needs to be undone.
 */
struct txn_data
{
  // In the ARIES paper:
  // - P (prepared / in doubt) - Transaction has
  // completed it's prepare phase in two phase
  // commit but hasn't yet received a commit / abort decision
  // - U (unprepared) - Transaction is active and hasn't prepared yet
  enum tx_state
  {
    // Not in the original ARIES paper,
    // using this for a placeholder during
    // normal execution, generally, it's just
    // removed once it's committed
    TX_RUNNING,

    // During restart, this just says we haven't
    // received a commit record yet, so we'll need
    // to remove it if it stays like this
    TX_CANDIDATE_FOR_UNDO, // (U)

    // Never actually set in normal processing - use
    // this in the restart recovery algorithm to know which
    // tx's to append end to
    TX_COMMITTED, // (P)

    // Just a special case for done transactions with end record
    // appended. You'd just remove the transaction from the
    // table when done
    TX_DONE,
  } state;

  /**
   * The minimum lsn of this transaction.
   *
   * This is the BEGIN record of the transaction
   */
  lsn min_lsn;

  /**
   * The maximum lsn of this transaction.
   *
   * This is the most recent log message
   * recorded on the log
   */
  lsn last_lsn;

  /**
   * During regular operation - this is just equivalent
   * to last_lsn.
   *
   * It's the next lsn we need to read
   * in the process of undoing. So during rollback or recovery
   * This number decreases as we work our way backwards through
   * the log records of this transaction
   */
  lsn undo_next_lsn;
};

struct txn_lock
{
  struct lt_lock lock;
  enum lock_mode mode;
  struct txn_lock *next;
};

struct txn
{
  txid tid;                     // Transaction id
  struct txn_data data;         // The transaction data
  struct hnode node;            // The node that indicates where this txn is in the att
  struct txn_lock *locks;       // All held locks for this transaction
  struct slab_alloc lock_alloc; // Allocates txn_locks
  latch l;                      // Thread safety
};

void txn_init (struct txn *dest, txid tid, struct txn_data data);
void txn_key_init (struct txn *dest, txid tid);

// Atomic Updates
void txn_update_data (struct txn *t, struct txn_data data);
void txn_update (struct txn *t, enum tx_state state, lsn last, lsn undo_next);
void txn_update_state (struct txn *t, enum tx_state new_state);
void txn_update_last_undo (struct txn *t, lsn last_lsn, lsn undo_next_lsn);
void txn_update_last_state (struct txn *t, lsn last_lsn,
                            enum tx_state new_state);
void txn_update_last (struct txn *t, lsn last_lsn);
void txn_update_undo_next (struct txn *t, lsn undo_next);

// Equality
bool txn_data_equal_unsafe (const struct txn_data *left, const struct txn_data *right);

// Locking
typedef void (*lock_func) (struct lt_lock lock, enum lock_mode mode, void *ctx);
err_t txn_newlock (struct txn *t, struct lt_lock lock, enum lock_mode mode,
                   error *e);
bool txn_haslock (struct txn *t, struct lt_lock lock);
void txn_close (struct txn *t);
void txn_foreach_lock (struct txn *t, lock_func func, void *ctx);

// Utilities
void i_log_txn (int log_level, struct txn *tx);
