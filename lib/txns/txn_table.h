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
#include "txns/txn.h"

/*
 * Active Transaction Table (ATT)
 *
 * The ATT is an intrusive hash table keyed by txid.  It tracks every
 * transaction that is currently in flight or that was active at the time of
 * the last crash.  The pager holds the authoritative ATT; a second, ephemeral
 * ATT is constructed inside aries_ctx during recovery and discarded when
 * recovery completes.
 *
 * The ATT has two primary uses:
 *   1. Undo navigation during pgr_rollback() and the ARIES undo phase:
 *      txnt_max_u_undo_lsn() returns the highest undo_next_lsn among all
 *      TX_CANDIDATE_FOR_UNDO entries, driving the undo loop.
 *   2. WAL truncation: txnt_min_lsn() returns the lowest min_lsn in the
 *      table; WAL records before this LSN are not needed by any active
 *      transaction and can be reclaimed at checkpoint time.
 *
 * Fuzzy checkpoint serialization requires a consistent snapshot of the ATT
 * while new log records may still be arriving.  txnt_freeze_active_txns_for_
 * serialization() locks the table and individual transaction latches until
 * each transaction has been serialized into the checkpoint END record.
 */
struct txn_table;

// Lifecycle
struct txn_table *txnt_open (error *e);
void txnt_close (struct txn_table *t);

/**
 * Locking / Unlocking and Serializing
 *
 * For fuzzy checkpoints, manually locking
 * is needed for the transaction table while
 * serializing the transaction
 *
 * The basic pattern is:
 *
 * lock(table.txns);
 *
 * for each txn in table:
 *    do something with txn
 *    unlock(txn);
 *
 * Nothing else to do - all txns are unlocked.
 *
 * Under the hood, for now this just locks the entire txn table
 * until the number of unlocks is equal to the number
 * of txns, but in the future this can be optimized
 */
void txnt_freeze_active_txns_for_serialization (struct txn_table *t);

/*
 * This Should only be used on error / failure
 * and should not be called after serialize
 * serialize already unfreezes iteratively
 */
void txnt_unfreeze (struct txn_table *t);

// Returns the same number as [txnt_get_serialize_size]
u32 txnt_serialize (u8 *dest, u32 dlen, struct txn_table *t);

// Deserialization - this has the same behavior as txnt_open
struct txn_table *txnt_deserialize (
    const u8 *src,
    struct txn *txn_bank,
    u32 slen,
    error *e);

// Returns the number of bytes needed to serialize t
u32 txnt_get_serialize_size (const struct txn_table *t);

// Number of transactions based on the length of the serialized data - inverse
// of serialize_size
u32 txnlen_from_serialized (u32 slen);

/**
 * Returns the maximum undo lsn for any entries
 * in the candidate for undo state.
 *
 * This record is used in the undo phase
 * of recovery.
 *
 * We start from the maximum because during undo
 * we want to read backwards linearly to avoid
 * random access file movement. This method is
 * called over and over
 *
 * (Later optimization - keep a list of these
 * internally so that I don't re search every time
 * I run this - this method is run in a loop)
 */
slsn txnt_max_u_undo_lsn (struct txn_table *t);

/**
 * Returns the transaction low water mark.
 *
 * The lowest lsn in the transaction table is
 * the lowest lsn we need to remain active. Anything
 * under that lsn can be deleted
 *
 * If the transaction table is empty,
 * this method returns -1
 */
slsn txnt_min_lsn (struct txn_table *t);

void i_log_txnt (int log_level, struct txn_table *t);

/**
 * Executes a function on each open transaction
 *
 * This does not lock internal transactions
 */
void txnt_foreach (const struct txn_table *t,
                   void (*action) (struct txn *, void *ctx),
                   void *ctx);

u32 txnt_get_size (
    const struct txn_table *dest);                     // Returns the number of transactions active
bool txn_exists (const struct txn_table *t, txid tid); // Fast path exists check

/**
 * Merges txn table [src] into [dest]
 *
 * Duplicate strategy:
 *   - On duplicate key, it skips the key. Therefore:
 *       1. If dest already has tid in src, dest stays the source of truth
 *
 *
 * If txn_dest or alloc are null, it doesn't copy transactions
 * over - they stay managed by whatever mechanism managed [src]'s memory
 */
err_t txnt_merge_into (
    struct txn_table *dest,      // The transaction table to merge txn's into
    struct txn_table *src,       // The table to merge from
    struct dbl_buffer *txn_dest, // Nullable
    struct slab_alloc *alloc,    // Nullable
    error *e);

// INSERT
void txnt_insert_txn (struct txn_table *t, struct txn *tx);
void txnt_insert_txn_if_not_exists (struct txn_table *t, struct txn *tx);

// GET
bool txnt_get (struct txn **dest, struct txn_table *t, txid tid);
void txnt_get_expect (struct txn **dest, struct txn_table *t, txid tid);

// REMOVE
void txnt_remove_txn (bool *exists, struct txn_table *t, const struct txn *tx);
void txnt_remove_txn_expect (struct txn_table *t, const struct txn *unsafe_tx);

// SERIALIZATION

bool txnt_equal_ignore_state (struct txn_table *left, struct txn_table *right);
err_t txnt_rand_populate (struct txn_table *t, struct alloc *alloc, error *e);
err_t txnt_determ_populate (
    struct txn_table *t,
    struct alloc *alloc,
    error *e);
void txnt_crash (struct txn_table *t);
