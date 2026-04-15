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

#include "tlclib_dev.h"

////////////////////////////////////////////////////////////
// DOMAIN TYPES

typedef u32 t_size;  // Represents the size of a single type in bytes
typedef i32 st_size; // Represents the size of a single type in bytes
typedef u32 p_size;  // To index inside a page
typedef i32 sp_size; // Signed p size
typedef u64 b_size;  // Bytes size to index into a contiguous rope bytes
typedef i64 sb_size; // Signed b size
typedef u64 pgno;    // Page number
typedef i64 spgno;   // Signed Page number
typedef u64 txid;    // Transaction id
typedef i64 stxid;   // Signed Transaction id
typedef i64 slsn;    // Wall Index (often called LSN)
typedef u64 lsn;     // Wall Index (often called LSN)
typedef u8 pgh;      // Page header
typedef u8 wlh;      // Wal Header

#define SLSN_MAX I64_MAX

#define PGNO_NULL U64_MAX
#define WLH_NULL U8_MAX

#define PRt_size PRIu32
#define PRsp_size PRId32
#define PRp_size PRIu32
#define PRb_size PRIu64
#define PRsb_size PRId64
#define PRspgno PRId64
#define PRpgno PRIu64
#define PRpgh PRIu8
#define PRtxid PRIu64
#define PRstxid PRId64
#define PRlsn PRIu64
#define PRslsn PRId64

typedef struct nsdb nsdb_t;
typedef struct txn txn_t;

/**
 * @brief Opens a database with the name specified by [dbname].
 * To maintain flexibility, no assumptions are made about how
 * name maps to the file system.
 *
 * @param dbname The name of the database
 * @param e Error catcher object
 * @return A new database
 */
nsdb_t *ns_open (const char *dbname, error *e);

/**
 * @brief Close a database - this safely closes a database by flushing
 * the buffer pool and closing os system resources, so it may error
 *
 * @param n The database to close
 * @param e The error catcher object
 * @return Error result
 */
err_t ns_close (nsdb_t *n, error *e);

/**
 * @brief Force closes a database - terminates any active transactions
 * Where close gracefully flushes the buffer pool, this method just closes
 * all system resources - it doesn't do any cleanup of ongoing transactions
 *
 * @param n The database to crash
 * @param e The error object
 * @return Error result
 */
err_t ns_crash (const nsdb_t *n, error *e);

/**
 * @brief Begins a new transaction
 * each transaction can only be used once (begin, commit)
 *
 * @param n The database to begin the txn on
 * @param e The error object
 * @return The error result
 */
txn_t *ns_begin_txn (const nsdb_t *n, error *e);

/**
 * @brief Commits a transaction. After this function, tx is no longer usable
 *
 * @param n The database to crash
 * @param tx The transaction to commit
 * @param e The error object
 * @return The error result
 */
err_t ns_commit (const nsdb_t *n, txn_t *tx, error *e);

/**
 * @brief Rolls back to the begin marker - in the future I might add
 * savepoints but my pgr_rollback function savepoint feature isn't done
 *
 * @param n The database to rollback on
 * @param tx The transaction to rollback
 * @param e The error object
 * @return The error result
 */
err_t ns_rollback_all (const nsdb_t *n, txn_t *tx, error *e);

/**
 * @brief Inserts data at a specified byte location in the database
 *
 * @return The number of bytes inserted, or a negative value on error
 */
sb_size ns_insert (
    nsdb_t *db,      ///< The database
    txn_t *tx,       ///< Transaction to attach this mutation to. If NULL, an auto transaction is used
    pgno *root,      ///< The root page. Passed by reference as insert may change the root
    const void *src, ///< The data source to insert from - must be slen bytes
    b_size slen,     ///< Number of bytes of src to insert
    b_size bofst,    ///< Byte offset to insert at. If bofst > len(root), inserts at the end
    error *e);       ///< The error object

/**
 * @brief Overwrites existing data in the tree without changing the structure
 *
 * Unlike ns_insert, this does not shift existing data or alter page layout -
 * it performs an in-place overwrite starting at the given byte offset.
 *
 * @return The number of bytes written, or a negative value on error
 */
sb_size ns_write (
    nsdb_t *db,      ///< The database
    txn_t *tx,       ///< Transaction to attach this mutation to. If NULL, an auto transaction is used
    pgno root,       ///< The root page of the tree (not modified by a write)
    const void *src, ///< The source data bytes to write
    t_size size,     ///< Size of each element in bytes
    b_size bofst,    ///< Byte offset into the array at which to begin writing
    sb_size stride,  ///< Bytes to advance between successive element writes. Use 1 for contiguous. Negative not yet supported
    b_size nelem,    ///< Number of elements to write
    error *e);       ///< The error object

/**
 * @brief Retrieves data from the tree into a destination buffer
 *
 * The caller is responsible for ensuring dest has sufficient capacity
 * (at least nelem * size bytes).
 *
 * @return The number of bytes read, or a negative value on error
 */
sb_size ns_read (
    nsdb_t *db,     ///< The database
    txn_t *tx,      ///< Transaction to attach this read to. If NULL, an auto transaction is used
    pgno root,      ///< The root page of the tree
    void *dest,     ///< Destination buffer to receive the data
    t_size size,    ///< Size of each element in bytes
    b_size bofst,   ///< Byte offset into the array at which to begin reading
    sb_size stride, ///< Bytes to advance between successive element reads. Use 1 for contiguous. Negative not yet supported
    b_size nelem,   ///< Number of elements to read
    error *e);      ///< The error object

/**
 * @brief Removes data from the tree and rebalances if necessary
 *
 * Shifts all subsequent data down after deletion. If removal causes an
 * underflow, pages are merged and the tree height may be reduced.
 *
 * @return The number of bytes removed, or a negative value on error
 */
sb_size ns_remove (
    nsdb_t *db,     ///< The database
    txn_t *tx,      ///< Transaction to attach this mutation to. If NULL, an auto transaction is used
    pgno *root,     ///< The root page. Passed by reference as a merge or height reduction may change it
    void *dest,     ///< Optional buffer to capture removed data before deletion. Pass NULL to discard
    t_size size,    ///< Size of each element in bytes
    b_size bofst,   ///< Byte offset into the array at which to begin removal
    sb_size stride, ///< Bytes to advance between successive element removals. Use 1 for contiguous. Negative not yet supported
    b_size nelem,   ///< Number of elements to remove
    error *e);      ///< The error object
