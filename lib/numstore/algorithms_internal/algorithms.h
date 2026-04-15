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

#include "paging/pager.h"

/**
 * These are "internal" algorithms that don't need to be exposed
 * to the outside user. Internal algorithms should be prefixed
 * with _ns to indicate that they are internal and shouldn't be
 * used in the published api
 *
 * I also used structs for lots of the parameters here
 * not much of a fan of that anymore, but whatever.
 */

/// The core database handle, pairing a pager with its database name
struct nsdb
{
  struct pager *p;    ///< The pager backing this database
  const char *dbname; ///< The name of the database
};

////////////////////////////////////////////////////////////
/// Utils / Routines

/// Begins a transaction if tx points to NULL, writing the new txn into *tx and auto_txn
///
/// If *tx is already non-NULL the caller's transaction is used as-is and no
/// auto transaction is started. Returns non-zero if an auto transaction was
/// started, which the caller must pass to _ns_auto_commit() to commit it.
int _ns_auto_begin_txn (
    const struct nsdb *n, ///< The database
    struct txn **tx,      ///< In/out transaction pointer; set to the active txn on return
    struct txn *auto_txn, ///< Storage for the auto transaction if one is created
    error *e);            ///< The error object

/// Commits and clears the auto transaction if one was started by _ns_auto_begin_txn()
///
/// If auto_txn_started is zero this is a no-op, leaving the caller's transaction untouched.
err_t _ns_auto_commit (
    const struct nsdb *n, ///< The database
    struct txn *tx,       ///< The transaction to commit
    int auto_txn_started, ///< Non-zero if _ns_auto_begin_txn() started the transaction
    error *e);            ///< The error object

/// Simulates a crash-recovery cycle by force-closing and reopening the database without flushing
err_t _ns_crash_reopen (
    struct nsdb *db, ///< The database to crash-reopen
    error *e);       ///< The error object

/// Performs a clean close and reopen of the database, flushing all pending state first
err_t _ns_close_reopen (
    struct nsdb *db, ///< The database to close and reopen
    error *e);       ///< The error object
