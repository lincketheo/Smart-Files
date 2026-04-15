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

#include "numstore/errors.h"
#include "numstore/stdtypes.h"
#include "numstore/types.h"

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
