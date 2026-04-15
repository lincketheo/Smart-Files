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

// Opens a new database - must be proceeded with ns_close
nsdb_t *ns_open (const char *dbname, error *e);

// Closes a database cleanly
err_t ns_close (nsdb_t *n, error *e);

// Force closes a database - terminates any active transactions
err_t ns_crash (const nsdb_t *n, error *e);

// Begins a new transaction
txn_t *ns_begin_txn (const nsdb_t *n, error *e);

// Commits a transaction
err_t ns_commit (const nsdb_t *n, txn_t *tx, error *e);

// Rolls back to the begin marker - in the future I might add
// savepoints but my pgr_rollback function savepoint feature isn't done
err_t ns_rollback_all (const nsdb_t *n, txn_t *tx, error *e);
