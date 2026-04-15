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

struct nsdb
{
  struct pager *p;    // The pager for this database
  const char *dbname; // The name of the database
};

////////////////////////////////////////////////////////////
/// Utils / Routines

int _ns_auto_begin_txn (
    const struct nsdb *n,
    struct txn **tx,
    struct txn *auto_txn,
    error *e);

err_t _ns_auto_commit (
    const struct nsdb *n,
    struct txn *tx,
    int auto_txn_started,
    error *e);

err_t _ns_crash_reopen (struct nsdb *db, error *e);

err_t _ns_close_reopen (struct nsdb *db, error *e);
