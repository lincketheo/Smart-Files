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

#include "numstore/numstore_core.h"

////////////////////////////////////////////////////////////
/// INSERT

// Based off of the root of the rptree - useful if you want a single file
sb_size ns_insert (
    nsdb_t *db,
    txn_t *tx,
    pgno *root,
    const void *src,
    b_size slen,
    b_size bofst,
    error *e);

////////////////////////////////////////////////////////////
/// WRITE

sb_size ns_write (
    nsdb_t *db,
    txn_t *tx,
    pgno root,
    const void *src,
    t_size size,
    b_size bofst,
    sb_size stride,
    b_size nelem,
    error *e);

////////////////////////////////////////////////////////////
/// READ

sb_size ns_read (
    nsdb_t *db,
    txn_t *tx,
    pgno root,
    void *dest,
    t_size size,
    b_size bofst,
    sb_size stride,
    b_size nelem,
    error *e);

////////////////////////////////////////////////////////////
/// REMOVE

sb_size ns_remove (
    nsdb_t *db,
    txn_t *tx,
    pgno *root,
    void *dest,
    t_size size,
    b_size bofst,
    sb_size stride,
    b_size nelem,
    error *e);
