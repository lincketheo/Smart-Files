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
    txn_t *tx,       // Optional tx
    pgno *root,      // The root - it changes on insert as the root gets rebalanced
    const void *src, // The source data bytes
    b_size slen,     // Length of data in bytes
    b_size bofst,    // Byte offset to start the insert
    error *e);

////////////////////////////////////////////////////////////
/// WRITE

// Overwrites existing data in the tree without changing the structure
sb_size ns_write (
    nsdb_t *db,
    txn_t *tx,       // Optional tx
    pgno root,       // The root of the tree
    const void *src, // The source data bytes
    t_size size,     // Size of each element in bytes
    b_size bofst,    // Byte offset to start the write
    sb_size stride,  // Number of bytes to skip between elements
    b_size nelem,    // Number of elements to write
    error *e);

////////////////////////////////////////////////////////////
/// READ

// Retrieves data from the tree into a destination buffer
sb_size ns_read (
    nsdb_t *db,
    txn_t *tx,      // Optional tx
    pgno root,      // The root of the tree
    void *dest,     // The destination buffer to receive the data
    t_size size,    // Size of each element in bytes
    b_size bofst,   // Byte offset to start the read
    sb_size stride, // Number of bytes to skip between elements
    b_size nelem,   // Number of elements to read
    error *e);

////////////////////////////////////////////////////////////
/// REMOVE

// Removes data and rebalances the tree if necessary
sb_size ns_remove (
    nsdb_t *db,
    txn_t *tx,      // Optional tx
    pgno *root,     // The root - it changes if the removal causes a merge or height reduction
    void *dest,     // Optional destination buffer to store removed data (can be NULL)
    t_size size,    // Size of each element in bytes
    b_size bofst,   // Byte offset to start the removal
    sb_size stride, // Number of bytes to skip between elements
    b_size nelem,   // Number of elements to remove
    error *e);
