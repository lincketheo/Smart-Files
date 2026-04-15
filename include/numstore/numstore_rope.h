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
