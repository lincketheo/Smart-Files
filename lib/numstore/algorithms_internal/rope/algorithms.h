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

#include "numstore/algorithms_internal/algorithms.h"

#include "tlclib/memory/chunk_alloc.h"
#include "tlclib/ds/stream.h"
#include "numstore/algorithms_internal/node_updates.h"
#include "paging/pager/page_h.h"

////////////////////////////////////////////////////////////
// Validation

/// Validates the structural integrity of an R+Tree's repeat-index tree
err_t _ns_rptree_valid (
    struct nsdb *db, ///< The database
    pgno rpt_root,   ///< Root page of the repeat-index tree to validate
    b_size nbytes,   ///< Expected total byte count the tree should account for
    error *e);       ///< The error object

////////////////////////////////////////////////////////////
// R+Tree Algorithms

/*
 * Parameters for _ns_insert().
 *
 * Inserts [nelem] elements of [size] bytes each from [src] into the R+Tree
 * rooted at [root], starting at byte offset [bofst].  When nelem == 0, bytes
 * are consumed from [src] until it is exhausted (unlimited insert).
 *
 * [root] is updated in place if the tree root changes (e.g. because the root
 * was split into a new level).
 */
struct _ns_insert_params
{
  struct nsdb *db; ///< The database

  struct stream *src; ///< Source stream to read inserted bytes from
  struct txn *tx;     ///< Transaction to attach this mutation to

  pgno root;    ///< Root page of the target tree (updated in place if root changes)
  b_size size;  ///< Element size in bytes
  b_size bofst; ///< Byte offset at which to begin inserting
  b_size nelem; ///< Number of elements to insert (0 = consume src until exhausted)
};

/// Parameters for _ns_write() — in-place overwrite of elements in an R+Tree
struct _ns_write_params
{
  struct nsdb *db; ///< The database

  struct stream *src; ///< Source stream to read replacement bytes from
  struct txn *tx;     ///< Transaction to attach this mutation to

  pgno root;      ///< Root page of the target tree
  t_size size;    ///< Size of each element in bytes
  b_size bofst;   ///< Byte offset at which to begin writing
  sb_size stride; ///< Bytes to advance between successive element writes (1 = contiguous)
  b_size nelem;   ///< Number of elements to write
};

/// Parameters for _ns_read() — element retrieval from an R+Tree into a stream
struct _ns_read_params
{
  struct nsdb *db; ///< The database

  struct stream *dest; ///< Destination stream to push read bytes into
  struct txn *tx;      ///< Transaction to attach this read to

  pgno root;      ///< Root page of the source tree
  t_size size;    ///< Size of each element in bytes
  b_size bofst;   ///< Byte offset at which to begin reading
  sb_size stride; ///< Bytes to advance between successive element reads (1 = contiguous)
  b_size nelem;   ///< Number of elements to read
};

/// Parameters for _ns_remove() — element deletion and optional capture from an R+Tree
struct _ns_remove_params
{
  struct nsdb *db; ///< The database

  struct stream *dest; ///< Optional stream to capture removed bytes before deletion (NULL to discard)
  struct txn *tx;      ///< Transaction to attach this mutation to

  pgno root;      ///< Root page of the target tree (updated in place if root changes)
  b_size size;    ///< Size of each element in bytes
  b_size bofst;   ///< Byte offset at which to begin removing
  sb_size stride; ///< Bytes to advance between successive element removals (1 = contiguous)
  b_size nelem;   ///< Number of elements to remove
};

/// Inserts elements into an R+Tree; returns bytes inserted or a negative error code
sb_size _ns_insert (struct _ns_insert_params *params, error *e);

/// Overwrites elements in an R+Tree; returns bytes written or a negative error code
sb_size _ns_write (struct _ns_write_params params, error *e);

/// Reads elements from an R+Tree; returns bytes read or a negative error code
sb_size _ns_read (struct _ns_read_params params, error *e);

/// Removes elements from an R+Tree; returns bytes removed or a negative error code
sb_size _ns_remove (struct _ns_remove_params *params, error *e);

////////////////////////////////////////////////////////////
/// SEEK

/// A single entry in the seek stack, identifying a page and a local index within it
struct seek_v
{
  page_h pg;   ///< Handle to the page (held in read mode)
  p_size lidx; ///< Local byte index within the page
};

/*
 * Parameters for _ns_seek().
 *
 * Traverses the R+Tree from root to the data-list page containing [bofst].
 * On return, [pg] holds the data-list page in read mode and [lidx] is the
 * local byte index within that page where [bofst] lands.
 *
 * If [save_stack] is true, each inner node visited during the descent is
 * saved into [pstack] (in read mode) rather than released.  This gives
 * _ns_rebalance() a pre-loaded path to the root without a second traversal.
 * The caller is responsible for releasing all pages in pstack[0...sp-1] on
 * the success path (or cancelling them on the error path).
 *
 * The stack depth is bounded at 20 levels; an R+Tree with IN_MAX_KEYS
 * children per node can index far more data than any practical storage device
 * before depth 20, so this bound is effectively unreachable.
 */
struct _ns_seek_params
{
  struct nsdb *db; ///< The database
  struct txn *tx;  ///< Transaction to attach this traversal to

  pgno root;       ///< Root page of the tree to seek into
  b_size bofst;    ///< Byte offset to locate
  bool save_stack; ///< If true, inner nodes are retained in pstack rather than released

  // Outputs (don't need to initialize these)
  struct seek_v pstack[20]; ///< Inner nodes visited during descent (valid if save_stack is true)
  u32 sp;                   ///< Number of valid entries in pstack
  page_h pg;                ///< Resulting data-list page, held in read mode (PHM_S)
  p_size lidx;              ///< Byte offset within pg where bofst lands
};

/// Traverses the R+Tree to the data-list page containing bofst
err_t _ns_seek (struct _ns_seek_params *a, error *e);

////////////////////////////////////////////////////////////
/// REBALANCE

/// Carries an updated root page number and a flag indicating whether it is the tree root
struct root_update
{
  pgno root;   ///< The (possibly new) root page number
  bool isroot; ///< True if this page is now the tree root
};

/// Parameters for _ns_balance_and_release()
struct _ns_balance_and_release_params
{
  struct nsdb *db; ///< The database
  struct txn *tx;  ///< Transaction to attach mutations to

  struct three_in_pair *output; ///< Receives the resulting (prev, cur, next) in_pairs for nupd accounting
  struct root_update *root;     ///< Updated with the new root if a merge reduces tree height
  page_h *prev;                 ///< Left sibling page (may be absent but must be non-NULL)
  page_h *cur;                  ///< The leaf page to balance (must be loaded)
  page_h *next;                 ///< Right sibling page (may be absent but must be non-NULL)
};

/*
 * Balance the leaf page cur against its siblings and release all three.
 *
 * Called after every data-level mutation (insert, remove) to enforce the
 * invariant that every non-root data-list page holds at least maxlen/2 bytes.
 *
 * next, prev don't necessarily need to be loaded. This algorithm will
 * minimize the number of page loads - they can be absent but non null and
 * the algorithm will check for existing pages first
 *
 * On success, prev, cur, and next are all released.  The caller obtains the
 * resulting (prev, cur, next) in_pairs via params.output for nupd accounting.
 */
err_t _ns_balance_and_release (struct _ns_balance_and_release_params params, error *e);

/// Parameters for the main bottom-up rebalance pass after an insert or remove
struct _ns_rebalance_params
{
  struct nsdb *db; ///< The database
  struct txn *tx;  ///< Transaction to attach mutations to

  pgno root; ///< Root page of the tree being rebalanced

  struct seek_v *pstack;         ///< Stack of inner-node pages saved during the preceding seek
  u32 sp;                        ///< Number of valid entries in pstack
  struct node_updates *input;    ///< Update set fed into this rebalance layer (swaps with output each iteration)
  struct node_updates *output;   ///< Update set produced by this rebalance layer
  struct root_update layer_root; ///< Carries the root update if this layer collapses to a new root

  // Stateful working variables
  page_h cur;   ///< Current inner-node page being processed
  page_h limit; ///< Sentinel page marking the end of the current layer
  p_size lidx;  ///< Local index within cur being updated
};

/// Propagates structural updates bottom-up through the inner nodes of the R+Tree
err_t _ns_rebalance (struct _ns_rebalance_params *params, error *e);
