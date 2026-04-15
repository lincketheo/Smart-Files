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

#include "core/chunk_alloc.h"
#include "core/stream.h"
#include "numstore/algorithms_internal/node_updates.h"
#include "paging/pager/page_h.h"

////////////////////////////////////////////////////////////
// Validation

err_t _ns_rptree_valid (struct nsdb *db, pgno rpt_root, b_size nbytes, error *e);

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
  struct nsdb *db;

  struct stream *src;
  struct txn *tx;

  pgno root;
  b_size size;  // element size in bytes
  b_size bofst; // byte offset at which to begin inserting
  b_size nelem; // number of elements (0 = unlimited)
};

struct _ns_write_params
{
  struct nsdb *db;

  struct stream *src;
  struct txn *tx;

  pgno root;
  t_size size;
  b_size bofst;
  sb_size stride;
  b_size nelem;
};

struct _ns_read_params
{
  struct nsdb *db;

  struct stream *dest;
  struct txn *tx;

  pgno root;
  t_size size;
  b_size bofst;
  sb_size stride;
  b_size nelem;
};

struct _ns_remove_params
{
  struct nsdb *db;

  struct stream *dest;
  struct txn *tx;

  pgno root;
  b_size size;
  b_size bofst;
  sb_size stride;
  b_size nelem;
};

sb_size _ns_insert (struct _ns_insert_params *params, error *e);
sb_size _ns_write (struct _ns_write_params params, error *e);
sb_size _ns_read (struct _ns_read_params params, error *e);
sb_size _ns_remove (struct _ns_remove_params *params, error *e);

////////////////////////////////////////////////////////////
/// SEEK

struct seek_v
{
  page_h pg;
  p_size lidx;
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
  struct nsdb *db;
  struct txn *tx;

  pgno root;
  b_size bofst;
  bool save_stack;

  // Outputs (don't need to initialize these)
  struct seek_v pstack[20]; // inner nodes visited during descent
  u32 sp;                   // number of valid entries in pstack
  page_h pg;                // resulting data-list page (PHM_S)
  p_size lidx;              // byte offset within pg where bofst lands
};

err_t _ns_seek (struct _ns_seek_params *a, error *e);

////////////////////////////////////////////////////////////
/// REBALANCE

struct root_update
{
  pgno root;
  bool isroot;
};

struct _ns_balance_and_release_params
{
  struct nsdb *db;
  struct txn *tx;

  struct three_in_pair *output;
  struct root_update *root;
  page_h *prev;
  page_h *cur;
  page_h *next;
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

// Main rebalance algorithm
struct _ns_rebalance_params
{
  struct nsdb *db;
  struct txn *tx;

  pgno root;

  struct seek_v *pstack;       // The stack of seeked pages
  u32 sp;                      // stack pointer
  struct node_updates *input;  // input to this layer (Swaps with output each time)
  struct node_updates *output; // output from this layer
  struct root_update layer_root;

  // Stateful stuff
  page_h cur;
  page_h limit;
  p_size lidx;
};

err_t _ns_rebalance (struct _ns_rebalance_params *params, error *e);
