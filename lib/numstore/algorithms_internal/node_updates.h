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

/*
 * node_updates — leaf-level mutation buffer for R+Tree rebalancing.
 *
 * After every insert or remove, the leaf level may have pages added,
 * removed, or resized.  These changes must propagate up through the inner-
 * node tree so each inner node's key array remains a valid prefix-sum table.
 * node_updates (nupd) is the courier that collects those changes from the
 * data-list level and delivers them to _ns_rebalance().
 *
 * STRUCTURE
 * ---------
 * An nupd object holds three logical lists:
 *
 *   pivot — the single leaf page at the seek point (the page that was
 *            mutated by the caller).  Stored as one in_pair {pgno, size}.
 *
 *   right[] — new or modified pages to the right of the pivot, in order.
 *             Each entry is an in_pair describing a page that _ns_rebalance
 *             must insert or update in the parent inner node.
 *
 *   left[]  — same, but to the left of the pivot.
 *
 * Entries are added with "commit" and "append" helpers; _ns_rebalance
 * consumes them with nupd_consume_right / nupd_consume_left.  Observation
 * (nupd_observe_*) reads entries from an existing inner node into right/left
 * so that _ns_rebalance can decide which keys to keep, update, or delete.
 */

#include "paging/pager/page_h.h"

/// Convenience macro: expands a page_h into its pgno and data-list size for use in in_pair initializers
#define pgh_unravel(pg) page_h_pgno (pg), dlgt_get_size (page_h_ro (pg))

/**
 * @brief Allocates and initializes a node_updates object with a given pivot page
 *
 * @param pg   Page number of the pivot leaf page
 * @param size Byte size of the pivot page's data
 * @param e    The error object
 * @return     A heap-allocated node_updates, or NULL on error
 */
struct node_updates *nupd_init (pgno pg, b_size size, error *e);

/**
 * @brief Resets an existing node_updates to a new pivot without reallocating
 *
 * @param ret  The node_updates to reset
 * @param pg   Page number of the new pivot leaf page
 * @param size Byte size of the new pivot page's data
 */
void nupd_reset (struct node_updates *ret, pgno pg, b_size size);

/**
 * @brief Frees all memory owned by a node_updates object
 *
 * @param n The node_updates to free
 */
void nupd_free (struct node_updates *n);

/**
 * @brief Returns the page number of the pivot leaf page
 *
 * @param n The node_updates to query
 * @return  The pivot page number
 */
pgno nupd_pivot_pg (const struct node_updates *n);

/**
 * @brief Commits the first right-side update, replacing any existing first-right entry
 *
 * @param s    The node_updates to modify
 * @param pg   Page number of the right neighbour
 * @param size Byte size of the right neighbour's data
 * @param e    The error object
 */
err_t nupd_commit_1st_right (struct node_updates *s, pgno pg, b_size size,
                             error *e);

/**
 * @brief Commits the first left-side update, replacing any existing first-left entry
 *
 * @param s    The node_updates to modify
 * @param pg   Page number of the left neighbour
 * @param size Byte size of the left neighbour's data
 * @param e    The error object
 */
err_t nupd_commit_1st_left (struct node_updates *s, pgno pg, b_size size, error *e);

/**
 * @brief Appends two entries to the right side of the update list
 *
 * @param s     The node_updates to modify
 * @param pg1   Page number of the first new right entry
 * @param size1 Byte size of the first new right entry
 * @param pg2   Page number of the second new right entry
 * @param size2 Byte size of the second new right entry
 * @param e     The error object
 */
err_t nupd_append_2nd_right (struct node_updates *s, pgno pg1, b_size size1, pgno pg2, b_size size2, error *e);

/**
 * @brief Appends two entries to the left side of the update list
 *
 * @param s     The node_updates to modify
 * @param pg1   Page number of the first new left entry
 * @param size1 Byte size of the first new left entry
 * @param pg2   Page number of the second new left entry
 * @param size2 Byte size of the second new left entry
 * @param e     The error object
 */
err_t nupd_append_2nd_left (struct node_updates *s, pgno pg1, b_size size1, pgno pg2, b_size size2, error *e);

/**
 * @brief Appends the three pages from a balance output to the right side of the update list
 *
 * @param s      The node_updates to modify
 * @param output The three-page result of a balance operation
 * @param e      The error object
 */
err_t nupd_append_tip_right (struct node_updates *s, struct three_in_pair output, error *e);

/**
 * @brief Appends the three pages from a balance output to the left side of the update list
 *
 * @param s      The node_updates to modify
 * @param output The three-page result of a balance operation
 * @param e      The error object
 */
err_t nupd_append_tip_left (struct node_updates *s, struct three_in_pair output, error *e);

/**
 * @brief Reads the pivot key from an inner-node page at lidx into the nupd pivot slot
 *
 * @param s    The node_updates to modify
 * @param pg   The inner-node page to read from
 * @param lidx Local key index within pg that corresponds to the pivot
 * @param e    The error object
 */
err_t nupd_observe_pivot (struct node_updates *s, page_h *pg, p_size lidx, error *e);

/**
 * @brief Reads inner-node keys to the right of lidx into the right observation list
 *
 * @param s    The node_updates to modify
 * @param pg   The inner-node page to read from
 * @param lidx Local key index to start reading from (exclusive)
 * @param e    The error object
 */
err_t nupd_observe_right_from (struct node_updates *s, const page_h *pg, p_size lidx, error *e);

/**
 * @brief Reads inner-node keys to the left of lidx into the left observation list
 *
 * @param s    The node_updates to modify
 * @param pg   The inner-node page to read from
 * @param lidx Local key index to stop reading before (exclusive)
 * @param e    The error object
 */
err_t nupd_observe_left_from (struct node_updates *s, const page_h *pg, p_size lidx, error *e);

/**
 * @brief Reads all keys in an inner-node page into the right observation list
 *
 * @param s  The node_updates to modify
 * @param pg The inner-node page to read from
 * @param e  The error object
 */
err_t nupd_observe_all_right (struct node_updates *s, const page_h *pg, error *e);

/**
 * @brief Reads all keys in an inner-node page into the left observation list
 *
 * @param s  The node_updates to modify
 * @param pg The inner-node page to read from
 * @param e  The error object
 */
err_t nupd_observe_all_left (struct node_updates *s, const page_h *pg, error *e);

/**
 * @brief Consumes and returns the next pending right-side update
 *
 * @param s The node_updates to consume from
 * @return  The next in_pair from the right list
 */
struct in_pair nupd_consume_right (struct node_updates *s);

/**
 * @brief Consumes and returns the next pending left-side update
 *
 * @param s The node_updates to consume from
 * @return  The next in_pair from the left list
 */
struct in_pair nupd_consume_left (struct node_updates *s);

/**
 * @brief Returns true if all left-side observations have been consumed
 *
 * @param s The node_updates to query
 */
bool nupd_done_observing_left (const struct node_updates *s);

/**
 * @brief Returns true if all right-side observations have been consumed
 *
 * @param s The node_updates to query
 */
bool nupd_done_observing_right (const struct node_updates *s);

/**
 * @brief Returns true if all left-side updates have been consumed
 *
 * @param s The node_updates to query
 */
bool nupd_done_consuming_left (const struct node_updates *s);

/**
 * @brief Returns true if all right-side updates have been consumed
 *
 * @param s The node_updates to query
 */
bool nupd_done_consuming_right (const struct node_updates *s);

/**
 * @brief Returns true if the left side has no remaining observations or pending updates
 *
 * @param s The node_updates to query
 */
bool nupd_done_left (struct node_updates *s);

/**
 * @brief Returns true if the right side has no remaining observations or pending updates
 *
 * @param s The node_updates to query
 */
bool nupd_done_right (struct node_updates *s);

/**
 * @brief Appends as many keys as possible from the left side of pg into the left update list
 *
 * Reads up to rlen keys from the right end of pg, stopping when the list is full.
 *
 * @param n    The node_updates to append into
 * @param pg   The inner-node page to read keys from
 * @param rlen Maximum number of keys to read from the right end of pg
 * @return     The number of keys actually appended
 */
p_size nupd_append_maximally_left (struct node_updates *n, const page_h *pg, p_size rlen);

/**
 * @brief Appends as many keys as possible from the right side of pg into the right update list
 *
 * Reads up to llen keys from the left end of pg, stopping when the list is full.
 *
 * @param n    The node_updates to append into
 * @param pg   The inner-node page to read keys from
 * @param llen Maximum number of keys to read from the left end of pg
 * @return     The number of keys actually appended
 */
p_size nupd_append_maximally_right (struct node_updates *n, const page_h *pg, p_size llen);

/**
 * @brief Fills the left update list first, then the right, from pg's key array
 *
 * @param n  The node_updates to append into
 * @param pg The inner-node page to read keys from
 * @return   The total number of keys appended across both sides
 */
p_size nupd_append_maximally_left_then_right (struct node_updates *n, page_h *pg);

/**
 * @brief Fills the right update list first, then the left, from pg's key array
 *
 * @param n  The node_updates to append into
 * @param pg The inner-node page to read keys from
 * @return   The total number of keys appended across both sides
 */
p_size nupd_append_maximally_right_then_left (struct node_updates *n, page_h *pg);
