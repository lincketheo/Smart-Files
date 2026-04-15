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
 *
 * BOUNDS
 * ------
 * NUPD_MAX_DATA_LENGTH is the maximum bytes a single insert/remove chunk
 * may mutate.  Staying below this bound guarantees that the number of inner-
 * node updates never overflows the slab-backed right/left arrays.
 */

// numstore
#include "paging/pager/page_h.h"

#define MAX_INNER_NODES_PER_NUPD 6
#define NUPD_LENGTH (MAX_INNER_NODES_PER_NUPD * IN_MAX_KEYS)
// #define NUPD_MAX_DATA_LENGTH ((MAX_INNER_NODES_PER_NUPD - 1) * IN_MAX_KEYS *
// DL_DATA_SIZE)
#define NUPD_MAX_DATA_LENGTH \
  ((MAX_INNER_NODES_PER_NUPD - 1) * (IN_MAX_KEYS - 100) * DL_DATA_SIZE)

#define pgh_unravel(pg) page_h_pgno (pg), dlgt_get_size (page_h_ro (pg))

struct node_updates *nupd_init (pgno pg, b_size size, error *e);
void nupd_reset (struct node_updates *ret, pgno pg, b_size size);
void nupd_free (struct node_updates *n);
pgno nupd_pivot_pg (const struct node_updates *n);

err_t nupd_commit_1st_right (struct node_updates *s, pgno pg, b_size size,
                             error *e);
err_t nupd_commit_1st_left (struct node_updates *s, pgno pg, b_size size,
                            error *e);

err_t nupd_append_2nd_right (struct node_updates *s, pgno pg1, b_size size1,
                             pgno pg2, b_size size2, error *e);
err_t nupd_append_2nd_left (struct node_updates *s, pgno pg1, b_size size1,
                            pgno pg2, b_size size2, error *e);

err_t nupd_append_tip_right (struct node_updates *s,
                             struct three_in_pair output, error *e);
err_t nupd_append_tip_left (struct node_updates *s,
                            struct three_in_pair output, error *e);

err_t nupd_observe_pivot (struct node_updates *s, page_h *pg, p_size lidx,
                          error *e);
err_t nupd_observe_right_from (struct node_updates *s, const page_h *pg, p_size lidx,
                               error *e);
err_t nupd_observe_left_from (struct node_updates *s, const page_h *pg, p_size lidx,
                              error *e);
err_t nupd_observe_all_right (struct node_updates *s, const page_h *pg, error *e);
err_t nupd_observe_all_left (struct node_updates *s, const page_h *pg, error *e);

struct in_pair nupd_consume_right (struct node_updates *s);
struct in_pair nupd_consume_left (struct node_updates *s);

bool nupd_done_observing_left (const struct node_updates *s);
bool nupd_done_observing_right (const struct node_updates *s);

bool nupd_done_consuming_left (const struct node_updates *s);
bool nupd_done_consuming_right (const struct node_updates *s);

bool nupd_done_left (struct node_updates *s);
bool nupd_done_right (struct node_updates *s);

// UTILS / SHORTHAND
p_size nupd_append_maximally_left (struct node_updates *n, const page_h *pg,
                                   p_size rlen);
p_size nupd_append_maximally_right (struct node_updates *n, const page_h *pg,
                                    p_size llen);
p_size nupd_append_maximally_left_then_right (struct node_updates *n,
                                              page_h *pg);
p_size nupd_append_maximally_right_then_left (struct node_updates *n,
                                              page_h *pg);
