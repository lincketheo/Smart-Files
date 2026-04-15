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

#include "tlclib/dev/assert.h"
#include "numstore/algorithms_internal/node_updates.h"
#include "numstore/algorithms_internal/rope/algorithms.h"
#include "paging/pager.h"
#include "paging/pager/page_h.h"
#include "paging/pages/page.h"

/*
 * R+Tree Inner-Node Rebalancing
 *
 * After an insert, write, or remove operation changes the byte count of one
 * or more leaf pages, the inner-node key array at every level of the tree
 * must be updated to reflect the new sizes.  If a leaf-level operation also
 * adds or removes data-list pages, the corresponding inner-node entries must
 * be inserted or deleted, which may cause inner nodes to overflow (>
 * IN_MAX_KEYS entries) or underflow (< IN_MIN_KEYS entries).  Overflow
 * requires splitting a node; underflow requires merging with a sibling or
 * borrowing entries from one.
 *
 * This is coordinated through a node_updates (nupd) object.  The bottom of
 * the stack populates a nupd that describes what changed at the leaf level:
 * which pages were added/removed and what their new byte counts are.  The
 * rebalancer walks up the inner-node stack and, at each level, applies the
 * nupd to the corresponding inner-node page.  This produces a new nupd
 * describing what changed at that level, which is then applied to the level
 * above, and so on until the root is reached.
 *
 * NOTATION USED IN INLINE DIAGRAMS
 * ---------------------------------
 *   + : an existing (valid) inner-node entry
 *   o : an entry that has been "observed" (logically consumed by nupd; the
 *       page it references has been accounted for but not yet written back)
 *   _ : a logically empty slot (has room for a new entry)
 *   - : a slot that is physically present but logically empty (will be
 *       overwritten before the node is released)
 *
 * RIGHT vs LEFT EXECUTION
 * -----------------------
 * When the nupd has changes to the right of the pivot (new pages inserted
 * after the current position), rb_execute_right() walks forward through the
 * sibling chain, consuming observed entries and emitting compacted ones.
 * rb_execute_left() does the symmetric thing for changes to the left.
 *
 * MOVE-UP TRANSITION
 * ------------------
 * _ns_rebalance_move_up_stack() pops one inner-node level off the seek stack,
 * applies the accumulated nupd to the popped node via
 * _ns_rebalance_apply_to_pivot(), then swaps input/output nupd buffers for
 * the next level.  If the popped level becomes the new root (isroot is set),
 * all remaining stack levels above it are deleted and the root pgno is
 * updated.
 */

// Key:
// +: An existing inner node page / key
// o: An observed inner node page / key (effectively deleted)
// _: An empty spot for inner node page / key
// -: A "logically empty" spot but the node might say it's occupied

/*
 * Delete an inner-node page and every page in its sibling chain in both
 * directions.
 *
 * Called when a rebalance determines that an entire level of inner nodes has
 * been collapsed into a single new root below it.  All the now-obsolete
 * sibling pages must be freed so their slots return to the FSM.
 */
static err_t
in_delete_chain (page_h *cur, struct txn *tx, struct pager *p, error *e)
{
  page_h next_next = page_h_create ();
  page_h next = page_h_create ();
  page_h prev = page_h_create ();
  page_h prev_prev = page_h_create ();

  pgno npg = in_get_next (page_h_ro (cur));
  if (npg != PGNO_NULL)
    {
      if (pgr_get_writable (&next, tx, PG_INNER_NODE, npg, p, e))
        {
          goto failed;
        }
    }

  pgno ppg = in_get_prev (page_h_ro (cur));
  if (ppg != PGNO_NULL)
    {
      if (pgr_get_writable (&prev, tx, PG_INNER_NODE, ppg, p, e))
        {
          goto failed;
        }
    }

  if (pgr_delete_and_release (p, tx, cur, e))
    {
      goto failed;
    }

  while (next.mode != PHM_NONE)
    {
      npg = in_get_next (page_h_ro (&next));
      if (npg != PGNO_NULL)
        {
          if (pgr_get_writable (&next_next, tx, PG_INNER_NODE, npg, p, e))
            {
              goto failed;
            }
        }
      if (pgr_delete_and_release (p, tx, &next, e))
        {
          goto failed;
        }
      page_h_xfer_ownership_ptr (&next, &next_next);
    }

  while (prev.mode != PHM_NONE)
    {
      ppg = in_get_prev (page_h_ro (&prev));
      if (ppg != PGNO_NULL)
        {
          if (pgr_get_writable (&prev_prev, tx, PG_INNER_NODE, ppg, p, e))
            {
              goto failed;
            }
        }
      if (pgr_delete_and_release (p, tx, &prev, e))
        {
          goto failed;
        }
      page_h_xfer_ownership_ptr (&prev, &prev_prev);
    }

  return SUCCESS;

failed:
  pgr_cancel_if_exists (p, cur);
  pgr_cancel_if_exists (p, &prev);
  pgr_cancel_if_exists (p, &next);
  pgr_cancel_if_exists (p, &prev_prev);
  pgr_cancel_if_exists (p, &next_next);

  return error_trace (e);
}

static err_t
rb_right_to_left (struct _ns_rebalance_params *params, error *e)
{
  struct root_update root;
  struct three_in_pair tip_out;
  page_h prev = page_h_create ();
  page_h next = page_h_xfer_ownership (&params->limit);

  if (nupd_done_left (params->input))
    {
      const struct _ns_balance_and_release_params bparams = {
        .db = params->db,
        .tx = params->tx,

        .output = &tip_out,
        .root = &root,

        .prev = &prev,
        .cur = &params->cur,
        .next = &next,
      };
      if (_ns_balance_and_release (bparams, e))
        {
          goto failed;
        }
      if (nupd_append_tip_right (params->output, tip_out, e))
        {
          goto failed;
        }
      params->layer_root = root;
      return SUCCESS;
    }

  // We go left, then right - so you never need to go right again
  UNREACHABLE ();

failed:
  pgr_cancel_if_exists (params->db->p, &prev);
  pgr_cancel_if_exists (params->db->p, &next);
  return error_trace (e);
}

static err_t
rb_left_to_right (struct _ns_rebalance_params *params, error *e)
{
  struct root_update root;
  struct three_in_pair tip_out;
  page_h prev = page_h_xfer_ownership (&params->limit);
  page_h next = page_h_create ();

  // Fully done
  if (nupd_done_right (params->input))
    {
      const struct _ns_balance_and_release_params bparams = {
        .db = params->db,
        .tx = params->tx,

        .output = &tip_out,
        .root = &root,

        .prev = &prev,
        .cur = &params->cur,
        .next = &next,
      };
      if (_ns_balance_and_release (bparams, e))
        {
          goto failed;
        }

      if (nupd_append_tip_left (params->output, tip_out, e))
        {
          goto failed;
        }
      params->layer_root = root;

      return SUCCESS;
    }

  // If cur == pivot, we don't need to rebalance - we can just start left
  if (page_h_pgno (&params->cur) != nupd_pivot_pg (params->output))
    {
      // Rebalance
      const struct _ns_balance_and_release_params bparams = {
        .db = params->db,
        .tx = params->tx,

        .output = &tip_out,
        .root = &root,

        .prev = &prev,
        .cur = &params->cur,
        .next = &next,
      };
      if (_ns_balance_and_release (bparams, e))
        {
          goto failed;
        }
      if (nupd_append_tip_left (params->output, tip_out, e))
        {
          goto failed;
        }

      // Fetch pivot
      const pgno pivot = nupd_pivot_pg (params->output);
      if (pgr_get_writable (&params->cur, params->tx, PG_INNER_NODE, pivot,
                            params->db->p, e))
        {
          goto failed;
        }
    }
  else
    {
      if (pgr_release_if_exists (params->db->p, &prev, PG_INNER_NODE, e))
        {
          goto failed;
        }
    }

  ASSERT (prev.mode == PHM_NONE);
  ASSERT (params->cur.mode == PHM_X);
  ASSERT (next.mode == PHM_NONE);

  params->lidx = in_get_len (page_h_ro (&params->cur));
  in_set_len (page_h_w (&params->cur), IN_MAX_KEYS);

  const pgno npg = in_get_next (page_h_ro (&params->cur));
  if (npg != PGNO_NULL && params->limit.mode == PHM_NONE)
    {
      if (pgr_get_writable (&params->limit, params->tx, PG_INNER_NODE, npg,
                            params->db->p, e))
        {
          goto failed;
        }
    }

  return SUCCESS;

failed:
  pgr_cancel_if_exists (params->db->p, &prev);
  pgr_cancel_if_exists (params->db->p, &next);
  return error_trace (e);
}

static err_t
rb_execute_right (struct _ns_rebalance_params *params, error *e)
{
  page_h next = page_h_create ();
  page_h next_next = page_h_create ();

  while (true)
    {
      // [+++++++++++_______]
      // ^
      // lidx
      // [a, b, c] p [d, e, f, g, h, i]
      // ^     ^        ^
      // rcons  rlen     robs
      if (nupd_done_observing_right (params->input))
        {
          params->lidx += nupd_append_maximally_right (
              params->input, &params->cur, params->lidx);

          // [++++++++++++++++++]
          // ^
          // lidx
          // [a, b, c] p [d, e, f, g, h, i]
          // ^  ^      ^
          // rlen rcons  robs
          // rcons didn't reach robs. That can
          // only happen if we filled up current
          // node
          if (!nupd_done_right (params->input))
            {
              ASSERT (params->lidx == IN_MAX_KEYS);

              if (nupd_commit_1st_right (
                      params->output,
                      page_h_pgno (&params->cur),
                      in_get_size (page_h_ro (&params->cur)), e))
                {
                  goto failed;
                }

              // cur -> limit
              // cur -> new -> limit
              // new -> limit
              if (pgr_maybe_make_writable (params->db->p, params->tx, &params->limit, e))
                {
                  goto failed;
                }
              if (pgr_new (&next, params->db->p, params->tx, PG_INNER_NODE, e))
                {
                  goto failed;
                }
              in_link (page_h_w (&params->cur), page_h_w (&next));
              in_link (page_h_w (&next), page_h_w_or_null (&params->limit));
              if (pgr_release (params->db->p, &params->cur, PG_INNER_NODE, e))
                {
                  goto failed;
                }
              page_h_xfer_ownership_ptr (&params->cur, &next);

              in_set_len (page_h_w (&params->cur), IN_MAX_KEYS);

              params->lidx = 0;

              continue;
            }
          else
            {
              // [++++++++----------]
              // ^
              // lidx
              // [++++++++__________]
              in_set_len (page_h_w (&params->cur), params->lidx);
              return rb_right_to_left (params, e);
            }
        }

      // [+++++++++++_______]
      // ^
      // lidx
      // [a, b, c] p [d, e, f, g, h, i]
      // ^     ^        ^
      // rcons  robs     rlen
      else
        {
          if (nupd_observe_all_right (params->input, &params->limit, e))
            {
              goto failed;
            }
          params->lidx += nupd_append_maximally_right (
              params->input, &params->cur, params->lidx);

          // [++++++++++++______]
          // ^
          // lidx
          // [a, b, c] p [d, e, f, g, h, i]
          // ^        ^  ^
          // rcons    robs rlen
          if (!nupd_done_right (params->input)
              && params->lidx > IN_MAX_KEYS / 2)
            {
              // Shift right (limit
              // is effectively
              // "empty" because it
              // was observed so we
              // can use it as a slot
              // for next) cur ->
              // NULL cur
              // -> limit limit limit
              // -> next
              if (params->limit.mode == PHM_NONE)
                {
                  if (pgr_new (&params->limit, params->db->p, params->tx,
                               PG_INNER_NODE, e))
                    {
                      goto failed;
                    }
                  in_link (page_h_w (&params->cur), page_h_w (&params->limit));
                }

              // cur -> limit
              // limit
              // limit -> next
              else
                {
                  // X(limit)
                  if (pgr_maybe_make_writable (params->db->p, params->tx,
                                               &params->limit, e))
                    {
                      goto failed;
                    }
                }

              // [++++++++----------]
              // ^
              // lidx
              // [++++++++__________]
              // ^
              // lidx
              in_set_len (page_h_w (&params->cur), params->lidx);

              if (nupd_commit_1st_right (
                      params->output, page_h_pgno (&params->cur),
                      in_get_size (page_h_ro (&params->cur)), e))
                {
                  goto failed;
                }

              if (pgr_release (params->db->p, &params->cur, PG_INNER_NODE, e))
                {
                  goto failed;
                }

              // cur = limit
              page_h_xfer_ownership_ptr (&params->cur, &params->limit);

              // Open cur for writes
              in_set_len (page_h_w (&params->cur), IN_MAX_KEYS);
              params->lidx = 0;

              // Shift right
              const pgno npg = in_get_next (page_h_ro (&params->cur));
              if (npg != PGNO_NULL && params->limit.mode == PHM_NONE)
                {
                  if (pgr_get_writable (&params->limit, params->tx,
                                        PG_INNER_NODE, npg, params->db->p, e))
                    {
                      goto failed;
                    }
                }
            }

          // [++++++____________]
          // ^
          // lidx
          // [a, b, c] p [d, e, f, g, h, i]
          // ^        ^  ^
          // rcons    robs rlen
          // OR
          // Node could be done:
          // TODO - (18) Maybe optimize this out
          // - right now there's an extra page
          // load [a, b, c] p [d, e, f, g, h, i]
          // ^  ^
          // rlen robs
          // rcons
          else
            {
              ASSERT (nupd_done_consuming_right (params->input));
              ASSERT (nupd_done_right (params->input)
                      || params->limit.mode != PHM_NONE);

              if (params->limit.mode != PHM_NONE)
                {
                  const pgno npg = page_h_pgno (&params->limit);

                  const pgno nnpg = in_get_next (page_h_ro (&params->limit));
                  if (nnpg != PGNO_NULL)
                    {
                      if (pgr_get_writable (&next_next, params->tx,
                                            PG_INNER_NODE, nnpg, params->db->p,
                                            e))
                        {
                          goto failed;
                        }
                    }
                  if (pgr_delete_and_release (params->db->p, params->tx,
                                              &params->limit, e))
                    {
                      goto failed;
                    }
                  in_link (page_h_w (&params->cur),
                           page_h_w_or_null (&next_next));
                  page_h_xfer_ownership_ptr (&params->limit, &next_next);

                  if (nupd_append_2nd_right (params->output,
                                             pgh_unravel (&params->cur), npg,
                                             0, e))
                    {
                      goto failed;
                    }
                }
            }
        }
    }

failed:
  pgr_cancel_if_exists (params->db->p, &next);
  pgr_cancel_if_exists (params->db->p, &next_next);
  return error_trace (e);
}

static err_t
rb_execute_left (struct _ns_rebalance_params *params, error *e)
{
  page_h prev = page_h_create ();
  page_h prev_prev = page_h_create ();

  while (true)
    {
      // [_______+++++++++++]
      // ^
      // lidx
      // [a, b, c, d, e, f] p [g, h, i]
      // ^        ^     ^
      // lobs     llen   lcons
      if (nupd_done_observing_left (params->input))
        {
          params->lidx -= nupd_append_maximally_left (
              params->input, &params->cur, params->lidx);

          // [++++++++++++++++++]
          // ^
          // lidx
          // [a, b, c, d, e, f] p [g, h, i]
          // ^     ^  ^
          // lobs lcons llen
          // lcons didn't reach lobs. That can
          // only happen if we filled up current
          // node
          if (!nupd_done_left (params->input))
            {
              ASSERT (params->lidx == 0);
              if (nupd_commit_1st_left (
                      params->output, page_h_pgno (&params->cur),
                      in_get_size (page_h_ro (&params->cur)), e))
                {
                  goto failed;
                }

              // limit <- cur
              // limit <- new <- cur
              // limit <- new
              if (pgr_maybe_make_writable (params->db->p, params->tx,
                                           &params->limit, e))
                {
                  goto failed;
                }
              if (pgr_new (&prev, params->db->p, params->tx, PG_INNER_NODE, e))
                {
                  goto failed;
                }
              in_link (page_h_w_or_null (&params->limit), page_h_w (&prev));
              in_link (page_h_w (&prev), page_h_w (&params->cur));
              if (pgr_release (params->db->p, &params->cur, PG_INNER_NODE, e))
                {
                  goto failed;
                }
              params->cur = page_h_xfer_ownership (&prev);

              in_set_len (page_h_w (&params->cur), IN_MAX_KEYS);
              params->lidx = IN_MAX_KEYS;

              continue;
            }
          else
            {
              // [----------++++++++]
              // ^
              // lidx
              // [++++++++__________]
              in_cut_left (page_h_w (&params->cur), params->lidx);
              params->lidx = in_get_len (page_h_ro (&params->cur));
              return rb_left_to_right (params, e);
            }
        }

      // [_______+++++++++++]
      // ^
      // lidx
      // [a, b, c, d, e, f] p [g, h, i]
      // ^        ^     ^
      // llen     lobs  lcons
      else
        {
          if (nupd_observe_all_left (params->input, &params->limit, e))
            {
              goto failed;
            }
          params->lidx -= nupd_append_maximally_left (
              params->input, &params->cur, params->lidx);

          // [_______+++++++++++]
          // ^
          // lidx
          // [a, b, c, d, e, f] p [g, h, i]
          // ^  ^        ^
          // llen lobs   lcons
          if (!nupd_done_left (params->input)
              && (IN_MAX_KEYS - params->lidx) > IN_MAX_KEYS / 2)
            {
              // Shift left (limit is
              // effectively "empty"
              // because it was
              // observed so we can
              // use it as a slot for
              // next) NULL <- cur
              // limit -> cur
              // limit
              // prev <- limit
              if (params->limit.mode == PHM_NONE)
                {
                  if (pgr_new (&params->limit, params->db->p, params->tx,
                               PG_INNER_NODE, e))
                    {
                      goto failed;
                    }
                  in_link (page_h_w (&params->limit), page_h_w (&params->cur));
                }

              // limit <- cur
              // limit
              // prev <- limit
              else
                {
                  if (pgr_maybe_make_writable (params->db->p, params->tx,
                                               &params->limit, e))
                    {
                      goto failed;
                    }
                }

              // [----------++++++++]
              // ^
              // lidx
              // [++++++++__________]
              in_cut_left (page_h_w (&params->cur), params->lidx);
              if (nupd_commit_1st_left (
                      params->output, page_h_pgno (&params->cur),
                      in_get_size (page_h_ro (&params->cur)), e))
                {
                  goto failed;
                }

              if (pgr_release (params->db->p, &params->cur, PG_INNER_NODE, e))
                {
                  goto failed;
                }

              // cur = limit
              page_h_xfer_ownership_ptr (&params->cur, &params->limit);

              // Open cur for writes
              in_set_len (page_h_w (&params->cur), IN_MAX_KEYS);
              params->lidx = IN_MAX_KEYS;

              // Shift left
              const pgno ppg = in_get_prev (page_h_ro (&params->cur));
              if (ppg != PGNO_NULL && params->limit.mode == PHM_NONE)
                {
                  if (pgr_get_writable (&params->limit, params->tx,
                                        PG_INNER_NODE, ppg, params->db->p, e))
                    {
                      goto failed;
                    }
                }
            }

          // [___________+++++++]
          // ^
          // lidx
          // [a, b, c, d, e, f] p [g, h, i]
          // ^  ^        ^
          // llen lobs   lcons
          // OR
          // Node could be done:
          // TODO - (18) Maybe optimize this out
          // - right now there's an extra page
          // load [a, b, c, d, e, f] p [g, h, i]
          // ^  ^
          // lobs llen
          // lcons
          else
            {
              ASSERT (nupd_done_consuming_left (params->input));
              ASSERT (nupd_done_left (params->input)
                      || params->limit.mode != PHM_NONE);

              if (params->limit.mode != PHM_NONE)
                {
                  const pgno ppg = page_h_pgno (&params->limit);

                  const pgno pppg = in_get_prev (page_h_ro (&params->limit));
                  if (pppg != PGNO_NULL)
                    {
                      if (pgr_get_writable (&prev_prev, params->tx,
                                            PG_INNER_NODE, pppg, params->db->p,
                                            e))
                        {
                          goto failed;
                        }
                    }
                  if (pgr_delete_and_release (params->db->p, params->tx,
                                              &params->limit, e))
                    {
                      goto failed;
                    }
                  in_link (page_h_w_or_null (&prev_prev),
                           page_h_w (&params->cur));
                  page_h_xfer_ownership_ptr (&params->limit, &prev_prev);

                  if (nupd_append_2nd_left (params->output,
                                            pgh_unravel (&params->cur), ppg, 0,
                                            e))
                    {
                      goto failed;
                    }
                }
            }
        }
    }

failed:
  pgr_cancel_if_exists (params->db->p, &prev);
  pgr_cancel_if_exists (params->db->p, &prev_prev);
  return error_trace (e);
}

static err_t
_ns_pop_stack (struct _ns_rebalance_params *params, error *e)
{
  struct seek_v *ref = &params->pstack[--(params->sp)];

  struct seek_v v = {
    .pg = page_h_xfer_ownership (&ref->pg),
    .lidx = ref->lidx,
  };

  if (params->cur.mode != PHM_NONE)
    {
      if (pgr_release (params->db->p, &params->cur,
                       PG_INNER_NODE | PG_DATA_LIST, e))
        {
          goto failed;
        }
    }

  params->cur = page_h_xfer_ownership (&v.pg);
  params->lidx = v.lidx;

  return SUCCESS;

failed:
  pgr_cancel_if_exists (params->db->p, &v.pg);
  return error_trace (e);
}

static err_t _ns_rebalance_move_up_stack (struct _ns_rebalance_params *params,
                                          error *e);

static err_t
_ns_rebalance_apply_to_pivot (struct _ns_rebalance_params *params, error *e)
{
  page_h prev = page_h_create ();
  page_h next = page_h_create ();

  // Output is empty
  /**
  ASSERT (params->output->lcons == 0);
  ASSERT (params->output->llen == 0);
  ASSERT (params->output->lobs == 0);
  ASSERT (params->output->rcons == 0);
  ASSERT (params->output->rlen == 0);
  ASSERT (params->output->rcons == 0);
  ASSERT (params->output->pivot.pg == page_h_pgno (&params->cur));
  ASSERT (params->output->pivot.key == in_get_size (page_h_ro
  (&params->cur)));

  // Input is not consumed
  ASSERT (params->input->lcons == 0);
  ASSERT (params->input->lobs == 0);
  ASSERT (params->input->rcons == 0);
  ASSERT (params->input->robs == 0);
  if (in_get_len (page_h_ro (&params->cur)) > 0)
  {
  ASSERT (params->input->pivot.pg == in_get_leaf (page_h_ro (&params->cur),
  params->lidx));
  }
  */

  if (nupd_observe_pivot (params->input, &params->cur, params->lidx, e))
    {
      goto failed;
    }
  in_set_len (page_h_w (&params->cur), IN_MAX_KEYS);

  // ----------> Append right
  // [++++++++++++++------]
  // Shift Right
  // [------++++++++++++++]
  // <--- Append Left
  // [--++++++++++++++++++]
  // ^
  // lidx
  // Continue in left mode
  params->lidx
      = IN_MAX_KEYS
        - nupd_append_maximally_right_then_left (params->input, &params->cur);

  if (nupd_done_left (params->input))
    {
      // [++++++++++++++++++__]
      // ^
      // lidx
      in_cut_left (page_h_w (&params->cur), params->lidx);
      params->lidx = IN_MAX_KEYS - params->lidx;

      // DONE EARLY
      if (nupd_done_right (params->input))
        {
          struct three_in_pair tip_out;

          const struct _ns_balance_and_release_params bparams = {
            .db = params->db,
            .tx = params->tx,

            .output = &tip_out,
            .root = &params->layer_root,

            .prev = &prev,
            .cur = &params->cur,
            .next = &next,
          };
          if (_ns_balance_and_release (bparams, e))
            {
              goto failed;
            }
          if (nupd_append_tip_right (params->output, tip_out, e))
            {
              goto failed;
            }

          return _ns_rebalance_move_up_stack (params, e);
        }

      // Open up for right updates
      // [++++++++++++++++++--]
      // ^
      // lidx
      in_set_len (page_h_w (&params->cur), IN_MAX_KEYS);

      // Right mode in read mode
      const pgno next_pg = in_get_next (page_h_ro (&params->cur));
      if (next_pg != PGNO_NULL)
        {
          if (pgr_get (&params->limit, PG_INNER_NODE,
                       in_get_next (page_h_ro (&params->cur)), params->db->p,
                       e))
            {
              goto failed;
            }
        }

      return SUCCESS;
    }

  // Left mode
  const pgno prev_pg = in_get_prev (page_h_ro (&params->cur));
  if (prev_pg != PGNO_NULL)
    {
      if (pgr_get (&params->limit, PG_INNER_NODE,
                   in_get_prev (page_h_ro (&params->cur)), params->db->p, e))
        {
          goto failed;
        }
    }

  return SUCCESS;

failed:
  pgr_cancel_if_exists (params->db->p, &prev);
  pgr_cancel_if_exists (params->db->p, &next);
  return error_trace (e);
}

static err_t
_ns_rebalance_move_up_stack (struct _ns_rebalance_params *params, error *e)
{
  if (params->layer_root.isroot)
    {
      // Delete all the next layers above
      while (params->sp != 0)
        {
          if (_ns_pop_stack (params, e))
            {
              goto failed;
            }
          if (in_delete_chain (&params->cur, params->tx, params->db->p, e))
            {
              goto failed;
            }
        }

      params->lidx = 0;
      params->root = params->layer_root.root;

      return SUCCESS;
    }
  else
    {
      if (params->sp == 0)
        {
          if (pgr_new (&params->cur, params->db->p, params->tx, PG_INNER_NODE,
                       e))
            {
              goto failed;
            }

          params->root = page_h_pgno (&params->cur);
          params->lidx = 0;
        }
      else
        {
          if (_ns_pop_stack (params, e))
            {
              goto failed;
            }
          if (pgr_make_writable (params->db->p, params->tx, &params->cur, e))
            {
              goto failed;
            }
        }

      // Swap node updates
      struct node_updates *input = params->input;
      struct node_updates *output = params->output;
      params->output = input;
      params->input = output;

      if (params->output == NULL)
        {
          params->output
              = nupd_init (page_h_pgno (&params->cur),
                           in_get_size (page_h_ro (&params->cur)), e);
          if (params->output == NULL)
            {
              goto failed;
            }
        }
      else
        {
          nupd_reset (params->output, page_h_pgno (&params->cur),
                      in_get_size (page_h_ro (&params->cur)));
        }

      return _ns_rebalance_apply_to_pivot (params, e);
    }

failed:
  return error_trace (e);
}

/*
 * Propagate size changes and structural updates up the inner-node stack.
 *
 * Outer loop: pop one level at a time from the seek stack.  For each level,
 * call _ns_rebalance_move_up_stack() which loads the inner-node page from the
 * stack and applies the current input nupd via _ns_rebalance_apply_to_pivot().
 *
 * After apply_to_pivot, the current input nupd may still have unconsumed left
 * or right updates (entries that need to move to sibling nodes).  These are
 * handled by rb_execute_left() and rb_execute_right() respectively, which
 * walk the sibling chain and pack or unpack entries until the nupd is fully
 * consumed.  Each execution function produces an output nupd describing what
 * changed at this level, which becomes the input nupd for the level above.
 *
 * When the layer_root is set (the current level is the tree root), the loop
 * exits and any obsolete levels above are deleted by the move_up function.
 */
err_t
_ns_rebalance (struct _ns_rebalance_params *params, error *e)
{
  params->cur = page_h_create ();
  params->limit = page_h_create ();
  params->lidx = 0;

  while (true)
    {
      if (_ns_rebalance_move_up_stack (params, e))
        {
          goto failed;
        }

      // Pop up the stack once
      if (params->layer_root.isroot)
        {
          return error_trace (e);
        }

      bool done = true;

      // Execute left
      if (!nupd_done_left (params->input))
        {
          done = false;
          if (rb_execute_left (params, e))
            {
              goto failed;
            }
        }

      // Execute right
      if (!nupd_done_right (params->input))
        {
          done = false;
          if (rb_execute_right (params, e))
            {
              goto failed;
            }
        }

      if (done)
        {
          return SUCCESS;
        }
    }

failed:
  pgr_cancel_if_exists (params->db->p, &params->cur);
  pgr_cancel_if_exists (params->db->p, &params->limit);
  return error_trace (e);
}
