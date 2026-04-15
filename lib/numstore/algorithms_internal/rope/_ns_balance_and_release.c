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

#include "core/math.h"
#include "numstore/algorithms_internal/rope/algorithms.h"
#include "paging/pager.h"
#include "paging/pager/page_fixture.h"
#include "paging/pager/page_h.h"
#include "paging/pages/page.h"
#include "paging/pages/page_delegate.h"
#include "test/testing.h"

/*
 * Rebalance a data-list leaf against its left (prev) sibling.
 * If cur is at or above the half-full threshold, no action is needed.
 *
 * If sizeof(cur) + sizeof(prev) >= max node length, then that means we
 * have enough data to balance at least max / 2 for each node. Otherwise,
 * we _have_ to delete a node.
 *
 * If we need to delete a node, it's always [cur]
 */
static void
dlgt_balance_with_prev (const page_h *prev, const page_h *cur)
{
  ASSERT (prev->mode == PHM_X);
  ASSERT (cur->mode == PHM_X);
  ASSERT (dlgt_valid_neighbors (page_h_ro (prev), page_h_ro (cur)));

  const p_size prev_len = dlgt_get_len (page_h_ro (prev));
  const p_size cur_len = dlgt_get_len (page_h_ro (cur));
  const p_size maxlen = dlgt_get_max_len (page_h_ro (prev));

  // Already valid
  if (cur_len == 0)
    {
      return;
    }

  // Also valid
  if (cur_len >= maxlen / 2)
    {
      return;
    }

  // There's enough data to balaance max / 2 for each node
  if (prev_len + cur_len >= maxlen)
    {
      dlgt_move_right (page_h_w (prev), page_h_w (cur), maxlen / 2 - cur_len);
      return;
    }

  // Move all the data left to prev
  dlgt_move_left (page_h_w (prev), page_h_w (cur), cur_len);
}

TEST (TT_UNIT, dlgt_balance_with_prev)
{
  struct pgr_fixture f;
  error *e = &f.e;
  pgr_fixture_create (&f);

  u8 _prev[DL_DATA_SIZE];
  u8 _cur[DL_DATA_SIZE];
  u32_arr_rand (_prev);
  u32_arr_rand (_cur);

  struct txn tx;
  pgr_begin_txn (&tx, f.p, &f.e);

  struct page_tree_builder builder = {
    .root = {
        .type = PG_INNER_NODE,
        .out = page_h_create (),
        .inner = {
            .dclen = 2,
            .clen = 2,
            .children = (struct page_desc[]){
                {
                    .type = PG_DATA_LIST,
                    .out = page_h_create (),
                    .size = DL_DATA_SIZE,
                    .data_list = {
                        .data = _prev,
                        .blen = DL_DATA_SIZE,
                    },
                },
                {
                    .type = PG_DATA_LIST,
                    .out = page_h_create (),
                    .size = DL_DATA_SIZE,
                    .data_list = {
                        .data = _cur,
                        .blen = DL_DATA_SIZE,
                    },
                },
            },
        },
    },
    .pager = f.p,
    .txn = &tx,
  };

  build_page_tree (&builder, &f.e);

  page_h *prev = &builder.root.inner.children[0].out;
  page_h *cur = &builder.root.inner.children[1].out;

  pgr_release (f.p, &builder.root.out, PG_INNER_NODE, e);

  TEST_CASE ("Both Full no change")
  {
    dlgt_balance_with_prev (prev, cur);
    test_assert_equal (dl_used (page_h_ro (prev)), DL_DATA_SIZE);
    test_assert_equal (dl_used (page_h_ro (cur)), DL_DATA_SIZE);
    test_assert_memequal (dl_get_data (page_h_ro (prev)), _prev, DL_DATA_SIZE);
    test_assert_memequal (dl_get_data (page_h_ro (cur)), _cur, DL_DATA_SIZE);
  }

  // BEFORE
  // [++++++++++++|****___10____]
  // [+++10+++____|_____________]
  // AFTER
  // [++++++++++++|_____________]
  // [****++++++++|_____________]
  TEST_CASE ("No Delete")
  {
    dl_memset (page_h_w (prev), _prev, DL_DATA_SIZE - 10);
    dl_memset (page_h_w (cur), _cur, 10);

    dlgt_balance_with_prev (prev, cur);
    test_assert_equal (dl_used (page_h_ro (prev)), DL_DATA_SIZE / 2 + DL_REM);
    test_assert_equal (dl_used (page_h_ro (cur)), DL_DATA_SIZE / 2);

    u32 i = 0;
    for (; i < DL_DATA_SIZE / 2 + DL_REM; ++i)
      {
        test_assert_equal (dl_get_byte (page_h_ro (prev), i), _prev[i]);
      }
    i = 0;
    for (; i < DL_DATA_SIZE - 10 - DL_DATA_SIZE / 2 - DL_REM; ++i)
      {
        test_assert_equal (dl_get_byte (page_h_ro (cur), i),
                           _prev[DL_DATA_SIZE / 2 + DL_REM + i]);
      }
    const u32 k = i;
    for (; i < DL_DATA_SIZE / 2; ++i)
      {
        test_assert_equal (dl_get_byte (page_h_ro (cur), i), _cur[i - k]);
      }
  }

  // BEFORE
  // [++++++++++++|++++___10____]
  // [***9***____|_____________]
  // AFTER
  // [++++++++++++|++++***9***_]
  // [____________|_____________]
  TEST_CASE ("Delete")
  {
    dl_memset (page_h_w (prev), _prev, DL_DATA_SIZE - 10);
    dl_memset (page_h_w (cur), _cur, 9);

    dlgt_balance_with_prev (prev, cur);
    test_assert_equal (dl_used (page_h_ro (prev)), DL_DATA_SIZE - 1);
    test_assert_equal (dl_used (page_h_ro (cur)), 0);

    u32 i = 0;
    // next data
    for (; i < DL_DATA_SIZE - 10; ++i)
      {
        test_assert_equal (dl_get_byte (page_h_ro (prev), i), _prev[i]);
      }
    const u32 k = i;
    for (; i < 9; ++i)
      {
        test_assert_equal (dl_get_byte (page_h_ro (prev), i), _cur[i - k]);
      }
  }

  pgr_release (f.p, prev, PG_DATA_LIST, e);
  pgr_delete_and_release (f.p, &tx, cur, e);

  pgr_commit (f.p, &tx, &f.e);

  pgr_fixture_teardown (&f);
}

/*
 * Rebalance a data-list leaf against its right (next) sibling.
 *
 * Mirror of dlgt_balance_with_prev.  If cur is below half-full and
 * cur + next exceed a page, bytes are moved from the head of next into the
 * tail of cur (borrow).  If they fit together, all of cur is moved into
 * next, leaving cur empty (merge).
 */
static void
dlgt_balance_with_next (const page_h *cur, const page_h *next)
{
  ASSERT (cur->mode == PHM_X);
  ASSERT (next->mode == PHM_X);
  ASSERT (dlgt_valid_neighbors (page_h_ro (cur), page_h_ro (next)));

  const p_size next_len = dlgt_get_len (page_h_ro (next));
  const p_size cur_len = dlgt_get_len (page_h_ro (cur));
  const p_size maxlen = dlgt_get_max_len (page_h_ro (next));

  // Already valid
  if (cur_len == 0)
    {
      return;
    }

  if (cur_len >= maxlen / 2)
    {
      return;
    }

  if (next_len + cur_len >= maxlen)
    {
      dlgt_move_left (page_h_w (cur), page_h_w (next), maxlen / 2 - cur_len);
      return;
    }

  dlgt_move_right (page_h_w (cur), page_h_w (next), cur_len);
}

TEST (TT_UNIT, dlgt_balance_with_next)
{
  struct pgr_fixture f;
  error *e = &f.e;
  pgr_fixture_create (&f);

  u8 _cur[DL_DATA_SIZE];
  u8 _next[DL_DATA_SIZE];
  u32_arr_rand (_next);
  u32_arr_rand (_cur);

  struct txn tx;
  pgr_begin_txn (&tx, f.p, &f.e);

  struct page_tree_builder builder = {
    .root = {
        .type = PG_INNER_NODE,
        .out = page_h_create (),
        .inner = {
            .dclen = 2,
            .clen = 2,
            .children = (struct page_desc[]){
                {
                    .type = PG_DATA_LIST,
                    .out = page_h_create (),
                    .size = DL_DATA_SIZE,
                    .data_list = {
                        .data = _cur,
                        .blen = DL_DATA_SIZE,
                    },
                },
                {
                    .type = PG_DATA_LIST,
                    .out = page_h_create (),
                    .size = DL_DATA_SIZE,
                    .data_list = {
                        .data = _next,
                        .blen = DL_DATA_SIZE,
                    },
                },
            },
        },
    },
    .pager = f.p,
    .txn = &tx,
  };

  build_page_tree (&builder, &f.e);

  page_h *cur = &builder.root.inner.children[0].out;
  page_h *next = &builder.root.inner.children[1].out;

  pgr_release (f.p, &builder.root.out, PG_INNER_NODE, e);

  TEST_CASE ("Both Full no change")
  {
    dlgt_balance_with_next (cur, next);
    test_assert_equal (dl_used (page_h_ro (cur)), DL_DATA_SIZE);
    test_assert_equal (dl_used (page_h_ro (next)), DL_DATA_SIZE);
    test_assert_memequal (dl_get_data (page_h_ro (cur)), _cur, DL_DATA_SIZE);
    test_assert_memequal (dl_get_data (page_h_ro (next)), _next, DL_DATA_SIZE);
  }

  // BEFORE
  // [+++10+++____|_____________]
  // [****++++++++|++++___10____]
  // AFTER
  // [+++10+++****|_____________]
  // [++++++++++++|___10____]
  TEST_CASE ("No Delete")
  {
    _Static_assert (DL_DATA_SIZE > 10, "This test needs DL_DATA_SIZE > 10");
    dl_memset (page_h_w (cur), _cur, 10);
    dl_memset (page_h_w (next), _next, DL_DATA_SIZE - 10);

    dlgt_balance_with_next (cur, next);
    test_assert_equal (dl_used (page_h_ro (cur)), DL_DATA_SIZE / 2);
    test_assert_equal (dl_used (page_h_ro (next)), DL_DATA_SIZE / 2 + DL_REM);

    u32 i = 0;
    for (; i < 10; ++i)
      {
        test_assert_equal (dl_get_byte (page_h_ro (cur), i), _cur[i]);
      }
    for (; i < DL_DATA_SIZE / 2; ++i)
      {
        test_assert_equal (dl_get_byte (page_h_ro (cur), i), _next[i - 10]);
      }
    i = 0;
    for (; i < DL_DATA_SIZE / 2 + DL_REM; ++i)
      {
        test_assert_equal (dl_get_byte (page_h_ro (next), i),
                           _next[i + DL_DATA_SIZE / 2 - 10]);
      }
  }

  // BEFORE
  // [+++10+++____|_____________]
  // [****++++++++|++++___10____]
  // AFTER
  // [+++10+++****|_____________]
  // [++++++++++++|___10____]
  TEST_CASE ("Delete")
  {
    dl_memset (page_h_w (cur), _cur, 9);
    dl_memset (page_h_w (next), _next, DL_DATA_SIZE - 10);

    dlgt_balance_with_next (cur, next);
    test_assert_equal (dl_used (page_h_ro (cur)), 0);
    test_assert_equal (dl_used (page_h_ro (next)), DL_DATA_SIZE - 1);

    u32 i = 0;
    for (; i < 9; ++i)
      {
        test_assert_equal (dl_get_byte (page_h_ro (next), i), _cur[i]);
      }

    // next data
    for (; i < DL_DATA_SIZE - 1; ++i)
      {
        test_assert_equal (dl_get_byte (page_h_ro (next), i), _next[i - 9]);
      }
  }

  pgr_release (f.p, next, PG_DATA_LIST, e);
  pgr_delete_and_release (f.p, &tx, cur, e);

  pgr_commit (f.p, &tx, &f.e);

  pgr_fixture_teardown (&f);
}

static struct three_in_pair
three_in_pair_from (const page_h *prev, const page_h *cur, const page_h *next)
{
  ASSERT (prev == NULL || prev->mode != PHM_NONE);
  ASSERT (cur == NULL || cur->mode != PHM_NONE);
  ASSERT (next == NULL || next->mode != PHM_NONE);

  struct three_in_pair ret = {
    .prev = in_pair_empty,
    .cur = in_pair_empty,
    .next = in_pair_empty,
  };

  if (prev)
    {
      ret.prev = in_pair_from_pgh (prev);
    }
  if (cur)
    {
      ret.cur = in_pair_from_pgh (cur);
    }
  if (next)
    {
      ret.next = in_pair_from_pgh (next);
    }

  return ret;
}

// TODO - graceful error handling and clean up of partial pages
err_t
_ns_balance_and_release (const struct _ns_balance_and_release_params params,
                         error *e)
{
  ASSERT (params.prev->mode == PHM_NONE
          || dlgt_valid_neighbors (page_h_ro (params.prev),
                                   page_h_ro (params.cur)));
  ASSERT (params.next->mode == PHM_NONE
          || dlgt_valid_neighbors (page_h_ro (params.cur),
                                   page_h_ro (params.next)));
  ASSERT (params.output);

  // Upgrade cur to writable - so far there's no garuntees that cur
  // is already writable on entry
  WRAP (pgr_maybe_make_writable (params.db->p, params.tx, params.cur, e));

  const p_size csize = dlgt_get_len (page_h_ro (params.cur));

  *params.output = three_in_pair_from (NULL, params.cur, NULL);

  // Cur needs balancing because it is less than maxlen / 2
  if (csize > 0 && csize < dlgt_get_max_len (page_h_ro (params.cur)) / 2)
    {
      // If next is present - try balancing with next
      if (params.next->mode != PHM_NONE)
        {
          WRAP (pgr_maybe_make_writable (params.db->p, params.tx, params.next,
                                         e));
          dlgt_balance_with_next (params.cur, params.next);
          *params.output = three_in_pair_from (NULL, params.cur, params.next);
        }

      // If prev is present - try balancing with prev
      else if (params.prev->mode != PHM_NONE)
        {
          WRAP (pgr_maybe_make_writable (params.db->p, params.tx, params.prev,
                                         e));
          dlgt_balance_with_prev (params.prev, params.cur);
          *params.output = three_in_pair_from (params.prev, params.cur, NULL);
        }

      // Loop back to next - load next and try again (if next even exists)
      else if (dlgt_get_next (page_h_ro (params.cur)) != PGNO_NULL)
        {
          WRAP (pgr_get_writable (
              params.next, params.tx, PG_INNER_NODE | PG_DATA_LIST,
              dlgt_get_next (page_h_ro (params.cur)), params.db->p, e));
          dlgt_balance_with_next (params.cur, params.next);
          *params.output = three_in_pair_from (NULL, params.cur, params.next);
        }

      // Loop back to start - load prev and try again (if prev even exists)
      else if (dlgt_get_prev (page_h_ro (params.cur)) != PGNO_NULL)
        {
          WRAP (pgr_get_writable (
              params.prev, params.tx, PG_INNER_NODE | PG_DATA_LIST,
              dlgt_get_prev (page_h_ro (params.cur)), params.db->p, e));
          dlgt_balance_with_prev (params.prev, params.cur);
          *params.output = three_in_pair_from (params.prev, params.cur, NULL);
        }
      else
        {
          // This balance was performed on a root  node
          ASSERT (dlgt_is_root (page_h_ro (params.cur)));
        }
    }
  else
    {
      // there's no need to balance
    }

  // Assume cur is not a root; override below if it is
  params.root->isroot = false;
  if (dlgt_is_root (page_h_ro (params.cur)))
    {
      params.root->isroot = true;
      params.root->root = page_h_pgno (params.cur);
    }

  // Need to delete cur
  if (dlgt_get_len (page_h_ro (params.cur)) == 0)
    {
      i_log_trace ("balance: deleting page %" PRpgno "\n", page_h_pgno (cur));

      // Fetch prev and next for link re writing
      if (!params.root->isroot)
        {
          // Load prev sibling if the caller did not already pin it
          if (params.prev->mode == PHM_NONE)
            {
              const pgno prev_pg = dlgt_get_prev (page_h_ro (params.cur));
              if (prev_pg != PGNO_NULL)
                {
                  WRAP (pgr_get_writable (params.prev, params.tx,
                                          PG_INNER_NODE | PG_DATA_LIST,
                                          prev_pg, params.db->p, e));
                }
            }

          // Load next sibling if the caller did not already pin it
          if (params.next->mode == PHM_NONE)
            {
              const pgno next_pg = dlgt_get_next (page_h_ro (params.cur));
              if (next_pg != PGNO_NULL)
                {
                  WRAP (pgr_get_writable (
                      params.next, params.tx, PG_INNER_NODE | PG_DATA_LIST,
                      dlgt_get_next (page_h_ro (params.cur)), params.db->p,
                      e));
                }
            }

          // Bridge the gap: prev->next = next, next->prev = prev
          dlgt_link (page_h_w_or_null (params.prev),
                     page_h_w_or_null (params.next));

          // We might have turned prev / next into a new root by deleting cur
          if (params.prev->mode != PHM_NONE
              && dlgt_is_root (page_h_ro (params.prev)))
            {
              params.root->root = page_h_pgno (params.prev);
              params.root->isroot = true;
            }
          else if (params.next->mode != PHM_NONE
                   && dlgt_is_root (page_h_ro (params.next)))
            {
              params.root->root = page_h_pgno (params.next);
              params.root->isroot = true;
            }
        }

      // Otherwise cur is still root but we will delete it so now it's NULL
      else
        {
          // balance performed on root and deleted
          params.root->root = PGNO_NULL;
        }

      WRAP (pgr_delete_and_release (params.db->p, params.tx, params.cur, e));
    }

  // One final common cleanup
  WRAP (pgr_release_if_exists (params.db->p, params.prev, PG_DATA_LIST | PG_INNER_NODE, e));
  WRAP (pgr_release_if_exists (params.db->p, params.cur, PG_DATA_LIST | PG_INNER_NODE, e));
  WRAP (pgr_release_if_exists (params.db->p, params.next, PG_DATA_LIST | PG_INNER_NODE, e));

  return SUCCESS;
}
