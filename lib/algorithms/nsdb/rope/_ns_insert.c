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

#include "algorithms/node_updates.h"
#include "algorithms/nsdb/rope/algorithms.h"
#include "c_specx.h"
#include "pager.h"
#include "pager/page_h.h"
#include "pages/data_list.h"
#include "pages/page.h"

/*
 * Insert data into the R+Tree at the byte offset given by params->bofst.
 *
 * Seeks to the target data-list page, splits it at the insertion point,
 * streams new bytes from params->src into the page chain, re-appends the
 * displaced tail, re-links the chain, then balances the leaf and propagates
 * size changes up the inner-node tree via _ns_rebalance().
 *
 * When nelem is 0, bytes are consumed from src until it is exhausted.
 * params->root is updated in place if the root changes.
 */
sb_size
_ns_insert (struct _ns_insert_params *params, error *e)
{
  page_h prev = page_h_create ();
  page_h cur = page_h_create ();
  page_h next = page_h_create ();

  u8 temp_buf[DL_DATA_SIZE];
  p_size tbw = 0;
  p_size tbl = 0;

  struct node_updates *output = NULL;
  struct node_updates *rb_nupd2 = NULL;
  struct three_in_pair tip_out;
  struct root_update root;

  p_size lidx = 0;
  b_size total_written = 0;

  struct _ns_seek_params seek = {
    .db = params->db,
    .tx = params->tx,
    .root = params->root,
    .bofst = params->bofst,
    .save_stack = true,
    .sp = 0,
  };

  if (params->root == PGNO_NULL)
    {
      if (pgr_new (&cur, params->db->p, params->tx, PG_DATA_LIST, e))
        {
          goto failed;
        }

      params->root = page_h_pgno (&cur);
    }
  else
    {
      if (_ns_seek (&seek, e))
        {
          goto failed;
        }

      cur = page_h_xfer_ownership (&seek.pg);
      lidx = seek.lidx;

      if (pgr_make_writable (params->db->p, params->tx, &cur, e))
        {
          goto failed;
        }
    }

  pgno last = dl_get_next (page_h_ro (&cur));
  tbl = dl_read_out_from (page_h_w (&cur), temp_buf, lidx);
  output = nupd_init (page_h_pgno (&cur), 0, e);
  if (output == NULL)
    {
      goto failed;
    }

  const b_size total_to_write = params->size * params->nelem;

  while (params->nelem == 0 || total_written < total_to_write)
    {
      p_size avail = dl_avail (page_h_ro (&cur));

      if (avail == 0)
        {
          ASSERT (lidx == DL_DATA_SIZE);

          if (pgr_new (&next, params->db->p, params->tx, PG_DATA_LIST, e))
            {
              goto failed;
            }

          dl_set_next (page_h_w (&cur), page_h_pgno (&next));
          dl_set_prev (page_h_w (&next), page_h_pgno (&cur));

          if (nupd_commit_1st_right (output, pgh_unravel (&cur), e))
            {
              goto failed;
            }

          if (pgr_release (params->db->p, &cur, PG_DATA_LIST, e))
            {
              goto failed;
            }

          cur = page_h_xfer_ownership (&next);
          lidx = 0;
          avail = dl_avail (page_h_ro (&cur));
        }

      p_size next_amount;
      if (params->nelem == 0)
        {
          next_amount = avail;
        }
      else
        {
          next_amount = MIN (avail, (p_size)(total_to_write - total_written));
        }

      i32 written = stream_bread (dl_avail_data (page_h_w (&cur)), 1,
                                  next_amount, params->src, e);
      if (written < 0)
        {
          goto failed;
        }

      if (written == 0 && stream_isdone (params->src))
        {
          break;
        }

      dl_set_used (page_h_w (&cur), dl_used (page_h_ro (&cur)) + written);
      lidx += (p_size)written;
      total_written += (b_size)written;
    }

  while (tbw < tbl)
    {
      p_size written = dl_append (page_h_w (&cur), temp_buf + tbw, tbl - tbw);

      lidx += written;
      tbw += written;

      if (lidx == DL_DATA_SIZE && tbw < tbl)
        {
          ASSERT (lidx == DL_DATA_SIZE);

          if (pgr_new (&next, params->db->p, params->tx, PG_DATA_LIST, e))
            {
              goto failed;
            }

          dl_set_next (page_h_w (&cur), page_h_pgno (&next));
          dl_set_prev (page_h_w (&next), page_h_pgno (&cur));

          if (nupd_commit_1st_right (output, pgh_unravel (&cur), e))
            {
              goto failed;
            }

          if (pgr_release (params->db->p, &cur, PG_DATA_LIST, e))
            {
              goto failed;
            }

          page_h_xfer_ownership_ptr (&cur, &next);
          lidx = 0;
        }
    }

  if (last != PGNO_NULL && last != dl_get_next (page_h_ro (&cur)))
    {
      if (pgr_get_writable (&next, params->tx, PG_DATA_LIST, last, params->db->p, e))
        {
          goto failed;
        }

      dlgt_link (page_h_w (&cur), page_h_w (&next));
    }

  struct _ns_balance_and_release_params bparams = {
    .db = params->db,
    .tx = params->tx,
    .output = &tip_out,
    .root = &root,
    .prev = &prev,
    .cur = &cur,
    .next = &next,
  };

  if (_ns_balance_and_release (bparams, e))
    {
      goto failed;
    }

  if (nupd_append_tip_right (output, tip_out, e))
    {
      goto failed;
    }

  struct _ns_rebalance_params rebalance = {
    .db = params->db,
    .tx = params->tx,
    .root = params->root,
    .pstack = seek.pstack,
    .sp = seek.sp,
    .input = rb_nupd2,
    .output = output,
    .layer_root = root,
  };

  output = NULL;
  rb_nupd2 = NULL;

  err_t ret = _ns_rebalance (&rebalance, e);

  if (rebalance.output)
    {
      nupd_free (rebalance.output);
    }
  if (rebalance.input)
    {
      nupd_free (rebalance.input);
    }

  if (ret)
    {
      goto failed;
    }

  params->root = rebalance.root;

  return (sb_size)total_written;

failed:
  pgr_cancel_if_exists (params->db->p, &prev);
  pgr_cancel_if_exists (params->db->p, &cur);
  pgr_cancel_if_exists (params->db->p, &next);

  if (output)
    {
      nupd_free (output);
    }
  if (rb_nupd2)
    {
      nupd_free (rb_nupd2);
    }

  for (u32 i = 0; i < seek.sp; ++i)
    {
      pgr_cancel_if_exists (params->db->p, &seek.pstack[i].pg);
    }

  return error_trace (e);
}
