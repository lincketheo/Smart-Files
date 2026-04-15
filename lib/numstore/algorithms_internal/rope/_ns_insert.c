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

#include "core/stream.h"
#include "numstore/algorithms_internal/node_updates.h"
#include "numstore/algorithms_internal/rope/algorithms.h"
#include "paging/pager.h"
#include "paging/pager/page_h.h"
#include "paging/pages/data_list.h"
#include "paging/pages/page.h"

/*
 * Insert one chunk of data into the R+Tree at a given byte offset.
 *
 * Seeks to the insertion point, then streams new bytes into the data-list
 * chain while preserving the bytes that originally followed the insertion
 * point:
 *
 *   1. Seek to the data-list page containing bofst, upgrade it for writing.
 *      Read and truncate the displaced tail (bytes from lidx to end of page)
 *      into temp_buf.
 *
 *   2. Write new data from params->src page-by-page.  When a page fills up,
 *      allocate the next one, link it in, and continue.  Size changes (new
 *      pages and byte counts) are recorded in the [output] node_updates list.
 *
 *   3. Re-append temp_buf after the new data, allocating more pages if
 *      needed.
 *
 *   4. Re-link cur to the original successor page if new pages were spliced
 *      in between them.
 *
 *   5. Call _ns_balance_and_release() to finalize the leaf level and emit the
 *      tip update, then call _ns_rebalance() to propagate size changes up the
 *      inner-node stack that _ns_seek() saved.
 *
 * Insert is chunked by the caller (_ns_insert) to avoid creating inner-node
 * updates that exceed NUPD_MAX_DATA_LENGTH in a single rebalance pass.
 */
static sb_size
_ns_insert_once (struct _ns_insert_params *params, error *e)
{
  page_h prev = page_h_create ();
  page_h cur = page_h_create ();
  page_h next = page_h_create ();

  u8 temp_buf[DL_DATA_SIZE]; // right half of the first page saved for
                             // re-appending
  p_size tbw = 0;            // bytes already written back from temp_buf
  p_size tbl = 0;            // total bytes saved in temp_buf

  struct node_updates *output = NULL;   // size changes discovered while writing the bottom level
  struct node_updates *rb_nupd2 = NULL; // rebalance produces a second round of updates
  struct three_in_pair tip_out;
  struct root_update root;

  p_size lidx = 0;          // byte offset within the current page
  b_size total_written = 0; // total bytes written from params->src

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
      // Tree is empty: allocate the first data-list page.
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

  // Save the page number of the node that originally followed cur so we
  // can re-link at the end of the write phase.  Then truncate cur at
  // lidx and stash the displaced bytes in temp_buf.
  pgno last = dl_get_next (page_h_ro (&cur));
  tbl = dl_read_out_from (page_h_w (&cur), temp_buf, lidx);
  output = nupd_init (page_h_pgno (&cur), 0, e);
  if (output == NULL)
    {
      goto failed;
    }

  // stream new data from params->src into the page chain.
  const b_size total_to_write = params->size * params->nelem;

  while (params->nelem == 0 || total_written < total_to_write)
    {
      p_size avail = dl_avail (page_h_ro (&cur));

      // Current page is full — allocate a new one and
      // advance.
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

      p_size next_amount = 0;
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

  // append the saved tail (temp_buf) back after the new data.
  while (tbw < tbl)
    {
      p_size written = dl_append (page_h_w (&cur), temp_buf + tbw, tbl - tbw);

      lidx += written;
      tbw += written;

      // Advance to a new page only when the current one is
      // full AND there is still more temp data to write.
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

  // Re-link cur to `last` only when new pages were actually inserted
  // between them (i.e. last is non-null and is no longer the direct
  // successor of cur).
  // */
  if (last != PGNO_NULL && last != dl_get_next (page_h_ro (&cur)))
    {
      if (pgr_get_writable (&next, params->tx, PG_DATA_LIST, last,
                            params->db->p, e))
        {
          goto failed;
        }

      dlgt_link (page_h_w (&cur), page_h_w (&next));
    }

  // balance cur and propagate size changes up the tree.
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

  // Rebalance
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

  // Transferred ownership to rebalance params
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

/*
 * Insert data into the R+Tree, chunking large inserts to bound rebalancing.
 *
 * Rebalancing must propagate size deltas up every level of the inner-node
 * tree.  A single inner node holds at most IN_MAX_KEYS children; if a single
 * insert touches far more bytes than fit in one node, the rebalance algorithm
 * would need to issue far more updates than the node_updates buffer supports.
 *
 * To avoid this, the insert is broken into chunks of at most
 * NUPD_MAX_DATA_LENGTH bytes.  Each chunk is inserted independently via
 * _ns_insert_once(), updating the root pointer after each call so that the
 * next chunk starts from the correct (possibly modified) root.
 *
 * When nelem is 0 the stream is read until exhausted, also chunked.
 */
sb_size
_ns_insert (struct _ns_insert_params *params, error *e)
{
  b_size total_written = 0;

  if (params->nelem > 0)
    {
      const b_size elem_size = (b_size)params->size;
      const b_size elems_per_chunk = MAX (1, NUPD_MAX_DATA_LENGTH / elem_size);
      b_size elems_remaining = (b_size)params->nelem;

      while (elems_remaining > 0)
        {
          const b_size chunk_elems = MIN (elems_remaining, elems_per_chunk);

          struct _ns_insert_params chunk_params = *params;
          chunk_params.nelem = (p_size)chunk_elems;
          chunk_params.bofst = params->bofst + total_written;

          const sb_size written = _ns_insert_once (&chunk_params, e);
          if (written < 0)
            {
              goto failed;
            }

          params->root = chunk_params.root;
          total_written += (b_size)written;
          elems_remaining -= chunk_elems;
        }
    }
  else
    {
      while (!stream_isdone (params->src))
        {
          struct stream_limit_ctx lctx;
          struct stream limited;
          stream_limit_init (&limited, &lctx, params->src,
                             NUPD_MAX_DATA_LENGTH);

          struct _ns_insert_params chunk_params = *params;
          chunk_params.src = &limited;
          chunk_params.nelem = 0;
          chunk_params.bofst = params->bofst + total_written;

          const sb_size written = _ns_insert_once (&chunk_params, e);
          if (written < 0)
            {
              goto failed;
            }

          params->root = chunk_params.root;
          total_written += (b_size)written;

          // If the limit wasn't reached the
          // underlying stream is exhausted.
          if ((b_size)written < NUPD_MAX_DATA_LENGTH)
            {
              break;
            }
        }
    }

  return (sb_size)total_written;

failed:
  return error_trace (e);
}
