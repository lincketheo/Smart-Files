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
 * Remove elements from the R+Tree with an optional stride.
 *
 * Unlike insert/write, remove must compact the byte stream in place: the gap
 * left by deleted bytes is closed by sliding the trailing data forward.  To
 * avoid re-reading pages, two cursors scan the leaf level simultaneously:
 *
 *   writer — the destination cursor; data is compacted into this position.
 *   reader — the source cursor; always ahead of (or equal to) writer.
 *
 * When writer == reader (reader.mode == PHM_NONE), there is no separation
 * yet and both cursors refer to the same page via s.writer.
 *
 * The outer loop alternates between two phases:
 *
 *   REMOVING — the reader advances by [size] bytes without copying them to
 *              the writer; this is what "removes" the elements.  If
 *              params->dest is non-NULL the removed bytes are streamed out
 *              before being discarded.  After [size] bytes, bnext resets to
 *              (stride-1)*size and phase switches to SKIPPING.
 *
 *   SKIPPING — [size*(stride-1)] bytes are copied from reader to writer
 *              (these are the elements that must survive).  The writer
 *              page is flushed when full, and the reader page is deleted
 *              once fully consumed if it is distinct from the writer page.
 *
 * After the remove/skip loop finishes ("drain" label), all remaining reader
 * data is copied into writer pages and exhausted reader pages are deleted.
 *
 * Phase 3 validates that total_removed is a multiple of [size], then calls
 * _ns_balance_and_release() and _ns_rebalance() to fix up the leaf level
 * and propagate size decrements up the inner-node tree.
 */
enum remove_phase
{
  REMOVING,
  SKIPPING,
};

struct remove_state
{
  // Pages
  page_h writer;
  page_h reader;

  // Indices
  p_size write_idx;
  p_size read_idx;

  // Accumulated node updates
  struct node_updates *output;

  // Pager / transaction context
  struct nsdb *db;
  struct txn *tx;

  // Remove progress
  b_size total_removed;
  b_size max_remove;
  p_size bnext;

  enum remove_phase phase;
};

static page_h *
remove_creader (struct remove_state *s)
{
  if (s->reader.mode == PHM_NONE)
    {
      return &s->writer;
    }
  return &s->reader;
}

/*
 * Flush the writer page and advance to the next one.
 *
 * Sets the page's used-byte count to write_idx, records the size delta in
 * output, releases the page, then advances writer to the next page in the
 * chain (or to the current reader page if one is open).
 */
static err_t
advance_writer (struct remove_state *s, error *e)
{
  ASSERT (s->write_idx > DL_DATA_SIZE / 2);

  in_set_len (page_h_w (&s->writer), s->write_idx);
  if (nupd_commit_1st_right (s->output, pgh_unravel (&s->writer), e))
    {
      goto failed;
    }

  if (s->reader.mode == PHM_NONE)
    {
      const pgno npg = in_get_next (page_h_ro (&s->writer));

      if (pgr_release (s->db->p, &s->writer, PG_DATA_LIST, e))
        {
          goto failed;
        }

      if (npg != PGNO_NULL)
        {
          if (pgr_get_writable (&s->writer, s->tx, PG_DATA_LIST, npg, s->db->p,
                                e))
            {
              goto failed;
            }
        }
    }
  else
    {
      if (pgr_release (s->db->p, &s->writer, PG_DATA_LIST, e))
        {
          goto failed;
        }
      page_h_xfer_ownership_ptr (&s->writer, &s->reader);
    }

  s->write_idx = 0;

  return SUCCESS;

failed:
  return error_trace (e);
}

/*
 * Advance the reader to the next page.
 *
 * Three cases:
 *   1. reader == writer (no separation yet): look at writer's next link;
 *      open the successor as the new reader.
 *   2. writer page is more than half full: flush writer first
 * (advance_writer), then open the current page's next as the new reader.
 *   3. reader page is fully consumed: delete it, re-link writer → next, open
 *      the next page as the new reader, and record the deletion in output.
 *
 * Sets *iseof=true and finalizes write_idx when the chain has no successor.
 */
static err_t
advance_reader (struct remove_state *s, bool *iseof, error *e)
{
  page_h next = page_h_create ();
  *iseof = false;

  if (s->reader.mode == PHM_NONE)
    {
      const pgno npg = dlgt_get_next (page_h_ro (&s->writer));

      if (npg != PGNO_NULL)
        {
          if (pgr_get_writable (&s->reader, s->tx, PG_DATA_LIST, npg, s->db->p,
                                e))
            {
              goto failed;
            }
        }
    }
  else if (s->write_idx > DL_DATA_SIZE / 2)
    {
      if (advance_writer (s, e))
        {
          goto failed;
        }

      ASSERT (page_h_pgno (&s->writer) == page_h_pgno (remove_creader (s)));

      const pgno npg = dlgt_get_next (page_h_ro (&s->writer));

      if (npg != PGNO_NULL)
        {
          if (pgr_get_writable (&s->reader, s->tx, PG_DATA_LIST, npg, s->db->p,
                                e))
            {
              goto failed;
            }
        }
    }
  else
    {
      const pgno rpg = page_h_pgno (&s->reader);
      const pgno npg = in_get_next (page_h_ro (&s->reader));

      if (npg != PGNO_NULL)
        {
          if (pgr_get_writable (&next, s->tx, PG_DATA_LIST, npg, s->db->p, e))
            {
              goto failed;
            }
        }

      if (pgr_delete_and_release (s->db->p, s->tx, &s->reader, e))
        {
          goto failed;
        }

      dlgt_link (page_h_w (&s->writer), page_h_w_or_null (&next));
      page_h_xfer_ownership_ptr (&s->reader, &next);

      if (nupd_append_2nd_right (s->output, pgh_unravel (&s->writer), rpg, 0,
                                 e))
        {
          goto failed;
        }
    }

  if (s->reader.mode == PHM_NONE)
    {
      in_set_len (page_h_w (&s->writer), s->write_idx);
      s->read_idx = s->write_idx;
      *iseof = true;
    }
  else
    {
      s->read_idx = 0;
    }

  return SUCCESS;

failed:
  pgr_cancel_if_exists (s->db->p, &next);
  return error_trace (e);
}

static p_size
removing_next (const struct remove_state *s, const page *sro)
{
  p_size next = s->bnext;
  next = MIN (next, dl_used (sro) - s->read_idx);

  if (s->max_remove > 0)
    {
      next = MIN (next, s->max_remove - s->total_removed);
    }

  return next;
}

static p_size
skipping_next (const struct remove_state *s, const page *sro)
{
  p_size next = s->bnext;
  next = MIN (next, DL_DATA_SIZE - s->write_idx);
  next = MIN (next, dl_used (sro) - s->read_idx);
  return next;
}

static p_size
drain_reader_next (const struct remove_state *s, const page *sro)
{
  p_size next = DL_DATA_SIZE - s->write_idx;
  next = MIN (next, dl_used (sro) - s->read_idx);
  return next;
}

static sb_size
_ns_remove_once (struct _ns_remove_params *params, error *e)
{
  struct remove_state s = {
    .writer = page_h_create (),
    .reader = page_h_create (),
    .write_idx = 0,
    .read_idx = 0,
    .total_removed = 0,
    .max_remove = params->nelem * params->size,
    .bnext = params->size,
    .phase = REMOVING,
    .output = NULL,
    .db = params->db,
    .tx = params->tx,
  };

  page_h prev = page_h_create ();
  page_h next = page_h_create ();

  struct node_updates *rb_nupd2 = NULL;
  struct three_in_pair tip_out;
  struct root_update root = { 0 };

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
      return 0;
    }

  if (_ns_seek (&seek, e))
    {
      goto failed;
    }

  s.writer = page_h_xfer_ownership (&seek.pg);
  s.write_idx = seek.lidx;

  if (pgr_make_writable (params->db->p, params->tx, &s.writer, e))
    {
      goto failed;
    }

  // Pivot page for nupd is the leaf at the seek point
  s.output = nupd_init (page_h_pgno (&s.writer),
                        dl_used (page_h_ro (&s.writer)), e);
  if (s.output == NULL)
    {
      goto failed;
    }
  // Both cursors start at the same byte; reader will pull ahead as bytes are
  // removed
  s.read_idx = s.write_idx;

  // Phase 1: Remove / Skip

  while (s.max_remove == 0 || s.total_removed < s.max_remove)
    {
      const page *sro = page_h_ro (remove_creader (&s));
      p_size rlen = dl_used (sro);

      switch (s.phase)
        {
        case REMOVING:
          {
            p_size next_amount = removing_next (&s, sro);

            if (next_amount == 0)
              {
                // Reader page exhausted; advance to the next
                // page
                ASSERT (s.read_idx == rlen);

                bool iseof;
                if (advance_reader (&s, &iseof, e))
                  {
                    goto failed;
                  }

                if (iseof)
                  {
                    goto drain;
                  }

                continue;
              }

            // Stream out removed bytes to caller if they want them
            if (params->dest)
              {
                i32 written
                    = stream_bwrite ((u8 *)dl_get_data (sro) + s.read_idx, 1,
                                     next_amount, params->dest, e);
                if (written < 0)
                  {
                    goto failed;
                  }
                ASSERT ((p_size)written == next_amount);
              }

            // Advance reader only; writer stays behind, creating
            // the gap
            s.read_idx += next_amount;
            s.total_removed += next_amount;
            s.bnext -= next_amount;

            if (s.bnext == 0)
              {
                // One element removed; transition to copying
                // the next (stride-1) survivors
                s.bnext = params->size * (params->stride - 1);
                if (s.bnext > 0)
                  {
                    s.phase = SKIPPING;
                  }
                else
                  {
                    // stride == 1: no survivors to copy,
                    // stay REMOVING
                    s.bnext = params->size;
                  }
              }

            if (s.max_remove > 0 && s.total_removed == s.max_remove)
              {
                goto drain;
              }

            break;
          }

        case SKIPPING:
          {
            p_size next_amount = skipping_next (&s, sro);

            if (next_amount == 0)
              {
                if (s.read_idx == rlen)
                  {
                    // Reader page exhausted; advance
                    bool iseof;
                    if (advance_reader (&s, &iseof, e))
                      {
                        goto failed;
                      }

                    if (iseof)
                      {
                        goto drain;
                      }

                    continue;
                  }
                else if (s.write_idx == DL_DATA_SIZE)
                  {
                    // Writer page full; flush and advance
                    // to the next writer page
                    if (advance_writer (&s, e))
                      {
                        goto failed;
                      }

                    continue;
                  }

                UNREACHABLE ();
              }

            // Copy surviving bytes from reader position to writer
            // position
            dl_dl_memmove_permissive (page_h_w (&s.writer),
                                      page_h_ro (remove_creader (&s)),
                                      s.write_idx, s.read_idx, next_amount);

            s.write_idx += next_amount;
            s.read_idx += next_amount;
            s.bnext -= next_amount;

            if (s.bnext == 0)
              {
                // Skip window done; start removing the next
                // element
                s.bnext = params->size;
                s.phase = REMOVING;
              }

            break;
          }
        }

      // Caller's destination stream full; stop removing and drain the
      // tail
      if (params->dest && stream_isdone (params->dest))
        {
          goto drain;
        }
    }

drain:
  // Phase 2: Drain remaining reader pages into writer

  while (true)
    {
      const page *sro = page_h_ro (remove_creader (&s));
      p_size rlen = dl_used (sro);

      p_size next_amount = drain_reader_next (&s, sro);

      if (next_amount == 0)
        {
          if (s.read_idx == rlen)
            {
              // Finalize current writer page size before moving
              // on
              dl_set_used (page_h_w (&s.writer), s.write_idx);

              if (s.reader.mode != PHM_NONE)
                {
                  pgno rpg = page_h_pgno (&s.reader);
                  pgno npg = in_get_next (page_h_ro (&s.reader));

                  // Prefetch the reader's successor so we
                  // can re-link past it
                  if (npg != PGNO_NULL)
                    {
                      if (pgr_get_writable (&next, params->tx, PG_DATA_LIST,
                                            npg, params->db->p, e))
                        {
                          goto failed;
                        }
                    }

                  // Reader page fully consumed; delete it
                  // and skip it in the chain
                  if (pgr_delete_and_release (params->db->p, params->tx,
                                              &s.reader, e))
                    {
                      goto failed;
                    }

                  dlgt_link (page_h_w (&s.writer), page_h_w_or_null (&next));
                  page_h_xfer_ownership_ptr (&s.reader, &next);

                  // Record the deletion in nupd so the inner
                  // tree is updated
                  if (nupd_append_2nd_right (s.output, pgh_unravel (&s.writer),
                                             rpg, 0, e))
                    {
                      goto failed;
                    }
                  s.read_idx = 0;

                  if (s.reader.mode == PHM_NONE)
                    {
                      break;
                    }

                  continue;
                }
              else
                {
                  // No separate reader; writer == reader,
                  // drain is complete
                  break;
                }
            }
          else if (s.write_idx >= DL_DATA_SIZE)
            {
              // Writer page full before reader page is
              // exhausted; flush writer
              if (advance_writer (&s, e))
                {
                  goto failed;
                }
              continue;
            }
          else
            {
              UNREACHABLE ();
            }
        }

      // Copy bytes from reader into the compacted writer position
      dl_dl_memmove_permissive (page_h_w (&s.writer),
                                page_h_ro (remove_creader (&s)), s.write_idx,
                                s.read_idx, next_amount);

      s.write_idx += next_amount;
      s.read_idx += next_amount;
    }

  // Phase 3: Validate, balance, rebalance

  if (s.total_removed % params->size != 0)
    {
      error_causef (e, ERR_CORRUPT,
                    "removed %" PRb_size
                    " bytes, not a multiple of element size "
                    "%" PRb_size,
                    s.total_removed, params->size);
      goto failed;
    }

  // Transfer reader (successor of the last written page) to next for balance
  next = page_h_xfer_ownership (&s.reader);

  struct _ns_balance_and_release_params bparams = {
    .db = params->db,
    .tx = params->tx,
    .output = &tip_out,
    .root = &root,
    .prev = &prev,
    .cur = &s.writer,
    .next = &next,
  };
  if (_ns_balance_and_release (bparams, e))
    {
      goto failed;
    }

  if (nupd_append_tip_right (s.output, tip_out, e))
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
    .output = s.output,
    .layer_root = root,
  };

  // Xfer ownership
  rb_nupd2 = NULL;
  s.output = NULL;

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

  return (sb_size)(s.total_removed / params->size);

failed:
  pgr_cancel_if_exists (params->db->p, &prev);
  pgr_cancel_if_exists (params->db->p, &s.writer);
  pgr_cancel_if_exists (params->db->p, &next);
  pgr_cancel_if_exists (params->db->p, &s.reader);

  if (rb_nupd2)
    {
      nupd_free (rb_nupd2);
    }
  if (s.output)
    {
      nupd_free (s.output);
    }

  for (u32 i = 0; i < seek.sp; ++i)
    {
      pgr_cancel_if_exists (params->db->p, &seek.pstack[i].pg);
    }

  return error_trace (e);
}

/*
 * Remove data from the R+Tree, chunked to bound rebalancing cost.
 *
 * Mirrors _ns_insert: each chunk removes at most NUPD_MAX_DATA_LENGTH /
 * elem_size elements so that the rebalance algorithm never sees more inner-
 * node updates than NUPD_MAX_DATA_LENGTH bytes per pass.  The root pointer
 * is threaded from one chunk to the next.
 */
sb_size
_ns_remove (struct _ns_remove_params *params, error *e)
{
  b_size total_removed = 0;
  const b_size elem_size = (b_size)params->size;
  const b_size elems_per_chunk = MAX (1, NUPD_MAX_DATA_LENGTH / elem_size);
  b_size elems_remaining = (b_size)params->nelem;

  while (elems_remaining > 0)
    {
      const b_size chunk_elems = MIN (elems_remaining, elems_per_chunk);

      struct _ns_remove_params chunk_params = *params;
      chunk_params.nelem = (p_size)chunk_elems;

      const sb_size removed = _ns_remove_once (&chunk_params, e);
      if (removed < 0)
        {
          goto failed;
        }

      params->root = chunk_params.root;
      total_removed += (b_size)removed;
      elems_remaining -= chunk_elems;
    }

  return (sb_size)total_removed;

failed:
  return error_trace (e);
}
