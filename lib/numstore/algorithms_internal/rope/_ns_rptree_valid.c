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
#include "tlclib/dev/error.h"
#include "tlclib/ds/hash_table.h"
#include "tlclib/core/macros.h"
#include "tlclib/memory/slab_alloc.h"
#include "numstore/algorithms_internal/rope/algorithms.h"
#include "numstore/errors.h"
#include "paging/pager/page_h.h"
#include "paging/pages/inner_node.h"
#include "paging/pages/page_delegate.h"

struct frame
{
  pgno pg;
  struct hnode node;
};

struct rptree_valid_ctx
{
  struct slab_alloc pg_alloc;
  struct htable *table;
};

static bool
frame_eq (const struct hnode *left, const struct hnode *right)
{
  const struct frame *_left = container_of (left, struct frame, node);
  const struct frame *_right = container_of (right, struct frame, node);
  return _left->pg == _right->pg;
}

static err_t
rptree_valid_recursive (struct nsdb *db, const pgno pg, bool isroot, const b_size exp_size,
                        struct rptree_valid_ctx *ctx, error *e)
{
  // Check if this page was double counted
  struct frame key = {
    .pg = pg,
  };
  hnode_init (&key.node, (u32)pg);
  struct hnode **dup = htable_lookup (ctx->table, &key.node, frame_eq);
  if (dup != NULL)
    {
      error_causef (e, ERR_CORRUPT, "Page: %" PRpgno " was double counted",
                    pg);
      goto failed;
    }
  else
    {
      struct frame *f = slab_alloc_alloc (&ctx->pg_alloc, e);
      if (f == NULL)
        {
          goto failed;
        }
      f->pg = pg;
      hnode_init (&f->node, pg);
      htable_insert (ctx->table, &f->node);
    }

  // Validate this page
  page_h pivot = page_h_create ();
  if (pgr_get (&pivot, PG_DATA_LIST | PG_VAR_PAGE, pg, db->p, e))
    {
      goto failed;
    }

  // Check that root matches
  if (isroot != dlgt_is_root (page_h_ro (&pivot)))
    {
      error_causef (e, ERR_CORRUPT, "page %" PRpgno " should be non-root", pg);
      goto failed;
    }

  // Check that size matches
  if (dlgt_get_size (page_h_ro (&pivot)) != exp_size)
    {
      error_causef (e, ERR_CORRUPT,
                    "page %" PRpgno ": expected %" PRb_size
                    " bytes, got %" PRb_size,
                    pg, exp_size, in_get_size (page_h_ro (&pivot)));
      goto failed;
    }

  switch (page_h_type (&pivot))
    {
    case PG_DATA_LIST:
      {
        if (pgr_release (db->p, &pivot, PG_DATA_LIST, e))
          {
            goto failed;
          }
        return error_trace (e);
      }
    case PG_VAR_PAGE:
      {
        struct in_pair nodes[IN_MAX_KEYS];
        const struct in_data data = in_get_data (page_h_ro (&pivot), nodes);

        if (pgr_release (db->p, &pivot, PG_DATA_LIST, e))
          {
            goto failed;
          }

        // Validate each child
        for (u32 i = 0; i < data.len; ++i)
          {
            if (rptree_valid_recursive (db, data.nodes[i].pg, false,
                                        data.nodes[i].key, ctx, e))
              {
                goto failed;
              }
          }

        return error_trace (e);
      }
    default:
      {
        UNREACHABLE ();
      }
    }

failed:
  pgr_cancel_if_exists (db->p, &pivot);
  return error_trace (e);
}

err_t
_ns_rptree_valid (struct nsdb *db, const pgno rpt_root, const b_size nbytes, error *e)
{
  struct rptree_valid_ctx ctx;
  slab_alloc_init (&ctx.pg_alloc, sizeof (struct frame), 512);

  ctx.table = htable_create (1000, e);
  if (ctx.table == NULL)
    {
      return error_trace (e);
    }

  if (rptree_valid_recursive (db, rpt_root, true, nbytes, &ctx, e))
    {
      goto theend;
    }

theend:
  htable_free (ctx.table);
  slab_alloc_destroy (&ctx.pg_alloc);
  return error_trace (e);
}
