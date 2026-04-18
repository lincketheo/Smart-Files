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

#include "algorithms/nsdb/var/algorithms.h"
#include "c_specx.h"
#include "nstypes.h"
#include "pager.h"
#include "pager/page_h.h"
#include "pages/page.h"
#include "pages/var_hash_page.h"
#include "pages/var_page.h"

/*
 * Delete a variable and reclaim all its storage.
 *
 * Three steps:
 *
 *   1. _find_var_page() in FP_FIND mode locates the variable's PG_VAR_PAGE,
 *      keeping prev (the page or hash-bucket that points to cur) pinned.
 *
 *   2. _ns_remove() deletes every element from the R+Tree.  After this,
 *      rparams.root is PGNO_NULL (the tree has been fully drained and freed).
 *
 *   3. Unlink cur from the hash chain.  Two sub-cases:
 *        - prev is PG_VAR_HASH_PAGE: the bucket pointer is cleared to
 *          cur->next (or PGNO_NULL if cur was the only node).
 *        - prev is PG_VAR_PAGE: prev->next is updated to skip cur.
 *      Then all PG_VAR_PAGE/PG_VAR_TAIL overflow pages chained from cur are
 *      deleted in sequence.
 */
err_t
_ns_var_delete (const struct _ns_var_delete_params params, error *e)
{
  page_h prev = page_h_create ();
  page_h cur = page_h_create ();
  page_h ovnext = page_h_create ();

  struct _ns_find_var_page_params fparams = {
    .db = params.db,
    .tx = params.tx,

    .vname = params.vname,
    .dvar = NULL,
    .mode = FP_FIND,

    .hpos = PGNO_NULL,
    .prev = &prev,
    .cur = &cur,
  };

  if (_ns_find_var_page (&fparams, e))
    {
      goto failed;
    }

  struct _ns_remove_params rparams = {
    .db = params.db,
    .dest = NULL,
    .tx = params.tx,
    .root = fparams.dvar->rpt_root,
    .size = 1,
    .bofst = 0,
    .stride = 1,
    .nelem = fparams.dvar->nbytes,
  };

  if (_ns_remove (&rparams, e))
    {
      goto failed;
    }

  ASSERT (rparams.root == PGNO_NULL);

  switch (page_h_type (&prev))
    {
      // Previous is the root hash page
    case PG_VAR_HASH_PAGE:
      {
        if (pgr_make_writable (params.db->p, params.tx, &prev, e))
          {
            goto failed;
          }

        vh_set_hash_value (page_h_w (&prev), fparams.hpos,
                           vp_get_next (page_h_ro (&cur)));

        if (pgr_release (params.db->p, &prev, PG_VAR_HASH_PAGE, e))
          {
            goto failed;
          }

        break;
      }

      // Otherwise, we just need to link prev->cur
    case PG_VAR_PAGE:
      {
        if (pgr_make_writable (params.db->p, params.tx, &prev, e))
          {
            goto failed;
          }

        vp_set_next (page_h_w (&prev), vp_get_next (page_h_ro (&cur)));

        if (pgr_release (params.db->p, &prev, PG_VAR_PAGE, e))
          {
            goto failed;
          }

        break;
      }
    default:
      {
        UNREACHABLE ();
      }
    }

  // Delete all overflow pages
  while (cur.mode != PHM_NONE)
    {
      pgno npg = vp_get_ovnext (page_h_ro (&cur));
      if (npg != PGNO_NULL)
        {
          if (pgr_get (&ovnext, PG_VAR_TAIL, npg, params.db->p, e))
            {
              goto failed;
            }
        }

      if (pgr_delete_and_release (params.db->p, params.tx, &cur, e))
        {
          goto failed;
        }

      page_h_xfer_ownership_ptr (&cur, &ovnext);
    }

  return error_trace (e);

failed:
  pgr_cancel_if_exists (params.db->p, &prev);
  pgr_cancel_if_exists (params.db->p, &cur);
  pgr_cancel_if_exists (params.db->p, &ovnext);

  return error_trace (e);
}
