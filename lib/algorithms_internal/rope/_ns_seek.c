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

#include "algorithms_internal/rope/algorithms.h"
#include "errors.h"
#include "pager.h"
#include "pager/page_h.h"
#include "pages/page.h"
#include "c_specx.h"

/*
 * Descend the R+Tree to the data-list page containing byte offset [bofst].
 *
 * Starting from the root, the tree is traversed level by level.  At each
 * inner node, in_choose_lidx() identifies the child subtree that contains
 * [bofst] and subtracts the cumulative key total for all preceding subtrees
 * from the remaining offset, so that [bofst] is expressed relative to the
 * chosen subtree.  This is the standard R+Tree prefix-sum descent.
 *
 * If [save_stack] is true, each inner node is pushed onto pstack before
 * following its child; this gives the caller a traversal path for rebalancing
 * without re-reading any pages.  If [save_stack] is false, each inner node
 * is released immediately after the child page is fetched.
 *
 * When the descent lands on a data-list page, lidx is set to
 * MIN(bofst, dl_used()) — it saturates at the page's current content length,
 * meaning seeks past EOF land at the end of the last page rather than
 * producing an error.  This is intentional: inserts after EOF are valid.
 */
err_t
_ns_seek (struct _ns_seek_params *a, error *e)
{
  page_h next = page_h_create ();
  a->pg = page_h_create ();
  a->sp = 0;
  a->lidx = 0;

  // Fetch the starting node
  if (pgr_get (&a->pg, PG_DATA_LIST | PG_INNER_NODE, a->root, a->db->p, e))
    {
      goto failed;
    }

  while (true)
    {
      switch (page_h_type (&a->pg))
        {
        case PG_INNER_NODE:
          {
            // Stack overflow
            if (a->sp == 20)
              {
                error_causef (
                    e, ERR_RPTREE_PAGE_STACK_OVERFLOW,
                    "page stack overflow (depth 20)");
                goto failed;
              }

            // Make decision
            b_size nleft;
            in_choose_lidx (&a->lidx, &nleft, page_h_ro (&a->pg), a->bofst);
            ASSERT (nleft <= a->bofst);
            a->bofst -= nleft;

            // Fetch that next page
            const pgno npg = in_get_leaf (page_h_ro (&a->pg), a->lidx);
            if (pgr_get (&next, PG_DATA_LIST | PG_INNER_NODE, npg, a->db->p, e))
              {
                goto failed;
              }

            // Append a->pg to the stack
            if (a->save_stack)
              {
                a->pstack[(a->sp)++] = (struct seek_v){
                  .pg = page_h_xfer_ownership (&a->pg),
                  .lidx = a->lidx,
                };
              }
            else
              {
                if (pgr_release (a->db->p, &a->pg, PG_INNER_NODE, e))
                  {
                    goto failed;
                  }
              }

            // Trade a->pg
            a->pg = page_h_xfer_ownership (&next);
            break;
          }

        case PG_DATA_LIST:
          {
            const p_size used = dl_used (page_h_ro (&a->pg));
            a->lidx = MIN (a->bofst, used);
            return SUCCESS;
          }

        default:
          {
            UNREACHABLE ();
          }
        }
    }

failed:
  // Release used pages
  pgr_cancel_if_exists (a->db->p, &a->pg);
  pgr_cancel_if_exists (a->db->p, &next);
  for (u32 i = 0; i < a->sp; ++i)
    {
      pgr_cancel_if_exists (a->db->p, &a->pstack[i].pg);
    }
  a->sp = 0;
  return error_trace (e);
}
