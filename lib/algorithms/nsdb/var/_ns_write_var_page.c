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
#include "pages/page_delegate.h"

/*
 * Allocate a new PG_VAR_TAIL overflow page and advance params->vp to it.
 *
 * When the current page's data area is exhausted during a variable write,
 * a fresh tail page is allocated, linked via the overflow pointer, and
 * params->vp is updated so subsequent writes continue into the new page.
 */
static err_t
_ns_write_var_page_advance (struct _ns_write_var_page_params *params, error *e)
{
  page_h next = page_h_create ();

  if (pgr_new (&next, params->db->p, params->tx, PG_VAR_TAIL, e))
    {
      goto failed;
    }

  dlgtovlink (page_h_w (params->vp), page_h_w (&next));

  if ((pgr_release (params->db->p, params->vp, PG_VAR_PAGE | PG_VAR_TAIL, e)))
    {
      goto failed;
    }
  page_h_xfer_ownership_ptr (params->vp, &next);

failed:
  return error_trace (e);
}

/*
 * Serialise a variable record into a PG_VAR_PAGE (plus any overflow tails).
 *
 * The inverse of _read_var_page.  The fixed-size fields (next, ovnext, vlen,
 * tlen, rpt_root, nbytes) are stamped into the page header first.  Then the
 * variable-length name and serialised type bytes are written sequentially
 * into the page's data area; overflow pages are allocated on demand via
 * write_var_page_advance() whenever the current page is full.
 *
 * On return, params->vp is re-wound to the original head page (the caller
 * still holds the page in write mode from before the call).
 */
err_t
_ns_write_var_page (struct _ns_write_var_page_params *params, error *e)
{
  ASSERT (params->vp->mode == PHM_X);
  ASSERT (page_h_type (params->vp) == PG_VAR_PAGE);
  ASSERT (params->tx);

  pgno start = page_h_pgno (params->vp);
  u8 *tstr = NULL;

  vp_set_next (page_h_w (params->vp), PGNO_NULL);
  vp_set_ovnext (page_h_w (params->vp), PGNO_NULL);
  vp_set_vlen (page_h_w (params->vp), params->var->vname.len);
  vp_set_root (page_h_w (params->vp), params->var->rpt_root);
  vp_set_nbytes (page_h_w (params->vp), params->var->nbytes);

  struct bytes head = dlgt_get_bytes (page_h_w (params->vp));
  p_size lwritten = 0;

  // Write all the variable length stuff
  p_size vwritten = 0;
  while (vwritten < params->var->vname.len)
    {
      // Fetch next node if we ran out of room
      if (lwritten == head.len)
        {
          if (_ns_write_var_page_advance (params, e))
            {
              goto failed;
            }
          lwritten = 0;
          head = dlgt_get_bytes (page_h_w (params->vp));
        }

      u16 next = MIN (head.len - lwritten, params->var->vname.len - vwritten);
      ASSERT (next > 0);
      memcpy (&head.head[lwritten], &params->var->vname.data[vwritten], next);
      vwritten += next;
      lwritten += next;
    }

  // Reset back to head page
  if (page_h_pgno (params->vp) != start)
    {
      if ((pgr_release (params->db->p, params->vp, PG_VAR_TAIL, e)))
        {
          goto theend;
        }
      if ((pgr_get (params->vp, PG_VAR_PAGE, start, params->db->p, e)))
        {
          goto theend;
        }
    }
  else
    {
      pgno pg = page_h_pgno (params->vp);
      if (pgr_release (params->db->p, params->vp, PG_VAR_PAGE, e))
        {
          goto theend;
        }

      if (pgr_get (params->vp, PG_VAR_PAGE, pg, params->db->p, e))
        {
          goto theend;
        }
    }

theend:
  i_free (tstr);
  return error_trace (e);

failed:
  return error_trace (e);
}
