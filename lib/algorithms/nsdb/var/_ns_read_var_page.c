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
#include "pager/page_h.h"
#include "pages/page_delegate.h"
#include "pages/var_page.h"
#include "smfile.h"

/*
 * Advance params->vp from the current PG_VAR_PAGE or PG_VAR_TAIL to the
 * next overflow page in the chain.  Used when a variable's serialised name
 * and type data spans more than one page.
 */
static err_t
_ns_read_var_page_advance (struct _ns_read_var_page_params *params, error *e)
{
  page_h next = page_h_create ();

  pgno npg = dlgt_get_ovnext (page_h_ro (params->vp));

  if (npg == PGNO_NULL)
    {
      error_causef (e, ERR_CORRUPT, "var page missing overflow pointer");
      goto failed;
    }

  WRAP (pgr_get_writable (&next, params->tx, PG_VAR_TAIL, npg, params->db->p,
                          e));

  if ((pgr_release (params->db->p, params->vp, PG_VAR_PAGE | PG_VAR_TAIL, e)))
    {
      goto failed;
    }

  page_h_xfer_ownership_ptr (params->vp, &next);

  return SUCCESS;

failed:
  return error_trace (e);
}

err_t
_ns_read_var_page (struct _ns_read_var_page_params *params, error *e)
{
  ASSERT (params->vp->mode != PHM_NONE);
  ASSERT (params->tx);
  ASSERT (page_h_type (params->vp) == PG_VAR_PAGE);

  p_size lread = 0;
  struct cbytes head;
  char *vstr = NULL;

  // Save for the end to reset var page
  pgno start = page_h_pgno (params->vp);

  u16 vlen = vp_get_vlen (page_h_ro (params->vp));
  pgno rpt_root = vp_get_root (page_h_ro (params->vp));
  b_size nbytes = vp_get_nbytes (page_h_ro (params->vp));
  pgno var_root = page_h_pgno (params->vp);

  // Quick check on the length
  if (params->check)
    {
      if (vlen != params->check->len)
        {
          params->matches = false;
          goto theend;
        }
    }

  // Allocate variable name
  vstr = chunk_malloc (params->alloc, 1, vlen, e);
  if (vstr == NULL)
    {
      goto failed;
    }

  // Read the variable name
  u16 rread = 0;
  head = dlgt_get_bytes_imut (page_h_ro (params->vp));
  while (rread < vlen)
    {
      // We exhausted this page - move forward one
      if (lread == head.len)
        {
          if (_ns_read_var_page_advance (params, e))
            {
              goto failed;
            }

          lread = 0;
          head = dlgt_get_bytes_imut (page_h_ro (params->vp));
        }

      // NAME READ
      u16 toread = vlen - rread;
      u16 bavail = head.len - lread;
      u16 next = MIN (bavail, toread);

      memcpy (&vstr[rread], &head.head[lread], next);
      rread += next;
      lread += next;
    }

  // Quick termination on string data
  if (params->check)
    {
      if (memcmp (params->check->data, vstr, vlen) != 0)
        {
          params->matches = false;
          goto theend;
        }
    }

  if (dlgt_get_ovnext (page_h_ro (params->vp)) != PGNO_NULL)
    {
      error_causef (e, ERR_CORRUPT,
                    "var page read complete but overflow "
                    "pointer is non-null");
      goto failed;
    }

  // Reset back to head page
  if (page_h_pgno (params->vp) != start)
    {
      if ((pgr_release (params->db->p, params->vp, PG_VAR_TAIL, e)))
        {
          goto failed;
        }
      if ((pgr_get (params->vp, PG_VAR_PAGE, start, params->db->p, e)))
        {
          goto failed;
        }
    }

  // Assign stuff
  if (params->dest)
    {
      *params->dest = (struct variable){
        .vname = (struct string){
            .data = vstr,
            .len = vlen,
        },
        .rpt_root = rpt_root,
        .nbytes = nbytes,
        .var_root = var_root,
      };
    }
  params->matches = true;

theend:
  return SUCCESS;

failed:
  return error_trace (e);
}
