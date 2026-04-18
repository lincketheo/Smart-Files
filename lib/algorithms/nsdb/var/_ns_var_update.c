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
#include "pager.h"
#include "pager/page_h.h"

/*
 * Update rpt_root and nbytes on a variable page addressed by page number.
 *
 * Used when the caller already holds the variable page's pgno from an
 * earlier _find_var_page() call, avoiding a second hash-chain traversal.
 */
static err_t
_ns_update_by_id (struct _ns_var_update_params params, error *e)
{
  page_h cur = page_h_create ();

  if (pgr_get_writable (&cur, params.tx, PG_VAR_PAGE, params.retr.root, params.db->p, e))
    {
      goto failed;
    }

  vp_set_root (page_h_w (&cur), params.newpg);
  vp_set_nbytes (page_h_w (&cur), params.nbytes);

  if (pgr_release (params.db->p, &cur, PG_VAR_PAGE, e))
    {
      goto failed;
    }

  goto failed;

failed:
  if (e->cause_code)
    {
      return error_trace (e);
    }
  else
    {
      return SUCCESS;
    }
}

/*
 * Update rpt_root and nbytes on a variable page addressed by variable name.
 *
 * Walks the hash chain via _find_var_page() in FP_FIND mode, then upgrades
 * the page to writable and stamps the new root pgno and byte count.
 */
static err_t
_ns_update_by_name (struct _ns_var_update_params params, error *e)
{
  page_h cur = page_h_create ();

  struct _ns_find_var_page_params fparams = {
    .db = params.db,
    .tx = params.tx,

    .vname = params.retr.vname,
    .dvar = NULL,
    .mode = FP_FIND,

    .hpos = PGNO_NULL,
    .prev = NULL,
    .cur = &cur,
  };

  if (_ns_find_var_page (&fparams, e))
    {
      goto failed;
    }

  if (pgr_make_writable (params.db->p, params.tx, &cur, e))
    {
      goto failed;
    }

  vp_set_root (page_h_w (&cur), params.newpg);
  vp_set_nbytes (page_h_w (&cur), params.nbytes);

  if (pgr_release (params.db->p, &cur, PG_VAR_PAGE, e))
    {
      goto failed;
    }

  return SUCCESS;

failed:
  pgr_cancel_if_exists (params.db->p, &cur);
  return error_trace (e);
}

err_t
_ns_var_update (struct _ns_var_update_params params, error *e)
{
  switch (params.retr.type)
    {
    case VR_NAME:
      {
        return _ns_update_by_name (params, e);
      }
    case VR_PG:
      {
        return _ns_update_by_id (params, e);
      }
    }
  UNREACHABLE ();
}
