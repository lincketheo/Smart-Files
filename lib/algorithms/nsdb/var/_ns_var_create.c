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
#include "pager.h"
#include "pager/page_h.h"

/*
 * Create a new variable record in the variable hash table.
 *
 * Uses _ns_find_var_page() in FP_CREATE mode to allocate a fresh PG_VAR_PAGE
 * in the correct hash-chain bucket.  The page is then initialised with the
 * variable's name, type, and zero nbytes/rpt_root (no data yet).
 *
 * Returns ERR_DUPLICATE_VARIABLE if a variable with this name already exists.
 */
spgno
_ns_var_create (const struct _ns_var_create_params params, error *e)
{
  page_h cur = page_h_create ();
  struct _ns_find_var_page_params fparams = {
    .tx = params.tx,
    .db = params.db,
    .alloc = NULL,

    .vname = params.vname,
    .dvar = NULL,
    .mode = FP_CREATE,

    .hpos = PGNO_NULL,
    .prev = NULL,
    .cur = &cur,
  };

  if (_ns_find_var_page (&fparams, e))
    {
      goto failed;
    }

  struct variable var = {
    .vname = params.vname,
    .nbytes = 0,
    .rpt_root = PGNO_NULL,
  };

  struct _ns_write_var_page_params write_params = {
    .db = params.db,
    .tx = params.tx,

    .vp = &cur,
    .var = &var,
  };

  if (_ns_write_var_page (&write_params, e))
    {
      goto failed;
    }

  pgno ret = page_h_pgno (&cur);

  if ((pgr_release (params.db->p, &cur, PG_VAR_PAGE, e)))
    {
      goto failed;
    }

  return SUCCESS;

failed:

  pgr_cancel_if_exists (params.db->p, &cur);

  return error_trace (e);
}
