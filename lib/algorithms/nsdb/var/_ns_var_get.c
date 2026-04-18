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

err_t
_ns_var_get (struct _ns_var_get_params *params, error *e)
{
  page_h cur = page_h_create ();

  // Find variable first
  struct _ns_find_var_page_params fparams = {
    .db = params->db,
    .tx = params->tx,
    .alloc = params->alloc,

    .vname = params->vname,
    .dvar = &params->dest,
    .mode = FP_FIND,

    .prev = NULL,
    .cur = &cur,
  };

  if (_ns_find_var_page (&fparams, e))
    {
      goto theend;
    }

  if (pgr_release (params->db->p, &cur, PG_VAR_PAGE, e))
    {
      goto theend;
    }

theend:

  return error_trace (e);
}
