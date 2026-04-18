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
#include "algorithms/smfile/smfile.h"
#include "c_specx/dev/error.h"
#include "c_specx/memory/chunk_alloc.h"
#include "errors.h"
#include "nsfile.h"

// Switch contexts
err_t
_smfile_load (smfile_t *ns, const char *vname, error *e)
{
  // Try to get the variable
  struct _ns_var_get_params params = {
    .db = &ns->db,
    .tx = &ns->tx,

    .vname = strfcstr (vname),
    .alloc = ns->staging,
  };

  err_t ret = _ns_var_get (&params, &ns->e);
  if (ret == ERR_VARIABLE_NE)
    {
      // Doesn't exist - reset and create it
      e->cause_code = SUCCESS;
      e->cmlen = 0;

      // Create the default variable
      struct _ns_var_create_params cparams = {
        .db = &ns->db,
        .tx = &ns->tx,
        .vname = strfcstr (vname),
      };
      if (_ns_var_create (cparams, e))
        {
          goto failed;
        }

      // Try again
      if (_ns_var_get (&params, &ns->e))
        {
          goto failed;
        }
    }
  else if (ret < 0)
    {
      goto failed;
    }

  // Swap the allocators (to commit)
  chunk_alloc_free_all (ns->alloc);
  struct chunk_alloc *alloc = ns->alloc;
  ns->alloc = ns->staging;
  ns->staging = alloc;

  // set the loaded variable
  ns->loaded = params.dest;

failed:
  chunk_alloc_free_all (ns->staging);
  return error_trace (e);
}
