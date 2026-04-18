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
#include "c_specx/dev/error.h"

err_t
_ns_var_get_or_create (struct _ns_var_get_or_create_params *params, error *e)
{
  // Try to get the variable
  struct _ns_var_get_params gparams = {
    .db = params->db,
    .tx = params->tx,
    .vname = params->vname,
    .alloc = params->alloc,
  };

  err_t err = _ns_var_get (&gparams, e);
  if (err == ERR_VARIABLE_NE)
    {
      // Doesn't exist - reset and create it
      e->cause_code = SUCCESS;
      e->cmlen = 0;

      // Create the variable
      struct _ns_var_create_params cparams = {
        .db = params->db,
        .tx = params->tx,
        .vname = params->vname,
      };
      if (_ns_var_create (cparams, e))
        {
          goto failed;
        }

      // Try again
      if (_ns_var_get (&gparams, e))
        {
          goto failed;
        }
    }
  else if (err < 0)
    {
      goto failed;
    }

  params->dest = gparams.dest;

failed:
  return error_trace (e);
}
