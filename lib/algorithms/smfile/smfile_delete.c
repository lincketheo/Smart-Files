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
#include "c_specx.h"
#include "c_specx/dev/assert.h"
#include "c_specx/dev/error.h"
#include "c_specx/ds/string.h"
#include "nsfile.h"

static err_t
_smfile_delete (struct smfile *db, const char *vname, error *e)
{
  struct txn auto_txn;

  // BEGIN TXN
  int auto_txn_start = _smfile_auto_begin_txn (db, e);
  if (auto_txn_start < 0)
    {
      goto failed;
    }

  struct string vnamestr = strfcstr (vname);
  if (string_equal (vnamestr, strfcstr (DEFAULT_VARIABLE)))
    {
      return error_causef (e, ERR_INVALID_ARGUMENT, "Cannot delete default variable: %s", DEFAULT_VARIABLE);
    }
  if (string_equal (vnamestr, db->loaded.vname))
    {
      // var is currently loaded - load the default
      if (_smfile_load (db, DEFAULT_VARIABLE, e))
        {
          goto failed;
        }
    }

  // DELETE
  struct _ns_var_delete_params iparams = {
    .db = &db->db,
    .tx = db->atx,
    .vname = strfcstr (vname),
  };
  err_t result = _ns_var_delete (iparams, e);
  if (result < 0)
    {
      goto failed_rollback;
    }

  // COMMIT
  if (_smfile_auto_commit (db, e))
    {
      goto failed_rollback;
    }

  return result;

failed_rollback:

  _smfile_auto_rollback (db);

failed:
  return error_trace (e);
}

int
smfile_delete (smfile_t *ns, const char *vname)
{
  ns->e.cause_code = SUCCESS;
  ns->e.cmlen = 0;
  return _smfile_delete (ns, vname, &ns->e);
}
