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
#include "errors.h"
#include "smfile.h"

static err_t
_smfile_delete (struct smfile *db, const char *vname, error *e)
{
  struct txn auto_txn;

  // BEGIN TXN
  WRAP_GOTO (_smfile_auto_begin_txn (db, e), failed);

  struct string vnamestr = strfcstr (vname);
  {
    // Cannot delete the default variable
    if (string_equal (vnamestr, strfcstr (DEFAULT_VARIABLE)))
      {
        return error_causef (e, ERR_INVALID_ARGUMENT, "Cannot delete default variable: %s", DEFAULT_VARIABLE);
      }

    // DELETE
    struct _ns_var_delete_params params = {
      .db = &db->root->db,
      .tx = db->atx,
      .vname = strfcstr (vname),
    };
    err_t err = _ns_var_delete (params, e);
    if (err == ERR_VARIABLE_NE)
      {
        // It's ok if it doesn't exist
        e->cause_code = 0;
        e->cmlen = 0;
        goto commit;
      }
    WRAP_GOTO (err, failed_rollback);

  commit:
    // COMMIT
    if (_smfile_auto_commit (db, e))
      {
        goto failed_rollback;
      }
  }

  return SUCCESS;

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
