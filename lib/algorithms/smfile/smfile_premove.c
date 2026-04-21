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

#include "algorithms/nsdb/rope/algorithms.h"
#include "algorithms/nsdb/var/algorithms.h"
#include "algorithms/smfile/_smfile.h"
#include "c_specx/dev/error.h"
#include "c_specx/memory/chunk_alloc.h"
#include "smfile.h"
#include "txns/txn.h"

static sb_size
_smfile_premove (
    struct smfile *db,
    const char *name,
    void *dest,
    const t_size size,
    const sb_size bofst,
    const sb_size stride,
    b_size nelem,
    error *e)
{
  sb_size ret;                                   // Return value
  b_size ofst;                                   // Resolved offset
  struct stream _output;                         // Output stream if present
  struct stream_obuf_ctx ctx;                    // Context for output stream
  struct stream *output = NULL;                  // Pointer to output stream
  struct chunk_alloc temp;                       // Allocator for get operation
  struct _ns_var_get_params gparams;             // Get operation
  struct _ns_remove_params rparams;              // Remove operation
  struct _ns_var_update_params uparams;          // Update operation
  struct string vname = vname_or_default (name); // Variable name

  // Parameter validation
  if (stride < 0)
    {
      return error_causef (e, ERR_INVALID_ARGUMENT, "Negative strides aren't supported yet");
    }
  if (stride == 0)
    {
      return error_causef (e, ERR_INVALID_ARGUMENT, "Cannot remove with stride == 0");
    }
  if (size == 0)
    {
      return error_causef (e, ERR_INVALID_ARGUMENT, "Cannot remove with size == 0");
    }
  if (nelem == 0)
    {
      return 0;
    }

  chunk_alloc_create_default (&temp);

  // BEGIN TXN
  WRAP_GOTO (_smfile_auto_begin_txn (db, e), failed);

  // GET VARIABLE
  {
    gparams = (struct _ns_var_get_params){
      .db = &db->root->db,
      .tx = db->atx,
      .vname = vname,
      .alloc = &temp,
    };
    err_t err = _ns_var_get (&gparams, e);
    if (err == ERR_VARIABLE_NE)
      {
        ret = 0;
        e->cause_code = SUCCESS;
        e->cmlen = 0;
        goto commit;
      }
    WRAP_GOTO (err, failed_rollback);
  }

  // Resolve sizes
  {
    ofst = var_resolve_index (&gparams.dest, bofst);
    nelem = var_resolve_nelem (&gparams.dest, ofst, nelem, size);
    if (nelem == 0)
      {
        ret = 0;
        goto commit;
      }
    if (dest)
      {
        stream_obuf_init (&_output, &ctx, dest, size * nelem);
        output = &_output;
      }
  }

  // REMOVE
  {
    rparams = (struct _ns_remove_params){
      .db = &db->root->db,
      .dest = output,
      .tx = db->atx,
      .root = gparams.dest.rpt_root,
      .size = size,
      .bofst = ofst,
      .stride = stride,
      .nelem = nelem,
    };
    ret = _ns_remove (&rparams, e);
    WRAP_GOTO (ret, failed_rollback);
  }

  // UPDATE VARIABLE
  {
    uparams = (struct _ns_var_update_params){
      .db = &db->root->db,
      .tx = db->atx,
      .retr = (struct var_retrieval){
          .type = VR_PG,
          .root = gparams.dest.var_root,
      },
      .newpg = rparams.root,
      .nbytes = gparams.dest.nbytes - ret * size,
    };
    WRAP_GOTO (_ns_var_update (uparams, e), failed_rollback);
  }

commit:

  // COMMIT
  WRAP_GOTO (_smfile_auto_commit (db, e), failed_rollback);
  chunk_alloc_free_all (&temp);
  return ret;

failed_rollback:

  _smfile_auto_rollback (db);

failed:
  chunk_alloc_free_all (&temp);
  return error_trace (e);
}

sb_size
smfile_premove (
    smfile_t *smf,
    const char *name,
    void *dest,
    t_size size,
    sb_size bofst,
    sb_size stride,
    b_size nelem)
{
  smf->e.cause_code = SUCCESS;
  smf->e.cmlen = 0;
  return _smfile_premove (smf, name, dest, size, bofst, stride, nelem, &smf->e);
}
