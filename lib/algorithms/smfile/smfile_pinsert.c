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
#include "algorithms/smfile/smfile.h"
#include "c_specx/dev/error.h"
#include "c_specx/ds/stride.h"
#include "c_specx/memory/chunk_alloc.h"
#include "smfile.h"
#include "txns/txn.h"

static sb_size
_smfile_pinsert (
    struct smfile *db,
    const char *name,
    const void *src,
    const b_size slen,
    sb_size bofst,
    error *e)
{
  sb_size ret;                                   // Return value
  b_size ofst;                                   // Resolved offset
  struct stream _input;                          // Input stream
  struct stream_ibuf_ctx ctx;                    // Context for input stream
  struct chunk_alloc temp;                       // Allocator for get operation
  struct _ns_var_get_or_create_params gparams;   // Get or create operation
  struct _ns_insert_params iparams;              // Insert operation
  struct _ns_var_update_params uparams;          // Update operation
  struct string vname = vname_or_default (name); // Variable name

  // Parameter validation
  if (slen == 0)
    {
      return 0;
    }

  chunk_alloc_create_default (&temp);
  stream_ibuf_init (&_input, &ctx, src, slen);

  // BEGIN TXN
  WRAP_GOTO (_smfile_auto_begin_txn (db, e), failed);

  // GET OR CREATE VARIABLE
  {
    gparams = (struct _ns_var_get_or_create_params){
      .db = &db->root->db,
      .tx = db->atx,
      .vname = vname,
      .alloc = &temp,
    };
    WRAP_GOTO (_ns_var_get_or_create (&gparams, e), failed_rollback);
  }

  // Resolve sizes
  {
    ofst = var_resolve_index (&gparams.dest, bofst);
  }

  // INSERT
  {
    iparams = (struct _ns_insert_params){
      .db = &db->root->db,
      .src = &_input,
      .tx = db->atx,
      .root = gparams.dest.rpt_root,
      .bofst = bofst,
    };
    ret = _ns_insert (&iparams, e);
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
      .newpg = iparams.root,
      .nbytes = gparams.dest.nbytes + ret,
    };
    WRAP_GOTO (_ns_var_update (uparams, e), failed_rollback);
  }

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
smfile_pinsert (
    smfile_t *smf,
    const char *name,
    const void *src,
    sb_size bofst,
    b_size slen)
{
  smf->e.cause_code = SUCCESS;
  smf->e.cmlen = 0;
  return _smfile_pinsert (smf, name, src, slen, bofst, &smf->e);
}
