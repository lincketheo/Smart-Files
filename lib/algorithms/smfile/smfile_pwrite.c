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
#include "smfile.h"
#include "txns/txn.h"

static sb_size
_smfile_pwrite (
    struct smfile *db,
    const char *name,
    const void *src,
    const t_size size,
    const sb_size bofst,
    const sb_size stride,
    const b_size nelem,
    error *e)
{
  sb_size ret;                                   // Return value
  sb_size inserted;                              // Number of bytes inserted
  b_size ofst;                                   // Resolved offset
  b_size write_nelem;                            // Elements that fit in existing variable
  b_size insert_nelem;                           // Remainder to insert past the end
  struct stream _input;                          // Input stream
  struct stream_ibuf_ctx ctx;                    // Context for input stream
  struct chunk_alloc temp;                       // Allocator for get operation
  struct _ns_var_get_or_create_params gparams;   // Get or create operation
  struct _ns_write_params wparams;               // Write operation
  struct _ns_insert_params iparams;              // Insert operation
  struct _ns_var_update_params uparams;          // Update operation
  struct string vname = vname_or_default (name); // Variable name

  // Parameter validation
  if (stride < 0)
    {
      return error_causef (e, ERR_INVALID_ARGUMENT, "Negative strides aren't supported yet");
    }
  if (stride == 0)
    {
      return error_causef (e, ERR_INVALID_ARGUMENT, "Cannot write with stride == 0");
    }
  if (size == 0)
    {
      return error_causef (e, ERR_INVALID_ARGUMENT, "Cannot write with size == 0");
    }
  if (nelem == 0)
    {
      return 0;
    }

  chunk_alloc_create_default (&temp);

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
    write_nelem = var_resolve_nelem (&gparams.dest, ofst, nelem, size);
    insert_nelem = nelem - write_nelem;
    if (insert_nelem > 0 && stride != 1)
      {
        error_causef (e, ERR_INVALID_ARGUMENT, "Cannot write past end with stride != 1");
        goto failed_rollback;
      }
  }

  // WRITE
  {
    stream_ibuf_init (&_input, &ctx, src, size * write_nelem);

    wparams = (struct _ns_write_params){
      .db = &db->root->db,
      .src = &_input,
      .tx = db->atx,
      .root = gparams.dest.rpt_root,
      .size = size,
      .bofst = ofst,
      .stride = stride,
      .nelem = write_nelem,
    };

    ret = _ns_write (wparams, e);
    WRAP_GOTO (ret, failed_rollback);
  }

  // INSERT REMAINDER
  if (insert_nelem > 0)
    {
      // INSERT
      {
        stream_ibuf_init (&_input, &ctx, (u8 *)src + write_nelem * size, insert_nelem * size);

        iparams = (struct _ns_insert_params){
          .db = &db->root->db,
          .src = &_input,
          .tx = db->atx,
          .root = wparams.root,
          .bofst = gparams.dest.nbytes, // Append
        };

        inserted = _ns_insert (&iparams, e);
        WRAP_GOTO (inserted, failed_rollback);
        ret += inserted;
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
          .nbytes = gparams.dest.nbytes + inserted,
        };
        WRAP_GOTO (_ns_var_update (uparams, e), failed_rollback);
      }
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
smfile_pwrite (
    smfile_t *smf,
    const char *name,
    const void *src,
    t_size size,
    b_size bofst,
    sb_size stride,
    b_size nelem)
{
  smf->e.cause_code = SUCCESS;
  smf->e.cmlen = 0;
  return _smfile_pwrite (smf, name, src, size, bofst, stride, nelem, &smf->e);
}
