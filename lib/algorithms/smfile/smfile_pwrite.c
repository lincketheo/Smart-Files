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
#include "nsfile.h"
#include "txns/txn.h"

static sb_size
_smfile_pwrite (
    struct smfile *db,
    const void *src,
    const char *name,
    const t_size size,
    const b_size bofst,
    const sb_size stride,
    const b_size nelem,
    error *e)
{
  struct stream input;
  struct stream_ibuf_ctx ctx;
  struct chunk_alloc temp;

  stream_ibuf_init (&input, &ctx, src, size * nelem);
  chunk_alloc_create_default (&temp);

  // BEGIN TXN
  const int auto_txn_start = _smfile_auto_begin_txn (db, e);
  if (auto_txn_start < 0)
    {
      goto failed;
    }

  struct string vname;
  if (name != NULL)
    {
      vname = strfcstr (name);
    }
  else
    {
      vname = strfcstr (DEFAULT_VARIABLE);
    }

  // GET OR CREATE VARIABLE
  struct _ns_var_get_or_create_params gparams = {
    .db = &db->root->db,
    .tx = db->atx,
    .vname = vname,
    .alloc = &temp,
  };

  if (_ns_var_get_or_create (&gparams, e))
    {
      goto failed;
    }

  // WRITE
  const struct _ns_write_params iparams = {
    .db = &db->root->db,
    .src = &input,
    .tx = db->atx,
    .root = gparams.dest.rpt_root,
    .size = size,
    .bofst = bofst,
    .stride = stride,
    .nelem = nelem,
  };
  const sb_size written = _ns_write (iparams, e);
  if (written < 0)
    {
      goto failed_rollback;
    }

  // COMMIT
  if (_smfile_auto_commit (db, e))
    {
      goto failed_rollback;
    }

  chunk_alloc_free_all (&temp);

  return written;

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
