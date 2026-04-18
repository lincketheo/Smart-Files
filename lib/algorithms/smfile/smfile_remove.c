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
_smfile_remove (
    struct smfile *db,
    void *dest,
    const t_size size,
    const b_size bofst,
    const sb_size stride,
    const b_size nelem,
    error *e)
{
  struct txn auto_txn;
  struct stream _output;
  struct stream_obuf_ctx ctx;
  struct stream *output = NULL;

  if (dest)
    {
      stream_obuf_init (&_output, &ctx, dest, size * nelem);
      output = &_output;
    }

  // BEGIN TXN
  const int auto_txn_start = _smfile_auto_begin_txn (db, e);
  if (auto_txn_start < 0)
    {
      goto failed;
    }

  // REMOVE
  struct _ns_remove_params iparams = {
    .db = &db->db,
    .dest = output,
    .tx = db->atx,
    .root = db->loaded.rpt_root,
    .size = size,
    .bofst = bofst,
    .stride = stride,
    .nelem = nelem,
  };
  const sb_size nremoved = _ns_remove (&iparams, e);
  if (nremoved < 0)
    {
      goto failed_rollback;
    }

  // Update new root
  db->loaded.rpt_root = iparams.root;
  db->loaded.nbytes -= nremoved * size;
  struct _ns_var_update_params uparams = {
    .db = &db->db,
    .tx = db->atx,
    .retr = (struct var_retrieval){
        .type = VR_PG,
        .root = db->loaded.var_root,
    },
    .newpg = db->loaded.rpt_root,
    .nbytes = db->loaded.nbytes,
  };

  // COMMIT
  if (_smfile_auto_commit (db, e))
    {
      goto failed_rollback;
    }

  return nremoved;

failed_rollback:

  _smfile_auto_rollback (db);

failed:
  return error_trace (e);
}

sb_size
smfile_remove (
    smfile_t *smf,
    void *dest,
    t_size size,
    b_size bofst,
    sb_size stride,
    b_size nelem)
{
  smf->e.cause_code = SUCCESS;
  smf->e.cmlen = 0;
  return _smfile_remove (smf, dest, size, bofst, stride, nelem, &smf->e);
}
