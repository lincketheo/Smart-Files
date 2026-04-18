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
_smfile_read (
    struct smfile *db,
    void *dest,
    const t_size size,
    const b_size bofst,
    const sb_size stride,
    const b_size nelem,
    error *e)
{
  struct txn auto_txn;
  struct stream output;
  struct stream_obuf_ctx ctx;

  stream_obuf_init (&output, &ctx, dest, size * nelem);

  // BEGIN TXN
  const int auto_txn_start = _smfile_auto_begin_txn (db, e);
  if (auto_txn_start < 0)
    {
      goto failed;
    }

  // READ
  const struct _ns_read_params iparams = {
    .db = &db->db,
    .dest = &output,
    .tx = db->atx,
    .root = db->loaded.rpt_root,
    .size = size,
    .bofst = bofst,
    .stride = stride,
    .nelem = nelem,
  };
  const sb_size nread = _ns_read (iparams, e);
  if (nread < 0)
    {
      goto failed_rollback;
    }

  // COMMIT
  if (_smfile_auto_commit (db, e))
    {
      goto failed_rollback;
    }

  return nread;

failed_rollback:

  // ROLLBACK
  _smfile_auto_rollback (db);

failed:
  return error_trace (e);
}

sb_size
smfile_read (
    smfile_t *smf,
    void *dest,
    t_size size,
    b_size bofst,
    sb_size stride,
    b_size nelem)
{
  smf->e.cause_code = SUCCESS;
  smf->e.cmlen = 0;
  return _smfile_read (smf, dest, size, bofst, stride, nelem, &smf->e);
}
