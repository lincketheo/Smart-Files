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
_smfile_insert (
    struct smfile *db,
    const void *src,
    const b_size slen,
    const b_size bofst,
    error *e)
{
  struct txn auto_txn;
  struct stream input;
  struct stream_ibuf_ctx ctx;

  stream_ibuf_init (&input, &ctx, src, slen);

  // BEGIN TXN
  const int auto_txn_start = _smfile_auto_begin_txn (db, e);
  if (auto_txn_start < 0)
    {
      goto failed;
    }

  // INSERT
  struct _ns_insert_params iparams = {
    .db = &db->db,
    .src = &input,
    .tx = db->atx,
    .root = db->loaded.rpt_root,
    .bofst = bofst,
  };
  const sb_size written = _ns_insert (&iparams, e);
  if (written < 0)
    {
      goto failed_rollback;
    }

  // Update new root
  db->loaded.rpt_root = iparams.root;
  db->loaded.nbytes += written;
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

  return written;

failed_rollback:

  _smfile_auto_rollback (db);

failed:
  return error_trace (e);
}

sb_size
smfile_insert (
    smfile_t *smf,
    const void *src,
    b_size bofst,
    b_size slen)
{
  smf->e.cause_code = SUCCESS;
  smf->e.cmlen = 0;
  return _smfile_insert (smf, src, slen, bofst, &smf->e);
}
