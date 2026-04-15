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

#include "algorithms_public/algorithms.h"

sb_size
ns_remove (
    struct nsdb *db,
    struct txn *tx,
    pgno *root,
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
  const int auto_txn_start = _ns_auto_begin_txn (db, &tx, &auto_txn, e);
  if (auto_txn_start < 0)
    {
      goto failed;
    }

  // REMOVE
  struct _ns_remove_params iparams = {
    .db = db,
    .dest = output,
    .tx = tx,
    .root = *root,
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
  *root = iparams.root;

  // COMMIT
  if (_ns_auto_commit (db, tx, auto_txn_start, e))
    {
      goto failed_rollback;
    }

  return nremoved;

failed_rollback:

  // ROLLBACK
  pgr_rollback (db->p, tx, 0, e);

failed:
  return error_trace (e);
}
