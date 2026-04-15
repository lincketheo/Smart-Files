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
ns_write (
    struct nsdb *db,
    struct txn *tx,
    const pgno root,
    const void *src,
    const t_size size,
    const b_size bofst,
    const sb_size stride,
    const b_size nelem,
    error *e)
{
  struct txn auto_txn;
  struct stream input;
  struct stream_ibuf_ctx ctx;

  stream_ibuf_init (&input, &ctx, src, size * nelem);

  // BEGIN TXN
  const int auto_txn_start = _ns_auto_begin_txn (db, &tx, &auto_txn, e);
  if (auto_txn_start < 0)
    {
      goto failed;
    }

  // WRITE
  const struct _ns_write_params iparams = {
    .db = db,
    .src = &input,
    .tx = tx,
    .root = root,
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
  if (_ns_auto_commit (db, tx, auto_txn_start, e))
    {
      goto failed_rollback;
    }

  return written;

failed_rollback:

  // ROLLBACK
  pgr_rollback (db->p, tx, 0, e);

failed:
  return error_trace (e);
}
