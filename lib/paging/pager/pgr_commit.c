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

#include "paging/lockt/lock_table.h"
#include "paging/pager.h"
#include "paging/wal/os_wal.h"

err_t
pgr_commit (struct pager *p, struct txn *tx, error *e)
{
  DBG_ASSERT (pager, p);

  if (tx->data.state != TX_RUNNING)
    {
      error_causef (e, ERR_DUPLICATE_COMMIT, "txn already committed");

      // Failure here is fine

      goto theend;
    }

  // COMMIT
  slsn l = oswal_append_commit_log (p->ww, tx->tid, tx->data.last_lsn, e);
  if (l < 0)
    {
      // Failure here is fine

      goto theend;
    }

  // FLUSH
  if (oswal_flush_to (p->ww, l, e))
    {
      // We have a commit log appended to the WAL.
      // It may or may not be written to disk.
      // If it is written to disk, then good, next recovery,
      // we batch it in the list of commits to append an end
      // to

      goto theend;
    }

  // END
  l = oswal_append_end_log (p->ww, tx->tid, l, e);
  if (l < 0)
    {
      // Failing to append an end log isn't a big deal,
      // we'll append it during the next pgr_recovery

      goto theend;
    }

  // Remove the transaction from the txn table
  txnt_remove_txn_expect (p->tnxt, tx);

  if (p->lt)
    {
      lockt_unlock_tx (p->lt, tx);
    }
  tx->data.state = TX_DONE;

theend:
  return error_trace (e);
}
