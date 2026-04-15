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

#include "paging/pager.h"

/*
 * Begin a new transaction.
 *
 * Assigns a monotonically increasing TID from the pager's counter, writes a
 * BEGIN record to the WAL (this is what sets min_lsn and last_lsn), and
 * inserts the transaction into the active transaction table (ATT).
 *
 * The BEGIN record must hit the WAL before any page updates are made so that
 * ARIES analysis can discover the transaction and reconstruct its state from
 * scratch if needed.
 */
err_t
pgr_begin_txn (struct txn *tx, struct pager *p, error *e)
{
  DBG_ASSERT (pager, p);

  // Generate a new transaction ID
  const txid tid = p->next_tid++;

  slsn l = 0;

  l = oswal_append_begin_log (p->ww, tid, e);
  if (l < 0)
    {
      // Ok to error here - other than maybe a failed wal
      // but wal should handle that
      return error_trace (e);
    }

  // Create a new transaction
  txn_init (tx, tid,
            (struct txn_data){
                .min_lsn = l,
                .last_lsn = l,
                .undo_next_lsn = 0,
                .state = TX_RUNNING,
            });

  // Create a new transaction entry
  txnt_insert_txn (p->tnxt, tx);

  return SUCCESS;
}
