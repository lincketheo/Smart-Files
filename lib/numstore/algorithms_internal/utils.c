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
#include "rope/algorithms.h"

int
_ns_auto_begin_txn (const struct nsdb *n, struct txn **tx, struct txn *auto_txn,
                    error *e)
{
  int auto_txn_started = 0;

  if (*tx == NULL)
    {
      if (pgr_begin_txn (auto_txn, n->p, e))
        {
          return error_trace (e);
        }
      auto_txn_started = true;
      *tx = auto_txn;
    }

  return auto_txn_started;
}

err_t
_ns_auto_commit (const struct nsdb *n, struct txn *tx, const int auto_txn_started,
                 error *e)
{
  if (auto_txn_started)
    {
      WRAP (pgr_commit (n->p, tx, e));
    }
  return SUCCESS;
}

err_t
_ns_crash_reopen (struct nsdb *db, error *e)
{
  WRAP (pgr_crash (db->p, e));
  db->p = pgr_open_single_file (db->dbname, e);
  if (db->p == NULL)
    {
      return error_trace (e);
    }
  return SUCCESS;
}

err_t
_ns_close_reopen (struct nsdb *db, error *e)
{
  WRAP (pgr_close (db->p, e));
  db->p = pgr_open_single_file (db->dbname, e);
  if (db->p == NULL)
    {
      return error_trace (e);
    }
  return SUCCESS;
}
