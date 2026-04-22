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

#include "aries.h"
#include "c_specx.h"
#include "c_specx/dev/assert.h"
#include "c_specx/dev/error.h"
#include "c_specx_dev.h"
#include "lockt/lock_table.h"
#include "pager.h"
#include "pager/page_h.h"
#include "pages/fsm_page.h"
#include "pages/page.h"
#include "txns/txn.h"
#include "wal/wal_rec_hdr.h"

////////////////////////////////////////////////////////////
// ROLLBACK (Figure 8)

err_t
pgr_rollback (struct pager *p, struct txn *tx, lsn save_lsn, error *e)
{
  struct wal_rec_hdr_read *log_rec = NULL;
  struct wal_clr_write clr;
  page_h ph = page_h_create ();

  lsn undo_nxt_lsn = tx->data.undo_next_lsn;
  slsn clr_lsn = undo_nxt_lsn;
  txid tid = tx->tid;

  if (pgr_flush_wall (p, e))
    {
      goto failed;
    }

  // WHILE SaveSN < UndoNxt DO:
  while (save_lsn < undo_nxt_lsn)
    {
      // LogRec := Log_Read(UndoNxt)
      if ((log_rec = oswal_read_entry (p->ww, undo_nxt_lsn, e)) == NULL)
        {
          return error_trace (e);
        }

      if (log_rec->type == WL_EOF)
        {
          return error_causef (e, ERR_CORRUPT, "Transaction does not have a valid top level log");
        }

      // SELECT (LogRec.Type)
      switch (log_rec->type)
        {
          // WHEN('update') DO;
        case WL_UPDATE:
          {
            if (wrh_is_undoable (log_rec))
              {
                pgno pg = wrh_get_affected_pg (log_rec);
                if (pgr_get_writable (&ph, NULL, PG_PERMISSIVE, pg, p, e))
                  {
                    goto failed;
                  }

                wrh_undo (log_rec, &ph);

                // Append a clr log
                clr_lsn = oswal_append_clr_log (
                    p->ww,
                    (struct wal_clr_write){
                        .type = WCLR_PHYSICAL,
                        .tid = log_rec->update.tid,
                        .prev = tx->data.last_lsn,
                        .undo_next = log_rec->update.prev,
                        .phys = {
                            .pg = pg,
                            .redo = log_rec->update.phys.undo,
                        },
                    },
                    e);
                if (clr_lsn < 0)
                  {
                    goto failed;
                  }

                // Set the last page lsn
                page_set_page_lsn (page_h_w (&ph), clr_lsn);

                // Set the update lsn
                tx->data.last_lsn = clr_lsn;

                pgr_unfix (p, &ph, PG_PERMISSIVE);
              }

            undo_nxt_lsn = log_rec->update.prev;

            if (undo_nxt_lsn == 0)
              {
                slsn l = oswal_append_end_log (p->ww, tx->tid, clr_lsn, e);
                if (l < 0)
                  {
                    goto failed;
                  }
                // We'll break next
              }
            break;
          }

        case WL_CLR:
          {
            undo_nxt_lsn = log_rec->clr.undo_next;
            break;
          }

        case WL_BEGIN:
          {
            undo_nxt_lsn = 0; // Done
            break;
          }
        case WL_COMMIT:
        case WL_END:
          {
            return error_causef (e, ERR_CORRUPT,
                                 "unexpected log record during rollback (lsn=%" PRlsn ")",
                                 undo_nxt_lsn);
          }

        case WL_EOF:
          {
            goto theend;
          }
        }

      tx->data.undo_next_lsn = undo_nxt_lsn;
    }

theend:
  lockt_unlock_tx (p->lt, tx);

  if (oswal_flush_to (p->ww, undo_nxt_lsn, e))
    {
      goto failed;
    }

  txnt_remove_txn_expect (p->tnxt, tx);
  tx->data.state = TX_DONE;

  return SUCCESS;

failed:
  return error_trace (e);
}

#ifndef NTEST
TEST (aries_rollback_basic)
{
  error e = error_create ();
  test_fail_if (pgr_delete_single_file ("testdb", &e));

  struct pager *p = pgr_open_single_file ("testdb", &e);
  struct txn tx;
  page_h fsm = page_h_create ();
  page_h pg = page_h_create ();

  // Create pages in a transaction
  {
    pgr_begin_txn (&tx, p, &e);

    for (int i = 0; i < 3; ++i)
      {
        page_h dl_page = page_h_create ();
        test_fail_if (pgr_new (&dl_page, p, &tx, PG_DATA_LIST, &e));
        dl_make_valid (page_h_w (&dl_page));
        test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));
      }

    pgr_flush_wall (p, &e);
  }

  // Verify FSM has all pages before rollback
  {
    test_fail_if (pgr_get (&fsm, PG_FREE_SPACE_MAP, 0, p, &e));

    for (p_size i = 0; i < FS_BTMP_NPGS; ++i)
      {
        if (i < 4)
          {
            test_assert_int_equal (fsm_get_bit (page_h_ro (&fsm), i), 1);
          }
        else
          {
            test_assert_int_equal (fsm_get_bit (page_h_ro (&fsm), i), 0);
          }
      }

    test_fail_if (pgr_release (p, &fsm, PG_FREE_SPACE_MAP, &e));
  }

  // Rollback the transaction
  {
    pgr_rollback (p, &tx, 0, &e);
  }

  // Verify pages are trash after rollback
  {
    for (int i = 1; i < 3; ++i)
      {
        pgr_get (&pg, PG_TRASH, i, p, &e);
        pgr_release (p, &pg, PG_TRASH, &e);
      }
  }

  // Verify FSM reflects rolled back state
  {
    test_fail_if (pgr_get (&fsm, PG_FREE_SPACE_MAP, 0, p, &e));

    for (p_size i = 0; i < FS_BTMP_NPGS; ++i)
      {
        if (i < 1)
          {
            test_assert_int_equal (fsm_get_bit (page_h_ro (&fsm), i), 1);
          }
        else
          {
            test_assert_int_equal (fsm_get_bit (page_h_ro (&fsm), i), 0);
          }
      }

    test_fail_if (pgr_release (p, &fsm, PG_FREE_SPACE_MAP, &e));
  }

  pgr_close (p, &e);
}

TEST (aries_rollback_multiple_updates)
{
  error e = error_create ();
  test_fail_if (pgr_delete_single_file ("testdb", &e));

  struct pager *p = pgr_open_single_file ("testdb", &e);
  struct txn tx;
  struct txn tx2;
  page_h dl_page = page_h_create ();
  pgno pgno1;
  u8 initial_data[DL_DATA_SIZE];

  // Txn 1: create and commit initial data
  {
    pgr_begin_txn (&tx, p, &e);

    test_fail_if (pgr_new (&dl_page, p, &tx, PG_DATA_LIST, &e));
    dl_make_valid (page_h_w (&dl_page));

    memset (initial_data, 0xAA, DL_DATA_SIZE);
    dl_set_data (page_h_w (&dl_page),
                 (struct dl_data){ .data = initial_data, .blen = DL_DATA_SIZE });

    pgno1 = page_h_ro (&dl_page)->pg;
    test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));
    pgr_commit (p, &tx, &e);
  }

  // Txn 2: make multiple updates then rollback
  {
    pgr_begin_txn (&tx2, p, &e);

    pgr_get_writable (&dl_page, &tx2, PG_DATA_LIST, pgno1, p, &e);
    u8 update1_data[DL_DATA_SIZE];
    memset (update1_data, 0xBB, DL_DATA_SIZE);
    dl_set_data (page_h_w (&dl_page),
                 (struct dl_data){ .data = update1_data, .blen = DL_DATA_SIZE });
    test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));

    pgr_get_writable (&dl_page, &tx2, PG_DATA_LIST, pgno1, p, &e);
    u8 update2_data[DL_DATA_SIZE];
    memset (update2_data, 0xCC, DL_DATA_SIZE);
    dl_set_data (page_h_w (&dl_page),
                 (struct dl_data){ .data = update2_data, .blen = DL_DATA_SIZE });
    test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));

    pgr_get_writable (&dl_page, &tx2, PG_DATA_LIST, pgno1, p, &e);
    u8 update3_data[DL_DATA_SIZE];
    memset (update3_data, 0xDD, DL_DATA_SIZE);
    dl_set_data (page_h_w (&dl_page),
                 (struct dl_data){ .data = update3_data, .blen = DL_DATA_SIZE });
    test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));

    pgr_flush_wall (p, &e);
    pgr_rollback (p, &tx2, 0, &e);
  }

  // Verify data is back to initial state
  {
    pgr_get (&dl_page, PG_DATA_LIST, pgno1, p, &e);
    test_assert_memequal (dl_get_data (page_h_ro (&dl_page)), initial_data,
                          DL_DATA_SIZE);
    pgr_release (p, &dl_page, PG_DATA_LIST, &e);
  }

  pgr_close (p, &e);
}

TEST (aries_rollback_with_crash_recovery)
{
  error e = error_create ();
  test_fail_if (pgr_delete_single_file ("testdb", &e));

  struct pager *p = pgr_open_single_file ("testdb", &e);
  struct txn tx;
  struct txn tx2;
  page_h dl_page = page_h_create ();
  pgno pgno1;
  u8 committed_data[DL_DATA_SIZE];

  // Txn 1: create and commit data
  {
    pgr_begin_txn (&tx, p, &e);

    test_fail_if (pgr_new (&dl_page, p, &tx, PG_DATA_LIST, &e));

    memset (committed_data, 0xAA, DL_DATA_SIZE);
    dl_set_data (page_h_w (&dl_page), (struct dl_data){ .data = committed_data, .blen = DL_DATA_SIZE });

    pgno1 = page_h_ro (&dl_page)->pg;
    test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));
    pgr_commit (p, &tx, &e);
  }

  // Txn 2: make changes then crash without commit or rollback
  {
    pgr_begin_txn (&tx2, p, &e);

    pgr_get_writable (&dl_page, &tx2, PG_DATA_LIST, pgno1, p, &e);
    u8 uncommitted_data[DL_DATA_SIZE];
    memset (uncommitted_data, 0xBB, DL_DATA_SIZE);
    dl_set_data (page_h_w (&dl_page), (struct dl_data){ .data = uncommitted_data, .blen = DL_DATA_SIZE });
    test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));

    pgr_flush_wall (p, &e);
    lockt_unlock_tx (p->lt, &tx2); // Unlock tx's to avoid memory leaks
    test_fail_if (pgr_crash (p, &e));
  }

  // Verify data is back to committed state after recovery
  {
    p = pgr_open_single_file ("testdb", &e);
    pgr_get (&dl_page, PG_DATA_LIST, pgno1, p, &e);
    test_assert_memequal (dl_get_data (page_h_ro (&dl_page)), committed_data, DL_DATA_SIZE);
    pgr_release (p, &dl_page, PG_DATA_LIST, &e);
  }

  pgr_close (p, &e);
}

TEST (aries_rollback_clr_not_undone)
{
  error e = error_create ();

  test_fail_if (pgr_delete_single_file ("testdb", &e));

  struct pager *p = pgr_open_single_file ("testdb", &e);
  struct txn tx;
  struct txn tx2;
  page_h dl_page = page_h_create ();
  pgno pgno1;
  u8 initial_data[DL_DATA_SIZE];

  // Txn 1 - normal commit
  {
    pgr_begin_txn (&tx, p, &e);

    test_fail_if (pgr_new (&dl_page, p, &tx, PG_DATA_LIST, &e));
    dl_make_valid (page_h_w (&dl_page));

    memset (initial_data, 0xAA, DL_DATA_SIZE);
    dl_set_data (page_h_w (&dl_page), (struct dl_data){ .data = initial_data, .blen = DL_DATA_SIZE });

    pgno1 = page_h_ro (&dl_page)->pg;
    test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));
    pgr_commit (p, &tx, &e);
  }

  // Txn 2: make update then rollback (generates CLRs)
  {
    pgr_begin_txn (&tx2, p, &e);

    pgr_get_writable (&dl_page, &tx2, PG_DATA_LIST, pgno1, p, &e);

    u8 temp_data[DL_DATA_SIZE];
    memset (temp_data, 0xBB, DL_DATA_SIZE);
    dl_set_data (page_h_w (&dl_page), (struct dl_data){ .data = temp_data, .blen = DL_DATA_SIZE });
    test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));

    pgr_rollback (p, &tx2, 0, &e);
  }

  // Verify data is back to initial
  {
    pgr_get (&dl_page, PG_DATA_LIST, pgno1, p, &e);
    test_assert_memequal (dl_get_data (page_h_ro (&dl_page)), initial_data, DL_DATA_SIZE);
    pgr_release (p, &dl_page, PG_DATA_LIST, &e);
  }

  // Crash and recover - verify CLRs were not undone
  {
    test_fail_if (pgr_crash (p, &e));
    p = pgr_open_single_file ("testdb", &e);
    pgr_get (&dl_page, PG_DATA_LIST, pgno1, p, &e);
    test_assert_memequal (dl_get_data (page_h_ro (&dl_page)), initial_data, DL_DATA_SIZE);
    pgr_release (p, &dl_page, PG_DATA_LIST, &e);
  }

  pgr_close (p, &e);
}

#endif
