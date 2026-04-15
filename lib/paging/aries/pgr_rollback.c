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
#include "tlclib/dev/error.h"
#include "numstore/stdtypes.h"
#include "paging/pager.h"
#include "paging/pager/page_h.h"
#include "paging/pages/fsm_page.h"
#include "paging/pages/page.h"
#include "paging/txns/txn.h"
#include "test/testing.h"

////////////////////////////////////////////////////////////
// ROLLBACK (Figure 8)

err_t
pgr_rollback (struct pager *p, struct txn *tx, const lsn save_lsn, error *e)
{
  struct wal_rec_hdr_read *log_rec = NULL;
  struct wal_clr_write clr;
  page_h ph = page_h_create ();

  // UndoNxt := Trans_Table[TransId].UndoNxtLSN
  lsn undo_nxt_lsn = tx->data.undo_next_lsn;
  txid tid = tx->tid;

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
        }

      // SELECT (LogRec.Type)
      switch (log_rec->type)
        {
          // WHEN('update') DO;
        case WL_UPDATE:
          {
            if (pgr_handle_rollback_update (p, log_rec, tx, &ph, &undo_nxt_lsn,
                                            e))
              {
                return error_trace (e);
              }
            break;
          }

        case WL_CLR:
          {
            // UndoNxt := LogRec.UndoNxtLSN
            undo_nxt_lsn = log_rec->clr.undo_next;
            break;
          }

        case WL_BEGIN:
          {
            undo_nxt_lsn = 0; // Done
            break;
          }
        case WL_COMMIT:
          {
            return error_causef (e, ERR_CORRUPT,
                                 "unexpected commit record "
                                 "during rollback "
                                 "(lsn=%" PRlsn ")",
                                 undo_nxt_lsn);
          }
        case WL_END:
          {
            return error_causef (e, ERR_CORRUPT,
                                 "unexpected end record "
                                 "during rollback "
                                 "(lsn=%" PRlsn ")",
                                 undo_nxt_lsn);
          }
        case WL_CKPT_BEGIN:
          {
            return error_causef (e, ERR_CORRUPT,
                                 "unexpected checkpoint "
                                 "begin during rollback "
                                 "(lsn=%" PRlsn ")",
                                 undo_nxt_lsn);
          }
        case WL_CKPT_END:
          {
            txnt_close (log_rec->ckpt_end.att);
            dpgt_close (log_rec->ckpt_end.dpt);
            if (log_rec->ckpt_end.txn_bank)
              {
                i_free (log_rec->ckpt_end.txn_bank);
              }
            return error_causef (e, ERR_CORRUPT,
                                 "unexpected checkpoint "
                                 "end during rollback "
                                 "(lsn=%" PRlsn ")",
                                 undo_nxt_lsn);
          }

        case WL_EOF:
          {
            goto theend;
          }
        }

      // Trans_Table[TransID].UndoNxtLSN := UndoNxt
      tx->data.undo_next_lsn = undo_nxt_lsn;
    }

theend:
  txnt_remove_txn_expect (p->tnxt, tx);
  tx->data.state = TX_DONE;

  return error_trace (e);
}

#ifndef NTEST
TEST (TT_UNIT, aries_rollback_basic)
{
  error e = error_create ();
  test_fail_if (pgr_delete_single_file ("testdb", &e));

  struct pager *p = pgr_open_single_file ("testdb", &e);
  // Create transaction and make changes
  struct txn tx;
  pgr_begin_txn (&tx, p, &e);

  // Create some data pages
  for (int i = 0; i < 3; ++i)
    {
      page_h dl_page = page_h_create ();
      test_fail_if (pgr_new (&dl_page, p, &tx, PG_DATA_LIST, &e));
      dl_make_valid (page_h_w (&dl_page));
      test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));
    }

  pgr_flush_wall (p, &e);

  page_h fsm = page_h_create ();
  test_fail_if (pgr_get (&fsm, PG_FREE_SPACE_MAP, 0, p, &e));

  // Check the free space map has all the pages
  for (p_size i = 0; i < FS_BTMP_NPGS; ++i)
    {
      if (i < 5)
        {
          test_assert_int_equal (fsm_get_bit (page_h_ro (&fsm), i), 1);
        }
      else
        {
          test_assert_int_equal (fsm_get_bit (page_h_ro (&fsm), i), 0);
        }
    }

  test_fail_if (pgr_release (p, &fsm, PG_FREE_SPACE_MAP, &e));

  // Rollback the transaction
  pgr_rollback (p, &tx, 0, &e);

  // Verify pages are trash after rollback
  page_h pg = page_h_create ();
  for (int i = 1; i < 4; ++i)
    {
      pgr_get (&pg, PG_TRASH, i, p, &e);
      pgr_release (p, &pg, PG_TRASH, &e);
    }

  test_fail_if (pgr_get (&fsm, PG_FREE_SPACE_MAP, 0, p, &e));

  // Check the free space map has all the pages
  for (p_size i = 0; i < FS_BTMP_NPGS; ++i)
    {
      if (i < 2)
        {
          test_assert_int_equal (fsm_get_bit (page_h_ro (&fsm), i), 1);
        }
      else
        {
          test_assert_int_equal (fsm_get_bit (page_h_ro (&fsm), i), 0);
        }
    }

  test_fail_if (pgr_release (p, &fsm, PG_FREE_SPACE_MAP, &e));

  pgr_close (p, &e);
}

TEST (TT_UNIT, aries_rollback_multiple_updates)
{
  error e = error_create ();

  test_fail_if (pgr_delete_single_file ("testdb", &e));

  struct pager *p = pgr_open_single_file ("testdb", &e);
  // Create and commit initial data
  struct txn tx;
  pgr_begin_txn (&tx, p, &e);

  page_h dl_page = page_h_create ();
  test_fail_if (pgr_new (&dl_page, p, &tx, PG_DATA_LIST, &e));
  dl_make_valid (page_h_w (&dl_page));

  // Write initial data
  u8 initial_data[DL_DATA_SIZE];
  memset (initial_data, 0xAA, DL_DATA_SIZE);
  dl_set_data (page_h_w (&dl_page),
               (struct dl_data){ .data = initial_data, .blen = DL_DATA_SIZE });

  const pgno pgno1 = page_h_ro (&dl_page)->pg;
  test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));
  pgr_commit (p, &tx, &e);

  // Start new transaction and make multiple updates
  struct txn tx2;
  pgr_begin_txn (&tx2, p, &e);

  // Update 1
  pgr_get_writable (&dl_page, &tx2, PG_DATA_LIST, pgno1, p, &e);
  u8 update1_data[DL_DATA_SIZE];
  memset (update1_data, 0xBB, DL_DATA_SIZE);
  dl_set_data (page_h_w (&dl_page),
               (struct dl_data){ .data = update1_data, .blen = DL_DATA_SIZE });
  test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));

  // Update 2
  pgr_get_writable (&dl_page, &tx2, PG_DATA_LIST, pgno1, p, &e);
  u8 update2_data[DL_DATA_SIZE];
  memset (update2_data, 0xCC, DL_DATA_SIZE);
  dl_set_data (page_h_w (&dl_page),
               (struct dl_data){ .data = update2_data, .blen = DL_DATA_SIZE });
  test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));

  // Update 3
  pgr_get_writable (&dl_page, &tx2, PG_DATA_LIST, pgno1, p, &e);
  u8 update3_data[DL_DATA_SIZE];
  memset (update3_data, 0xDD, DL_DATA_SIZE);
  dl_set_data (page_h_w (&dl_page),
               (struct dl_data){ .data = update3_data, .blen = DL_DATA_SIZE });
  test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));

  pgr_flush_wall (p, &e);

  // Rollback all updates
  pgr_rollback (p, &tx2, 0, &e);

  // Verify data is back to initial state
  pgr_get (&dl_page, PG_DATA_LIST, pgno1, p, &e);
  test_assert_memequal (dl_get_data (page_h_ro (&dl_page)), initial_data,
                        DL_DATA_SIZE);
  pgr_release (p, &dl_page, PG_DATA_LIST, &e);

  pgr_close (p, &e);
}

TEST (TT_UNIT, aries_rollback_with_crash_recovery)
{
  error e = error_create ();

  test_fail_if (pgr_delete_single_file ("testdb", &e));

  struct pager *p = pgr_open_single_file ("testdb", &e);
  // Transaction 1: commit data
  struct txn tx;
  pgr_begin_txn (&tx, p, &e);

  page_h dl_page = page_h_create ();
  test_fail_if (pgr_new (&dl_page, p, &tx, PG_DATA_LIST, &e));

  u8 committed_data[DL_DATA_SIZE];
  memset (committed_data, 0xAA, DL_DATA_SIZE);
  dl_set_data (page_h_w (&dl_page), (struct dl_data){ .data = committed_data,
                                                      .blen = DL_DATA_SIZE });

  const pgno pgno1 = page_h_ro (&dl_page)->pg;
  test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));
  pgr_commit (p, &tx, &e);

  // Transaction 2: make changes but don't commit or rollback (will be
  // rolled back on crash)
  struct txn tx2;
  pgr_begin_txn (&tx2, p, &e);

  pgr_get_writable (&dl_page, &tx2, PG_DATA_LIST, pgno1, p, &e);
  u8 uncommitted_data[DL_DATA_SIZE];
  memset (uncommitted_data, 0xBB, DL_DATA_SIZE);
  dl_set_data (page_h_w (&dl_page), (struct dl_data){ .data = uncommitted_data,
                                                      .blen = DL_DATA_SIZE });
  test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));

  pgr_flush_wall (p, &e);

  // Crash without commit or rollback
  test_fail_if (pgr_crash (p, &e));
  p = pgr_open_single_file ("testdb", &e);
  // Verify data is back to committed state (&tx2 was rolled back during
  // recovery)
  pgr_get (&dl_page, PG_DATA_LIST, pgno1, p, &e);
  test_assert_memequal (dl_get_data (page_h_ro (&dl_page)), committed_data,
                        DL_DATA_SIZE);
  pgr_release (p, &dl_page, PG_DATA_LIST, &e);

  pgr_close (p, &e);
}

TEST (TT_UNIT, aries_rollback_clr_not_undone)
{
  error e = error_create ();

  test_fail_if (pgr_delete_single_file ("testdb", &e));

  struct pager *p = pgr_open_single_file ("testdb", &e);
  // Create and commit initial page
  struct txn tx;
  pgr_begin_txn (&tx, p, &e);

  page_h dl_page = page_h_create ();
  test_fail_if (pgr_new (&dl_page, p, &tx, PG_DATA_LIST, &e));
  dl_make_valid (page_h_w (&dl_page));

  u8 initial_data[DL_DATA_SIZE];
  memset (initial_data, 0xAA, DL_DATA_SIZE);
  dl_set_data (page_h_w (&dl_page),
               (struct dl_data){ .data = initial_data, .blen = DL_DATA_SIZE });

  const pgno pgno1 = page_h_ro (&dl_page)->pg;
  test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));
  pgr_commit (p, &tx, &e);

  // Transaction 2: make update then rollback (generates CLRs)
  struct txn tx2;
  pgr_begin_txn (&tx2, p, &e);

  pgr_get_writable (&dl_page, &tx2, PG_DATA_LIST, pgno1, p, &e);
  u8 temp_data[DL_DATA_SIZE];
  memset (temp_data, 0xBB, DL_DATA_SIZE);
  dl_set_data (page_h_w (&dl_page),
               (struct dl_data){ .data = temp_data, .blen = DL_DATA_SIZE });
  test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));

  pgr_flush_wall (p, &e);

  // Rollback &tx2 (this writes CLRs)
  pgr_rollback (p, &tx2, 0, &e);

  // Verify data is back to initial
  pgr_get (&dl_page, PG_DATA_LIST, pgno1, p, &e);
  test_assert_memequal (dl_get_data (page_h_ro (&dl_page)), initial_data,
                        DL_DATA_SIZE);
  pgr_release (p, &dl_page, PG_DATA_LIST, &e);

  // Crash and recover to ensure CLRs are handled correctly
  test_fail_if (pgr_crash (p, &e));
  p = pgr_open_single_file ("testdb", &e);
  // Verify data is still initial (CLRs were not undone during recovery)
  pgr_get (&dl_page, PG_DATA_LIST, pgno1, p, &e);
  test_assert_memequal (dl_get_data (page_h_ro (&dl_page)), initial_data,
                        DL_DATA_SIZE);
  pgr_release (p, &dl_page, PG_DATA_LIST, &e);

  pgr_close (p, &e);
}

#endif
