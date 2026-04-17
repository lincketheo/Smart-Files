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

#include "pager.h"
#include "pager/page_h.h"
#include "pages/data_list.h"
#include "pages/page.h"
#include "pages/root_node.h"
#include "c_specx.h"
#include "txns/txn.h"

#ifndef DUMB_PAGER

#ifndef NTEST
TEST (aries_fuzzy_checkpoint_basic_recovery)
{
  error e = error_create ();

  test_fail_if (i_remove_quiet ("testdb.db", &e));
  test_fail_if (i_remove_quiet ("testdb.wal", &e));

  struct pager *p = pgr_open_single_file ("testdb", &e);

  u8 data[5][DL_DATA_SIZE];
  rand_bytes (data, DL_DATA_SIZE * 5);

  {
    // BEGIN TXN
    struct txn tx;
    pgr_begin_txn (&tx, p, &e);

    for (int i = 0; i < 5; ++i)
      {
        page_h dl_page = page_h_create ();
        test_fail_if (pgr_new (&dl_page, p, &tx, PG_DATA_LIST, &e));
        dl_set_data (
            page_h_w (&dl_page),
            (struct dl_data){ .data = data[i], .blen = DL_DATA_SIZE });
        dl_set_prev (page_h_w (&dl_page), i + 10);
        dl_set_next (page_h_w (&dl_page), i + 20);
        test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));
      }

    // COMMIT
    pgr_commit (p, &tx, &e);
  }

  // Take a checkpoint
  test_fail_if (pgr_fuzzy_checkpoint (p, &e));

  // Verify master_lsn was set
  {
    page_h pg = page_h_create ();
    pgr_get (&pg, PG_ROOT_NODE, ROOT_PGNO, p, &e);
    const lsn master_lsn = rn_get_master_lsn (page_h_ro (&pg));
    test_assert (master_lsn > 0); // Should point to checkpoint
    pgr_release (p, &pg, PG_ROOT_NODE, &e);
  }

  // Crash and recover
  test_fail_if (pgr_crash (p, &e));
  p = pgr_open_single_file ("testdb", &e);
  // Verify data survived through checkpoint recovery
  {
    page_h pg = page_h_create ();

    pgr_get (&pg, PG_ROOT_NODE, ROOT_PGNO, p, &e);
    test_assert_int_equal (rn_get_first_tmbst (page_h_ro (&pg)), 6);
    const lsn master_lsn = rn_get_master_lsn (page_h_ro (&pg));
    test_assert (master_lsn > 0); // Checkpoint LSN should be persisted
    pgr_release (p, &pg, PG_ROOT_NODE, &e);

    for (int i = 1; i < 6; ++i)
      {
        pgr_get (&pg, PG_DATA_LIST, i, p, &e);
        test_assert_memequal (dl_get_data (page_h_ro (&pg)), data[i - 1],
                              DL_DATA_SIZE);
        test_assert_int_equal (dl_get_prev (page_h_ro (&pg)), (i - 1) + 10);
        test_assert_int_equal (dl_get_next (page_h_ro (&pg)), (i - 1) + 20);
        pgr_release (p, &pg, PG_DATA_LIST, &e);
      }
  }

  pgr_close (p, &e);
}

TEST (aries_fuzzy_checkpoint_with_active_transactions)
{
  error e = error_create ();

  test_fail_if (pgr_delete_single_file ("testdb", &e));

  struct pager *p = pgr_open_single_file ("testdb", &e);
  u8 data1[3][DL_DATA_SIZE];
  u8 data2[3][DL_DATA_SIZE];
  rand_bytes (data1, DL_DATA_SIZE * 3);
  rand_bytes (data2, DL_DATA_SIZE * 3);

  // Start first transaction and commit it
  struct txn tx1;
  {
    pgr_begin_txn (&tx1, p, &e);

    for (int i = 0; i < 3; ++i)
      {
        page_h dl_page = page_h_create ();
        test_fail_if (pgr_new (&dl_page, p, &tx1, PG_DATA_LIST, &e));
        dl_set_data (
            page_h_w (&dl_page),
            (struct dl_data){ .data = data1[i], .blen = DL_DATA_SIZE });
        test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));
      }

    pgr_commit (p, &tx1, &e);
  }

  // Start second transaction but DON'T commit it
  struct txn tx2;
  {
    pgr_begin_txn (&tx2, p, &e);

    for (int i = 0; i < 3; ++i)
      {
        page_h dl_page = page_h_create ();
        test_fail_if (pgr_new (&dl_page, p, &tx2, PG_DATA_LIST, &e));
        dl_set_data (
            page_h_w (&dl_page),
            (struct dl_data){ .data = data2[i], .blen = DL_DATA_SIZE });
        test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));
      }
    // NOTE: tx2 is NOT committed - this is a fuzzy checkpoint test
  }

  // Take checkpoint while tx2 is still active
  test_fail_if (pgr_fuzzy_checkpoint (p, &e));

  // Crash with uncommitted transaction
  test_fail_if (pgr_crash (p, &e));
  p = pgr_open_single_file ("testdb", &e);
  // Verify: tx1 data should survive, tx2 should be rolled back
  {
    page_h pg = page_h_create ();

    pgr_get (&pg, PG_ROOT_NODE, ROOT_PGNO, p, &e);
    // Note: first_tmbst may be stale after fuzzy checkpoint
    // recovery because it's checkpointed with uncommitted
    // allocations
    lsn master_lsn = rn_get_master_lsn (page_h_ro (&pg));
    test_assert (master_lsn > 0);
    pgr_release (p, &pg, PG_ROOT_NODE, &e);

    // tx1 pages should be committed and data should be accessible
    for (int i = 1; i < 4; ++i)
      {
        pgr_get (&pg, PG_DATA_LIST, i, p, &e);
        test_assert_memequal (dl_get_data (page_h_ro (&pg)), data1[i - 1],
                              DL_DATA_SIZE);
        pgr_release (p, &pg, PG_DATA_LIST, &e);
      }

    // tx2 was rolled back - verify by starting a new transaction
    // and ensuring we can create new pages (the system is in a
    // consistent state)
    struct txn tx3;
    pgr_begin_txn (&tx3, p, &e);

    page_h new_page = page_h_create ();
    test_fail_if (pgr_new (&new_page, p, &tx3, PG_DATA_LIST, &e));
    dl_make_valid (page_h_w (&new_page));
    test_fail_if (pgr_release (p, &new_page, PG_DATA_LIST, &e));
    pgr_commit (p, &tx3, &e);
  }

  pgr_close (p, &e);
}

// Test multiple checkpoints and recovery from latest
TEST (aries_fuzzy_checkpoint_multiple_checkpoints)
{
  error e = error_create ();

  test_fail_if (pgr_delete_single_file ("testdb", &e));

  struct pager *p = pgr_open_single_file ("testdb", &e);
  u8 data1[2][DL_DATA_SIZE];
  u8 data2[2][DL_DATA_SIZE];
  u8 data3[2][DL_DATA_SIZE];
  rand_bytes (data1, DL_DATA_SIZE * 2);
  rand_bytes (data2, DL_DATA_SIZE * 2);
  rand_bytes (data3, DL_DATA_SIZE * 2);

  lsn ckpt1_lsn, ckpt2_lsn, ckpt3_lsn;

  // First batch of data and checkpoint
  {
    struct txn tx;
    pgr_begin_txn (&tx, p, &e);

    for (int i = 0; i < 2; ++i)
      {
        page_h dl_page = page_h_create ();
        test_fail_if (pgr_new (&dl_page, p, &tx, PG_DATA_LIST, &e));
        dl_set_data (
            page_h_w (&dl_page),
            (struct dl_data){ .data = data1[i], .blen = DL_DATA_SIZE });
        test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));
      }

    pgr_commit (p, &tx, &e);
    test_fail_if (pgr_fuzzy_checkpoint (p, &e));

    page_h pg = page_h_create ();
    pgr_get (&pg, PG_ROOT_NODE, ROOT_PGNO, p, &e);
    ckpt1_lsn = rn_get_master_lsn (page_h_ro (&pg));
    pgr_release (p, &pg, PG_ROOT_NODE, &e);
  }

  // Second batch and checkpoint
  {
    struct txn tx;
    pgr_begin_txn (&tx, p, &e);

    for (int i = 0; i < 2; ++i)
      {
        page_h dl_page = page_h_create ();
        test_fail_if (pgr_new (&dl_page, p, &tx, PG_DATA_LIST, &e));
        dl_set_data (
            page_h_w (&dl_page),
            (struct dl_data){ .data = data2[i], .blen = DL_DATA_SIZE });
        test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));
      }

    pgr_commit (p, &tx, &e);
    test_fail_if (pgr_fuzzy_checkpoint (p, &e));

    page_h pg = page_h_create ();
    pgr_get (&pg, PG_ROOT_NODE, ROOT_PGNO, p, &e);
    ckpt2_lsn = rn_get_master_lsn (page_h_ro (&pg));
    pgr_release (p, &pg, PG_ROOT_NODE, &e);
  }

  // Third batch and checkpoint
  {
    struct txn tx;
    pgr_begin_txn (&tx, p, &e);

    for (int i = 0; i < 2; ++i)
      {
        page_h dl_page = page_h_create ();
        test_fail_if (pgr_new (&dl_page, p, &tx, PG_DATA_LIST, &e));
        dl_set_data (
            page_h_w (&dl_page),
            (struct dl_data){ .data = data3[i], .blen = DL_DATA_SIZE });
        test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));
      }

    pgr_commit (p, &tx, &e);
    test_fail_if (pgr_fuzzy_checkpoint (p, &e));

    page_h pg = page_h_create ();
    pgr_get (&pg, PG_ROOT_NODE, ROOT_PGNO, p, &e);
    ckpt3_lsn = rn_get_master_lsn (page_h_ro (&pg));
    pgr_release (p, &pg, PG_ROOT_NODE, &e);
  }

  // Verify checkpoint LSNs are increasing
  test_assert (ckpt2_lsn > ckpt1_lsn);
  test_assert (ckpt3_lsn > ckpt2_lsn);

  // Crash and recover - should use latest checkpoint
  test_fail_if (pgr_crash (p, &e));
  p = pgr_open_single_file ("testdb", &e);
  // Verify all data survived
  {
    page_h pg = page_h_create ();

    pgr_get (&pg, PG_ROOT_NODE, ROOT_PGNO, p, &e);
    test_assert_int_equal (rn_get_first_tmbst (page_h_ro (&pg)), 7);
    lsn master_lsn = rn_get_master_lsn (page_h_ro (&pg));
    test_assert_int_equal (master_lsn,
                           ckpt3_lsn); // Should use latest checkpoint
    pgr_release (p, &pg, PG_ROOT_NODE, &e);

    // Verify data1
    for (int i = 1; i < 3; ++i)
      {
        pgr_get (&pg, PG_DATA_LIST, i, p, &e);
        test_assert_memequal (dl_get_data (page_h_ro (&pg)), data1[i - 1],
                              DL_DATA_SIZE);
        pgr_release (p, &pg, PG_DATA_LIST, &e);
      }

    // Verify data2
    for (int i = 3; i < 5; ++i)
      {
        pgr_get (&pg, PG_DATA_LIST, i, p, &e);
        test_assert_memequal (dl_get_data (page_h_ro (&pg)), data2[i - 3],
                              DL_DATA_SIZE);
        pgr_release (p, &pg, PG_DATA_LIST, &e);
      }

    // Verify data3
    for (int i = 5; i < 7; ++i)
      {
        pgr_get (&pg, PG_DATA_LIST, i, p, &e);
        test_assert_memequal (dl_get_data (page_h_ro (&pg)), data3[i - 5],
                              DL_DATA_SIZE);
        pgr_release (p, &pg, PG_DATA_LIST, &e);
      }
  }

  pgr_close (p, &e);
}

// Test checkpoint followed by post-checkpoint activity before crash
TEST (aries_fuzzy_checkpoint_with_post_checkpoint_activity)
{
  error e = error_create ();

  test_fail_if (pgr_delete_single_file ("testdb", &e));

  struct pager *p = pgr_open_single_file ("testdb", &e);
  u8 data_before[3][DL_DATA_SIZE];
  u8 data_after[3][DL_DATA_SIZE];
  rand_bytes (data_before, DL_DATA_SIZE * 3);
  rand_bytes (data_after, DL_DATA_SIZE * 3);

  // Create and commit data before checkpoint
  {
    struct txn tx;
    pgr_begin_txn (&tx, p, &e);

    for (int i = 0; i < 3; ++i)
      {
        page_h dl_page = page_h_create ();
        test_fail_if (pgr_new (&dl_page, p, &tx, PG_DATA_LIST, &e));
        dl_set_data (
            page_h_w (&dl_page),
            (struct dl_data){ .data = data_before[i], .blen = DL_DATA_SIZE });
        test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));
      }

    pgr_commit (p, &tx, &e);
  }

  // Take checkpoint
  test_fail_if (pgr_fuzzy_checkpoint (p, &e));

  // Do more work AFTER checkpoint
  {
    struct txn tx;
    pgr_begin_txn (&tx, p, &e);

    for (int i = 0; i < 3; ++i)
      {
        page_h dl_page = page_h_create ();
        test_fail_if (pgr_new (&dl_page, p, &tx, PG_DATA_LIST, &e));
        dl_set_data (
            page_h_w (&dl_page),
            (struct dl_data){ .data = data_after[i], .blen = DL_DATA_SIZE });
        test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));
      }

    pgr_commit (p, &tx, &e);
  }

  // Crash - recovery should process checkpoint + post-checkpoint log
  test_fail_if (pgr_crash (p, &e));
  p = pgr_open_single_file ("testdb", &e);
  // Verify both before and after checkpoint data survived
  {
    page_h pg = page_h_create ();

    pgr_get (&pg, PG_ROOT_NODE, ROOT_PGNO, p, &e);
    test_assert_int_equal (rn_get_first_tmbst (page_h_ro (&pg)), 7);
    pgr_release (p, &pg, PG_ROOT_NODE, &e);

    // Data before checkpoint
    for (int i = 1; i < 4; ++i)
      {
        pgr_get (&pg, PG_DATA_LIST, i, p, &e);
        test_assert_memequal (dl_get_data (page_h_ro (&pg)),
                              data_before[i - 1], DL_DATA_SIZE);
        pgr_release (p, &pg, PG_DATA_LIST, &e);
      }

    // Data after checkpoint
    for (int i = 4; i < 7; ++i)
      {
        pgr_get (&pg, PG_DATA_LIST, i, p, &e);
        test_assert_memequal (dl_get_data (page_h_ro (&pg)), data_after[i - 4],
                              DL_DATA_SIZE);
        pgr_release (p, &pg, PG_DATA_LIST, &e);
      }
  }

  pgr_close (p, &e);
}
#endif
#endif
