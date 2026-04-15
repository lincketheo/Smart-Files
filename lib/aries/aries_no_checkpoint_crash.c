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
#include "tlclib.h"

#ifndef NTEST

TEST (aries_crash)
{
  TEST_CASE ("One uncommitted transaction")
  {
    error e = error_create ();
    struct pager *p = NULL;

    // Resource acquisition
    {
      test_fail_if (pgr_delete_single_file ("testdb", &e));

      p = pgr_open_single_file ("testdb", &e);
    }

    // Create 5 data list pages without a commit
    {
      struct txn tx1;
      pgr_begin_txn (&tx1, p, &e);

      // Make a bunch of changes to ensure
      // at least one UPDATE is written to the wal
      for (int i = 0; i < 5; ++i)
        {
          // TID1
          page_h dl_page = page_h_create ();
          test_fail_if (pgr_new (&dl_page, p, &tx1, PG_DATA_LIST, &e));
          dl_make_valid (page_h_w (&dl_page));
          test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));
        }

      // Flush WAL to make the test harder to pass
      pgr_flush_wall (p, &e);
    }

    // Crash our db and reopen
    test_fail_if (pgr_crash (p, &e));
    p = pgr_open_single_file ("testdb", &e);
    // Read all 5 pages that we wrote to and ensure they're
    // tombstones (they were rolled back)
    {
      page_h pg = page_h_create ();

      // Root node was committed
      pgr_get (&pg, PG_ROOT_NODE, ROOT_PGNO, p, &e);
      test_assert_int_equal (rn_get_first_tmbst (page_h_ro (&pg)), 1);
      test_assert_int_equal (rn_get_master_lsn (page_h_ro (&pg)), 0);
      pgr_release (p, &pg, PG_ROOT_NODE, &e);

      for (int i = 1; i < 6; ++i)
        {
          // TID1
          // pgr_get (&pg, PG_TOMBSTONE, i, p,
          // &e); test_assert_int_equal
          // (tmbst_get_next (page_h_ro (&pg)), i
          // + 1); pgr_release (p, &pg,
          // PG_TOMBSTONE, &e);
        }
    }

    pgr_close (p, &e);
  }

  TEST_CASE ("multiple open txns")
  {
    error e = error_create ();
    struct pager *p = NULL;

    // Resource acquisition
    {
      test_fail_if (pgr_delete_single_file ("testdb", &e));

      p = pgr_open_single_file ("testdb", &e);
    }

    struct txn tx1, tx2;

    // NORMAL PROCESSING
    {
      pgr_begin_txn (&tx1, p, &e);

      // Make a bunch of changes to ensure
      // at least one UPDATE is written to the wal
      for (int i = 0; i < 5; ++i)
        {
          // TID1
          page_h dl_page = page_h_create ();
          test_fail_if (pgr_new (&dl_page, p, &tx1, PG_DATA_LIST, &e));
          dl_make_valid (page_h_w (&dl_page));
          test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));
        }

      pgr_flush_wall (p, &e);
    }

    // REOPEN
    test_fail_if (pgr_crash (p, &e));
    p = pgr_open_single_file ("testdb", &e);
    // NORMAL PROCESSING
    {
      pgr_begin_txn (&tx2, p, &e);
      test_assert (tx1.tid != tx2.tid); // ARIES found the maximum transaction

      // Make a bunch of changes to ensure
      // at least one UPDATE is written to the wal
      for (int i = 0; i < 5; ++i)
        {
          // TID1
          page_h dl_page = page_h_create ();
          test_fail_if (pgr_new (&dl_page, p, &tx2, PG_DATA_LIST, &e));
          dl_make_valid (page_h_w (&dl_page));
          test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));
        }

      pgr_flush_wall (p, &e);
    }

    // REOPEN
    test_fail_if (pgr_crash (p, &e));
    p = pgr_open_single_file ("testdb", &e);
    // VALIDATION
    {
      page_h pg = page_h_create ();

      // Root node was committed
      pgr_get (&pg, PG_ROOT_NODE, ROOT_PGNO, p, &e);
      test_assert_int_equal (rn_get_first_tmbst (page_h_ro (&pg)), 1);
      test_assert_int_equal (rn_get_master_lsn (page_h_ro (&pg)), 0);
      pgr_release (p, &pg, PG_ROOT_NODE, &e);

      // The data lists weren't commited (and therefore
      // tombstones)
      for (int i = 1; i < 6; ++i)
        {
          // TID1
          // pgr_get (&pg, PG_TOMBSTONE, i, p,
          // &e); test_assert_int_equal
          // (tmbst_get_next (page_h_ro (&pg)), i
          // + 1); pgr_release (p, &pg,
          // PG_TOMBSTONE, &e);
        }
    }

    pgr_close (p, &e);
  }

  TEST_CASE ("No end log appended")
  {
    error e = error_create ();
    struct pager *p = NULL;

    // Resource acquisition
    {
      test_fail_if (pgr_delete_single_file ("testdb", &e));

      p = pgr_open_single_file ("testdb", &e);
    }

    u8 data[5][DL_DATA_SIZE];
    rand_bytes (data, DL_DATA_SIZE * 5);

    // NORMAL PROCESSING
    {
      struct txn tx1, tx;
      pgr_begin_txn (&tx1, p, &e);
      pgr_begin_txn (&tx, p, &e);

      // Make a bunch of changes to ensure
      // at least one UPDATE is written to the wal
      for (int i = 0; i < 5; ++i)
        {
          // TID1
          page_h dl_page = page_h_create ();
          test_fail_if (pgr_new (&dl_page, p, &tx1, PG_DATA_LIST, &e));
          dl_set_data (
              page_h_w (&dl_page),
              (struct dl_data){ .data = data[i], .blen = DL_DATA_SIZE });
          dl_set_prev (page_h_w (&dl_page), i + 20);
          dl_set_next (page_h_w (&dl_page), i + 100);
          test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));
        }

      pgr_commit (p, &tx1, &e);
    }

    // REOPEN
    test_fail_if (pgr_crash (p, &e));
    p = pgr_open_single_file ("testdb", &e);
    // VALIDATION
    {
      page_h pg = page_h_create ();

      // Root node was committed
      pgr_get (&pg, PG_ROOT_NODE, ROOT_PGNO, p, &e);
      test_assert_int_equal (rn_get_first_tmbst (page_h_ro (&pg)), 6);
      test_assert_int_equal (rn_get_master_lsn (page_h_ro (&pg)), 0);
      pgr_release (p, &pg, PG_ROOT_NODE, &e);

      // The data lists was commited
      for (int i = 1; i < 6; ++i)
        {
          // TID1
          pgr_get (&pg, PG_DATA_LIST, i, p, &e);
          test_assert_memequal (dl_get_data (page_h_ro (&pg)), data[i - 1],
                                DL_DATA_SIZE);
          test_assert_int_equal (dl_get_prev (page_h_ro (&pg)), (i - 1) + 20);
          test_assert_int_equal (dl_get_next (page_h_ro (&pg)), (i - 1) + 100);
          pgr_release (p, &pg, PG_DATA_LIST, &e);
        }
    }

    pgr_close (p, &e);
  }

  TEST_CASE ("After commiting multiple")
  {
    error e = error_create ();
    struct pager *p = NULL;

    // Resource acquisition
    {
      test_fail_if (pgr_delete_single_file ("testdb", &e));

      p = pgr_open_single_file ("testdb", &e);
    }

    u8 data[10][DL_DATA_SIZE];
    rand_bytes (data, DL_DATA_SIZE * 10);

    struct txn tx1, tx2;

    // NORMAL PROCESSING
    {
      pgr_begin_txn (&tx1, p, &e);

      // Make a bunch of changes to ensure
      // at least one UPDATE is written to the wal
      for (int i = 0; i < 5; ++i)
        {
          // TID1
          page_h dl_page = page_h_create ();
          test_fail_if (pgr_new (&dl_page, p, &tx1, PG_DATA_LIST, &e));
          dl_set_data (
              page_h_w (&dl_page),
              (struct dl_data){ .data = data[i], .blen = DL_DATA_SIZE });
          dl_set_prev (page_h_w (&dl_page), i + 20);
          dl_set_next (page_h_w (&dl_page), i + 100);
          test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));
        }

      pgr_commit (p, &tx1, &e);
    }

    // REOPEN
    test_fail_if (pgr_crash (p, &e));
    p = pgr_open_single_file ("testdb", &e);
    // NORMAL PROCESSING
    {
      pgr_begin_txn (&tx2, p, &e);
      test_assert (tx2.tid > tx1.tid); // ARIES Found max page

      // Make a bunch of changes to ensure
      // at least one UPDATE is written to the wal
      for (int i = 5; i < 10; ++i)
        {
          // TID1
          page_h dl_page = page_h_create ();
          test_fail_if (pgr_new (&dl_page, p, &tx2, PG_DATA_LIST, &e));
          dl_set_data (
              page_h_w (&dl_page),
              (struct dl_data){ .data = data[i], .blen = DL_DATA_SIZE });
          dl_set_prev (page_h_w (&dl_page), i + 20);
          dl_set_next (page_h_w (&dl_page), i + 100);
          test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));
        }

      pgr_commit (p, &tx2, &e);
    }

    // REOPEN
    test_fail_if (pgr_crash (p, &e));
    p = pgr_open_single_file ("testdb", &e);
    // VALIDATION
    {
      page_h pg = page_h_create ();

      // Root node was committed
      pgr_get (&pg, PG_ROOT_NODE, ROOT_PGNO, p, &e);
      test_assert_int_equal (rn_get_first_tmbst (page_h_ro (&pg)), 11);
      test_assert_int_equal (rn_get_master_lsn (page_h_ro (&pg)), 0);
      pgr_release (p, &pg, PG_ROOT_NODE, &e);

      // The data lists was commited
      for (int i = 1; i < 6; ++i)
        {
          // TID1
          pgr_get (&pg, PG_DATA_LIST, i, p, &e);
          test_assert_memequal (dl_get_data (page_h_ro (&pg)), data[i - 1],
                                DL_DATA_SIZE);
          test_assert_int_equal (dl_get_prev (page_h_ro (&pg)), (i - 1) + 20);
          test_assert_int_equal (dl_get_next (page_h_ro (&pg)), (i - 1) + 100);
          pgr_release (p, &pg, PG_DATA_LIST, &e);
        }

      // The data lists was commited
      for (int i = 6; i < 11; ++i)
        {
          // TID1
          pgr_get (&pg, PG_DATA_LIST, i, p, &e);
          test_assert_memequal (dl_get_data (page_h_ro (&pg)), data[i - 1],
                                DL_DATA_SIZE);
          test_assert_int_equal (dl_get_prev (page_h_ro (&pg)), (i - 1) + 20);
          test_assert_int_equal (dl_get_next (page_h_ro (&pg)), (i - 1) + 100);
          pgr_release (p, &pg, PG_DATA_LIST, &e);
        }
    }

    pgr_close (p, &e);
  }

  TEST_CASE ("first commit ok second commit not ok")
  {
    error e = error_create ();
    struct pager *p = NULL;

    // Resource acquisition
    {
      test_fail_if (pgr_delete_single_file ("testdb", &e));

      p = pgr_open_single_file ("testdb", &e);
    }

    u8 data[10][DL_DATA_SIZE];
    rand_bytes (data, DL_DATA_SIZE * 10);

    struct txn tx1, tx2;

    // NORMAL PROCESSING - this one commits fine
    {
      pgr_begin_txn (&tx1, p, &e);

      // Make a bunch of changes to ensure
      // at least one UPDATE is written to the wal
      for (int i = 0; i < 5; ++i)
        {
          // TID1
          page_h dl_page = page_h_create ();
          test_fail_if (pgr_new (&dl_page, p, &tx1, PG_DATA_LIST, &e));
          dl_set_data (
              page_h_w (&dl_page),
              (struct dl_data){ .data = data[i], .blen = DL_DATA_SIZE });
          dl_set_prev (page_h_w (&dl_page), i + 20);
          dl_set_next (page_h_w (&dl_page), i + 100);
          test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));
        }

      pgr_commit (p, &tx1, &e);
    }

    // REOPEN
    test_fail_if (pgr_crash (p, &e));
    p = pgr_open_single_file ("testdb", &e);
    // NORMAL PROCESSING - does NOT commit
    {
      pgr_begin_txn (&tx2, p, &e);
      test_assert (tx2.tid > tx1.tid); // ARIES Found max page

      // Make a bunch of changes to ensure
      // at least one UPDATE is written to the wal
      for (int i = 5; i < 10; ++i)
        {
          // TID1
          page_h dl_page = page_h_create ();
          test_fail_if (pgr_new (&dl_page, p, &tx2, PG_DATA_LIST, &e));
          dl_set_data (
              page_h_w (&dl_page),
              (struct dl_data){ .data = data[i], .blen = DL_DATA_SIZE });
          dl_set_prev (page_h_w (&dl_page), i + 20);
          dl_set_next (page_h_w (&dl_page), i + 100);
          test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));
        }
      pgr_flush_wall (p, &e);
    }

    // REOPEN
    test_fail_if (pgr_crash (p, &e));
    return;
    p = pgr_open_single_file ("testdb", &e);
    pgr_close (p, &e);
    return;

    // VALIDATION
    {
      page_h pg = page_h_create ();

      // Root node was committed
      pgr_get (&pg, PG_ROOT_NODE, ROOT_PGNO, p, &e);
      test_assert_int_equal (rn_get_first_tmbst (page_h_ro (&pg)), 6);
      test_assert_int_equal (rn_get_master_lsn (page_h_ro (&pg)), 0);
      pgr_release (p, &pg, PG_ROOT_NODE, &e);

      // The data lists was commited
      for (int i = 1; i < 6; ++i)
        {
          // TID1
          pgr_get (&pg, PG_DATA_LIST, i, p, &e);
          test_assert_memequal (dl_get_data (page_h_ro (&pg)), data[i - 1],
                                DL_DATA_SIZE);
          test_assert_int_equal (dl_get_prev (page_h_ro (&pg)), (i - 1) + 20);
          test_assert_int_equal (dl_get_next (page_h_ro (&pg)), (i - 1) + 100);
          pgr_release (p, &pg, PG_DATA_LIST, &e);
        }

      // The data lists was commited
      for (int i = 6; i < 11; ++i)
        {
          // TID2
          // pgr_get (&pg, PG_TOMBSTONE, i, p,
          // &e); test_assert_int_equal
          // (tmbst_get_next (page_h_ro (&pg)), i
          // + 1); pgr_release (p, &pg,
          // PG_TOMBSTONE, &e);
        }
    }

    pgr_close (p, &e);
  }
}

TEST (aries_crash_1)
{
  TEST_CASE ("Same thing but with a commit")
  {
    error e = error_create ();
    struct pager *p = NULL;

    // Resource acquisition
    {
      test_fail_if (pgr_delete_single_file ("testdb", &e));

      p = pgr_open_single_file ("testdb", &e);
    }

    pgno last_pg = 0;

    // Create 5 data list pages without a commit
    {
      struct txn tx1;
      pgr_begin_txn (&tx1, p, &e);

      // Make a bunch of changes to ensure
      // at least one UPDATE is written to the wal
      for (int i = 0; i < 5; ++i)
        {
          // TID1
          page_h dl_page = page_h_create ();
          test_fail_if (pgr_new (&dl_page, p, &tx1, PG_DATA_LIST, &e));
          dl_make_valid (page_h_w (&dl_page));
          test_fail_if (pgr_release (p, &dl_page, PG_DATA_LIST, &e));
        }

      // Flush WAL to make the test harder to pass
      pgr_commit (p, &tx1, &e);
    }

    // Crash our db and reopen
    test_fail_if (pgr_crash (p, &e));
    p = pgr_open_single_file ("testdb", &e);
    pgr_close (p, &e);
    return;

    // Read all 5 pages that we wrote to and ensure they're
    // tombstones (they were rolled back)
    {
      page_h pg = page_h_create ();

      // Root node was committed
      pgr_get (&pg, PG_ROOT_NODE, ROOT_PGNO, p, &e);
      test_assert_type_equal (rn_get_first_tmbst (page_h_ro (&pg)), 5, pgno,
                              PRpgno);
      test_assert_int_equal (rn_get_master_lsn (page_h_ro (&pg)), 0);
      pgr_release (p, &pg, PG_ROOT_NODE, &e);

      for (int i = 1; i < 6; ++i)
        {
          // TID1
          pgr_get (&pg, PG_DATA_LIST, i, p, &e);
          pgr_release (p, &pg, PG_DATA_LIST, &e);
        }
    }

    pgr_close (p, &e);
  }
}
#endif
