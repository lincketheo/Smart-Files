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

#include "aries/aries.h"
#include "c_specx.h"
#include "os_pager/file_pager.h"
#include "pager.h"
#include "pager/page_h.h"
#include "pages/fsm_page.h"
#include "pages/root_node.h"
#include "wal/wal.h"

#include <limits.h>

#ifndef PATH_MAX
#define PATH_MAX 260
#endif

/*
 * pgr_open — open or create a database using caller-supplied I/O abstractions.
 *
 * Takes ownership of [fp] and [ww].  On success they live inside the returned
 * pager.  On failure both are closed/freed before returning NULL.
 *
 * NEW DATABASE (ospgr_get_npages(fp) == 0):
 *   Resets the page store and WAL, commits an initial transaction that writes
 *   the root page, and sets PGR_ISNEW so the caller can distinguish a fresh
 *   database from an existing one.
 *
 * EXISTING DATABASE:
 *   Reads the root page to recover master_lsn.  If oswal_is_recoverable(ww)
 *   is true, runs the three-phase ARIES restart (analysis, redo, undo).
 *   If false the database file is trusted to be consistent and recovery is
 *   skipped; next_tid is initialised to 1 in that case since there is no WAL
 *   to scan for the highest TID.
 */
struct pager *
pgr_open (struct os_pager *fp, struct os_wal *ww, error *e)
{
  page_h root = page_h_create ();
  struct pager *ret = NULL;

  if ((ret = i_calloc (1, sizeof *ret, e)) == NULL)
    {
      goto failed;
    }

  ret->fp = fp;
  ret->ww = ww;

  // Initialize the dirty page table
  ret->dpt = dpgt_open (e);
  if (ret->dpt == NULL)
    {
      goto failed;
    }

  // Initialize the hash table from pgno -> table index
  ht_init_idx (&ret->pgno_to_value, ret->_hdata, MEMORY_PAGE_LEN);

  // Simple variables
  ret->clock = 0;
  ret->next_tid = 1;

  // Check if we need to create a new database or not
  if (ospgr_get_npages (ret->fp) == 0)
    {
      i_log_info ("new database\n");
      ret->flags = PGR_ISNEW;

      // Reset any data in the file pager
      if (ospgr_reset (ret->fp, e))
        {
          goto failed;
        }

      // Reset the wal
      if (oswal_reset (ret->ww, e))
        {
          goto failed;
        }

      // Open a new transaction table
      ret->tnxt = txnt_open (e);
      if (ret->tnxt == NULL)
        {
          goto failed;
        }

      // Start transactions at 1
      ret->next_tid = 1;

      // Make the initial transactions - create the root page
      {
        struct txn tx;

        if (pgr_begin_txn (&tx, ret, e))
          {
            goto failed;
          }

        if (pgr_new (&root, ret, &tx, PG_ROOT_NODE, e))
          {
            goto failed;
          }

        if (pgr_release (ret, &root, PG_ROOT_NODE, e))
          {
            goto failed;
          }

        if (pgr_commit (ret, &tx, e))
          {
            goto failed;
          }
      }
    }
  else
    {
      i_log_info ("open database\n");

      ret->flags = 0;

      // Read in the root page to get the last flushed master
      // lsn (our best guess, maybe we could scan the wal for
      // this?)
      page root_raw;
      if (ospgr_read (ret->fp, root_raw.raw, ROOT_PGNO, e))
        {
          goto failed;
        }
      root_raw.pg = ROOT_PGNO;
      ret->master_lsn = rn_get_master_lsn (&root_raw);

      // Open the transaction table
      ret->tnxt = txnt_open (e);
      if (ret->tnxt == NULL)
        {
          goto failed;
        }

      if (oswal_is_recoverable (ret->ww))
        {
          // Run ARIES recovery
          i_log_info ("ARIES recovery (analysis/redo/undo)\n");
          i_log_info ("master LSN: %" PRlsn "\n", ret->master_lsn);

          if (ret->master_lsn == 0)
            {
              i_log_info ("no checkpoint, recovering from beginning\n");
            }
          else
            {
              i_log_info ("recovering from checkpoint at LSN %" PRlsn "\n",
                          ret->master_lsn);
            }

          struct aries_ctx ctx;
          if (aries_ctx_create (&ctx, ret->master_lsn, e))
            {
              goto failed;
            }

          if (pgr_restart (ret, &ctx, e))
            {
              goto failed;
            }

          i_log_info ("ARIES recovery complete\n");

          // Advance next_tid past the highest TID seen during recovery so
          // we never reuse a TID from a previous run.
          ret->next_tid = ctx.max_tid + 1;
        }
      else
        {
          // WAL is non-recoverable trust the database
          // file is already consistent.  TID counter starts at 1 since
          // there is no WAL to scan for the previous high-water mark.
          i_log_info ("WAL not recoverable — skipping ARIES recovery\n");
          ret->next_tid = 1;
        }
    }

  return ret;

failed:
  ASSERT (e->cause_code);
  if (ret)
    {
      pgr_cancel_if_exists (ret, &root);
      if (ret->dpt)
        {
          dpgt_close (ret->dpt);
        }
      if (ret->tnxt)
        {
          txnt_close (ret->tnxt);
        }
      i_free (ret);
    }
  /* Close the I/O objects — pgr_open owns them. */
  if (ww)
    {
      oswal_close (ww, e);
    }
  if (fp)
    {
      ospgr_close (fp, e);
    }
  return NULL;
}

/*
 * pgr_open_single_file — standard file-backed entry point.
 *
 * Creates [dbname] if it does not exist, constructs a file_pager and a
 * file-backed WAL, then delegates to pgr_open().  Directory cleanup on
 * first-open failure is handled here because only this function knows the
 * path.
 *
 * NEW DATABASE (file is empty):
 *   Sets PGR_ISNEW so the caller can distinguish new databases from existing.
 *
 * EXISTING DATABASE:
 *   Runs the three-phase ARIES restart via pgr_open().
 */
struct pager *
pgr_open_single_file (const char *dbname, error *e)
{
  char fname[PATH_MAX];
  char walname[PATH_MAX];
  snprintf (fname, sizeof fname, "%s.db", dbname);
  snprintf (walname, sizeof walname, "%s.wal", dbname);

  struct os_pager *fp = fpgr_open_os (fname, e);
  if (fp == NULL)
    {
      return NULL;
    }

  struct os_wal *ww = wal_open_os (walname, e);
  if (ww == NULL)
    {
      ospgr_close (fp, e);
      return NULL;
    }

  /*
   * Capture whether this is a new database before handing off to pgr_open,
   * so that we can clean up the directory on failure.
   */
  bool is_new = (ospgr_get_npages (fp) == 0);

  struct pager *p = pgr_open (fp, ww, e);
  if (p == NULL)
    {
      /* fp and ww already closed by pgr_open on failure. */
      if (is_new)
        {
          i_rm_rf (dbname, e);
        }
      return NULL;
    }

  return p;
}

#ifndef NTEST
TEST (pager_open)
{
  error e = error_create ();

  // Green path
  {
    test_fail_if (pgr_delete_single_file ("testdb", &e));

    struct pager *p = pgr_open_single_file ("testdb", &e);

    pgr_close (p, &e);
  }
}
#endif

#ifndef NTEST
TEST (pgr_open_basic)
{
  error e = error_create ();
  test_fail_if (pgr_delete_single_file ("testdb", &e));
  test_fail_if (i_rm_rf ("testdb", &e));
  test_fail_if (i_mkdir ("testdb", &e));

  i_file fp = { 0 };
  i_open_rw (&fp, "testdb.db", &e);

  // File is shorter than page size
  test_fail_if (i_truncate (&fp, PAGE_SIZE - 1, &e));
  struct pager *p = pgr_open_single_file ("testdb", &e);
  test_assert_int_equal (e.cause_code, ERR_CORRUPT);
  test_assert_equal (p, NULL);
  e.cause_code = SUCCESS;

  // Half a page
  test_fail_if (i_truncate (&fp, PAGE_SIZE / 2, &e));
  p = pgr_open_single_file ("testdb", &e);
  test_assert_int_equal (e.cause_code, ERR_CORRUPT);
  test_assert_equal (p, NULL);
  e.cause_code = SUCCESS;

  // 0 pages
  test_fail_if (i_truncate (&fp, 0, &e));
  p = pgr_open_single_file ("testdb", &e);
  test_assert_int_equal (e.cause_code, SUCCESS);
  test_assert_int_equal ((int)pgr_get_npages (p), FS_BTMP_NPGS);
  test_fail_if (pgr_close (p, &e));

  // Tear down
  test_fail_if (i_close (&fp, &e));
  test_fail_if (pgr_delete_single_file ("testdb", &e));
}
#endif
