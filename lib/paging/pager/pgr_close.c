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
#include "test/testing.h"

/*
 * Shut down the pager and release all resources.
 *
 * Steps:
 *   1. Checkpoint — flush all non-write-locked dirty pages to disk.
 *   2. Join the background checkpoint thread if it is running: signal it to
 *      stop, post the wake semaphore so it wakes up, then wait for cp_done.
 *   3. Evict every present frame (walks the full pool via clock).
 *   4. Free all subsystems: WAL, transaction table, file pager, DPT, and
 *      the pager struct itself.
 */
err_t
pgr_close (struct pager *p, error *e)
{
  // TODO - (2) checkpoint
  pgr_checkpoint (p, e);

  DBG_ASSERT (pager, p);

  // Maybe join checkpoint thread
  if (p->checkpoint_thread_running)
    {
      p->checkpoint_stop = true;

      i_semaphore_post (&p->cp_wake);

      // Unlock temporarily while waiting
      i_semaphore_wait (&p->cp_done);

      i_semaphore_free (&p->cp_wake);
      i_semaphore_free (&p->cp_done);
    }

  // Evict all pages
  for (u32 i = 0; i < MEMORY_PAGE_LEN; ++i)
    {
      struct page_frame *mp = &p->pages[p->clock];

      if (mp->flags & PW_PRESENT)
        {
          pgr_evict (p, mp, e);
        }

      p->clock = (p->clock + 1) % MEMORY_PAGE_LEN;
    }

  // Free resources
  oswal_close (p->ww, e);
  txnt_close (p->tnxt);
  ospgr_close (p->fp, e);
  dpgt_close (p->dpt);

  i_free (p);

  return error_trace (e);
}

#ifndef NTEST
TEST (TT_UNIT, pgr_close_success)
{
  error e = error_create ();
  test_fail_if (pgr_delete_single_file ("testdb", &e));

  struct pager *p = pgr_open_single_file ("testdb", &e);
  // Delete file i_close should fail
  test_assert_equal (pgr_close (p, &e), SUCCESS);
  test_fail_if (pgr_delete_single_file ("foodir", &e));
}
#endif
