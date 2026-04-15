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

/**
static void
pgr_checkpoint_thread (void *_ctx)
{
  struct pager *p = _ctx;
  error e = error_create ();

  while (!p->checkpoint_stop)
    {
      // Wait for cp_wake
      i_semaphore_timed_wait (&p->cp_wake, 1);

      if (p->checkpoint_stop)
        {
          break;
        }

      if (pgr_deletion_blocking_checkpoint (p, &e))
        {
          error_log_consume (&e);
        }
    }

  i_semaphore_post (&p->cp_done);
}
*/

err_t
pgr_launch_checkpoint_thread (struct pager *p, u64 msec, error *e)
{
  p->checkpoint_stop = false;

  if (i_semaphore_create (&p->cp_wake, 0, e))
    {
      goto theend;
    }

  if (i_semaphore_create (&p->cp_done, 0, e))
    {
      goto fail_wake;
    }

  /**
  if (tp_add_task (p->tp, pgr_checkpoint_thread, p, e))
  {
  goto fail_done;
  }
  */

  p->checkpoint_thread_running = true;

  goto theend;

  // fail_done:
  i_semaphore_free (&p->cp_done);
fail_wake:
  i_semaphore_free (&p->cp_wake);

theend:
  return error_trace (e);
}
