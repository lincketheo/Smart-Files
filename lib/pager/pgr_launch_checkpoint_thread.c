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

#include "c_specx/intf/logging.h"
#include "c_specx/intf/os/threading.h"
#include "pager.h"

static void
pgr_do_checkpoint (void *ctx)
{
  struct pager *p = ctx;
  error e = error_create ();
  i_log_info ("Executing checkpoint\n");
  if (pgr_deletion_blocking_checkpoint (p, &e))
    {
      error_log_consume (&e);
    }
}

err_t
pgr_launch_checkpoint_thread (struct pager *p, u64 msec, error *e)
{
  return periodic_task_start (&p->checkpoint_task, msec, pgr_do_checkpoint, p, e);
}
