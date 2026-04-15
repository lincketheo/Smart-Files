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

#define WIN32_LEAN_AND_MEAN
#include "core/assert.h"
#include "core/error.h"
#include "intf/logging.h"
#include "intf/os/threading.h"
#include "windows.h"

////////////////////////////////////////////////////////////
// Mutex
//
// CRITICAL_SECTION is always recursive on Windows. There is no
// non-recursive variant. In debug builds we emulate ERRORCHECK
// behavior by tracking the owning thread and asserting on
// re-entry and unlock-without-lock.

#ifndef NDEBUG
static DWORD
cs_owner (i_mutex *m)
{
  // OwningThread is documented as a HANDLE but is actually the
  // thread ID cast to a HANDLE on all shipping Windows versions.
  return (DWORD)(uintptr_t)((CRITICAL_SECTION *)&m->m)->OwningThread;
}
#endif

err_t
i_mutex_create (i_mutex *dest, error *e)
{
  ASSERT (dest);
  (void)e;
  // dwSpinCount=0: no spinning, go straight to kernel wait.
  // Use a non-zero value (e.g. 4000) if profiling shows contention.
  InitializeCriticalSectionAndSpinCount (&dest->m, 0);
  return SUCCESS;
}

void
i_mutex_free (i_mutex *m)
{
  ASSERT (m);
#ifndef NDEBUG
  DWORD owner = cs_owner (m);
  if (owner != 0)
    {
      i_log_error ("mutex_destroy: still locked by thread %lu\n", owner);
      UNREACHABLE ();
    }
#endif
  DeleteCriticalSection (&m->m);
}

void
i_mutex_lock (i_mutex *m)
{
  ASSERT (m);
#ifndef NDEBUG
  DWORD tid = GetCurrentThreadId ();
  if (cs_owner (m) == tid)
    {
      i_log_error ("mutex_lock: deadlock — thread %lu already owns mutex\n",
                   tid);
      UNREACHABLE ();
    }
#endif
  EnterCriticalSection (&m->m);
}

bool
i_mutex_try_lock (i_mutex *m)
{
  ASSERT (m);
#ifndef NDEBUG
  DWORD tid = GetCurrentThreadId ();
  if (cs_owner (m) == tid)
    {
      i_log_error ("mutex_try_lock: thread %lu already owns mutex\n", tid);
      UNREACHABLE ();
    }
#endif
  return TryEnterCriticalSection (&m->m) != 0;
}

void
i_mutex_unlock (i_mutex *m)
{
  ASSERT (m);
#ifndef NDEBUG
  DWORD tid = GetCurrentThreadId ();
  if (cs_owner (m) != tid)
    {
      i_log_error ("mutex_unlock: thread %lu does not own mutex\n", tid);
      UNREACHABLE ();
    }
#endif
  LeaveCriticalSection (&m->m);
}
