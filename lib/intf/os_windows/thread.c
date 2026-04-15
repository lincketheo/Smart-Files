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
// Trampoline
//
// Windows thread start routine is DWORD WINAPI f(LPVOID), but
// our API is void *(*f)(void *). We bridge them with a small
// trampoline allocated on the heap so the args outlive the
// CreateThread call.

typedef struct
{
  void *(*func) (void *);
  void *arg;
} thread_trampoline_args;

static DWORD WINAPI
thread_trampoline (LPVOID param)
{
  thread_trampoline_args *args = (thread_trampoline_args *)param;
  void *(*func) (void *) = args->func;
  void *arg = args->arg;
  HeapFree (GetProcessHeap (), 0, args);
  func (arg);
  return 0;
}

////////////////////////////////////////////////////////////
// Thread

err_t
i_thread_create (i_thread *dest, void *(*func) (void *), void *context,
                 error *e)
{
  ASSERT (dest);

  thread_trampoline_args *args = HeapAlloc (GetProcessHeap (), 0,
                                            sizeof *args);
  if (!args)
    return error_causef (e, ERR_NOMEM, "thread_create: HeapAlloc failed");

  args->func = func;
  args->arg = context;

  dest->handle = CreateThread (NULL, 0, thread_trampoline, args, 0,
                               &dest->id);
  if (!dest->handle)
    {
      HeapFree (GetProcessHeap (), 0, args);
      char buf[256];
      FormatMessageA (FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
                      NULL, GetLastError (), 0, buf, sizeof (buf), NULL);
      return error_causef (e, ERR_IO, "CreateThread: %s", buf);
    }

  return SUCCESS;
}

err_t
i_thread_join (i_thread *t, error *e)
{
  ASSERT (t);
  ASSERT (t->handle);

  DWORD ret = WaitForSingleObject (t->handle, INFINITE);
  if (ret != WAIT_OBJECT_0)
    {
      char buf[256];
      FormatMessageA (FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
                      NULL, GetLastError (), 0, buf, sizeof (buf), NULL);
      return error_causef (e, ERR_IO, "thread_join: %s", buf);
    }

  CloseHandle (t->handle);
  t->handle = NULL;
  return SUCCESS;
}

void
i_thread_cancel (i_thread *t)
{
  ASSERT (t);
  ASSERT (t->handle);

  // TerminateThread is asynchronous and does not run destructors,
  // flush buffers, or release locks. It is the closest Windows
  // equivalent to pthread_cancel with deferred cancellation disabled.
  // numstore uses cancel only for WAL background flush threads that
  // hold no locks at cancel points, so this is safe in practice.
  if (!TerminateThread (t->handle, 0))
    {
      i_log_error ("thread_cancel: TerminateThread failed: %lu\n",
                   GetLastError ());
      UNREACHABLE ();
    }

  CloseHandle (t->handle);
  t->handle = NULL;
}

u64
get_available_threads (void)
{
  SYSTEM_INFO si;
  GetSystemInfo (&si);
  ASSERT (si.dwNumberOfProcessors > 0);
  return (u64)si.dwNumberOfProcessors;
}
