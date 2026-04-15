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
#include "tlclib/dev/assert.h"
#include "tlclib/error.h"
#include "tlclib/intf/logging.h"
#include "tlclib/intf/os/threading.h"
#include "tlclib/windows.h"

////////////////////////////////////////////////////////////
// Semaphore
//
// Backed by a Win32 semaphore HANDLE. LONG_MAX as the maximum
// count is effectively unbounded for numstore's usage.

err_t
i_semaphore_create (i_semaphore *s, unsigned int value, error *e)
{
  ASSERT (s);
  s->handle = CreateSemaphoreA (NULL, (LONG)value, LONG_MAX, NULL);
  if (!s->handle)
    {
      char buf[256];
      FormatMessageA (FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
                      NULL, GetLastError (), 0, buf, sizeof (buf), NULL);
      return error_causef (e, ERR_IO, "CreateSemaphore: %s", buf);
    }
  return SUCCESS;
}

void
i_semaphore_free (i_semaphore *s)
{
  ASSERT (s);
  ASSERT (s->handle);
  CloseHandle (s->handle);
  s->handle = NULL;
}

void
i_semaphore_post (i_semaphore *s)
{
  ASSERT (s);
  if (!ReleaseSemaphore (s->handle, 1, NULL))
    {
      i_log_error ("semaphore_post: ReleaseSemaphore failed: %lu\n",
                   GetLastError ());
      UNREACHABLE ();
    }
}

void
i_semaphore_wait (i_semaphore *s)
{
  ASSERT (s);
  DWORD ret = WaitForSingleObject (s->handle, INFINITE);
  if (ret != WAIT_OBJECT_0)
    {
      i_log_error ("semaphore_wait: WaitForSingleObject failed: %lu\n",
                   GetLastError ());
      UNREACHABLE ();
    }
}

bool
i_semaphore_try_wait (i_semaphore *s)
{
  ASSERT (s);
  DWORD ret = WaitForSingleObject (s->handle, 0);
  if (ret == WAIT_OBJECT_0)
    return true;
  if (ret == WAIT_TIMEOUT)
    return false;
  i_log_error ("semaphore_try_wait: WaitForSingleObject failed: %lu\n",
               GetLastError ());
  UNREACHABLE ();
}

bool
i_semaphore_timed_wait (i_semaphore *s, long msec)
{
  ASSERT (s);
  DWORD ret = WaitForSingleObject (s->handle, (DWORD)msec);
  if (ret == WAIT_OBJECT_0)
    return true;
  if (ret == WAIT_TIMEOUT)
    return false;
  i_log_error ("semaphore_timed_wait: WaitForSingleObject failed: %lu\n",
               GetLastError ());
  UNREACHABLE ();
}
