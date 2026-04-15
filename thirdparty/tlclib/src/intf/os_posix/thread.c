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

#include "tlclib/dev/assert.h"
#include "tlclib/dev/error.h"
#include "tlclib/intf/logging.h"
#include "tlclib/intf/os.h"

#include <errno.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>

////////////////////////////////////////////////////////////
// Condition Variable

err_t
i_thread_create (i_thread *dest, void *(*func) (void *), void *context,
                 error *e)
{
  ASSERT (dest);

#ifndef NDEBUG
  pthread_attr_t attr;
  int r1 = pthread_attr_init (&attr);
  ASSERT (!r1);

  // Examples:
  // pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  // pthread_attr_setstacksize(&attr, 1 << 20);
  // pthread_attr_setguardsize(&attr, 4096);
  // pthread_attr_setinheritsched(&attr, PTHREAD_EXPLICIT_SCHED);
  // pthread_attr_setschedpolicy(&attr, SCHED_OTHER);
  // pthread_attr_setschedparam(&attr, &sched_param);

  const int r2 = pthread_create (&dest->thread, &attr, func, context);

  r1 = pthread_attr_destroy (&attr);
  ASSERT (!r1);
  if (r2)
#else
  if (pthread_create (&dest->thread, NULL, func, context))
#endif
    {
      switch (errno)
        {
        case EAGAIN:
          {
            return error_causef (e, ERR_IO, "pthread_create: %s", strerror (errno));
          }
        case EINVAL:
          {
            i_log_error ("pthread_create: invalid "
                         "attributes: %s\n",
                         strerror (errno));
            UNREACHABLE ();
          }
        case EPERM:
          {
            i_log_error ("pthread_create: "
                         "insufficient "
                         "permissions: %s\n",
                         strerror (errno));
            UNREACHABLE ();
          }
        default:
          {
            UNREACHABLE ();
          }
        }
    }

  return SUCCESS;
}

err_t
i_thread_join (i_thread *t, error *e)
{
  ASSERT (t);

  const int r = pthread_join (t->thread, NULL);

  if (r != 0)
    {
      switch (r)
        {
        case EDEADLK:
          {
            i_log_error ("pthread_join: "
                         "deadlock: %s\n",
                         strerror (r));
            UNREACHABLE ();
          }
        case EINVAL:
          {
            i_log_error ("pthread_join: not "
                         "joinable: %s\n",
                         strerror (r));
            UNREACHABLE ();
          }
        case ESRCH:
          {
            i_log_error ("pthread_join: no such "
                         "thread: %s\n",
                         strerror (r));
            UNREACHABLE ();
          }
        default:
          {
            UNREACHABLE ();
          }
        }
    }

  return SUCCESS;
}

void
i_thread_cancel (i_thread *t)
{
  ASSERT (t);

  const int r = pthread_cancel (t->thread);
  if (r != 0)
    {
      switch (r)
        {
        case ESRCH:
          {
            i_log_error ("pthread_cancel: no such "
                         "thread: %s\n",
                         strerror (r));
            UNREACHABLE ();
          }
        default:
          {
            UNREACHABLE ();
          }
        }
    }
}

u64
get_available_threads (void)
{
  const long ret = sysconf (_SC_NPROCESSORS_ONLN);
  ASSERT (ret > 0);
  return (u64)ret;
}
