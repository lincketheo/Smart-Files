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

#include "core/assert.h"
#include "core/error.h"
#include "intf/logging.h"
#include "intf/os.h"

#include <errno.h>
#include <pthread.h>
#include <string.h>

////////////////////////////////////////////////////////////
// Mutex

struct i_mutex_s
{
  pthread_mutex_t mutex;
};

err_t
i_mutex_create (i_mutex *dest, error *e)
{
  errno = 0;
#ifndef NDEBUG
  pthread_mutexattr_t attr;

  // I just don't want to handle errors for debug code
  int r1 = pthread_mutexattr_init (&attr);
  ASSERT (!r1);

  r1 = pthread_mutexattr_settype (&attr, PTHREAD_MUTEX_ERRORCHECK);
  ASSERT (!r1);

  const int r2 = pthread_mutex_init (&dest->m, NULL);

  r1 = pthread_mutexattr_destroy (&attr);
  ASSERT (!r1);
  if (r2)
#else
  if (pthread_mutex_init (&dest->m, NULL))
#endif
    {
      switch (errno)
        {
        case EAGAIN:
          {
            return error_causef (e, ERR_IO, "mutex_init: %s",
                                 strerror (errno));
          }
        case ENOMEM:
          {
            return error_causef (e, ERR_NOMEM, "mutex_init: %s",
                                 strerror (errno));
          }
        case EPERM:
          {
            i_log_error ("mutex_init: insufficient "
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

void
i_mutex_free (i_mutex *m)
{
  ASSERT (m);

  errno = 0;
  if (pthread_mutex_destroy (&m->m))
    {
      switch (errno)
        {
        case EBUSY:
          {
            i_log_error ("mutex_destroy: still "
                         "locked: %s\n",
                         strerror (errno));
            UNREACHABLE ();
          }
        case EINVAL:
          {
            i_log_error ("mutex_destroy: "
                         "invalid: %s\n",
                         strerror (errno));
            UNREACHABLE ();
          }
        default:
          {
            UNREACHABLE ();
          }
        }
    }
}

void
i_mutex_lock (i_mutex *m)
{
  ASSERT (m);

  errno = 0;
  if (pthread_mutex_lock (&m->m))
    {
      switch (errno)
        {
        case EINVAL:
          {
            i_log_error ("mutex_lock: "
                         "invalid: %s\n",
                         strerror (errno));
            UNREACHABLE ();
          }
        case EAGAIN:
          {
            i_log_error ("mutex_lock: recursive "
                         "lock: %s\n",
                         strerror (errno));
            UNREACHABLE ();
          }
        case EDEADLK:
          {
            i_log_error ("mutex_lock: "
                         "deadlock: %s\n",
                         strerror (errno));
            UNREACHABLE ();
          }
        default:
          {
            i_log_error ("mutex_lock: %s\n", strerror (errno));
            UNREACHABLE ();
          }
        }
    }
}

bool
i_mutex_try_lock (i_mutex *m)
{
  ASSERT (m);

  errno = 0;
  if (pthread_mutex_trylock (&m->m))
    {
      switch (errno)
        {
        case EINVAL:
          {
            i_log_error ("mutex_lock: "
                         "invalid: %s\n",
                         strerror (errno));
            UNREACHABLE ();
          }
        case EAGAIN:
          {
            i_log_error ("mutex_lock: recursive "
                         "lock: %s\n",
                         strerror (errno));
            UNREACHABLE ();
          }
        case EDEADLK:
          {
            i_log_error ("mutex_lock: "
                         "deadlock: %s\n",
                         strerror (errno));
            UNREACHABLE ();
          }
        case EBUSY:
          {
            return false;
          }
        default:
          {
            i_log_error ("mutex_lock: %s\n", strerror (errno));
            UNREACHABLE ();
          }
        }
    }
  return true;
}

void
i_mutex_unlock (i_mutex *m)
{
  ASSERT (m);

  errno = 0;
  if (pthread_mutex_unlock (&m->m))
    {
      switch (errno)
        {
        case EINVAL:
          {
            i_log_error ("mutex_unlock: "
                         "invalid: %s\n",
                         strerror (errno));
            UNREACHABLE ();
          }
        case EAGAIN:
          {
            i_log_error ("mutex_unlock: recursive "
                         "lock: %s\n",
                         strerror (errno));
            UNREACHABLE ();
          }
        case EPERM:
          {
            i_log_error ("mutex_unlock: "
                         "not owner: %s\n",
                         strerror (errno));
            UNREACHABLE ();
          }
        default:
          {
            i_log_error ("mutex_unlock: %s\n", strerror (errno));
            UNREACHABLE ();
          }
        }
    }
}
