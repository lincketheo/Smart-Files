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

////////////////////////////////////////////////////////////
// RW Lock

err_t
i_rwlock_create (i_rwlock *dest, error *e)
{
  errno = 0;
#ifndef NDEBUG
  pthread_rwlockattr_t attr;
  int r1 = pthread_rwlockattr_init (&attr);
  ASSERT (!r1);
  // Set prefer-writer or other attributes if needed
  // pthread_rwlockattr_setkind_np(&attr,
  // PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
  const int r2 = pthread_rwlock_init (&dest->lock, &attr);
  r1 = pthread_rwlockattr_destroy (&attr);
  ASSERT (!r1);
  if (r2)
#else
  if (pthread_rwlock_init (&dest->lock, NULL))
#endif
    {
      switch (errno)
        {
        case EAGAIN:
          {
            return error_causef (e, ERR_IO, "rwlock_init: %s", strerror (errno));
          }
        case ENOMEM:
          {
            return error_causef (e, ERR_NOMEM, "rwlock_init: %s", strerror (errno));
          }
        case EPERM:
          {
            i_log_error ("rwlock_init: "
                         "insufficient "
                         "permissions: %s\n",
                         strerror (errno));
            UNREACHABLE ();
          }
        case EINVAL:
          {
            i_log_error ("rwlock_init: invalid "
                         "attributes: %s\n",
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
i_rwlock_free (i_rwlock *m)
{
  ASSERT (m);
  errno = 0;
  if (pthread_rwlock_destroy (&m->lock))
    {
      switch (errno)
        {
        case EBUSY:
          {
            i_log_error ("rwlock_destroy: still "
                         "locked: %s\n",
                         strerror (errno));
            UNREACHABLE ();
          }
        case EINVAL:
          {
            i_log_error ("rwlock_destroy: "
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
i_rwlock_rdlock (i_rwlock *m)
{
  ASSERT (m);
  errno = 0;
  if (pthread_rwlock_rdlock (&m->lock))
    {
      switch (errno)
        {
        case EBUSY:
          {
            i_log_error ("rwlock_rdlock: already "
                         "locked for reading: %s\n",
                         strerror (errno));
            UNREACHABLE ();
          }
        case EDEADLK:
          {
            i_log_error ("rwlock_rdlock: "
                         "deadlock: %s\n",
                         strerror (errno));
            UNREACHABLE ();
          }
        default:
          {
            i_log_error ("rwlock_rdlock: %s\n", strerror (errno));
            UNREACHABLE ();
          }
        }
    }
}

void
i_rwlock_wrlock (i_rwlock *m)
{
  ASSERT (m);
  errno = 0;
  if (pthread_rwlock_wrlock (&m->lock))
    {
      switch (errno)
        {
        case EDEADLK:
          {
            i_log_error ("rwlock_wrlock: "
                         "deadlock: %s\n",
                         strerror (errno));
            UNREACHABLE ();
          }
        default:
          {
            i_log_error ("rwlock_wrlock: %s\n", strerror (errno));
            UNREACHABLE ();
          }
        }
    }
}

bool
i_rwlock_try_rdlock (i_rwlock *m)
{
  ASSERT (m);
  errno = 0;
  const int result = pthread_rwlock_tryrdlock (&m->lock);

  if (result == 0)
    {
      return true; // Lock acquired
    }
  else if (result == EBUSY)
    {
      return false; // Lock not available, but not an error
    }
  else
    {
      switch (errno)
        {
        default:
          {
            i_log_error ("rwlock_try_rdlock: %s\n", strerror (errno));
            UNREACHABLE ();
          }
        }
    }
}

bool
i_rwlock_try_wrlock (i_rwlock *m)
{
  ASSERT (m);
  errno = 0;
  const int result = pthread_rwlock_trywrlock (&m->lock);

  if (result == 0)
    {
      return true; // Lock acquired
    }
  else if (result == EBUSY)
    {
      return false; // Lock not available, but not an error
    }
  else
    {
      switch (errno)
        {
        default:
          {
            i_log_error ("rwlock_try_wrlock: %s\n", strerror (errno));
            UNREACHABLE ();
          }
        }
    }
}

void
i_rwlock_unlock (i_rwlock *m)
{
  ASSERT (m);
  errno = 0;
  if (pthread_rwlock_unlock (&m->lock))
    {
      switch (errno)
        {
        case EINVAL:
          {
            i_log_error ("rwlock_unlock: "
                         "invalid: %s\n",
                         strerror (errno));
            UNREACHABLE ();
          }
        case EPERM:
          {
            i_log_error ("rwlock_unlock: "
                         "not owner: %s\n",
                         strerror (errno));
            UNREACHABLE ();
          }
        default:
          {
            i_log_error ("rwlock_unlock: %s\n", strerror (errno));
            UNREACHABLE ();
          }
        }
    }
}
