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
// Condition Variable

err_t
i_cond_create (i_cond *c, error *e)
{
  ASSERT (c);

#ifndef NDEBUG
  pthread_condattr_t attr;

  // I just don't want to handle errors for debug code
  int r1 = pthread_condattr_init (&attr);
  ASSERT (r1 == 0);

  const int r2 = pthread_cond_init (&c->cond, &attr);

  r1 = pthread_condattr_destroy (&attr);
  ASSERT (r1 == 0);

  if (r2)
#else
  if (pthread_cond_init (&c->cond, NULL))
#endif
    {
      switch (errno)
        {
        case EAGAIN:
          {
            return error_causef (e, ERR_IO, "pthread_cond_init: %s", strerror (errno));
          }

        case ENOMEM:
          {
            return error_causef (e, ERR_NOMEM, "pthread_cond_init: %s",
                                 strerror (errno));
          }

        case EBUSY:
          {
            i_log_error ("cond_create: cond "
                         "already initialized: "
                         "%s\n",
                         strerror (errno));
            UNREACHABLE ();
          }

        case EINVAL:
          {
            i_log_error ("cond_create: invalid "
                         "attributes or cond: %s\n",
                         strerror (errno));
            UNREACHABLE ();
          }

        default:
          {
            i_log_error ("cond_create: unknown "
                         "error: %s\n",
                         strerror (errno));
            UNREACHABLE ();
          }
        }
    }

  return SUCCESS;
}

void
i_cond_free (i_cond *c)
{
  ASSERT (c);

  errno = 0;
  if (pthread_cond_destroy (&c->cond))
    {
      switch (errno)
        {
        case EBUSY:
          {
            i_log_error ("cond_free: cond has "
                         "active waiters: %s\n",
                         strerror (errno));
            UNREACHABLE ();
          }

        case EINVAL:
          {
            i_log_error ("cond_free: invalid or "
                         "uninitialized cond: %s\n",
                         strerror (errno));
            UNREACHABLE ();
          }

        default:
          {
            i_log_error ("cond_free: unknown "
                         "error: %s\n",
                         strerror (errno));
            UNREACHABLE ();
          }
        }
    }
}

void
i_cond_wait (i_cond *c, i_mutex *m)
{
  ASSERT (c);
  ASSERT (m);

  errno = 0;
  if (pthread_cond_wait (&c->cond, &m->m))
    {
      switch (errno)
        {
        case EINVAL:
          {
            i_log_error ("cond_wait: invalid cond "
                         "or mutex: %s\n",
                         strerror (errno));
            UNREACHABLE ();
          }

        case EPERM:
          {
            i_log_error ("cond_wait: mutex not "
                         "owned by thread: %s\n",
                         strerror (errno));
            UNREACHABLE ();
          }

        default:
          {
            i_log_error ("cond_wait: unknown "
                         "error: %s\n",
                         strerror (errno));
            UNREACHABLE ();
          }
        }
    }
}

void
i_cond_signal (i_cond *c)
{
  ASSERT (c);

  errno = 0;
  if (pthread_cond_signal (&c->cond))
    {
      switch (errno)
        {
        case EINVAL:
          {
            i_log_error ("cond_signal: invalid or "
                         "uninitialized cond: %s\n",
                         strerror (errno));
            UNREACHABLE ();
          }

        default:
          {
            i_log_error ("cond_signal: unknown "
                         "error: %s\n",
                         strerror (errno));
            UNREACHABLE ();
          }
        }
    }
}

void
i_cond_broadcast (i_cond *c)
{
  ASSERT (c);

  errno = 0;
  if (pthread_cond_broadcast (&c->cond))
    {
      switch (errno)
        {
        case EINVAL:
          {
            i_log_error ("cond_broadcast: invalid "
                         "or uninitialized cond: "
                         "%s\n",
                         strerror (errno));
            UNREACHABLE ();
          }

        default:
          {
            i_log_error ("cond_broadcast: unknown "
                         "error: %s\n",
                         strerror (errno));
            UNREACHABLE ();
          }
        }
    }
}
