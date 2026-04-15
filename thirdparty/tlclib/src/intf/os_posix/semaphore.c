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
#include <time.h>

err_t
i_semaphore_create (i_semaphore *s, const unsigned int value, error *e)
{
  if (pthread_mutex_init (&s->mutex, NULL) != 0)
    return error_causef (e, ERR_IO, "pthread_mutex_init: %s", strerror (errno));

  if (pthread_cond_init (&s->cond, NULL) != 0)
    {
      pthread_mutex_destroy (&s->mutex);
      return error_causef (e, ERR_IO, "pthread_cond_init: %s", strerror (errno));
    }

  s->count = value;
  return SUCCESS;
}

void
i_semaphore_free (i_semaphore *s)
{
  ASSERT (s);
  pthread_cond_destroy (&s->cond);
  pthread_mutex_destroy (&s->mutex);
}

void
i_semaphore_post (i_semaphore *s)
{
  ASSERT (s);
  pthread_mutex_lock (&s->mutex);
  s->count++;
  pthread_cond_signal (&s->cond);
  pthread_mutex_unlock (&s->mutex);
}

void
i_semaphore_wait (i_semaphore *s)
{
  ASSERT (s);
  pthread_mutex_lock (&s->mutex);
  while (s->count == 0)
    pthread_cond_wait (&s->cond, &s->mutex);
  s->count--;
  pthread_mutex_unlock (&s->mutex);
}

bool
i_semaphore_try_wait (i_semaphore *s)
{
  ASSERT (s);
  pthread_mutex_lock (&s->mutex);
  bool acquired = s->count > 0;
  if (acquired)
    s->count--;
  pthread_mutex_unlock (&s->mutex);
  return acquired;
}

bool
i_semaphore_timed_wait (i_semaphore *s, const long msec)
{
  ASSERT (s);

  struct timespec ts;
  clock_gettime (CLOCK_REALTIME, &ts);
  ts.tv_sec += msec / 1000;
  ts.tv_nsec += (msec % 1000) * 1000000L;
  if (ts.tv_nsec >= 1000000000L)
    {
      ts.tv_sec += 1;
      ts.tv_nsec -= 1000000000L;
    }

  pthread_mutex_lock (&s->mutex);
  while (s->count == 0)
    {
      const int rc = pthread_cond_timedwait (&s->cond, &s->mutex, &ts);
      if (rc == ETIMEDOUT)
        {
          pthread_mutex_unlock (&s->mutex);
          return false;
        }
    }
  s->count--;
  pthread_mutex_unlock (&s->mutex);
  return true;
}
