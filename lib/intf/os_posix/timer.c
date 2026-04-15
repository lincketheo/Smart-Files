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
#include "intf/os.h"

#include <errno.h>
#include <string.h>
#include <time.h>

////////////////////////////////////////////////////////////
// TIMING

// Timer API - handle-based monotonic timer
err_t
i_timer_create (i_timer *timer, error *e)
{
  ASSERT (timer);

  if (clock_gettime (CLOCK_MONOTONIC, &timer->start) != 0)
    {
      return error_causef (e, ERR_IO, "clock_gettime: %s", strerror (errno));
    }

  return SUCCESS;
}

void
i_timer_free (i_timer *timer)
{
  ASSERT (timer);
  // No cleanup needed for POSIX timers
}

u64
i_timer_now_ns (i_timer *timer)
{
  ASSERT (timer);

  struct timespec now;
  clock_gettime (CLOCK_MONOTONIC, &now);

  // Calculate elapsed time in nanoseconds
  const i64 sec_diff = (i64)now.tv_sec - (i64)timer->start.tv_sec;
  const i64 nsec_diff = (i64)now.tv_nsec - (i64)timer->start.tv_nsec;

  return (u64)(sec_diff * 1000000000LL + nsec_diff);
}

u64
i_timer_now_us (i_timer *timer)
{
  return i_timer_now_ns (timer) / 1000ULL;
}

u64
i_timer_now_ms (i_timer *timer)
{
  return i_timer_now_ns (timer) / 1000000ULL;
}

f64
i_timer_now_s (i_timer *timer)
{
  return (f64)i_timer_now_ns (timer) / 1000000000.0;
}

// Legacy API (deprecated - use i_timer instead)
void
i_get_monotonic_time (struct timespec *ts)
{
  clock_gettime (CLOCK_MONOTONIC, ts);
}

void
i_sleep_us (const u64 us)
{
  const struct timespec ts = { .tv_sec = (time_t)(us / 1000000ULL),
                               .tv_nsec = (long)((us % 1000000ULL) * 1000ULL) };
  nanosleep (&ts, NULL);
}
