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

#if defined(_WIN32)

#define WIN32_LEAN_AND_MEAN
#include "core/assert.h"
#include "core/error.h"
#include "intf/os/time.h"
#include "windows.h"

////////////////////////////////////////////////////////////
// Timing
//
// QueryPerformanceCounter is the Windows monotonic clock.
// QueryPerformanceFrequency returns ticks/sec and is fixed
// at boot — safe to query once per timer.

err_t
i_timer_create (i_timer *timer, error *e)
{
  ASSERT (timer);

  if (!QueryPerformanceFrequency (&timer->frequency))
    {
      char buf[256];
      FormatMessageA (FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
                      NULL, GetLastError (), 0, buf, sizeof (buf), NULL);
      return error_causef (e, ERR_IO, "QueryPerformanceFrequency: %s", buf);
    }

  if (!QueryPerformanceCounter (&timer->start))
    {
      char buf[256];
      FormatMessageA (FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
                      NULL, GetLastError (), 0, buf, sizeof (buf), NULL);
      return error_causef (e, ERR_IO, "QueryPerformanceCounter: %s", buf);
    }

  return SUCCESS;
}

void
i_timer_free (i_timer *timer)
{
  ASSERT (timer);
  // No cleanup needed.
}

u64
i_timer_now_ns (i_timer *timer)
{
  ASSERT (timer);

  LARGE_INTEGER now;
  QueryPerformanceCounter (&now);

  LONGLONG elapsed = now.QuadPart - timer->start.QuadPart;

  // elapsed * 1e9 / frequency — multiply before divide to preserve precision.
  // Risk of overflow: elapsed * 1e9 overflows LONGLONG at ~9.2 seconds if
  // frequency is 1 GHz. Most systems are 10–100 MHz, giving ~92–920 seconds
  // before overflow. For long-running timers use the 128-bit path below.
  //
  // Safe path: scale to ns using (elapsed / freq) * 1e9 + remainder.
  LONGLONG freq = timer->frequency.QuadPart;
  LONGLONG sec = elapsed / freq;
  LONGLONG rem = elapsed % freq;
  return (u64)(sec * 1000000000LL + (rem * 1000000000LL) / freq);
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
  ASSERT (timer);

  LARGE_INTEGER now;
  QueryPerformanceCounter (&now);

  LONGLONG elapsed = now.QuadPart - timer->start.QuadPart;
  return (f64)elapsed / (f64)timer->frequency.QuadPart;
}

void
i_sleep_us (u64 us)
{
  // Sleep() takes milliseconds; round up to avoid sleeping too short.
  DWORD ms = (DWORD)((us + 999ULL) / 1000ULL);
  Sleep (ms);
}

#endif // _WIN32
