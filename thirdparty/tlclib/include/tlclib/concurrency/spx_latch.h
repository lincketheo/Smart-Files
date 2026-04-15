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

#pragma once

#include "tlclib/intf/os.h"
#include "tlclib/numstore/types.h"

#include <stdatomic.h>

typedef _Atomic (unsigned int) sx_latch;

#define S_MASK 0x0000FFFFu // [15:0]
#define X 0x00010000u      // [16]

#define SLOCKED(val) (val & S_MASK)
#define XLOCKED(val) (val & X)

static inline void
spx_latch_init (sx_latch *l)
{
  *l = 0;
}

static inline void
spx_lock_s (sx_latch *l)
{
  u32 val = atomic_load_explicit (l, memory_order_relaxed);

  // If not X locked, fast path to acquire S lock
  if (likely (!XLOCKED (val)
              && atomic_compare_exchange_weak_explicit (l, &val, val + 1,
                                                        memory_order_acquire,
                                                        memory_order_relaxed)))
    {
      return;
    }

  // Otherwise, wait for XLOCK to drain
  do
    {
      do
        {
          spin_pause ();
          val = atomic_load_explicit (l, memory_order_relaxed);
        }
      while (XLOCKED (val));
    }
  while (!atomic_compare_exchange_weak_explicit (
      l, &val, val + 1, memory_order_acquire, memory_order_relaxed));
}

static inline void
spx_unlock_s (sx_latch *l)
{
  atomic_fetch_sub_explicit (l, 1, memory_order_release);
}

static inline void
spx_upgrade_s_x (sx_latch *l)
{
  u32 val = atomic_load_explicit (l, memory_order_relaxed);

  while (true)
    {
      // If we aren't X locked,
      if (likely (!(XLOCKED (val))))
        {
          // Try to lock
          if (atomic_compare_exchange_weak_explicit (l, &val, (val - 1) | X,
                                                     memory_order_acquire,
                                                     memory_order_relaxed))
            {
              break;
            }

          // That failed, drop down to next step
          continue;
        }

      // X is already held.  Release my S so they can drain.
      // */
      spx_unlock_s (l);

      // Wait for them to finish.
      do
        {
          spin_pause ();
          val = atomic_load_explicit (l, memory_order_relaxed);
        }
      while (XLOCKED (val));

      // Start over from S
      spx_lock_s (l);
      val = atomic_load_explicit (l, memory_order_relaxed);
    }

  // X is set, our S is decremented.  Drain remaining readers.
  while (SLOCKED (atomic_load_explicit (l, memory_order_acquire)))
    {
      spin_pause ();
    }
}

static inline void
spx_unlock_x (sx_latch *l)
{
  atomic_store_explicit (l, 0, memory_order_release);
}
