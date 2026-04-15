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

#include "tlclib/dev/signatures.h"
#include "tlclib/intf/os.h"

#include <stdatomic.h>

// TODO - (16) ABA problem

typedef _Atomic (int) latch;

HEADER_FUNC void
latch_init (latch *l)
{
  *l = 0;
}

HEADER_FUNC void
latch_lock (latch *l)
{
  int val = 0;

  if (likely (atomic_compare_exchange_weak_explicit (l, &val, 1, memory_order_acquire, memory_order_relaxed)))
    {
      return;
    }

  do
    {
      do
        {
          spin_pause ();
          val = atomic_load_explicit (l, memory_order_relaxed);
        }
      while (val != 0);
    }
  while (!atomic_compare_exchange_weak_explicit (l, &val, 1, memory_order_acquire, memory_order_relaxed));
}

HEADER_FUNC void
latch_unlock (latch *l)
{
  atomic_store_explicit (l, 0, memory_order_release);
}
