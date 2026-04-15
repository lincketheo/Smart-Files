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

#if defined(_MSC_VER)

#define WIN32_LEAN_AND_MEAN
#include "intrin.h"
#include "windows.h"

// All _Atomic(T) types are represented as volatile LONG (32-bit).
#define _Atomic(T) volatile LONG
typedef volatile LONG atomic_int;

// Memory-order constants – kept for source compatibility; the MSVC
// Interlocked functions always act as full barriers on x86/x64.
typedef enum
{
  memory_order_relaxed = 0,
  memory_order_consume = 1,
  memory_order_acquire = 2,
  memory_order_release = 3,
  memory_order_acq_rel = 4,
  memory_order_seq_cst = 5,
} memory_order;

// atomic_load_explicit
#define atomic_load_explicit(obj, order) \
  ((LONG)InterlockedAdd ((volatile LONG *)(obj), 0))

// atomic_store_explicit / atomic_store
#define atomic_store_explicit(obj, val, order) \
  ((void)InterlockedExchange ((volatile LONG *)(obj), (LONG)(val)))
#define atomic_store(obj, val) \
  ((void)InterlockedExchange ((volatile LONG *)(obj), (LONG)(val)))

// CAS: returns 1 on success, 0 on failure (updates *expected on failure)
static __inline int
_ns_cmpxchg32 (volatile LONG *obj, LONG *expected, LONG desired)
{
  LONG prev = InterlockedCompareExchange (obj, desired, *expected);
  if (prev == *expected)
    return 1;
  *expected = prev;
  return 0;
}
#define atomic_compare_exchange_weak_explicit(obj, expected, desired, succ, \
                                              fail)                         \
  _ns_cmpxchg32 ((volatile LONG *)(obj), (LONG *)(expected), (LONG)(desired))

// atomic_fetch_sub_explicit (returns old value)
#define atomic_fetch_sub_explicit(obj, val, order) \
  InterlockedExchangeAdd ((volatile LONG *)(obj), -(LONG)(val))

// atomic_fetch_add (returns old value)
#define atomic_fetch_add(obj, val) \
  InterlockedExchangeAdd ((volatile LONG *)(obj), (LONG)(val))

// atomic_signal_fence (compiler barrier)
#define atomic_signal_fence(order) _ReadWriteBarrier ()

#else
// GCC, Clang, and modern MSVC with /std:c11
#include <stdatomic.h>
#endif
