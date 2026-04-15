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

#include "tlclib/concurrency/latch.h"

#include "tlclib/dev/testing.h"
#include "tlclib/intf/os/threading.h"

#ifndef NTEST
struct data
{
  const u32 iters;
  int value;
  latch l;
};

static void *
data_thread (void *_data)
{
  struct data *d = _data;

  for (u32 i = 0; i < d->iters; ++i)
    {
      latch_lock (&d->l);
      d->value += 1;
      latch_unlock (&d->l);
    }

  return NULL;
}

TEST (latch)
{
  error e = error_create ();

  struct data d = {
    .iters = 1000,
    .value = 0,
    .l = 0,
  };

  i_thread threads[1000];

  for (u32 i = 0; i < 1000; ++i)
    {
      i_thread_create (&threads[i], data_thread, &d, &e);
    }

  for (u32 i = 0; i < 1000; ++i)
    {
      i_thread_join (&threads[i], &e);
    }

  test_assert_int_equal (d.value, 1000 * 1000);
}
#endif
