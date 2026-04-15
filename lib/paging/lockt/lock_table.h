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

#include "tlclib/concurrency/gr_lock.h"
#include "tlclib/ds/hash_table.h"
#include "tlclib/concurrency/latch.h"
#include "tlclib/memory/slab_alloc.h"
#include "paging/lockt/lt_lock.h"
#include "paging/txns/txn.h"

struct lockt
{
  struct slab_alloc lock_alloc; // Allocate gr locks
  struct htable *table;         // The table of locks
  latch l;                      // Latch for modifications
};

err_t lockt_init (struct lockt *t, error *e);
void lockt_destroy (struct lockt *t);

err_t lockt_lock (
    struct lockt *t,
    struct lt_lock lock,
    enum lock_mode mode,
    struct txn *tx,
    error *e);

err_t lockt_unlock (
    struct lockt *t,
    struct lt_lock lock,
    enum lock_mode mode,
    error *e);

void lockt_unlock_tx (struct lockt *t, struct txn *tx);

void i_log_lockt (int log_level, const struct lockt *t);
