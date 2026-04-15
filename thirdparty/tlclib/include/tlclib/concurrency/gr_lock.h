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

#include "tlclib/dev/error.h"
#include "tlclib/intf/os/threading.h"

enum lock_mode
{
  LM_IS = 0,
  LM_IX = 1,
  LM_S = 2,
  LM_SIX = 3,
  LM_X = 4,
  LM_COUNT = 5
};

/**
 * lock waiters are allocated on the stack - so a waiter only exists
 * for as long as gr_lock is waiting. Therefore, there is no array of
 * granted lock groups, instead, I use a counter to count how many locks
 * of each type are granted.
 *
 * This comes with drawbacks.
 *
 * 1. There is no priority - The only priority that's ensured is that the order
 *    of the condition variable signals is the same as the order that they came in,
 *    which is ok, but not the best.
 *
 * In order to add priority I need to keep track of the list of granted locks - which
 * would mean some sort of allocation or intrusive data structure.
 *
 * One idea would be to have a gr_lock live on the stack and it's lifecycle is tied
 * to one lock unlock flow, which is probably what I'll do, but for now, locks have
 * loose priority until it becomes a performance problem
 */
struct gr_lock_waiter
{
  enum lock_mode mode;
  i_cond cond;
  struct gr_lock_waiter *prev;
  struct gr_lock_waiter *next;
};

struct gr_lock
{
  i_mutex mutex;
  int holder_counts[LM_COUNT];
  struct gr_lock_waiter *head;
};

err_t gr_lock_init (struct gr_lock *l, error *e);

void gr_lock_destroy (struct gr_lock *l);

err_t gr_lock (struct gr_lock *l, enum lock_mode mode, error *e);
bool gr_trylock (struct gr_lock *l, enum lock_mode mode);
void gr_unlock (struct gr_lock *l, enum lock_mode mode);

const char *gr_lock_mode_name (enum lock_mode mode);
enum lock_mode get_parent_mode (enum lock_mode child_mode);
