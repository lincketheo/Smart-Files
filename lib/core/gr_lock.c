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

#include "core/gr_lock.h"

#include "core/assert.h"
#include "intf/os.h"
#include "numstore/types.h"
#include "test/testing.h"

#include <stdatomic.h>
#include <string.h>

// clang-format off
static const bool compatible[LM_COUNT][LM_COUNT] = {
  //         IS      IX      S       SIX     X
  [LM_IS]  = { true,  true,  true,  true,  false },
  [LM_IX]  = { true,  true,  false, false, false },
  [LM_S]   = { true,  false, true,  false, false },
  [LM_SIX] = { true,  false, false, false, false },
  [LM_X]   = { false, false, false, false, false },
};
// clang-format on
static const char *mode_names[LM_COUNT] = { "IS", "IX", "S", "SIX", "X" };

err_t
gr_lock_init (struct gr_lock *l, error *e)
{
  const err_t result = i_mutex_create (&l->mutex, e);
  if (result != SUCCESS)
    {
      return result;
    }

  memset (l->holder_counts, 0, sizeof (l->holder_counts));
  l->head = NULL;

  return SUCCESS;
}

void
gr_lock_destroy (struct gr_lock *l)
{
  i_mutex_free (&l->mutex);

  // Note: This assumes no threads are still waiting
  // TODO - (6) Caller must ensure all threads have released locks

  while (l->head)
    {
      struct gr_lock_waiter *w = l->head;
      l->head = w->next;
      i_cond_free (&w->cond);
    }
}

/**
 * Example:
 *
 * Granted Group:
 * IS IX IS IS IS
 *
 * Granted Group Count:
 * IS = 4
 * IX = 1
 *
 * Lock S is compatible?
 * IS > 0 -> IS + S = GOOD
 * IX > 1 -> IX + S = BAD
 * - Not Compatible
 *
 * Lock IS is compatible?
 * IS > 0 -> IS + IS = GOOD
 * IX > 1 -> IX + IS = GOOD
 * - Compatible
 */
static bool
is_compatible (const struct gr_lock *l, const enum lock_mode mode)
{
  for (int i = 0; i < LM_COUNT; i++)
    {
      if (l->holder_counts[i] > 0 && !compatible[mode][i])
        {
          return false;
        }
    }
  return true;
}

static void
wake_waiters (struct gr_lock *l)
{
  for (struct gr_lock_waiter *w = l->head; w; w = w->next)
    {
      if (is_compatible (l, w->mode))
        {
          i_cond_signal (&w->cond);
        }
    }
}

static err_t
gr_lock_waiter_init (struct gr_lock_waiter *dest, const enum lock_mode mode, error *e)
{
  dest->mode = mode;
  dest->prev = NULL;
  dest->next = NULL;

  if (i_cond_create (&dest->cond, e))
    {
      return error_trace (e);
    }

  return SUCCESS;
}

static void
gr_lock_waiter_append_unsafe (struct gr_lock *l, struct gr_lock_waiter *w)
{
  // Append on the front
  if (l->head == NULL)
    {
      l->head = w;
    }
  else
    {
      // Search for the end
      struct gr_lock_waiter *head = l->head;
      while (head->next != NULL)
        {
          head = head->next;
        }

      // Append on the end
      head->next = w;
      w->prev = head;
    }
}

static void
gr_lock_waiter_remove_unsafe (struct gr_lock *l, const struct gr_lock_waiter *w)
{
  if (w->prev != NULL)
    {
      w->prev->next = w->next;
    }
  else
    {
      ASSERT (l->head == w);
      l->head = w->next;
    }

  if (w->next != NULL)
    {
      w->next->prev = w->prev;
    }
}

err_t
gr_lock (struct gr_lock *l, const enum lock_mode mode, error *e)
{
  i_mutex_lock (&l->mutex);

  // Is compatible - add this lock mode to the lock group
  // and move on
  if (is_compatible (l, mode))
    {
      l->holder_counts[mode]++;
      i_mutex_unlock (&l->mutex);
      return SUCCESS;
    }

  // Create a new waiter
  struct gr_lock_waiter waiter;
  if (gr_lock_waiter_init (&waiter, mode, e))
    {
      i_mutex_unlock (&l->mutex);
      return error_trace (e);
    }

  // Append it to the end of the waiter list
  gr_lock_waiter_append_unsafe (l, &waiter);

  // Main wait code
  while (!is_compatible (l, mode))
    {
      i_cond_wait (&waiter.cond, &l->mutex);
    }

  // Remove from waiters list
  gr_lock_waiter_remove_unsafe (l, &waiter);

  i_cond_free (&waiter.cond);

  // Acquire the lock
  l->holder_counts[mode]++;

  i_mutex_unlock (&l->mutex);

  return SUCCESS;
}

bool
gr_trylock (struct gr_lock *l, const enum lock_mode mode)
{
  ASSERT (l);

  if (!i_mutex_try_lock (&l->mutex))
    {
      return false;
    }

  if (!is_compatible (l, mode))
    {
      i_mutex_unlock (&l->mutex);
      return false;
    }

  l->holder_counts[mode]++;
  i_mutex_unlock (&l->mutex);

  return true;
}

void
gr_unlock (struct gr_lock *l, const enum lock_mode mode)
{
  i_mutex_lock (&l->mutex);

  ASSERT (l->holder_counts[mode] > 0);

  l->holder_counts[mode]--;

  // Wake any compatible waiters
  if (l->head)
    {
      wake_waiters (l);
    }

  i_mutex_unlock (&l->mutex);
}

const char *
gr_lock_mode_name (const enum lock_mode mode)
{
  if (mode >= 0 && mode < LM_COUNT)
    {
      return mode_names[mode];
    }
  return "INVALID";
}

enum lock_mode
get_parent_mode (const enum lock_mode child_mode)
{
  switch (child_mode)
    {
    case LM_IS:
    case LM_S:
      {
        return LM_IS;
      }
    case LM_IX:
    case LM_SIX:
    case LM_X:
      {
        return LM_IX;
      }
    case LM_COUNT:
      {
        UNREACHABLE ();
      }
    }
  UNREACHABLE ();
}

#ifndef NTEST

struct lock_test_ctx
{
  struct gr_lock *lock;
  atomic_int t1_acquired;
  atomic_int t2_blocked;
  atomic_int t2_acquired;
  atomic_int counter;
  enum lock_mode mode1;
  enum lock_mode mode2;
};

static void *
busy_wait_short (void)
{
  for (volatile int i = 0; i < 1000000; ++i)
    {
    }
  return NULL;
}

// Test basic lock/unlock
TEST (TT_UNIT, gr_lock_basic)
{
  struct gr_lock lock;
  error e = error_create ();

  gr_lock_init (&lock, &e);

  // Acquire and release each mode
  for (int mode = 0; mode < LM_COUNT; mode++)
    {
      gr_lock (&lock, mode, &e);
      test_assert_equal (lock.holder_counts[mode], 1);
      gr_unlock (&lock, mode);
      test_assert_equal (lock.holder_counts[mode], 0);
    }

  gr_lock_destroy (&lock);
}

// Test multiple holders of same mode
TEST (TT_UNIT, gr_lock_multiple_holders)
{
  struct gr_lock lock;
  error e = error_create ();

  gr_lock_init (&lock, &e);

  // S locks can be held multiple times
  gr_lock (&lock, LM_S, &e);
  gr_lock (&lock, LM_S, &e);
  gr_lock (&lock, LM_S, &e);
  test_assert_equal (lock.holder_counts[LM_S], 3);

  gr_unlock (&lock, LM_S);
  test_assert_equal (lock.holder_counts[LM_S], 2);
  gr_unlock (&lock, LM_S);
  test_assert_equal (lock.holder_counts[LM_S], 1);
  gr_unlock (&lock, LM_S);
  test_assert_equal (lock.holder_counts[LM_S], 0);

  gr_lock_destroy (&lock);
}

// Helper thread functions for compatibility tests
static void *
thread_acquire_wait_release (void *arg)
{
  struct lock_test_ctx *ctx = arg;
  error e = error_create ();
  const err_t result = gr_lock (ctx->lock, ctx->mode1, &e);
  if (result != SUCCESS)
    {
      return NULL;
    }
  ctx->t1_acquired = 1;
  i_sleep_us (100000); // Hold lock for 100ms
  gr_unlock (ctx->lock, ctx->mode1);
  return NULL;
}

static void *
thread_try_acquire (void *arg)
{
  struct lock_test_ctx *ctx = arg;
  error e = error_create ();
  i_sleep_us (20000); // Let t1 acquire first
  ctx->t2_blocked = 1;
  const err_t result = gr_lock (ctx->lock, ctx->mode2, &e);
  if (result != SUCCESS)
    {
      return NULL;
    }
  ctx->t2_acquired = 1;
  ctx->t2_blocked = 0;
  gr_unlock (ctx->lock, ctx->mode2);
  return NULL;
}

// Test IS + IS (compatible)
TEST (TT_UNIT, gr_lock_is_is_compatible)
{
  struct gr_lock lock;
  error e = error_create ();

  gr_lock_init (&lock, &e);

  struct lock_test_ctx ctx = { .lock = &lock, .mode1 = LM_IS, .mode2 = LM_IS };
  i_thread t1, t2;

  test_assert_equal (
      i_thread_create (&t1, thread_acquire_wait_release, &ctx, &e), SUCCESS);
  test_assert_equal (i_thread_create (&t2, thread_try_acquire, &ctx, &e),
                     SUCCESS);

  i_sleep_us (50000); // Check if both acquired

  i_thread_join (&t1, &e);
  i_thread_join (&t2, &e);

  test_assert (ctx.t1_acquired);
  test_assert (ctx.t2_acquired);

  gr_lock_destroy (&lock);
}

// Test IS + IX (compatible)
TEST (TT_UNIT, gr_lock_is_ix_compatible)
{
  struct gr_lock lock;
  error e = error_create ();

  gr_lock_init (&lock, &e);

  struct lock_test_ctx ctx = { .lock = &lock, .mode1 = LM_IS, .mode2 = LM_IX };
  i_thread t1, t2;
  test_assert_equal (
      i_thread_create (&t1, thread_acquire_wait_release, &ctx, &e), SUCCESS);
  test_assert_equal (i_thread_create (&t2, thread_try_acquire, &ctx, &e),
                     SUCCESS);

  i_sleep_us (50000);

  i_thread_join (&t1, &e);
  i_thread_join (&t2, &e);

  test_assert (ctx.t1_acquired);
  test_assert (ctx.t2_acquired);

  gr_lock_destroy (&lock);
}

// Test IS + S (compatible)
TEST (TT_UNIT, gr_lock_is_s_compatible)
{
  struct gr_lock lock;
  error e = error_create ();

  gr_lock_init (&lock, &e);

  struct lock_test_ctx ctx = { .lock = &lock, .mode1 = LM_IS, .mode2 = LM_S };
  i_thread t1, t2;
  test_assert_equal (
      i_thread_create (&t1, thread_acquire_wait_release, &ctx, &e), SUCCESS);
  test_assert_equal (i_thread_create (&t2, thread_try_acquire, &ctx, &e),
                     SUCCESS);

  i_sleep_us (50000);

  i_thread_join (&t1, &e);
  i_thread_join (&t2, &e);

  test_assert (ctx.t1_acquired);
  test_assert (ctx.t2_acquired);

  gr_lock_destroy (&lock);
}

// Test IS + SIX (compatible)
TEST (TT_UNIT, gr_lock_is_six_compatible)
{
  struct gr_lock lock;
  error e = error_create ();

  gr_lock_init (&lock, &e);

  struct lock_test_ctx ctx
      = { .lock = &lock, .mode1 = LM_IS, .mode2 = LM_SIX };
  i_thread t1, t2;
  test_assert_equal (
      i_thread_create (&t1, thread_acquire_wait_release, &ctx, &e), SUCCESS);
  test_assert_equal (i_thread_create (&t2, thread_try_acquire, &ctx, &e),
                     SUCCESS);

  i_sleep_us (50000);

  i_thread_join (&t1, &e);
  i_thread_join (&t2, &e);

  test_assert (ctx.t1_acquired);
  test_assert (ctx.t2_acquired);

  gr_lock_destroy (&lock);
}

// Test IS + X (blocks)
TEST (TT_UNIT, gr_lock_is_x_blocks)
{
  struct gr_lock lock;
  error e = error_create ();

  gr_lock_init (&lock, &e);

  struct lock_test_ctx ctx = { .lock = &lock, .mode1 = LM_IS, .mode2 = LM_X };
  i_thread t1, t2;
  test_assert_equal (
      i_thread_create (&t1, thread_acquire_wait_release, &ctx, &e), SUCCESS);
  test_assert_equal (i_thread_create (&t2, thread_try_acquire, &ctx, &e),
                     SUCCESS);

  i_sleep_us (50000);
  // Sample state while threads are still running
  const int t1_acquired_sample = ctx.t1_acquired;
  const int t2_blocked_sample = ctx.t2_blocked;
  const int t2_acquired_sample = ctx.t2_acquired;

  i_thread_join (&t1, &e);
  i_thread_join (&t2, &e);

  test_assert (t1_acquired_sample);
  test_assert (t2_blocked_sample);
  test_assert (!t2_acquired_sample);

  gr_lock_destroy (&lock);
}

// Test IX + IX (compatible)
TEST (TT_UNIT, gr_lock_ix_ix_compatible)
{
  struct gr_lock lock;
  error e = error_create ();

  gr_lock_init (&lock, &e);

  struct lock_test_ctx ctx = { .lock = &lock, .mode1 = LM_IX, .mode2 = LM_IX };
  i_thread t1, t2;
  test_assert_equal (
      i_thread_create (&t1, thread_acquire_wait_release, &ctx, &e), SUCCESS);
  test_assert_equal (i_thread_create (&t2, thread_try_acquire, &ctx, &e),
                     SUCCESS);

  i_sleep_us (50000);

  i_thread_join (&t1, &e);
  i_thread_join (&t2, &e);

  test_assert (ctx.t1_acquired);
  test_assert (ctx.t2_acquired);

  gr_lock_destroy (&lock);
}

// Test IX + S (blocks)
TEST (TT_UNIT, gr_lock_ix_s_blocks)
{
  struct gr_lock lock;
  error e = error_create ();

  gr_lock_init (&lock, &e);

  struct lock_test_ctx ctx = { .lock = &lock, .mode1 = LM_IX, .mode2 = LM_S };
  i_thread t1, t2;
  test_assert_equal (
      i_thread_create (&t1, thread_acquire_wait_release, &ctx, &e), SUCCESS);
  test_assert_equal (i_thread_create (&t2, thread_try_acquire, &ctx, &e),
                     SUCCESS);

  i_sleep_us (50000);
  // Sample state while threads are still running
  const int t1_acquired_sample = ctx.t1_acquired;
  const int t2_blocked_sample = ctx.t2_blocked;

  i_thread_join (&t1, &e);
  i_thread_join (&t2, &e);

  test_assert (t1_acquired_sample);
  test_assert (t2_blocked_sample);

  gr_lock_destroy (&lock);
}

// Test S + S (compatible)
TEST (TT_UNIT, gr_lock_s_compatible)
{
  struct gr_lock lock;
  error e = error_create ();

  gr_lock_init (&lock, &e);

  struct lock_test_ctx ctx = { .lock = &lock, .mode1 = LM_S, .mode2 = LM_S };
  i_thread t1, t2;
  test_assert_equal (
      i_thread_create (&t1, thread_acquire_wait_release, &ctx, &e), SUCCESS);
  test_assert_equal (i_thread_create (&t2, thread_try_acquire, &ctx, &e),
                     SUCCESS);

  i_sleep_us (50000);

  i_thread_join (&t1, &e);
  i_thread_join (&t2, &e);

  test_assert (ctx.t1_acquired);
  test_assert (ctx.t2_acquired);

  gr_lock_destroy (&lock);
}

// Test S + X (blocks)
TEST (TT_UNIT, gr_lock_x_blocks)
{
  struct gr_lock lock;
  error e = error_create ();

  gr_lock_init (&lock, &e);

  struct lock_test_ctx ctx = { .lock = &lock, .mode1 = LM_S, .mode2 = LM_X };
  i_thread t1, t2;
  test_assert_equal (
      i_thread_create (&t1, thread_acquire_wait_release, &ctx, &e), SUCCESS);
  test_assert_equal (i_thread_create (&t2, thread_try_acquire, &ctx, &e),
                     SUCCESS);

  i_sleep_us (50000);
  // Sample state while threads are still running
  const int t1_acquired_sample = ctx.t1_acquired;
  const int t2_blocked_sample = ctx.t2_blocked;

  i_thread_join (&t1, &e);
  i_thread_join (&t2, &e);

  test_assert (t1_acquired_sample);
  test_assert (t2_blocked_sample);

  gr_lock_destroy (&lock);
}

// Test SIX + IS (compatible - only compatible pair for SIX)
TEST (TT_UNIT, gr_lockix_is_compatible)
{
  struct gr_lock lock;
  error e = error_create ();

  gr_lock_init (&lock, &e);

  struct lock_test_ctx ctx
      = { .lock = &lock, .mode1 = LM_SIX, .mode2 = LM_IS };
  i_thread t1, t2;
  test_assert_equal (
      i_thread_create (&t1, thread_acquire_wait_release, &ctx, &e), SUCCESS);
  test_assert_equal (i_thread_create (&t2, thread_try_acquire, &ctx, &e),
                     SUCCESS);

  i_sleep_us (50000);

  i_thread_join (&t1, &e);
  i_thread_join (&t2, &e);

  test_assert (ctx.t1_acquired);
  test_assert (ctx.t2_acquired);

  gr_lock_destroy (&lock);
}

// Test SIX + IX (blocks)
TEST (TT_UNIT, gr_lockix_ix_blocks)
{
  struct gr_lock lock;
  error e = error_create ();

  gr_lock_init (&lock, &e);

  struct lock_test_ctx ctx
      = { .lock = &lock, .mode1 = LM_SIX, .mode2 = LM_IX };
  i_thread t1, t2;
  test_assert_equal (
      i_thread_create (&t1, thread_acquire_wait_release, &ctx, &e), SUCCESS);
  test_assert_equal (i_thread_create (&t2, thread_try_acquire, &ctx, &e),
                     SUCCESS);

  i_sleep_us (50000);
  // Sample state while threads are still running
  const int t1_acquired_sample = ctx.t1_acquired;
  const int t2_blocked_sample = ctx.t2_blocked;

  i_thread_join (&t1, &e);
  i_thread_join (&t2, &e);

  test_assert (t1_acquired_sample);
  test_assert (t2_blocked_sample);

  gr_lock_destroy (&lock);
}

// Test SIX + S (blocks)
TEST (TT_UNIT, gr_lockix_s_blocks)
{
  struct gr_lock lock;
  error e = error_create ();

  gr_lock_init (&lock, &e);

  struct lock_test_ctx ctx = { .lock = &lock, .mode1 = LM_SIX, .mode2 = LM_S };
  i_thread t1, t2;
  test_assert_equal (
      i_thread_create (&t1, thread_acquire_wait_release, &ctx, &e), SUCCESS);
  test_assert_equal (i_thread_create (&t2, thread_try_acquire, &ctx, &e),
                     SUCCESS);

  i_sleep_us (50000);
  // Sample state while threads are still running
  const int t1_acquired_sample = ctx.t1_acquired;
  const int t2_blocked_sample = ctx.t2_blocked;

  i_thread_join (&t1, &e);
  i_thread_join (&t2, &e);

  test_assert (t1_acquired_sample);
  test_assert (t2_blocked_sample);

  gr_lock_destroy (&lock);
}

// Test X + X (blocks)
TEST (TT_UNIT, gr_lock_sx_blocks)
{
  struct gr_lock lock;
  error e = error_create ();

  gr_lock_init (&lock, &e);

  struct lock_test_ctx ctx = { .lock = &lock, .mode1 = LM_X, .mode2 = LM_X };
  i_thread t1, t2;
  test_assert_equal (
      i_thread_create (&t1, thread_acquire_wait_release, &ctx, &e), SUCCESS);
  test_assert_equal (i_thread_create (&t2, thread_try_acquire, &ctx, &e),
                     SUCCESS);

  i_sleep_us (50000);
  // Sample state while threads are still running
  const int t1_acquired_sample = ctx.t1_acquired;
  const int t2_blocked_sample = ctx.t2_blocked;

  i_thread_join (&t1, &e);
  i_thread_join (&t2, &e);

  test_assert (t1_acquired_sample);
  test_assert (t2_blocked_sample);

  gr_lock_destroy (&lock);
}

// Test concurrent readers (multiple S locks)
static void *
reader_thread (void *arg)
{
  struct lock_test_ctx *ctx = arg;
  error e = error_create ();
  const err_t result = gr_lock (ctx->lock, LM_S, &e);
  if (result != SUCCESS)
    {
      return NULL;
    }
  atomic_fetch_add (&ctx->counter, 1);
  busy_wait_short ();
  gr_unlock (ctx->lock, LM_S);
  return NULL;
}

TEST (TT_UNIT, gr_lock_concurrent_readers)
{
  struct gr_lock lock;
  error e = error_create ();

  gr_lock_init (&lock, &e);

  struct lock_test_ctx ctx = { .lock = &lock };
  i_thread threads[5];
  for (int i = 0; i < 5; i++)
    {
      test_assert_equal (i_thread_create (
                             &threads[i],
                             reader_thread,
                             &ctx,
                             &e),
                         SUCCESS);
    }

  for (int i = 0; i < 5; i++)
    {
      i_thread_join (&threads[i], &e);
    }

  test_assert_equal (ctx.counter, 5); // All readers should acquire

  gr_lock_destroy (&lock);
}

// Test data race protection with exclusive locks
static void *
increment_thread (void *arg)
{
  struct lock_test_ctx *ctx = arg;
  error e = error_create ();
  for (int i = 0; i < 100; i++)
    {
      const err_t result = gr_lock (ctx->lock, LM_X, &e);
      if (result != SUCCESS)
        {
          return NULL;
        }
      const int old = ctx->counter;
      busy_wait_short ();
      ctx->counter = old + 1;
      gr_unlock (ctx->lock, LM_X);
    }
  return NULL;
}

TEST (TT_UNIT, gr_lock_data_race_protection)
{
  struct gr_lock lock;
  error e = error_create ();

  gr_lock_init (&lock, &e);

  struct lock_test_ctx ctx = { .lock = &lock };
  i_thread threads[5];
  for (int i = 0; i < 5; i++)
    {
      test_assert_equal (
          i_thread_create (&threads[i], increment_thread, &ctx, &e), SUCCESS);
    }

  for (int i = 0; i < 5; i++)
    {
      i_thread_join (&threads[i], &e);
    }

  test_assert_equal (ctx.counter, 500); // 5 threads * 100 increments

  gr_lock_destroy (&lock);
}

#endif
