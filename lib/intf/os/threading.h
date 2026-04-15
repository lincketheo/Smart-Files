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

#include "numstore/errors.h"

#include <stdio.h>

#if defined(_WIN32)
#define WIN32_LEAN_AND_MEAN
#include "windows.h"
#else
#include <pthread.h>
#include <semaphore.h>
#include <time.h>
#endif

////////////////////////////////////////////////////////////
// Mutex

typedef struct
{
#if defined(_WIN32)
  CRITICAL_SECTION m;
#else
  pthread_mutex_t m;
#endif
} i_mutex;

err_t i_mutex_create (i_mutex *m, error *e);
void i_mutex_free (i_mutex *m);
void i_mutex_lock (i_mutex *m);
bool i_mutex_try_lock (i_mutex *m);
void i_mutex_unlock (i_mutex *m);

////////////////////////////////////////////////////////////
// Semaphore

typedef struct
{
#if defined(_WIN32)
  HANDLE handle;
#else
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  unsigned int count;
#endif
} i_semaphore;

err_t i_semaphore_create (i_semaphore *s, unsigned int value, error *e);
void i_semaphore_free (i_semaphore *s);
void i_semaphore_post (i_semaphore *s);
void i_semaphore_wait (i_semaphore *s);
bool i_semaphore_try_wait (i_semaphore *s);
bool i_semaphore_timed_wait (i_semaphore *s, long nsec);

////////////////////////////////////////////////////////////
// RW Lock

typedef struct
{
#if defined(_WIN32)
  SRWLOCK lock;
  bool write_locked;
#else
  pthread_rwlock_t lock;
#endif
} i_rwlock;

err_t i_rwlock_create (i_rwlock *rw, error *e);
void i_rwlock_free (i_rwlock *rw);
void i_rwlock_rdlock (i_rwlock *rw);
void i_rwlock_wrlock (i_rwlock *rw);
void i_rwlock_unlock (i_rwlock *rw);
bool i_rwlock_try_rdlock (i_rwlock *rw);
bool i_rwlock_try_wrlock (i_rwlock *rw);

////////////////////////////////////////////////////////////
// Thread

typedef struct
{
#if defined(_WIN32)
  HANDLE handle;
  DWORD id;
#else
  pthread_t thread;
#endif
} i_thread;

err_t i_thread_create (i_thread *t, void *(*start_routine) (void *), void *arg,
                       error *e);
err_t i_thread_join (i_thread *t, error *e);
void i_thread_cancel (i_thread *t);
u64 get_available_threads (void);

////////////////////////////////////////////////////////////
// Condition Variable

typedef struct
{
#if defined(_WIN32)
  CONDITION_VARIABLE cond;
#else
  pthread_cond_t cond;
#endif
} i_cond;

err_t i_cond_create (i_cond *c, error *e);
void i_cond_free (i_cond *c);
void i_cond_wait (i_cond *c, i_mutex *m);
void i_cond_signal (i_cond *c);
void i_cond_broadcast (i_cond *c);
