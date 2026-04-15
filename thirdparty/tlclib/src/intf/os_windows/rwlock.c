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

#define WIN32_LEAN_AND_MEAN
#include "tlclib/dev/assert.h"
#include "tlclib/error.h"
#include "tlclib/intf/logging.h"
#include "tlclib/intf/os/threading.h"
#include "tlclib/windows.h"

////////////////////////////////////////////////////////////
// RW Lock
//
// Backed by SRWLOCK. No init/destroy needed by Windows, but
// we keep the API for symmetry.
//
// write_locked tracks whether the current holder acquired it
// exclusively, since ReleaseSRWLock* has two separate callsites.
// This is only safe because numstore's rwlock contract requires
// a single writer OR multiple readers — not concurrent mixed use
// on the same i_rwlock instance from different threads calling
// unlock simultaneously.

err_t
i_rwlock_create (i_rwlock *dest, error *e)
{
  ASSERT (dest);
  (void)e;
  InitializeSRWLock (&dest->lock);
  dest->write_locked = false;
  return SUCCESS;
}

void
i_rwlock_free (i_rwlock *m)
{
  ASSERT (m);
  // No destroy for SRWLOCK — no-op.
  // Asserting not held is not possible without extra tracking.
}

void
i_rwlock_rdlock (i_rwlock *m)
{
  ASSERT (m);
  AcquireSRWLockShared (&m->lock);
}

void
i_rwlock_wrlock (i_rwlock *m)
{
  ASSERT (m);
  AcquireSRWLockExclusive (&m->lock);
  m->write_locked = true;
}

bool
i_rwlock_try_rdlock (i_rwlock *m)
{
  ASSERT (m);
  return TryAcquireSRWLockShared (&m->lock) != 0;
}

bool
i_rwlock_try_wrlock (i_rwlock *m)
{
  ASSERT (m);
  if (TryAcquireSRWLockExclusive (&m->lock))
    {
      m->write_locked = true;
      return true;
    }
  return false;
}

void
i_rwlock_unlock (i_rwlock *m)
{
  ASSERT (m);
  if (m->write_locked)
    {
      m->write_locked = false;
      ReleaseSRWLockExclusive (&m->lock);
    }
  else
    {
      ReleaseSRWLockShared (&m->lock);
    }
}
