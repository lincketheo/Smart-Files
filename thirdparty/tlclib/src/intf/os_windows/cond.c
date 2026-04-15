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

#include "tlclib/dev/assert.h"
#include "tlclib/error.h"
#include "tlclib/intf/logging.h"
#include "tlclib/intf/os.h"

////////////////////////////////////////////////////////////
// Condition Variable
//
// Windows CONDITION_VARIABLE needs no explicit init/destroy —
// InitializeConditionVariable is a no-op stub and there is no
// destroy. We keep create/free for API symmetry.

err_t
i_cond_create (i_cond *c, error *e)
{
  ASSERT (c);
  (void)e;
  InitializeConditionVariable (&c->cond);
  return SUCCESS;
}

void
i_cond_free (i_cond *c)
{
  ASSERT (c);
  // No-op: CONDITION_VARIABLE has no destroy function.
}

void
i_cond_wait (i_cond *c, i_mutex *m)
{
  ASSERT (c);
  ASSERT (m);

  if (!SleepConditionVariableCS (&c->cond, &m->m, INFINITE))
    {
      i_log_error ("cond_wait: SleepConditionVariableCS failed: %lu\n",
                   GetLastError ());
      UNREACHABLE ();
    }
}

void
i_cond_signal (i_cond *c)
{
  ASSERT (c);
  WakeConditionVariable (&c->cond);
}

void
i_cond_broadcast (i_cond *c)
{
  ASSERT (c);
  WakeAllConditionVariable (&c->cond);
}
