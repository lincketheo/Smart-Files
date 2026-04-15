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

#include "core/assert.h"
#include "core/bounds.h"
#include "core/error.h"
#include "intf/os.h"
#include "test/testing.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

////////////////////////////////////////////////////////////
// MEMORY
void *
i_malloc (const u32 nelem, const u32 size, error *e)
{
  ASSERT (nelem > 0);
  ASSERT (size > 0);

  u32 bytes;
  if (!safe_mul_u32 (&bytes, nelem, size))
    {
      error_causef (e, ERR_NOMEM, "malloc %d*%d: overflow", nelem, size);
      return NULL;
    }

  errno = 0;
  void *ret = malloc ((size_t)bytes);
  if (ret == NULL)
    {
      if (errno == ENOMEM)
        {
          error_causef (e, ERR_NOMEM, "malloc %d*%d: %s", nelem, size,
                        strerror (errno));
        }
      else
        {
          error_causef (e, ERR_NOMEM, "malloc: %s", strerror (errno));
        }
    }
  return ret;
}

void *
i_calloc (const u32 nelem, const u32 size, error *e)
{
  ASSERT (nelem > 0);
  ASSERT (size > 0);

  u32 bytes = 0;
  if (!safe_mul_u32 (&bytes, nelem, size))
    {
      error_causef (e, ERR_NOMEM, "malloc %d*%d: overflow", nelem, size);
      return NULL;
    }

  ASSERT (bytes > 0);

  errno = 0;
  void *ret = calloc ((size_t)nelem, (size_t)size);
  if (ret == NULL)
    {
      if (errno == ENOMEM)
        {
          error_causef (e, ERR_NOMEM, "calloc %d*%d: %s", nelem, size,
                        strerror (errno));
        }
      else
        {
          error_causef (e, ERR_NOMEM, "calloc: %s", strerror (errno));
        }
    }
  return ret;
}

// =======================================================
// Core realloc wrapper (used by all)
// =======================================================
static void *
i_realloc (void *ptr, const u32 nelem, const u32 size, error *e)
{
  ASSERT (nelem > 0);
  ASSERT (size > 0);

  u32 bytes = 0;
  {
    bool ok = safe_mul_u32 (&bytes, nelem, size);
    ASSERT (ok);
    if (!ok)
      {
        error_causef (e, ERR_NOMEM, "realloc %u*%u: overflow", nelem, size);
        return NULL;
      }
  }

  errno = 0;
  void *ret = realloc (ptr, (size_t)bytes);
  if (ret == NULL)
    {
      error_causef (e, ERR_NOMEM, "realloc %u bytes: %s", bytes,
                    strerror (errno));
      return NULL;
    }
  return ret;
}

#ifndef NTEST
TEST (TT_UNIT, i_realloc_basic)
{
  error e = error_create ();
  u32 *a = i_realloc (NULL, 10, sizeof *a, &e); // behaves like malloc
  for (u32 i = 0; i < 10; i++)
    {
      a[i] = i;
    }

  u32 *b = i_realloc (a, 20, sizeof *b, &e);
  for (u32 i = 0; i < 10; i++)
    {
      test_assert (b[i] == i);
    }
  i_free (b);
}
#endif

// =======================================================
// RIGHT REALLOC (no shifting) — caller passes old_nelem
// =======================================================
void *
i_realloc_right (void *ptr, const u32 old_nelem, const u32 new_nelem, const u32 size, error *e)
{
  ASSERT (size > 0);
  if (ptr == NULL)
    {
      ASSERT (old_nelem == 0);
    }
  ASSERT (new_nelem > 0);

  return i_realloc (ptr, new_nelem, size, e);
}

#ifndef NTEST
TEST (TT_UNIT, i_realloc_right)
{
  error e = error_create ();
  u32 *a = i_malloc (10, sizeof *a, &e);
  for (u32 i = 0; i < 10; i++)
    {
      a[i] = i;
    }

  a = i_realloc_right (a, 10, 20, sizeof *a, &e);
  for (u32 i = 0; i < 10; i++)
    {
      test_assert (a[i] == i);
    }

  // shrink path
  a = i_realloc_right (a, 20, 10, sizeof *a, &e);
  for (u32 i = 0; i < 10; i++)
    {
      test_assert (a[i] == i);
    }
  i_free (a);
}
#endif

// LEFT REALLOC (grow prepends space; shrink drops left)
void *
i_realloc_left (void *ptr, const u32 old_nelem, const u32 new_nelem, const u32 size, error *e)
{
  ASSERT (size > 0);
  if (ptr == NULL)
    {
      ASSERT (old_nelem == 0);
    }
  ASSERT (new_nelem > 0);

  if (old_nelem == new_nelem)
    {
      return ptr;
    }

  u32 old_bytes32 = 0, new_bytes32 = 0;
  {
    bool ok_old = safe_mul_u32 (&old_bytes32, old_nelem, size);
    ASSERT (ok_old);
    bool ok_new = safe_mul_u32 (&new_bytes32, new_nelem, size);
    ASSERT (ok_new);
    if (!ok_old || !ok_new)
      {
        error_causef (e, ERR_NOMEM, "realloc_left: overflow");
        return NULL;
      }
  }

  const size_t old_bytes = (size_t)old_bytes32;
  const size_t new_bytes = (size_t)new_bytes32;

  if (ptr == NULL)
    {
      return i_realloc (NULL, new_nelem, size, e);
    }

  if (new_bytes < old_bytes)
    {
      // keep the last new_bytes bytes; move them to start
      const size_t keep = new_bytes;
      const size_t shift = old_bytes - keep;
      memmove (ptr, (char *)ptr + shift, keep);
      return i_realloc (ptr, new_nelem, size, e);
    }
  else
    {
      // prepend = new space on the left
      const size_t prepend = new_bytes - old_bytes;

      void *ret = i_realloc (ptr, new_nelem, size, e);
      if (ret == NULL)
        {
          return NULL;
        }

      if (old_bytes > 0)
        {
          memmove ((char *)ret + prepend, ret, old_bytes);
        }
      return ret;
    }
}

#ifndef NTEST
TEST (TT_UNIT, i_realloc_left)
{
  error e = error_create ();
  u32 *a = i_malloc (10, sizeof *a, &e);
  for (u32 i = 0; i < 10; i++)
    {
      a[i] = i;
    }

  // grow-left: old payload should appear starting at index 10
  a = i_realloc_left (a, 10, 20, sizeof *a, &e);
  for (u32 i = 0; i < 10; i++)
    {
      test_assert (a[10 + i] == i);
    }

  // shrink-left: keep the *last* 10 elements moved to start
  a = i_realloc_left (a, 20, 10, sizeof *a, &e);
  for (u32 i = 0; i < 10; i++)
    {
      test_assert (a[i] == i); // after previous state, kept tail
                               // [10..19] which were [0..9]
    }
  i_free (a);
}
#endif

// =======================================================
// C-Right: zero-fill new tail
// =======================================================
void *
i_crealloc_right (void *ptr, const u32 old_nelem, const u32 new_nelem, const u32 size, error *e)
{
  ASSERT (size > 0);
  if (ptr == NULL)
    {
      ASSERT (old_nelem == 0);
    }
  ASSERT (new_nelem > 0);

  u32 old_bytes32 = 0, new_bytes32 = 0;
  {
    bool ok_old = safe_mul_u32 (&old_bytes32, old_nelem, size);
    ASSERT (ok_old);
    bool ok_new = safe_mul_u32 (&new_bytes32, new_nelem, size);
    ASSERT (ok_new);
    if (!ok_old || !ok_new)
      {
        error_causef (e, ERR_NOMEM, "crealloc_right: overflow");
        return NULL;
      }
  }

  const size_t old_bytes = (size_t)old_bytes32;
  const size_t new_bytes = (size_t)new_bytes32;

  void *ret = i_realloc (ptr, new_nelem, size, e);
  if (ret == NULL)
    {
      return NULL;
    }

  if (new_bytes > old_bytes)
    {
      memset ((char *)ret + old_bytes, 0, new_bytes - old_bytes);
    }
  return ret;
}

#ifndef NTEST
TEST (TT_UNIT, i_crealloc_right)
{
  error e = error_create ();
  u32 *a = i_malloc (10, sizeof *a, &e);
  for (u32 i = 0; i < 10; i++)
    {
      a[i] = i;
    }

  a = i_crealloc_right (a, 10, 20, sizeof *a, &e);
  for (u32 i = 0; i < 10; i++)
    {
      test_assert (a[i] == i);
    }
  for (u32 i = 10; i < 20; i++)
    {
      test_assert (a[i] == 0);
    }

  // shrink keeps prefix
  a = i_crealloc_right (a, 20, 10, sizeof *a, &e);
  for (u32 i = 0; i < 10; i++)
    {
      test_assert (a[i] == i);
    }
  i_free (a);
}
#endif

// =======================================================
// C-Left: zero-fill new head; shrink drops head
// =======================================================
void *
i_crealloc_left (void *ptr, const u32 old_nelem, const u32 new_nelem, const u32 size, error *e)
{
  ASSERT (size > 0);
  if (ptr == NULL)
    {
      ASSERT (old_nelem == 0);
    }
  ASSERT (new_nelem > 0);

  if (old_nelem == new_nelem)
    {
      return ptr;
    }

  u32 old_bytes32 = 0, new_bytes32 = 0;
  {
    bool ok_old = safe_mul_u32 (&old_bytes32, old_nelem, size);
    ASSERT (ok_old);
    bool ok_new = safe_mul_u32 (&new_bytes32, new_nelem, size);
    ASSERT (ok_new);
    if (!ok_old || !ok_new)
      {
        error_causef (e, ERR_NOMEM, "crealloc_left: overflow");
        return NULL;
      }
  }

  const size_t old_bytes = (size_t)old_bytes32;
  const size_t new_bytes = (size_t)new_bytes32;

  if (ptr == NULL)
    {
      void *ret = i_realloc (NULL, new_nelem, size, e);
      if (ret != NULL)
        {
          memset (ret, 0, new_bytes);
        }
      return ret;
    }

  if (new_bytes < old_bytes)
    {
      const size_t keep = new_bytes;
      const size_t shift = old_bytes - keep;
      memmove (ptr, (char *)ptr + shift, keep);
      return i_realloc (ptr, new_nelem, size, e);
    }
  else
    {
      const size_t prepend = new_bytes - old_bytes;

      void *ret = i_realloc (ptr, new_nelem, size, e);
      if (ret == NULL)
        {
          return NULL;
        }

      if (old_bytes > 0)
        {
          memmove ((char *)ret + prepend, ret, old_bytes);
        }
      memset (ret, 0, prepend);
      return ret;
    }
}

#ifndef NTEST
TEST (TT_UNIT, i_crealloc_left)
{
  error e = error_create ();
  u32 *a = i_malloc (10, sizeof *a, &e);
  for (u32 i = 0; i < 10; i++)
    {
      a[i] = i;
    }

  // grow-left: zeros in new head; old payload shifted right
  a = i_crealloc_left (a, 10, 20, sizeof *a, &e);
  for (u32 i = 0; i < 10; i++)
    {
      test_assert (a[i] == 0);
    }
  for (u32 i = 0; i < 10; i++)
    {
      test_assert (a[10 + i] == i);
    }

  // shrink-left: drop head, keep last 10 at start
  a = i_crealloc_left (a, 20, 10, sizeof *a, &e);
  for (u32 i = 0; i < 10; i++)
    {
      test_assert (a[i] == i);
    }
  i_free (a);
}
#endif

void
i_free (void *ptr)
{
  ASSERT (ptr);
  free (ptr);
}
