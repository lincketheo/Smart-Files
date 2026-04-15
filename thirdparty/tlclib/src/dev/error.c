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

#include "tlclib/dev/error.h"

#include "tlclib/core/macros.h"
#include "tlclib/dev/assert.h"
#include "tlclib/intf/logging.h"

#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

#ifdef ERR_T_FAIL_FAST
#endif

DEFINE_DBG_ASSERT (error, error, e, {
  ASSERT (e);
  if (e->cause_code != SUCCESS)
    {
      ASSERT (e->cmlen > 0);
    }
})

error
error_create (void)
{
  const error ret = {
    .cause_code = SUCCESS,
    .cmlen = 0,
  };

  DBG_ASSERT (error, &ret);

  return ret;
}

err_t
error_causef (error *e, const err_t c, const char *fmt, ...)
{
  if (e == NULL)
    {
      return c;
    }

  ASSERT (fmt);

  va_list ap;
  va_start (ap, fmt);

  char tmpbuf[256];
  va_list ap_copy;
  va_copy (ap_copy, ap);
  u32 cmlen = vsnprintf (tmpbuf, sizeof (tmpbuf), fmt, ap_copy);
  cmlen = MIN (cmlen, 255);
  va_end (ap_copy);

  if (e->cause_code != SUCCESS)
    {
      i_log_error ("TRACE: %s\n", tmpbuf);
    }

  if (e->cause_code == SUCCESS)
    {
      memcpy (e->cause_msg, tmpbuf, cmlen);
      e->cause_code = c;
      e->cause_msg[cmlen] = '\0';
      e->cmlen = cmlen;

      i_log_error ("%.*s\n", e->cmlen, e->cause_msg);
    }

  va_end (ap);

  return c;
}

err_t
error_change_causef_from (error *e, const err_t from, const err_t to,
                          const char *fmt, ...)
{
  ASSERT (fmt);

  if (e)
    {
      DBG_ASSERT (error, e);
      if (e->cause_code == from)
        {
          ASSERT (e->cause_code == from);
          e->cause_code = to;

          // Print into cause message
          // (255 because of null terminator)
          va_list ap;
          va_start (ap, fmt);

          va_list ap_copy;
          va_copy (ap_copy, ap);
          const u32 cmlen = vsnprintf (e->cause_msg, 256, fmt, ap_copy);
          e->cmlen = MIN (cmlen, 255);
          va_end (ap_copy);
          va_end (ap);
        }

      i_log_error ("%.*s\n", e->cmlen, e->cause_msg);
    }

  return error_trace (e);
}

err_t
error_change_causef (error *e, const err_t c, const char *fmt, ...)
{
  ASSERT (fmt);
  if (e)
    {
      DBG_ASSERT (error, e);
      ASSERT (e->cause_code != SUCCESS); // Can only go from good -> cause
      e->cause_code = c;

      // Print into cause message
      // (255 because of null terminator)
      va_list ap;
      va_start (ap, fmt);

      va_list ap_copy;
      va_copy (ap_copy, ap);
      const u32 cmlen = vsnprintf (e->cause_msg, 256, fmt, ap_copy);
      e->cmlen = MIN (cmlen, 255);
      va_end (ap_copy);
      va_end (ap);
    }

  i_log_error ("%.*s\n", e->cmlen, e->cause_msg);

  return c;
}

void
error_log_consume (error *e)
{
  DBG_ASSERT (error, e);
  ASSERT (e->cause_code != SUCCESS);
  i_log_error ("%.*s\n", e->cmlen, e->cause_msg);
  e->cmlen = 0;
  e->cause_code = SUCCESS;
}

bool
error_equal (const error *left, const error *right)
{
  DBG_ASSERT (error, left);
  DBG_ASSERT (error, right);
  if (left->cause_code != right->cause_code)
    {
      return false;
    }

  if (left->cause_code)
    {
      if (left->cmlen != right->cmlen)
        {
          return false;
        }
      if (strncmp (left->cause_msg, right->cause_msg, left->cmlen) != 0)
        {
          return false;
        }
    }

  return true;
}

// Consuming errors (these both reset the error)
void
err_t_perror (FILE *output, error *e)
{
  fprintf (output, "%.*s", e->cmlen, e->cause_msg);
  e->cause_code = SUCCESS;
  e->cmlen = 0;
}

void
error_fatal (const char *fmt, ...)
{
  ASSERT (fmt);
  // Print into cause message
  // (255 because of null terminator)
  va_list ap;
  va_start (ap, fmt);

  va_list ap_copy;
  va_copy (ap_copy, ap);
  vfprintf (stderr, fmt, ap_copy);
  va_end (ap_copy);
  va_end (ap);

  panic ("Unreachable error!");
  abort (); // Guarantee noreturn when NDEBUG disables panic
}
