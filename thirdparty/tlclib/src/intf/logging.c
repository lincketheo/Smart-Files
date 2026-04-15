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

#include "tlclib/intf/logging.h"

#include <stdarg.h>
#include <stdio.h>

////////////////////////////////////////////////////////////
// LOGGING
void
i_log_internal (const char *prefix, const char *color, const char *fmt,
                ...)
{
  va_list args;
  va_start (args, fmt);
  fprintf (stderr, "%s[%-8.8s]: ", color, prefix);
  vfprintf (stderr, fmt, args);
  fprintf (stderr, "%s", RESET);
  va_end (args);
}

void
i_log_flush (void)
{
  fflush (stderr);
}
