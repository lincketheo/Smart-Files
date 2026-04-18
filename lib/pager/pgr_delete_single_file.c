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

#include "c_specx.h"
#include "c_specx_dev.h"
#include "pager.h"

#include <limits.h>

#ifndef PATH_MAX
#define PATH_MAX 260
#endif

err_t
pgr_delete_single_file (const char *dbname, error *e)
{
  char fname[PATH_MAX];
  char walname[PATH_MAX];
  snprintf (fname, sizeof fname, "%s.db", dbname);
  snprintf (walname, sizeof walname, "%s.wal", dbname);

  i_remove_quiet (fname, e);
  i_remove_quiet (walname, e);

  return error_trace (e);
}
