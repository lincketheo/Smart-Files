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

#include "core/math.h"
#include "intf/logging.h"
#include "paging/os_pager/file_pager.h"
#include "paging/pager.h"
#include "paging/pages/page.h"
#include "paging/pages/root_node.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void
_simple_page_print (struct file_pager *p, const pgno pg, error *e)
{
  page raw;
  raw.pg = pg;

  if (fpgr_read (p, raw.raw, pg, e))
    {
      error_log_consume (e);
      return;
    }

  i_log_page (LOG_INFO, &raw);
}

static void
simple_page_print (const char *fname)
{
  error e = error_create ();

  struct file_pager *fp = fpgr_open (fname, &e);

  for (u32 i = 0; i < fpgr_get_npages (fp); ++i)
    {
      _simple_page_print (fp, i, &e);
    }

  fpgr_close (fp, &e);
}

int
main (const int argc, char **argv)
{
  if (argc != 2)
    {
      printf ("USAGE: simple_nspprint FNAME\n");
      return -1;
    }

  char *fname = argv[1];

  simple_page_print (fname);

  return 0;
}
