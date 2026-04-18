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

#include "c_specx/dev/error.h"
#include "pager.h"
#include "pager/page_h.h"
#include "pages/page.h"
#include "smfile.h"

#include <stdio.h>
#include <stdlib.h>

static void
dl_contents_one_page (FILE *out, struct pager *p, const page_h *cur, error *e)
{
  fprintf (stderr, "============================= %" PRpgno "\n",
           page_h_pgno (cur));
  fprintf (stderr, "DATA_LIST\n");
  fprintf (stderr, "next: %" PRpgno "\n", dl_get_next (page_h_ro (cur)));
  fprintf (stderr, "prev: %" PRpgno "\n", dl_get_prev (page_h_ro (cur)));
  fprintf (stderr, "blen: %" PRp_size "\n", dl_used (page_h_ro (cur)));

  u8 output[DL_DATA_SIZE];
  const p_size read = dl_read (page_h_ro (cur), output, 0, DL_DATA_SIZE);

  size_t total = 0;
  while (total < read)
    {
      const size_t written = fwrite (output + total, 1, read - total, out);
      if (written == 0)
        {
          if (ferror (out))
            {
              perror ("fwrite");
              break;
            }
          break;
        }
      total += written;
    }

  fprintf (stderr, "Wrote: %zu\n", total);
}

static void
dl_contents (FILE *out, const char *fname, const pgno pg)
{
  error e = error_create ();
  page_h next = page_h_create ();

  struct pager *p = pgr_open_single_file (fname, &e);
  if (p == NULL)
    {
      error_log_consume (&e);
      return;
    }

  page_h cur = page_h_create ();
  if (pgr_get (&cur, PG_DATA_LIST | PG_INNER_NODE, pg, p, &e))
    {
      error_log_consume (&e);
      return;
    }

  while (true)
    {
      if (cur.mode == PHM_NONE)
        {
          pgr_close (p, &e);
          return;
        }

      dl_contents_one_page (out, p, &cur, &e);

      const pgno npg = dlgt_get_next (page_h_ro (&cur));
      const enum page_type type = page_get_type (page_h_ro (&cur));

      if (pgr_release (p, &cur, type, &e))
        {
          error_log_consume (&e);
          return;
        }

      if (npg != PGNO_NULL)
        {
          if (pgr_get (&next, type, npg, p, &e))
            {
              error_log_consume (&e);
              return;
            }
        }
    }
}

int
main (const int argc, char **argv)
{
  if (argc != 3)
    {
      printf ("USAGE: dlread FNAME PGNO\n");
      return -1;
    }

  int pg = atoi (argv[2]);
  char *fname = argv[1];

  dl_contents (stdout, fname, pg);

  return 0;
}
