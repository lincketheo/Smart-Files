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

#include "core/error.h"
#include "intf/logging.h"
#include "paging/wal/wal.h"
#include "paging/wal/wal_rec_hdr.h"

#include <stdio.h>
#include <stdlib.h>

static void
walf_print (const char *fname)
{
  error e = error_create ();

  struct wal *wf = wal_open (fname, &e);

  if (wf == NULL)
    {
      error_log_consume (&e);
      return;
    }

  while (true)
    {
      lsn rlsn;
      struct wal_rec_hdr_read *out = wal_read_next (wf, &rlsn, &e);

      if (out == NULL)
        {
          error_log_consume (&e);
          goto theend;
        }

      i_print_wal_rec_hdr_read_light (LOG_INFO, out, rlsn);

      if (out->type == WL_EOF)
        {
          break;
        }
    }

theend:
  wal_close (wf, &e);
}

int
main (const int argc, char **argv)
{
  if (argc != 2)
    {
      printf ("USAGE: walfprint FNAME\n");
      return -1;
    }

  char *fname = argv[1];

  walf_print (fname);

  return 0;
}
