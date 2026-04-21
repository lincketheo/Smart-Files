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

#include "smfile.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>

/**
 * This demo shows the basics of transactions
 * in smart files. In general, when you don't specify
 * begin / commit / rollback, operations are auto committed,
 * e.g. they either happen or they don't. If you specify begin / commit
 * you can group various opperations together.
 */
int
main (void)
{
  sb_size n;

  smfile_cleanup ("sample2_txn");

  // Open a new smart file
  smfile_t *smf = smfile_open ("sample2_txn");

  // Start from scratch (remove all data)
  smfile_remove (smf, NULL, 0, SMF_END);

  // Do 1 full committed transaction
  {
    smfile_begin (smf);

    uint8_t header[8];
    uint8_t body[64];
    uint8_t footer[8];

    memset (header, 1, sizeof (header));
    for (int i = 0; i < 64; ++i)
      {
        body[i] = (uint8_t)i;
      }
    memset (footer, 99, sizeof (footer));

    // Three inserts in a row
    smfile_insert (smf, header, 0, sizeof (header));  // [0..7]   = 1
    smfile_insert (smf, body, 8, sizeof (body));      // [8..71]  = 0...64
    smfile_insert (smf, footer, 72, sizeof (footer)); // [72..79] = 99

    smfile_commit (smf);
  }

  // Do 1 full roll'ed back transaction
  {
    smfile_begin (smf);

    uint8_t zeros[80];
    memset (zeros, 0, sizeof (zeros));
    smfile_write (smf, zeros, 0, sizeof (zeros)); // overwrite everything with 0x00

    smfile_rollback (smf);
  }

  // Do a read of the roll'ed back data
  {
    uint8_t verify[12];
    n = smfile_read (smf, verify, 68, 12);

    printf ("bytes [68..79] after rollback:\n");
    for (sb_size i = 0; i < n; ++i)
      {
        printf ("  [%Id] = %d\n", 68 + i, verify[i]);
      }
  }

  // A committed transaction
  {
    smfile_begin (smf);

    uint8_t extra[4];
    memset (extra, 0xCC, sizeof (extra));
    smfile_insert (smf, extra, 80, sizeof (extra)); // append 4 bytes of 0xcc

    smfile_commit (smf);
  }

  // A roll'ed back transaction
  {
    smfile_begin (smf);
    smfile_remove (smf, NULL, 80, 4); // attempt to remove what we just appended
    smfile_rollback (smf);
  }

  // Final read
  {
    uint8_t tail[4];
    n = smfile_read (smf, tail, 80, 4);

    printf ("bytes [80..83]: ");
    for (sb_size i = 0; i < n; ++i)
      {
        printf ("%d ", tail[i]);
      }
    printf ("\n");
  }

  return smfile_close (smf);
}
