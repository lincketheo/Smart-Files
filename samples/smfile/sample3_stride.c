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

/**
 * Showing off the power user functionality of strides.
 *
 * Strides allow for skipping elements. Skipping data is a first class
 * support.
 *
 * Also, the simple api doesn't expose the fact that you can actually
 * read / write data from different "variables". You can specify which data
 * source in the file you're writing to - and each one is distinct.
 *
 * This example just inserts everything into the "floats" data source.
 * See sample4 for more usage
 */
int
main (void)
{
  smfile_t *smf = smfile_open ("sample4_stride");

  // Remove all the data (this might error - that's ok - just says there's no dataset with that name)
  smfile_premove (smf, "floats", NULL, 1, 0, 1, SMF_END);

  // Insert some data
  float data[16];
  for (int i = 0; i < 16; ++i)
    {
      data[i] = (float)i;
    }
  smfile_pinsert (smf, "floats", data, 0, sizeof (data));

  // Read just the even numbers (stride = 2)
  float evens[8];
  sb_size n = smfile_pread (smf, "floats", evens, sizeof (float), 0, 2, 8);
  printf ("every other float (expect 0 2 4 6 8 10 12 14):\n");
  for (int i = 0; i < (int)n; ++i)
    {
      printf ("  [%d] = %.1f\n", i, evens[i]);
    }

  // Write negative elements starting at byte offset 4 (1 float in) - with stride 2 and 8 total elements
  float neg[8];
  for (int i = 0; i < 8; ++i)
    {
      neg[i] = -1.0f;
    }
  smfile_pwrite (smf, "floats", neg, sizeof (float), 4, 2, 8);

  // Read back 16 elements - stride = 1
  float readback[16];
  n = smfile_pread (smf, "floats", readback, sizeof (float), 0, 1, 16);
  printf ("after stride write (odd positions -> -1):\n");
  for (int i = 0; i < (int)n; ++i)
    {
      printf ("  [%d] = %.1f\n", i, readback[i]);
    }

  // Remove 8 elements - stride = 2
  float removed[8];
  n = smfile_premove (smf, "floats", removed, sizeof (float), 0, 2, 8);
  printf ("removed even positions (expect 0 2 4 6 8 10 12 14):\n");
  for (int i = 0; i < (int)n; ++i)
    {
      printf ("  [%d] = %.1f\n", i, removed[i]);
    }

  // Read data
  n = smfile_pread (smf, "floats", readback, sizeof (float), 0, 1, 8);
  printf ("remaining floats (expect all -1):\n");
  for (int i = 0; i < (int)n; ++i)
    {
      printf ("  [%d] = %.1f\n", i, readback[i]);
    }

  return smfile_close (smf);
}
