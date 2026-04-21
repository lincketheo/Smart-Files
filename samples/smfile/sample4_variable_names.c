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
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

/**
 * This example shows how to write data to different named "variables" inside a single file.
 */
int
main (void)
{
  smfile_cleanup ("sample4_named");
  smfile_t *smf = smfile_open ("sample4_named");

  // Write independent streams into separate named variables
  uint8_t temps[8] = { 20, 21, 22, 23, 24, 25, 26, 27 };
  uint8_t humidity[8] = { 55, 56, 57, 58, 59, 60, 61, 62 };
  uint8_t pressure[8] = { 10, 11, 12, 13, 14, 15, 16, 17 };

  smfile_pinsert (smf, "temps", temps, 0, sizeof (temps));
  smfile_pinsert (smf, "humidity", humidity, 0, sizeof (humidity));
  smfile_pinsert (smf, "pressure", pressure, 0, sizeof (pressure));

  // Append more data to each independently (-1 means the end of the array)
  uint8_t more_temps[4] = { 28, 29, 30, 31 };
  uint8_t more_humidity[4] = { 63, 64, 65, 66 };
  smfile_pinsert (smf, "temps", more_temps, -1, sizeof (more_temps));
  smfile_pinsert (smf, "humidity", more_humidity, -1, sizeof (more_humidity));

  // Overwrite a value in temps only — humidity and pressure untouched
  uint8_t correction = 99;
  smfile_pwrite (smf, "temps", &correction, 1, 4, 1, 1);

  // Read them all back
  uint8_t buf[16];
  sb_size n;

  n = smfile_pread (smf, "temps", buf, 1, 0, 1, 12);
  printf ("temps    (%lld): ", (long long)n);
  for (int i = 0; i < (int)n; ++i)
    {
      printf ("%3d", buf[i]);
    }
  printf ("\n");

  n = smfile_pread (smf, "humidity", buf, 1, 0, 1, 12);
  printf ("humidity (%lld): ", (long long)n);
  for (int i = 0; i < (int)n; ++i)
    {
      printf ("%3d", buf[i]);
    }
  printf ("\n");

  n = smfile_pread (smf, "pressure", buf, 1, 0, 1, 8);
  printf ("pressure (%lld): ", (long long)n);
  for (int i = 0; i < (int)n; ++i)
    {
      printf ("%3d", buf[i]);
    }
  printf ("\n");

  // Remove first two entries from pressure only
  uint8_t evicted[2];
  n = smfile_premove (smf, "pressure", evicted, 1, 0, 1, 2);
  printf ("removed from pressure: %d %d\n", evicted[0], evicted[1]);

  n = smfile_pread (smf, "pressure", buf, 1, 0, 1, 8);
  printf ("pressure after remove (%lld): ", (long long)n);
  for (int i = 0; i < (int)n; ++i)
    {
      printf ("%3d", buf[i]);
    }
  printf ("\n");

  return smfile_close (smf);
}
