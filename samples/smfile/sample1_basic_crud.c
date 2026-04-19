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

int
main (void)
{
  smfile_t *smf = smfile_open ("sample1_crud");
  if (!smf)
    {
      fprintf (stderr, "smfile_open failed\n");
      return 1;
    }

  b_size total = 0;

  // Remove all data in this file to start from scratch
  sb_size removed = smfile_remove (smf, NULL, 0, SMF_END);
  if (removed < 0)
    {
      smfile_perror (smf, "remove");
      return 1;
    }

  // We'll insert some data into the database
  const char *initial = "The quick brown fox jumps over the lazy dog";
  sb_size written = smfile_insert (smf, initial, 0, strlen (initial));
  if (written < 0)
    {
      smfile_perror (smf, "insert initial");
      return 1;
    }
  total += written;

  // Then insert really between "the" and "lazy" (index 34)
  const char *adverb = " really";
  written = smfile_insert (smf, adverb, 34, strlen (adverb));
  if (written < 0)
    {
      smfile_perror (smf, "insert adverb");
      return 1;
    }
  total += written;

  // Do a read of all the data - you could call smfile_size - but we already know the size
  char *buf = malloc (total + 1);
  sb_size n = smfile_read (smf, buf, 0, SMF_END);
  if (n < 0)
    {
      smfile_perror (smf, "read");
      return 1;
    }
  buf[n] = '\0';
  printf ("after insert:  \"%s\"\n", buf);

  // "The quick brown fox jumps over the really lazy dog"

  // Replace "fox" with "cat"
  if (smfile_write (smf, "cat", 16, 3) < 0)
    {
      smfile_perror (smf, "write");
      return 1;
    }

  // Do a read
  n = smfile_read (smf, buf, 0, total);
  if (n < 0)
    {
      smfile_perror (smf, "read");
    }
  buf[n] = '\0';
  printf ("after write:   \"%s\"\n", buf);
  // "The quick brown cat jumps over the really lazy dog"

  // Remove "really" - you can pass a buffer to capture the data you removed
  // otherwise, you'd just set dest to NULL
  char evicted[8];
  n = smfile_remove (smf, evicted, 34, 7);
  if (n < 0)
    {
      smfile_perror (smf, "remove");
      return 1;
    }
  total -= n;
  evicted[n] = '\0';
  printf ("removed:       \"%s\"\n", evicted);
  // " really"

  // One last read
  n = smfile_read (smf, buf, 0, total);
  if (n < 0)
    {
      smfile_perror (smf, "read");
    }
  buf[n] = '\0';
  printf ("after remove:  \"%s\"\n", buf);
  // "The quick brown cat jumps over the lazy dog"

  free (buf);

  return smfile_close (smf);
}
