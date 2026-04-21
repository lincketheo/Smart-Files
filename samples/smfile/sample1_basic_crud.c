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

/**
 * This example shows basic first class operations of smart files
 * Namely:
 *
 * 1. Insert (insert data into the middle of an array)
 * 2. Read (normal read)
 * 3. Write (overwrite data in the middle of the array)
 * 4. Remove (remove chunks of data from the middle of an array)
 */
int
main (void)
{
  // Open a new data file
  smfile_t *smf = smfile_open ("sample1_crud");

  // Remove all data from the file
  smfile_remove (
      smf,
      NULL,   // You can optionally supply a destination for the data - we'll just pass NULL
      0,      // The starting offset to remove
      SMF_END // This let's you remove all the data
  );

  // Insert out initial sentence
  const char *initial = "The quick brown fox jumps over the lazy dog";
  smfile_insert (
      smf,
      initial,         // The data we want to write
      0,               // The starting offset to write to
      strlen (initial) // The length of the data we're writing
  );

  // Inserting in the middle is a first class operation
  const char *adverb = " really";
  smfile_insert (
      smf,
      adverb,
      34, // Inserting in the middle is a first class operation
      strlen (adverb));

  // Read the entire array
  char buf[64];
  sb_size n = smfile_read (smf, buf, 0, SMF_END);
  buf[n] = '\0';
  printf ("after insert:  \"%s\"\n", buf);

  // Writing in the middle is a first class operation
  smfile_write (smf, "cat", 16, 3);

  // Read the entire array
  n = smfile_read (smf, buf, 0, SMF_END);
  buf[n] = '\0';
  printf ("after write:   \"%s\"\n", buf);

  // Removing in the middle of the array is first class
  char evicted[8];
  n = smfile_remove (smf, evicted, 34, 7);
  evicted[n] = '\0';
  printf ("removed:       \"%s\"\n", evicted);

  // Read the result
  n = smfile_read (smf, buf, 0, SMF_END);
  buf[n] = '\0';
  printf ("after remove:  \"%s\"\n", buf);

  return smfile_close (smf);
}
