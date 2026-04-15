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

#include "numstore.h"
#include "tlclib_dev.h"

#include <string.h>

int
main (void)
{
  // --- Setup ---
  u8 src[20000];
  u8 buf_a[200];
  u8 buf_b[200];
  error e = error_create ();
  nsdb_t *db = ns_open ("test", &e);

  // Fill source: src[i] = i % 256
  for (int i = 0; i < 20000; ++i)
    {
      src[i] = (u8)i;
    }

  // Two independent variables, each allocated on first insert
  pgno root_a = PGNO_NULL;
  pgno root_b = PGNO_NULL;

  // --- Populate root_a ---
  // Insert the first 10 bytes at offset 0, then splice 10 more in at offset 5
  {
    txn_t *tx = ns_begin_txn (db, &e);
    ns_insert (db, tx, &root_a, src, 10, 0, &e);      // append: 0 1 2 3 4 5 6 7 8 9
    ns_insert (db, tx, &root_a, src + 10, 10, 5, &e); // splice at offset 5

    // root_a: 0 1 2 3 4 | 10 11 12 13 14 15 16 17 18 19 | 5 6 7 8 9
    ns_commit (db, tx, &e);
  }

  // --- Populate root_b ---
  {
    txn_t *tx = ns_begin_txn (db, &e);
    ns_insert (db, tx, &root_b, src + 50, 20, 0, &e); // src[50..69] at offset 0
    // root_b: 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64 65 66 67 68 69
    ns_commit (db, tx, &e);
  }

  // --- Strided read from root_a ---
  {
    txn_t *tx = ns_begin_txn (db, &e);
    ns_read (db, tx, root_a, buf_a, 1, 0, 2, 5, &e); // read[offset=0, stride=2, nelem=5]
    // Picks root_a[0, 2, 4, 6, 8] = 0, 2, 4, 12, 14
    ns_commit (db, tx, &e);

    printf ("root_a strided read (every other element): ");
    for (int i = 0; i < 5; ++i)
      printf ("%d ", buf_a[i]);
    printf ("\n");
    // Expected: 0 2 4 12 14
  }

  // --- Cross-variable strided write: copy root_a even indices into root_b ---
  // buf_a still holds { 0, 2, 4, 12, 14 } from the read above
  {
    txn_t *tx = ns_begin_txn (db, &e);
    ns_write (db, tx, root_b, buf_a, 1, 0, 2, 5, &e); // write[offset=0, stride=2, nelem=5]
    // Patches root_b[0, 2, 4, 6, 8] with { 0, 2, 4, 12, 14 }
    // root_b: 0 51 2 53 4 55 12 57 14 59 60 61 62 63 64 65 66 67 68 69
    ns_commit (db, tx, &e);
  }

  // --- Strided remove from root_a ---
  {
    txn_t *tx = ns_begin_txn (db, &e);
    ns_remove (db, tx, &root_a, NULL, 1, 1, 2, 3, &e); // remove[offset=1, stride=2, nelem=3]
    // Removes root_a[1, 3, 5] = { 1, 3, 10 }
    // root_a: 0 2 4 | 11 12 13 14 15 16 17 18 19 | 5 6 7 8 9
    ns_commit (db, tx, &e);
  }

  // --- Read root_a after strided remove ---
  {
    txn_t *tx = ns_begin_txn (db, &e);
    ns_read (db, tx, root_a, buf_a, 1, 0, 1, 10, &e); // read[offset=0, stride=1, nelem=10]
    ns_commit (db, tx, &e);

    printf ("root_a after strided remove (first 10): ");
    for (int i = 0; i < 10; ++i)
      printf ("%d ", buf_a[i]);
    printf ("\n");
    // Expected: 0 2 4 11 12 13 14 15 16 17
  }

  // --- Overwrite a contiguous sentinel range in root_b ---
  {
    memset (buf_b, 0xFF, 5);
    txn_t *tx = ns_begin_txn (db, &e);
    ns_write (db, tx, root_b, buf_b, 1, 5, 1, 5, &e); // write[offset=5, stride=1, nelem=5]
    // root_b[5..9] = { 255 255 255 255 255 }
    // root_b: 0 51 2 53 4 255 255 255 255 255 60 61 62 63 64 ...
    ns_commit (db, tx, &e);
  }

  // --- Final verification: read back both roots ---
  {
    txn_t *tx = ns_begin_txn (db, &e);
    ns_read (db, tx, root_a, buf_a, 1, 0, 1, 15, &e); // read[offset=0, stride=1, nelem=15]
    ns_read (db, tx, root_b, buf_b, 1, 0, 1, 15, &e); // read[offset=0, stride=1, nelem=15]
    ns_commit (db, tx, &e);

    printf ("root_a final: ");
    for (int i = 0; i < 15; ++i)
      printf ("%d ", buf_a[i]);
    printf ("\n");
    // Expected: 0 2 4 11 12 13 14 15 16 17 18 19 5 6 7

    printf ("root_b final: ");
    for (int i = 0; i < 15; ++i)
      printf ("%d ", buf_b[i]);
    printf ("\n");
    // Expected: 0 51 2 53 4 255 255 255 255 255 60 61 62 63 64
  }

  return ns_close (db, &e);
}
