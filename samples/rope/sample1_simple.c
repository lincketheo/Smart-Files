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

int
main (void)
{
  // --- Setup ---
  u8 data[20000];
  u8 read_data[100];
  error e = error_create ();
  nsdb_t *db = ns_open ("test", &e);

  // Fill source buffer: data[i] = i % 256
  for (int i = 0; i < 20000; ++i)
    {
      data[i] = (u8)i;
    }

  // Single variable, allocated on first insert
  pgno root = PGNO_NULL;

  // --- Insert ---
  // Insert the full buffer at offset 0, then splice the same buffer in at offset 3
  {
    txn_t *tx = ns_begin_txn (db, &e);
    ns_insert (db, tx, &root, data, sizeof (data), 0, &e); // append: 0 1 2 3 4 5 ...
    ns_insert (db, tx, &root, data, sizeof (data), 3, &e); // splice at offset 3
                                                           //
    // root: 0 1 2 | 0 1 2 3 4 5 6 7 ... | 3 4 5 ...
    ns_commit (db, tx, &e);
  }

  // --- Read ---
  // Strided read: pick every other element starting at offset 1
  {
    txn_t *tx = ns_begin_txn (db, &e);
    ns_read (db, tx, root, read_data, 1, 2, 2, 100, &e); // read[offset=1, stride=2, nelem=100]
    // Picks root[1, 3, 5, 7, ...] = 1, 0, 2, 4, 6, ...
    ns_commit (db, tx, &e);

    printf ("strided read (every other element from offset 1): ");
    for (int i = 0; i < 100; ++i)
      {
        printf ("%d ", read_data[i]);
      }
    printf ("\n");
    // Expected: 1 0 2 4 6 8 ...
  }

  // --- Remove ---
  // Remove every other element starting at offset 1, collapsing the gaps in place
  {
    txn_t *tx = ns_begin_txn (db, &e);
    ns_remove (db, tx, &root, NULL, 1, 1, 2, 10, &e); // remove[offset=1, stride=2, nelem=10]

    // Removes root[1, 3, 5, 7, 9, 11, 13, 15, 17, 19]
    // root: 0 0 2 4 6 8 ...
    ns_commit (db, tx, &e);
  }

  // --- Read after remove ---
  {
    txn_t *tx = ns_begin_txn (db, &e);
    ns_read (db, tx, root, read_data, 1, 2, 2, 100, &e); // read[offset=1, stride=2, nelem=100]
    // Same stride pattern, but gaps are now gone so the sequence has shifted
    ns_commit (db, tx, &e);

    printf ("strided read after remove: ");
    for (int i = 0; i < 100; ++i)
      {
        printf ("%d ", read_data[i]);
      }
    printf ("\n");
    // Expected: 0 2 4 6 8 ...
  }

  // --- Write ---
  // Overwrite every other element starting at offset 1 with a fresh 0..99 sequence
  {
    for (int i = 0; i < 100; ++i)
      {
        read_data[i] = (u8)i;
      }

    txn_t *tx = ns_begin_txn (db, &e);
    ns_write (db, tx, root, read_data, 1, 2, 2, 100, &e); // write[offset=1, stride=2, nelem=100]
    // Patches root[1, 3, 5, 7, ...] with { 0, 1, 2, 3, ... }
    // root: 0 0 0 1 0 2 0 3 ...
    ns_commit (db, tx, &e);
  }

  // --- Read after write ---
  {
    txn_t *tx = ns_begin_txn (db, &e);
    ns_read (db, tx, root, read_data, 1, 2, 2, 100, &e); // read[offset=1, stride=2, nelem=100]
    // Should read back exactly what was written
    ns_commit (db, tx, &e);

    printf ("strided read after write: ");
    for (int i = 0; i < 100; ++i)
      {
        printf ("%d ", read_data[i]);
      }
    printf ("\n");
    // Expected: 0 1 2 3 4 5 ...
  }

  return ns_close (db, &e);
}
