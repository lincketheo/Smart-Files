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

#include <stdio.h>

int
main (void)
{
  error e = error_create ();
  pgno root = PGNO_NULL;

  // Phase 1: Create the stream and insert 50000 elements [0 .. 255] repeating.
  {
    nsdb_t *db = ns_open ("insert_demo", &e);
    txn_t *tx = ns_begin_txn (db, &e);

    u8 data[50000];
    for (int i = 0; i < 50000; ++i)
      {
        data[i] = (u8)i;
      }

    ns_insert (db, tx, &root, data, 50000, 0, &e);

    ns_commit (db, tx, &e);
    ns_close (db, &e);
  }

  // Phase 2: Splice 10 bytes of 111 at offset 10000, commit normally.
  {
    nsdb_t *db = ns_open ("insert_demo", &e);
    txn_t *tx = ns_begin_txn (db, &e);

    u8 patch[10];
    for (int i = 0; i < 10; ++i)
      {
        patch[i] = 111;
      }

    ns_insert (db, tx, &root, patch, 10, 10000, &e);
    // stream[10000..10009] = { 111 x10 }, everything after shifts right

    ns_commit (db, tx, &e);
    ns_close (db, &e);
  }

  // Phase 3: Splice 10 bytes of 222 at offset 20000, commit, then crash.
  {
    nsdb_t *db = ns_open ("insert_demo", &e);
    txn_t *tx = ns_begin_txn (db, &e);

    u8 patch[10];
    for (int i = 0; i < 10; ++i)
      {
        patch[i] = 222;
      }

    ns_insert (db, tx, &root, patch, 10, 20000, &e);

    ns_commit (db, tx, &e);
    ns_crash (db, &e);
  }

  // Phase 4: Splice 10 bytes of 200 at offset 30000, do NOT commit, then crash.
  {
    nsdb_t *db = ns_open ("insert_demo", &e);
    txn_t *tx = ns_begin_txn (db, &e);

    u8 patch[10];
    for (int i = 0; i < 10; ++i)
      {
        patch[i] = 200;
      }

    ns_insert (db, tx, &root, patch, 10, 30000, &e);

    ns_crash (db, &e);
  }

  // Phase 5: Splice 10 bytes of 199 at offset 40000, explicitly roll back and close.
  {
    nsdb_t *db = ns_open ("insert_demo", &e);
    txn_t *tx = ns_begin_txn (db, &e);

    u8 patch[10];
    for (int i = 0; i < 10; ++i)
      {
        patch[i] = 199;
      }

    ns_insert (db, tx, &root, patch, 10, 40000, &e);

    ns_rollback_all (db, tx, &e);
    ns_close (db, &e);
  }

  // Phase 6: Reopen and verify each zone.
  //
  // [9998:10012]  — Phase 2 committed normally:     111 patch visible.
  // [19998:20012] — Phase 3 committed before crash:  222 patch visible.
  // [29998:30012] — Phase 4 crashed without commit:  original values restored.
  // [39998:40012] — Phase 5 explicit rollback:       original values restored.
  {
    nsdb_t *db = ns_open ("insert_demo", &e);
    txn_t *tx = ns_begin_txn (db, &e);

    u8 zone_a[14], zone_b[14], zone_c[14], zone_d[14];
    ns_read (db, tx, root, zone_a, 1, 9998, 1, 14, &e);
    ns_read (db, tx, root, zone_b, 1, 19998, 1, 14, &e);
    ns_read (db, tx, root, zone_c, 1, 29998, 1, 14, &e);
    ns_read (db, tx, root, zone_d, 1, 39998, 1, 14, &e);

    printf ("zone A (committed normally):\n");
    // 16 17 111 111 111 111 111 111 111 111 111 111 16 17
    for (int i = 0; i < 14; ++i)
      {
        printf ("  stream[%d] = %u\n", 9998 + i, zone_a[i]);
      }

    printf ("zone B (committed before crash):\n");
    // 16 17 222 222 222 222 222 222 222 222 222 222 16 17
    for (int i = 0; i < 14; ++i)
      {
        printf ("  stream[%d] = %u\n", 19998 + i, zone_b[i]);
      }

    printf ("zone C (crashed without commit — insert undone):\n");
    // 46 47 48 49 50 51 52 53 54 55 56 57 58 59
    for (int i = 0; i < 14; ++i)
      {
        printf ("  stream[%d] = %u\n", 29998 + i, zone_c[i]);
      }

    printf ("zone D (explicit rollback — insert undone):\n");
    // 64 65 66 67 68 69 70 71 72 73 74 75 76 77
    for (int i = 0; i < 14; ++i)
      {
        printf ("  stream[%d] = %u\n", 39998 + i, zone_d[i]);
      }

    ns_commit (db, tx, &e);
    ns_close (db, &e);
  }

  return 0;
}
