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

// =============================================================================
// sample5_rollback_commit — WAL durability and crash recovery, end to end.
//
// This sample exercises the four outcomes a transaction can have and verifies
// what survives to the next open:
//
//   committed + clean close  → survives (the baseline)
//   committed + crash        → survives (WAL replay on next open)
//   not committed + crash    → lost     (no WAL record to replay)
//   explicit rollback        → lost     (WAL record marked aborted)
//
// Crash simulation:
//   A real crash is an OS-level process termination with no cleanup handlers.
//   We simulate it with fork() + _Exit(1) in the child. _Exit bypasses all
//   atexit() handlers and C++ destructors — the kernel reclaims file
//   descriptors and memory immediately, just like SIGKILL. The parent waits
//   for the child, then re-opens the file and triggers WAL recovery.
//
// Data layout:
//   Phase 1 inserts 50 000 bytes: data[i] = (uint8_t)i (wraps at 256).
//   Subsequent phases splice 10-byte patches at fixed offsets:
//     offset 10 000 → 0x11 (committed, clean close)   — Phase 2
//     offset 20 000 → 0x22 (committed, then crash)     — Phase 3
//     offset 30 000 → 0xCC (never committed, crash)    — Phase 4
//     offset 40 000 → 0xBB (explicit rollback)         — Phase 5
//   Phase 6 reopens and reads 14 bytes around each patch zone to verify.
//
// Note on offsets in Phase 6:
//   Each committed splice grows the file by 10 bytes. Phase 3 splices at
//   20 000 in the post-Phase-2 file (which is already 50 010 bytes). The
//   verification reads use offsets into the final committed state (50 020
//   bytes), accounting for the cumulative shift from Phases 2 and 3.
// =============================================================================

#include "smfile.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <unistd.h>

#define PATH "sample5_durability"

// Run fn(path) in a child process. If crash=1, the child calls _Exit(1)
// after fn returns, bypassing smfile_close and all cleanup — simulating a
// hard crash. If crash=0, the child calls _Exit(0) for a clean exit.
//
// The parent always waits for the child before returning. On the next call
// to smfile_open(PATH), the WAL recovery logic decides what to replay.
static void
run_phase (int crash, void (*fn) (smfile_t *))
{
  pid_t pid = fork ();
  if (pid < 0)
    {
      perror ("fork");
      exit (1);
    }

  if (pid == 0)
    {
      smfile_t *smf = smfile_open (PATH);
      if (!smf)
        {
          fprintf (stderr, "child: smfile_open failed\n");
          _Exit (1);
        }

      fn (smf);

      if (crash)
        _Exit (1); // bypass smfile_close: OS terminates, WAL not marked clean
      else
        {
          smfile_close (smf);
          _Exit (0);
        }
    }

  waitpid (pid, NULL, 0);
}

// ─── Phase handlers ──────────────────────────────────────────────────────────

// Phase 1: initial population — 50 000 bytes, data[i] = (uint8_t)i.
// Committed and closed cleanly. This is the baseline the other phases mutate.
static void
phase1_populate (smfile_t *smf)
{
  uint8_t data[50000];
  for (int i = 0; i < 50000; ++i)
    data[i] = (uint8_t)i;

  if (smfile_begin (smf) < 0)
    {
      smfile_perror (smf, "phase1 begin");
      return;
    }
  smfile_insert (smf, data, 0, sizeof (data));
  smfile_commit (smf);
}

// Phase 2: splice 10 bytes of 0x11 at offset 10 000. Committed, clean close.
// File grows from 50 000 → 50 010 bytes. The patch is durable.
static void
phase2_commit_clean (smfile_t *smf)
{
  uint8_t patch[10];
  for (int i = 0; i < 10; ++i)
    patch[i] = 0x11;

  smfile_begin (smf);
  smfile_insert (smf, patch, 10000, 10);
  smfile_commit (smf);
  // smfile_close called by run_phase (crash=0)
}

// Phase 3: splice 10 bytes of 0x22 at offset 20 000. Committed, THEN crash.
// The WAL has a commit record, so recovery on next open replays this splice.
// File grows from 50 010 → 50 020 bytes after recovery.
static void
phase3_commit_then_crash (smfile_t *smf)
{
  uint8_t patch[10];
  for (int i = 0; i < 10; ++i)
    patch[i] = 0x22;

  smfile_begin (smf);
  smfile_insert (smf, patch, 20000, 10);
  smfile_commit (smf);
  // run_phase(crash=1) will call _Exit(1) — no smfile_close, no clean WAL seal
}

// Phase 4: splice 10 bytes of 0xCC at offset 30 000. NO commit, then crash.
// No WAL commit record → recovery discards this splice entirely.
// File stays at 50 020 bytes after recovery.
static void
phase4_no_commit_crash (smfile_t *smf)
{
  uint8_t patch[10];
  for (int i = 0; i < 10; ++i)
    patch[i] = 0xCC;

  smfile_begin (smf);
  smfile_insert (smf, patch, 30000, 10);
  // deliberate: no smfile_commit before _Exit(1)
}

// Phase 5: splice 10 bytes of 0xBB at offset 40 000. Explicit rollback, clean close.
// smfile_rollback undoes the in-progress splice and marks the transaction
// aborted in the WAL. File stays at 50 020 bytes.
static void
phase5_rollback (smfile_t *smf)
{
  uint8_t patch[10];
  for (int i = 0; i < 10; ++i)
    patch[i] = 0xBB;

  smfile_begin (smf);
  smfile_insert (smf, patch, 40000, 10);
  smfile_rollback (smf);
  // smfile_close called by run_phase (crash=0)
}

// ─── Phase 6: verify ─────────────────────────────────────────────────────────

static void
print_zone (smfile_t *smf, const char *label, b_size bofst, b_size nelem)
{
  uint8_t buf[32];
  if (nelem > sizeof (buf))
    nelem = sizeof (buf);

  sb_size n = smfile_read (smf, buf, bofst, nelem);
  if (n < 0)
    {
      smfile_perror (smf, label);
      return;
    }
  printf ("%s (offset %llu, %lld bytes):\n", label, (unsigned long long)bofst, (long long)n);
  for (sb_size i = 0; i < n; ++i)
    printf ("  [%llu] = %02x\n", (unsigned long long)(bofst + (b_size)i), buf[i]);
}

// =============================================================================
// main
// =============================================================================

int
main (void)
{
  // Phase 1: populate (committed, clean close)
  run_phase (0, phase1_populate);
  printf ("[main] phase 1 done: 50 000 bytes populated\n");

  // Phase 2: patch at 10 000 with 0x11 (committed, clean close)
  run_phase (0, phase2_commit_clean);
  printf ("[main] phase 2 done: 0x11 patch committed at offset 10 000\n");

  // Phase 3: patch at 20 000 with 0x22 (committed, THEN crash)
  // _Exit(1) in child — WAL has the commit record, recovery will replay.
  run_phase (1, phase3_commit_then_crash);
  printf ("[main] phase 3 done: 0x22 patch committed then crashed\n");

  // Phase 4: patch at 30 000 with 0xCC (no commit, crash)
  // _Exit(1) in child — no WAL commit record, recovery discards.
  run_phase (1, phase4_no_commit_crash);
  printf ("[main] phase 4 done: 0xCC patch inserted without commit, crashed\n");

  // Phase 5: patch at 40 000 with 0xBB (explicit rollback, clean close)
  run_phase (0, phase5_rollback);
  printf ("[main] phase 5 done: 0xBB patch rolled back explicitly\n");

  // ─── Phase 6: verify ──────────────────────────────────────────────────────
  //
  // smfile_open triggers WAL recovery here: the Phase 3 commit is replayed,
  // Phase 4's uncommitted splice is discarded, Phase 5 was already rolled back.
  //
  // Final committed file: 50 020 bytes
  //   [0    .. 9 999] original data (data[i] = i%256)
  //   [10 000..10 009] 0x11×10       (Phase 2)
  //   [10 010..19 999] original[10 000..19 989]
  //   [20 000..20 009] 0x22×10       (Phase 3, recovered from WAL)
  //   [20 010..50 019] original[19 990..49 999]
  smfile_t *smf = smfile_open (PATH);
  if (!smf)
    {
      fprintf (stderr, "phase 6: smfile_open failed\n");
      return 1;
    }

  printf ("\n--- Phase 6: verification ---\n");

  // Zone A: around Phase 2 splice (offset 10 000). Expect original bytes
  // flanking 10 bytes of 0x11. data[9 998]=9998%%256=0x0e, data[9999]=0x0f.
  print_zone (smf, "Zone A — Phase 2 (committed, clean): expect 0x11×10 visible",
              9998, 14);

  // Zone B: around Phase 3 splice (offset 20 000 in post-Phase-2 file).
  // Phase 3 committed before crash; WAL replay should restore the 0x22 patch.
  print_zone (smf, "Zone B — Phase 3 (committed, crashed): expect 0x22×10 visible",
              19998, 14);

  // Zone C: around Phase 4 insertion point (offset 30 000 in the final file).
  // No commit record → splice was discarded on recovery. Expect original bytes.
  print_zone (smf, "Zone C — Phase 4 (crashed, no commit): expect original bytes",
              29998, 14);

  // Zone D: around Phase 5 insertion point (offset 40 000 in the final file).
  // Explicit rollback → splice was undone. Expect original bytes.
  print_zone (smf, "Zone D — Phase 5 (explicit rollback): expect original bytes",
              39998, 14);

  return smfile_close (smf);
}
