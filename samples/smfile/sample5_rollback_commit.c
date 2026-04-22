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

/// sample5_durability.c
///
/// This sample demonstrates WAL (Write-Ahead Logging) durability guarantees.
///
/// The core question: if a process crashes at various points in a transaction's
/// lifecycle, what state is the file in when we reopen it?
///
/// Rule 1 — COMMITTED + CRASHED: data must survive  (WAL replay on reopen)
/// Rule 2 — NOT COMMITTED + CRASHED: data must vanish (WAL discarded)
/// Rule 3 — ROLLED BACK: data must vanish            (explicit undo)
///
/// Each "phase" runs in a child process so we can simulate a crash by calling
/// _Exit() without giving smfile a chance to flush or clean up.
///
/// Final layout after all phases (offsets 0-13):
///
///   offset:  0  1  2  3  4  5  6  7  8  9  10 11 12 13
///   byte:    A  A  A  B  B  A  A  C  C  A  A  A  A  A
///                    ^--ph2    ^     ^--ph3
///                          ph5(rb)
///
/// Phases 4 (no-commit crash) and 5 (rollback) leave their slots as 'A'.

#include "smfile.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include "windows.h"
#else
#include <sys/wait.h>
#include <unistd.h>
#endif

#define PATH "sample5_durability"

// ---------------------------------------------------------------------------
// Phase functions — each represents one transaction scenario
// ---------------------------------------------------------------------------

static void phase1_populate (smfile_t *smf);
static void phase2_commit_clean (smfile_t *smf);
static void phase3_commit_then_crash (smfile_t *smf);
static void phase4_no_commit_crash (smfile_t *smf);
static void phase5_rollback (smfile_t *smf);

typedef void (*phase_fn) (smfile_t *);
static phase_fn phases[] = {
  NULL,
  phase1_populate,
  phase2_commit_clean,
  phase3_commit_then_crash,
  phase4_no_commit_crash,
  phase5_rollback,
};

// ---------------------------------------------------------------------------
// Child process runner
//
// Each phase runs in its own child process. This is the key mechanism that
// lets us simulate a crash: when crash=1, the child calls _Exit() instead
// of smfile_close(), bypassing any cleanup smfile would normally do on exit.
// The parent simply waits for the child to finish before continuing.
// ---------------------------------------------------------------------------

static void
run_phase (int phase_num, int crash, const char *exe)
{
  char arg[32];
  snprintf (arg, sizeof (arg), "%d", phase_num);

#ifdef _WIN32
  // Windows doesn't have fork(), so we re-exec the binary with a phase
  // number argument and let the child handle it in main().
  char cmd[512];
  snprintf (cmd, sizeof (cmd), "\"%s\" %s", exe, arg);
  STARTUPINFOA si = { sizeof (si) };
  PROCESS_INFORMATION pi;
  if (!CreateProcessA (NULL, cmd, NULL, NULL, FALSE, 0, NULL, NULL, &si, &pi))
    {
      fprintf (stderr, "CreateProcess failed: %lu\n", GetLastError ());
      exit (1);
    }
  WaitForSingleObject (pi.hProcess, INFINITE);
  CloseHandle (pi.hProcess);
  CloseHandle (pi.hThread);
  (void)crash;
#else
  pid_t pid = fork ();
  if (pid == 0)
    {
      smfile_t *smf = smfile_open (PATH);
      phases[phase_num](smf);
      if (crash)
        {
          _Exit (1); // Simulate crash: no smfile_close(), no WAL flush
        }
      smfile_close (smf);
      _Exit (0);
    }
  waitpid (pid, NULL, 0);
  (void)exe;
#endif
}

// ---------------------------------------------------------------------------
// Phase 1 — Baseline
//
// Write ten 'A' bytes starting at offset 0 and commit cleanly.
// This is the initial known state everything else is measured against.
//
//   before: (empty)
//   after:  A A A A A A A A A A
// ---------------------------------------------------------------------------

static void
phase1_populate (smfile_t *smf)
{
  smfile_begin (smf);
  smfile_insert (smf, "AAAAAAAAAA", 0, 10);
  smfile_commit (smf);
}

// ---------------------------------------------------------------------------
// Phase 2 — Commit, clean exit
//
// Overwrite offsets 3-4 with "BB" and commit. The process exits normally.
// This is the happy path: committed data must be readable afterwards.
//
//   before: A A A A A A A A A A
//   after:  A A A B B A A A A A
// ---------------------------------------------------------------------------

static void
phase2_commit_clean (smfile_t *smf)
{
  smfile_begin (smf);
  smfile_insert (smf, "BB", 3, 2);
  smfile_commit (smf);
}

// ---------------------------------------------------------------------------
// Phase 3 — Commit, then crash
//
// Overwrite offsets 7-8 with "CC" and commit, then crash immediately.
// The WAL entry is committed before the crash, so smfile must replay it
// on the next open and make "CC" visible.
//
//   before: A A A B B A A A A A
//   after:  A A A B B A A C C A   (WAL replay restores CC on next open)
// ---------------------------------------------------------------------------

static void
phase3_commit_then_crash (smfile_t *smf)
{
  smfile_begin (smf);
  smfile_insert (smf, "CC", 7, 2);
  smfile_commit (smf);
  // _Exit() called by run_phase — crash happens here
}

// ---------------------------------------------------------------------------
// Phase 4 — No commit, crash
//
// Write "DD" at offsets 11-12 but never commit, then crash.
// Because the transaction was never committed, smfile must discard the
// WAL entry on the next open. Offsets 11-12 must still read as 'A'.
//
//   before: A A A B B A A C C A A A A A
//   after:  A A A B B A A C C A A A A A   (no change — DD discarded)
// ---------------------------------------------------------------------------

static void
phase4_no_commit_crash (smfile_t *smf)
{
  smfile_begin (smf);
  smfile_insert (smf, "DD", 11, 2);
  // No smfile_commit() — _Exit() called by run_phase
}

// ---------------------------------------------------------------------------
// Phase 5 — Explicit rollback
//
// Write "EE" at offsets 5-6 and then explicitly roll back. No crash here;
// this tests that the rollback path correctly undoes the write.
// Offsets 5-6 must still read as 'A'.
//
//   before: A A A B B A A C C A A A A A
//   after:  A A A B B A A C C A A A A A   (no change — EE rolled back)
// ---------------------------------------------------------------------------

static void
phase5_rollback (smfile_t *smf)
{
  smfile_begin (smf);
  smfile_insert (smf, "EE", 5, 2);
  smfile_rollback (smf);
}

// ---------------------------------------------------------------------------
// Verification helper
//
// Reads nelem bytes at bofst, prints expected vs actual so you can see at a
// glance whether the durability guarantee held.
// ---------------------------------------------------------------------------

static void
check_zone (smfile_t *smf, const char *label, b_size bofst, b_size nelem,
            const char *expected)
{
  char buf[64];
  sb_size n = smfile_read (smf, buf, bofst, nelem);
  buf[n] = '\0';
  printf ("%s\n", label);
  printf ("  expected: \"%s\"\n", expected);
  printf ("  actual:   \"%s\"\n\n", buf);
}

// ---------------------------------------------------------------------------
// Main — orchestrate phases, then verify
// ---------------------------------------------------------------------------

int
main (int argc, char *argv[])
{
#ifdef _WIN32
  // Windows child: re-execed with a phase number, run that phase and exit.
  if (argc == 2)
    {
      int phase_num = atoi (argv[1]);
      int crash = (phase_num == 3 || phase_num == 4);
      smfile_t *smf = smfile_open (PATH);
      phases[phase_num](smf);
      if (crash)
        exit (1);
      smfile_close (smf);
      return 0;
    }
#endif
  (void)argc;
  (void)argv;

  run_phase (1, 0, argv[0]);
  printf ("[main] phase 1 done: inserted \"AAAAAAAAAA\" at 0 (baseline)\n");

  run_phase (2, 0, argv[0]);
  printf ("[main] phase 2 done: inserted \"BB\" at 3 (committed, clean exit)\n");

  run_phase (3, 1, argv[0]);
  printf ("[main] phase 3 done: inserted \"CC\" at 7 (committed, then crashed)\n");

  run_phase (4, 1, argv[0]);
  printf ("[main] phase 4 done: inserted \"DD\" at 11 (no commit, crashed — should vanish)\n");

  run_phase (5, 0, argv[0]);
  printf ("[main] phase 5 done: inserted \"EE\" at 5 (rolled back — should vanish)\n");

  // Open the file fresh — this triggers WAL replay for any committed-but-
  // unwritten entries (i.e. phase 3's "CC").
  smfile_t *smf = smfile_open (PATH);

  printf ("\n--- Phase 6: verification ---\n\n");
  check_zone (smf, "full contents (ph2 + ph3 visible, ph4 + ph5 gone):", 0, 14, "AAABBAACCAAAAA");
  check_zone (smf, "ph2 — BB committed, clean exit:", 3, 2, "BB");
  check_zone (smf, "ph3 — CC committed then crashed (WAL replay):", 7, 2, "CC");
  check_zone (smf, "ph4 — DD no commit + crash (must still be A):", 11, 1, "A");
  check_zone (smf, "ph5 — EE rolled back (must still be A):", 5, 1, "A");

  return smfile_close (smf);
}
