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

// sample5_durability
#define PATH "sample5_durability"

// sample5_durability
#define PATH "sample5_durability"

static void
run_phase (int crash, void (*fn) (smfile_t *))
{
  pid_t pid = fork ();
  if (pid == 0)
    {
      smfile_t *smf = smfile_open (PATH);
      fn (smf);
      if (crash)
        _Exit (1);
      else
        {
          smfile_close (smf);
          _Exit (0);
        }
    }
  waitpid (pid, NULL, 0);
}

// Phase 1: insert "AAAAAAAAAA" (10 A's) — committed, clean close
static void
phase1_populate (smfile_t *smf)
{
  smfile_begin (smf);
  smfile_insert (smf, "AAAAAAAAAA", 0, 10);
  smfile_commit (smf);
}

// Phase 2: insert "BB" at offset 3 — committed, clean close
// AAAAAAAAAA -> AAA BB AAAAAAA
static void
phase2_commit_clean (smfile_t *smf)
{
  smfile_begin (smf);
  smfile_insert (smf, "BB", 3, 2);
  smfile_commit (smf);
}

// Phase 3: insert "CC" at offset 7 — committed, THEN crash
// AAABBAAAAAA -> AAABBAA CC AAAA
// WAL has commit record, recovery replays this
static void
phase3_commit_then_crash (smfile_t *smf)
{
  smfile_begin (smf);
  smfile_insert (smf, "CC", 7, 2);
  smfile_commit (smf);
}

// Phase 4: insert "DD" at offset 11 — no commit, crash
// Recovery discards this entirely
static void
phase4_no_commit_crash (smfile_t *smf)
{
  smfile_begin (smf);
  smfile_insert (smf, "DD", 11, 2);
}

// Phase 5: insert "EE" at offset 5 — explicit rollback, clean close
// Rolled back, file unchanged
static void
phase5_rollback (smfile_t *smf)
{
  smfile_begin (smf);
  smfile_insert (smf, "EE", 5, 2);
  smfile_rollback (smf);
}

static void
check_zone (smfile_t *smf, const char *label, b_size bofst, b_size nelem, const char *expected)
{
  char buf[64];
  sb_size n = smfile_read (smf, buf, bofst, nelem);
  buf[n] = '\0';
  printf ("%s\n", label);
  printf ("  expected: \"%s\"\n", expected);
  printf ("  actual:   \"%s\"\n\n", buf);
}

int
main (void)
{
  run_phase (0, phase1_populate);
  printf ("[main] phase 1 done: inserted \"AAAAAAAAAA\"\n");

  run_phase (0, phase2_commit_clean);
  printf ("[main] phase 2 done: inserted \"BB\" at 3 (committed, clean)\n");

  run_phase (1, phase3_commit_then_crash);
  printf ("[main] phase 3 done: inserted \"CC\" at 7 (committed, crashed)\n");

  run_phase (1, phase4_no_commit_crash);
  printf ("[main] phase 4 done: inserted \"DD\" at 11 (no commit, crashed)\n");

  run_phase (0, phase5_rollback);
  printf ("[main] phase 5 done: inserted \"EE\" at 5 (rolled back)\n");

  smfile_t *smf = smfile_open (PATH);
  printf ("\n--- Phase 6: verification ---\n\n");

  // Final committed content: "AAABBAACC AAAA" = 14 bytes
  check_zone (smf, "full contents (phase 2 + 3 visible, phase 4 + 5 gone):", 0, 14, "AAABBAACCAAAAA");
  check_zone (smf, "phase 2 — BB committed and clean:", 3, 2, "BB");
  check_zone (smf, "phase 3 — CC committed then crashed (WAL recovery):", 7, 2, "CC");
  check_zone (smf, "phase 4 — DD no commit crashed (should be original A):", 11, 1, "A");
  check_zone (smf, "phase 5 — EE rolled back (should be original A):", 5, 1, "A");

  return smfile_close (smf);
}
