#include "smfile.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#  include <windows.h>
#else
#  include <sys/wait.h>
#  include <unistd.h>
#endif

#define PATH "sample5_durability"

static void phase1_populate (smfile_t *smf);
static void phase2_commit_clean (smfile_t *smf);
static void phase3_commit_then_crash (smfile_t *smf);
static void phase4_no_commit_crash (smfile_t *smf);
static void phase5_rollback (smfile_t *smf);

typedef void (*phase_fn)(smfile_t *);
static phase_fn phases[] = {
  NULL,
  phase1_populate,
  phase2_commit_clean,
  phase3_commit_then_crash,
  phase4_no_commit_crash,
  phase5_rollback,
};

static void
run_phase (int phase_num, int crash, const char *exe)
{
  char arg[32];
  snprintf (arg, sizeof (arg), "%d", phase_num);

#ifdef _WIN32
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
        _Exit (1);
      smfile_close (smf);
      _Exit (0);
    }
  waitpid (pid, NULL, 0);
  (void)exe;
#endif
}

static void
phase1_populate (smfile_t *smf)
{
  smfile_begin (smf);
  smfile_insert (smf, "AAAAAAAAAA", 0, 10);
  smfile_commit (smf);
}

static void
phase2_commit_clean (smfile_t *smf)
{
  smfile_begin (smf);
  smfile_insert (smf, "BB", 3, 2);
  smfile_commit (smf);
}

static void
phase3_commit_then_crash (smfile_t *smf)
{
  smfile_begin (smf);
  smfile_insert (smf, "CC", 7, 2);
  smfile_commit (smf);
}

static void
phase4_no_commit_crash (smfile_t *smf)
{
  smfile_begin (smf);
  smfile_insert (smf, "DD", 11, 2);
}

static void
phase5_rollback (smfile_t *smf)
{
  smfile_begin (smf);
  smfile_insert (smf, "EE", 5, 2);
  smfile_rollback (smf);
}

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

int
main (int argc, char *argv[])
{
#ifdef _WIN32
  /* Child process: run a specific phase and exit */
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
  (void)argc; (void)argv;

  run_phase (1, 0, argv[0]);
  printf ("[main] phase 1 done: inserted \"AAAAAAAAAA\"\n");
  run_phase (2, 0, argv[0]);
  printf ("[main] phase 2 done: inserted \"BB\" at 3 (committed, clean)\n");
  run_phase (3, 1, argv[0]);
  printf ("[main] phase 3 done: inserted \"CC\" at 7 (committed, crashed)\n");
  run_phase (4, 1, argv[0]);
  printf ("[main] phase 4 done: inserted \"DD\" at 11 (no commit, crashed)\n");
  run_phase (5, 0, argv[0]);
  printf ("[main] phase 5 done: inserted \"EE\" at 5 (rolled back)\n");

  smfile_t *smf = smfile_open (PATH);
  printf ("\n--- Phase 6: verification ---\n\n");
  check_zone (smf, "full contents (phase 2 + 3 visible, phase 4 + 5 gone):", 0, 14, "AAABBAACCAAAAA");
  check_zone (smf, "phase 2 — BB committed and clean:", 3, 2, "BB");
  check_zone (smf, "phase 3 — CC committed then crashed (WAL recovery):", 7, 2, "CC");
  check_zone (smf, "phase 4 — DD no commit crashed (should be original A):", 11, 1, "A");
  check_zone (smf, "phase 5 — EE rolled back (should be original A):", 5, 1, "A");
  return smfile_close (smf);
}
