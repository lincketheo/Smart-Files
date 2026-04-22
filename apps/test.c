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

#include "c_specx/ds/dbl_buffer.h"
#include "c_specx/ds/string.h"
#include "c_specx/intf/logging.h"
#include "c_specx/intf/os/time.h"
#include "c_specx_dev.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CLI_MAX_FILTERS 32
#define CLI_MAX_SUITES 16
#define streq(a, b) (strcmp (a, b) == 0)

////////////////////////////////////////////////////////////
/// SUITES

TEST_SUITE (smartfiles, 256);

////////////////////////////////////////////////////////////
/// SUITE STRUCT

struct suite
{
  const char *name;
  test *tests;
  u32 len;
};

////////////////////////////////////////////////////////////
/// CLI

struct test_cli
{
  char *filters[CLI_MAX_FILTERS];
  int flen;
  char *suites[CLI_MAX_SUITES];
  int slen;
  bool help_printed;
};

static void
test_print_help (const char *prog)
{
  fprintf (stderr, "Usage: %s [TYPE] [--suite NAME]... [filter...]\n", prog);
  fprintf (stderr, "\nSuites:\n");
  fprintf (stderr, "  --suite NAME     Run only tests in NAME (repeatable)\n");
  fprintf (stderr, "  Available:       core, intf, smartfiles, paging\n");
  fprintf (stderr, "\nFilters:\n");
  fprintf (stderr, "  [filter...]      Run tests whose names contain any filter\n");
  fprintf (stderr, "  If omitted, all tests of the selected type/suite run.\n");
  fprintf (stderr, "\nFlags:\n");
  fprintf (stderr, "  --help, -h       Show this message\n");
  fprintf (stderr, "\nExamples:\n");
  fprintf (stderr, "  %s\n", prog);
  fprintf (stderr, "  %s HEAVY\n", prog);
  fprintf (stderr, "  %s --suite core --suite intf\n", prog);
  fprintf (stderr, "  %s HEAVY --suite paging wal\n", prog);
}

static int
test_parse_cli_params (
    char **argv,
    const int argc,
    struct test_cli *p)
{
  p->flen = 0;
  p->slen = 0;
  p->help_printed = false;

  for (int i = 1; i < argc; i++)
    {
      char *arg = argv[i];

      if (streq (arg, "--help") || streq (arg, "-h"))
        {
          test_print_help (argv[0]);
          p->help_printed = true;
          return 0;
        }

      if (streq (arg, "--suite"))
        {
          if (i + 1 >= argc)
            {
              fprintf (stderr, "Error: --suite requires a value\n");
              test_print_help (argv[0]);
              return -1;
            }
          if (p->slen >= CLI_MAX_SUITES)
            {
              fprintf (stderr, "Error: too many --suite args (max %d)\n", CLI_MAX_SUITES);
              return -1;
            }
          p->suites[p->slen++] = argv[++i];
          continue;
        }

      bool matched = false;

      if (!matched)
        {
          if (p->flen >= CLI_MAX_FILTERS)
            {
              fprintf (stderr, "Error: too many filters (max %d)\n", CLI_MAX_FILTERS);
              return -1;
            }
          p->filters[p->flen++] = arg;
        }
    }

  return 0;
}

////////////////////////////////////////////////////////////
/// RUNNER

static bool
should_run (const test *t, const char *suite_name, const struct test_cli *p)
{
  if (p->slen > 0)
    {
      bool matched = false;
      for (int i = 0; i < p->slen; i++)
        {
          if (streq (p->suites[i], suite_name))
            {
              matched = true;
              break;
            }
        }
      if (!matched)
        return false;
    }

  if (p->flen == 0)
    return true;

  const struct string tn = strfcstr (t->test_name);
  for (int i = 0; i < p->flen; i++)
    {
      const struct string f = strfcstr (p->filters[i]);
      if (string_contains (tn, f))
        return true;
    }

  return false;
}

////////////////////////////////////////////////////////////
/// MAIN

int
main (const int argc, char **argv)
{
  struct test_cli p;
  if (test_parse_cli_params (argv, argc, &p))
    {
      return -1;
    }

  if (p.help_printed)
    {
      return 0;
    }

  // node_updates.c
  REGISTER (smartfiles, nupd_init);
  REGISTER (smartfiles, nupd_append_right);
  REGISTER (smartfiles, nupd_append_left);
  REGISTER (smartfiles, nupd_append_tip_right);
  REGISTER (smartfiles, nupd_append_tip_left);
  REGISTER (smartfiles, nupd_consume_right);
  REGISTER (smartfiles, nupd_consume_left);
  REGISTER (smartfiles, nupd_done_observing_left);
  REGISTER (smartfiles, nupd_done_observing_right);
  REGISTER (smartfiles, nupd_done_consuming_left);
  REGISTER (smartfiles, nupd_done_consuming_right);
  REGISTER (smartfiles, nupd_done_left);
  REGISTER (smartfiles, nupd_done_right);

  // _ns_balance_and_release.c
  REGISTER (smartfiles, dlgt_balance_with_prev);
  REGISTER (smartfiles, dlgt_balance_with_next);

  // aries_fuzzy_checkpoint.c
  // REGISTER (smartfiles, aries_fuzzy_checkpoint_basic_recovery);
  // REGISTER (smartfiles, aries_fuzzy_checkpoint_with_active_transactions);
  // REGISTER (smartfiles, aries_fuzzy_checkpoint_multiple_checkpoints);
  // REGISTER (smartfiles, aries_fuzzy_checkpoint_with_post_checkpoint_activity);

  // aries_no_checkpoint_crash.c
  // REGISTER (smartfiles, aries_crash);
  // REGISTER (smartfiles, aries_crash_1);

  // pgr_rollback.c
  REGISTER (smartfiles, aries_rollback_basic);
  REGISTER (smartfiles, aries_rollback_multiple_updates);
  REGISTER (smartfiles, aries_rollback_with_crash_recovery);
  REGISTER (smartfiles, aries_rollback_clr_not_undone);

  // dirty_page_table_tests.c
  REGISTER (smartfiles, dpgt_open);
  REGISTER (smartfiles, dpgt_merge_into);
  REGISTER (smartfiles, dpgt_min_rec_lsn);
  REGISTER (smartfiles, dpgt_exists);
  REGISTER (smartfiles, dpgt_add);
  REGISTER (smartfiles, dpgt_get);
  REGISTER (smartfiles, dpgt_remove);
  REGISTER (smartfiles, dpgt_serialize);
  REGISTER (smartfiles, dpgt_equal);

  // dpgt_concurrency_tests.c
  REGISTER (smartfiles, dpgt_concurrent);

  // lock_table.c
  // REGISTER (smartfiles, lock_table_exclusivity);

  // file_pager.c
  REGISTER (smartfiles, fpgr_open);
  REGISTER (smartfiles, fpgr_new);
  REGISTER (smartfiles, fpgr_read_write);

  // page_fixture.c
  REGISTER (smartfiles, build_page_tree);

  // pager.c
  REGISTER (smartfiles, pager_fill_ht);
  REGISTER (smartfiles, wal_int);

  // pgr_close.c
  REGISTER (smartfiles, pgr_close_success);

  // pgr_delete.c
  REGISTER (smartfiles, pgr_delete);

  // pgr_get.c
  REGISTER (smartfiles, pgr_get_invalid_checksum);

  // pgr_mt_test.c
  // REGISTER (smartfiles, pager_mt);

  // pgr_new.c
  REGISTER (smartfiles, pgr_new_get_save);
  REGISTER (smartfiles, pgr_new_multiple_fsm);

  // pgr_open.c
  REGISTER (smartfiles, pager_open);
  REGISTER (smartfiles, pgr_open_basic);

  // data_list.c
  REGISTER (smartfiles, dl_validate);
  REGISTER (smartfiles, dl_set_get);
  REGISTER (smartfiles, dl_read);
  REGISTER (smartfiles, dl_read_out_from);
  REGISTER (smartfiles, dl_append);
  REGISTER (smartfiles, dl_write);
  REGISTER (smartfiles, dl_memset);
  REGISTER (smartfiles, dl_move_left);
  REGISTER (smartfiles, dl_shift_right);
  REGISTER (smartfiles, dl_move_right);

  // inner_node.c
  REGISTER (smartfiles, in_validate_for_db);
  REGISTER (smartfiles, in_set_get_simple);
  REGISTER (smartfiles, in_push_end);
  REGISTER (smartfiles, in_memcpy);
  REGISTER (smartfiles, in_move_left);
  REGISTER (smartfiles, in_move_left_two_keys);
  REGISTER (smartfiles, in_move_left_all_keys);
  REGISTER (smartfiles, in_move_left_into_empty);
  REGISTER (smartfiles, in_push_left);
  REGISTER (smartfiles, in_push_left_into_empty);
  REGISTER (smartfiles, in_push_left_to_full);
  REGISTER (smartfiles, in_move_right);
  REGISTER (smartfiles, in_move_right_two_keys);
  REGISTER (smartfiles, in_move_right_all_keys);
  REGISTER (smartfiles, in_move_right_into_empty_right);
  REGISTER (smartfiles, in_choose_lidx);
  REGISTER (smartfiles, in_cut_left);
  REGISTER (smartfiles, in_cut_left_all_at_once);
  REGISTER (smartfiles, in_cut_left_from_empty);
  REGISTER (smartfiles, in_cut_left_to_one);

  // page.c
  REGISTER (smartfiles, page_set_get_simple);

  // var_page.c
  REGISTER (smartfiles, vp_init_empty);
  REGISTER (smartfiles, vp_validate);

  // var_tail.c
  REGISTER (smartfiles, vt_init_empty);
  REGISTER (smartfiles, vt_validate);

  // txn.c
  REGISTER (smartfiles, txn_basic);

  // txn_table.c
  REGISTER (smartfiles, txnt_open);
  REGISTER (smartfiles, txnt_merge_into);
  REGISTER (smartfiles, txnt_max_u_undo_lsn);
  REGISTER (smartfiles, txnt_min_lsn);
  REGISTER (smartfiles, txnt_exists);
  REGISTER (smartfiles, txnt_insert);
  REGISTER (smartfiles, txnt_get);
  REGISTER (smartfiles, txnt_remove);
  REGISTER (smartfiles, txnt_serialize);
  REGISTER (smartfiles, txnt_equal_ignore_state);

  // txnt_concurrency_tests.c
  REGISTER (smartfiles, txnt_concurrent);

  // wal_tests.c
  // REGISTER (smartfiles, wal_multi_threaded);
  REGISTER (smartfiles, wal);
  REGISTER (smartfiles, wal_single_entry);

  REGISTER (smartfiles, smfile_data_writer);

  struct suite all_suites[] = {
    { "smartfiles", smartfiles_tests, (u32)smartfiles_count },
  };

  error e = error_create ();

  i_timer timer;
  if (i_timer_create (&timer, &e) != SUCCESS)
    return -1;

  struct dbl_buffer f;
  if (dblb_create (&f, sizeof (char *), 1, &e))
    return -1;

  for (u32 s = 0; s < arrlen (all_suites); s++)
    {
      const struct suite *suite = &all_suites[s];
      for (u32 i = 0; i < suite->len; i++)
        {
          test *t = &suite->tests[i];
          if (!should_run (t, suite->name, &p))
            continue;

          if (!t->test ())
            {
              char **n = &t->test_name;
              if (dblb_append (&f, n, 1, &e))
                {
                  dblb_free (&f);
                  return -1;
                }
            }
        }
    }

  printf ("Time: %llu ms\n", (unsigned long long)i_timer_now_ms (&timer));
  i_timer_free (&timer);

  char **fl = f.data;
  if (f.nelem > 0)
    {
      i_log_failure ("FAILED TESTS:\n");
      for (u32 i = 0; i < f.nelem; i++)
        i_log_failure ("  %s\n", fl[i]);
    }
  else
    {
      i_log_passed ("ALL TESTS PASSED\n");
    }

  dblb_free (&f);
  return test_ret;
}
