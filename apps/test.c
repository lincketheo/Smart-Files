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

#include "tlclib/ds/dbl_buffer.h"
#include "tlclib/ds/string.h"
#include "tlclib/intf/logging.h"
#include "tlclib/intf/os/time.h"
#include "tlclib_dev.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CLI_MAX_FILTERS 32
#define CLI_MAX_SUITES 16
#define streq(a, b) (strcmp (a, b) == 0)

////////////////////////////////////////////////////////////
/// SUITES

TEST_SUITE (numstore, 128);
TEST_SUITE (paging, 128);

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
  fprintf (stderr, "  Available:       core, intf, numstore, paging\n");
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
  if (test_parse_cli_params (argv, argc, &p)){
    return -1;
}

  if (p.help_printed){
    return 0;
  }

  // --------------------------------------------------------
  // numstore
  // --------------------------------------------------------

  // node_updates.c
  REGISTER (numstore, nupd_init);
  REGISTER (numstore, nupd_append_right);
  REGISTER (numstore, nupd_append_left);
  REGISTER (numstore, nupd_append_tip_right);
  REGISTER (numstore, nupd_append_tip_left);
  REGISTER (numstore, nupd_consume_right);
  REGISTER (numstore, nupd_consume_left);
  REGISTER (numstore, nupd_done_observing_left);
  REGISTER (numstore, nupd_done_observing_right);
  REGISTER (numstore, nupd_done_consuming_left);
  REGISTER (numstore, nupd_done_consuming_right);
  REGISTER (numstore, nupd_done_left);
  REGISTER (numstore, nupd_done_right);
  // _ns_balance_and_release.c
  REGISTER (numstore, dlgt_balance_with_prev);
  REGISTER (numstore, dlgt_balance_with_next);

  // --------------------------------------------------------
  // paging
  // --------------------------------------------------------

  // aries_fuzzy_checkpoint.c
  // REGISTER (paging, aries_fuzzy_checkpoint_basic_recovery);
  // REGISTER (paging, aries_fuzzy_checkpoint_with_active_transactions);
  // REGISTER (paging, aries_fuzzy_checkpoint_multiple_checkpoints);
  // REGISTER (paging, aries_fuzzy_checkpoint_with_post_checkpoint_activity);
  // aries_no_checkpoint_crash.c
  // REGISTER (paging, aries_crash);
  // REGISTER (paging, aries_crash_1);
  // pgr_rollback.c
  REGISTER (paging, aries_rollback_basic);
  REGISTER (paging, aries_rollback_multiple_updates);
  REGISTER (paging, aries_rollback_with_crash_recovery);
  REGISTER (paging, aries_rollback_clr_not_undone);
  // dirty_page_table_tests.c
  REGISTER (paging, dpgt_open);
  REGISTER (paging, dpgt_merge_into);
  REGISTER (paging, dpgt_min_rec_lsn);
  REGISTER (paging, dpgt_exists);
  REGISTER (paging, dpgt_add);
  REGISTER (paging, dpgt_get);
  REGISTER (paging, dpgt_remove);
  REGISTER (paging, dpgt_serialize);
  REGISTER (paging, dpgt_equal);
  // dpgt_concurrency_tests.c
  REGISTER (paging, dpgt_concurrent);
  // lock_table.c
  REGISTER (paging, lock_table_exclusivity);
  // file_pager.c
  REGISTER (paging, fpgr_open);
  REGISTER (paging, fpgr_new);
  REGISTER (paging, fpgr_read_write);
  // page_fixture.c
  REGISTER (paging, build_page_tree);
  // pager.c
  REGISTER (paging, pager_fill_ht);
  REGISTER (paging, wal_int);
  // pgr_close.c
  REGISTER (paging, pgr_close_success);
  // pgr_delete.c
  REGISTER (paging, pgr_delete);
  // pgr_get.c
  REGISTER (paging, pgr_get_invalid_checksum);
  // pgr_mt_test.c
  // REGISTER (paging, pager_mt);
  // pgr_new.c
  REGISTER (paging, pgr_new_get_save);
  REGISTER (paging, pgr_new_multiple_fsm);
  // pgr_open.c
  REGISTER (paging, pager_open);
  REGISTER (paging, pgr_open_basic);
  // data_list.c
  REGISTER (paging, dl_validate);
  REGISTER (paging, dl_set_get);
  REGISTER (paging, dl_read);
  REGISTER (paging, dl_read_out_from);
  REGISTER (paging, dl_append);
  REGISTER (paging, dl_write);
  REGISTER (paging, dl_memset);
  REGISTER (paging, dl_move_left);
  REGISTER (paging, dl_shift_right);
  REGISTER (paging, dl_move_right);
  // inner_node.c
  REGISTER (paging, in_validate_for_db);
  REGISTER (paging, in_set_get_simple);
  REGISTER (paging, in_push_end);
  REGISTER (paging, in_memcpy);
  REGISTER (paging, in_move_left);
  REGISTER (paging, in_move_left_two_keys);
  REGISTER (paging, in_move_left_all_keys);
  REGISTER (paging, in_move_left_into_empty);
  REGISTER (paging, in_push_left);
  REGISTER (paging, in_push_left_into_empty);
  REGISTER (paging, in_push_left_to_full);
  REGISTER (paging, in_move_right);
  REGISTER (paging, in_move_right_two_keys);
  REGISTER (paging, in_move_right_all_keys);
  REGISTER (paging, in_move_right_into_empty_right);
  REGISTER (paging, in_choose_lidx);
  REGISTER (paging, in_cut_left);
  REGISTER (paging, in_cut_left_all_at_once);
  REGISTER (paging, in_cut_left_from_empty);
  REGISTER (paging, in_cut_left_to_one);
  // page.c
  REGISTER (paging, page_set_get_simple);
  // root_node.c
  REGISTER (paging, rn_init_empty_and_zeroes);
  REGISTER (paging, rn_init_empty);
  REGISTER (paging, rn_validate_for_db);
  REGISTER (paging, rn_set_get_simple);
  // var_page.c
  REGISTER (paging, vp_init_empty);
  REGISTER (paging, vp_validate);
  // var_tail.c
  REGISTER (paging, vt_init_empty);
  REGISTER (paging, vt_validate);
  // txn.c
  REGISTER (paging, txn_basic);
  // txn_table.c
  REGISTER (paging, txnt_open);
  REGISTER (paging, txnt_merge_into);
  REGISTER (paging, txnt_max_u_undo_lsn);
  REGISTER (paging, txnt_min_lsn);
  REGISTER (paging, txnt_exists);
  REGISTER (paging, txnt_insert);
  REGISTER (paging, txnt_get);
  REGISTER (paging, txnt_remove);
  REGISTER (paging, txnt_serialize);
  REGISTER (paging, txnt_equal_ignore_state);
  // txnt_concurrency_tests.c
  REGISTER (paging, txnt_concurrent);
  // wal_tests.c
  REGISTER (paging, wal_multi_threaded);
  REGISTER (paging, wal);
  REGISTER (paging, wal_single_entry);

  struct suite all_suites[] = {
    { "numstore", numstore_tests, (u32)numstore_count },
    { "paging", paging_tests, (u32)paging_count },
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
