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

#include "include/tlclib/dev/stdtypes.h"
#include "include/tlclib/ds/dbl_buffer.h"
#include "include/tlclib/ds/string.h"
#include "tlclib/dev/testing.h"
#include "tlclib/intf/logging.h"
#include "tlclib/intf/os/time.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CLI_MAX_FILTERS 32
#define CLI_MAX_SUITES 16
#define streq(a, b) (strcmp (a, b) == 0)

TEST_SUITE (core, 128);
TEST_SUITE (intf, 16);

struct suite
{
  const char *name;
  test *tests;
  u32 len;
};

struct cli_flag
{
  const char *name;
  int value;
};

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
  fprintf (stderr, "  %s --suite core --suite intf\n", prog);
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
        {
          return true;
        }
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

  // --------------------------------------------------------
  // core
  // --------------------------------------------------------

  // block_array.c
  REGISTER (core, block_insert_read);
  REGISTER (core, block_insert_remove_read);
  REGISTER (core, block_insert_write_read);
  REGISTER (core, block_random);
  // cbuffer.c
  REGISTER (core, cbuffer_isempty);
  REGISTER (core, cbuffer_len);
  REGISTER (core, cbuffer_avail);
  REGISTER (core, cbuffer_get_next_data_bytes);
  REGISTER (core, cbuffer_get_nbytes);
  REGISTER (core, cbuffer_fakewrite);
  REGISTER (core, cbuffer_fakeread);
  REGISTER (core, cbuffer_read);
  REGISTER (core, cbuffer_copy);
  REGISTER (core, cbuffer_write);
  REGISTER (core, cbuffer_cbuffer_move);
  REGISTER (core, cbuffer_cbuffer_copy);
  REGISTER (core, cbuffer_get_no_check);
  REGISTER (core, cbuffer_get);
  REGISTER (core, cbuffer_peek_back);
  REGISTER (core, cbuffer_peek_front);
  REGISTER (core, cbuffer_push_back);
  REGISTER (core, cbuffer_push_front);
  REGISTER (core, cbuffer_pop_back);
  REGISTER (core, cbuffer_pop_front);
  // checksums.c
  REGISTER (core, checksum_execute_simple);
  REGISTER (core, checksum_execute_deterministic);
  REGISTER (core, checksum_execute_incremental);
  // dbl_buffer.c
  REGISTER (core, dblb_create_basic);
  REGISTER (core, dblb_append_single);
  REGISTER (core, dblb_append_multiple);
  REGISTER (core, dblb_append_triggers_realloc);
  REGISTER (core, dblb_append_alloc_basic);
  REGISTER (core, dblb_append_alloc_sequential);
  REGISTER (core, dblb_append_alloc_triggers_realloc);
  REGISTER (core, dblb_different_element_sizes);
  REGISTER (core, dblb_struct_elements);
  REGISTER (core, dblb_free_resets);
  REGISTER (core, dblb_large_append);
  // ext_array.c
  REGISTER (core, ext_array_insert_read);
  REGISTER (core, ext_array_write);
  REGISTER (core, ext_array_remove);
  REGISTER (core, ext_array_random);
  // filenames.c
  REGISTER (core, file_basename);
  // gr_lock.c
  REGISTER (core, gr_lock_basic);
  REGISTER (core, gr_lock_multiple_holders);
  REGISTER (core, gr_lock_is_is_compatible);
  REGISTER (core, gr_lock_is_ix_compatible);
  REGISTER (core, gr_lock_is_s_compatible);
  REGISTER (core, gr_lock_is_six_compatible);
  REGISTER (core, gr_lock_is_x_blocks);
  REGISTER (core, gr_lock_ix_ix_compatible);
  REGISTER (core, gr_lock_ix_s_blocks);
  REGISTER (core, gr_lock_s_compatible);
  REGISTER (core, gr_lock_x_blocks);
  REGISTER (core, gr_lockix_is_compatible);
  REGISTER (core, gr_lockix_ix_blocks);
  REGISTER (core, gr_lockix_s_blocks);
  REGISTER (core, gr_lock_sx_blocks);
  REGISTER (core, gr_lock_concurrent_readers);
  REGISTER (core, gr_lock_data_race_protection);
  // hash_table.c
  REGISTER (core, htable);
  // hashing.c
  REGISTER (core, fnv1a_hash_empty);
  REGISTER (core, fnv1a_hash_single_char);
  REGISTER (core, fnv1a_hash_known_value);
  REGISTER (core, fnv1a_hash_deterministic);
  // lalloc.c
  REGISTER (core, lalloc_edge_cases);
  // latch.c
  REGISTER (core, latch);
  // llist.c
  REGISTER (core, llist);
  // numbers.c
  REGISTER (core, parse_i32_expect);
  REGISTER (core, parse_f32_expect);
  REGISTER (core, py_mod_f32);
  REGISTER (core, py_mod_i32);
  // random.c
  REGISTER (core, randu32);
  REGISTER (core, randu32r);
  REGISTER (core, randi32r);
  REGISTER (core, randu64r);
  REGISTER (core, randi64r);
  // slab_alloc.c
  REGISTER (core, slab_alloc_simple);
  REGISTER (core, slab_alloc_cap_one);
  REGISTER (core, slab_alloc_no_duplicates);
  REGISTER (core, slab_alloc_free_all_realloc);
  REGISTER (core, slab_alloc_interleaved_patterns);
  REGISTER (core, slab_alloc_free_head_slab);
  REGISTER (core, slab_alloc_free_middle_slab);
  REGISTER (core, slab_alloc_minimum_size);
  REGISTER (core, slab_alloc_stress_random);
  // stride.c
  REGISTER (core, stride_resolve);
  // string.c
  REGISTER (core, strings_all_unique);
  REGISTER (core, string_contains);
  // core_extra_tests.c
  REGISTER (core, f16_to_f32_normals_and_specials);
  REGISTER (core, f16_to_f32_nan_is_nan);
  REGISTER (core, f16_to_f32_smallest_subnormal_correct_value);
  REGISTER (core, parse_i32_boundary_values);
  REGISTER (core, parse_i64_boundary_values);
  REGISTER (core, ext_array_capacity_doubles_on_growth);
  REGISTER (core, ext_array_remove_all_produces_empty);
  REGISTER (core, llist_append_maintaififo_order);
  REGISTER (core, llist_find_returnode_and_index);
  REGISTER (core, llist_remove_from_head_middle_tail);
  REGISTER (core, llist_remove_absent_node_is_noop);
  REGISTER (core, checksum_known_crc32c_vector);
  REGISTER (core, checksum_distinct_bytes_differ);
  REGISTER (core, serializer_write_at_capacity_then_overflow);
  REGISTER (core, serializer_incremental_write_overflow);
  REGISTER (core, stride_constructors_resolve_correctly);
  REGISTER (core, string_ordering_operators);
  REGISTER (core, line_length_newline_found);
  REGISTER (core, string_equal_cases);
  REGISTER (core, strings_are_disjoint_cases);
  REGISTER (core, string_plus_concatenates);
  REGISTER (core, cbuffer_discard_all_resets_state);
  REGISTER (core, cbuffer_read_write_wraparound);
  REGISTER (core, cbuffer_cbuffer_move_transfers_bytes);
  // robin_hood_ht_tests.c
  REGISTER (core, ht_insert_idx_regression_trigger_swap);
  REGISTER (core, robin_hood_ht);

  // --------------------------------------------------------
  // intf
  // --------------------------------------------------------

  // memory.c
  REGISTER (intf, i_realloc_basic);
  REGISTER (intf, i_realloc_right);
  REGISTER (intf, i_realloc_left);
  REGISTER (intf, i_crealloc_right);
  REGISTER (intf, i_crealloc_left);

  struct suite all_suites[] = {
    { "core", core_tests, (u32)core_count },
    { "intf", intf_tests, (u32)intf_count },
  };

  error e = error_create ();

  i_timer timer;
  if (i_timer_create (&timer, &e) != SUCCESS)
    {
      return -1;
    }

  struct dbl_buffer f;
  if (dblb_create (&f, sizeof (char *), 1, &e))
    {
      return -1;
    }

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
        {
          i_log_failure ("  %s\n", fl[i]);
        }
    }
  else
    {
      i_log_passed ("ALL TESTS PASSED\n");
    }

  dblb_free (&f);
  return test_ret;
}
