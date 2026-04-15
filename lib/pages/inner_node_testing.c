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

#include "inner_node_testing.h"

#include "pages/inner_node.h"
#include "pages/page.h"

#ifndef NTEST

void
inner_node_init_for_testing (page *in, const pgno *pgs, const b_size *keys,
                             const p_size len)
{
  page_init_empty (in, PG_INNER_NODE);
  in_set_len (in, 0);

  for (p_size i = 0; i < len; ++i)
    {
      in_push_end (in, keys[i], pgs[i]);
    }
}

void
in_print (const page *in)
{
  for (p_size i = 0; i < in_get_len (in); ++i)
    {
      const pgno pg = in_get_leaf (in, i);
      const b_size key = in_get_key (in, i);
      printf ("(%" PRpgno " %" PRb_size "), ", pg, key);
    }
  printf ("\n");
}

void
in_print_as_arrays (const pgno *pgs, const b_size *keys, const p_size len)
{
  for (p_size i = 0; i < len; ++i)
    {
      const pgno pg = pgs[i];
      const b_size key = keys[i];
      printf ("(%" PRpgno " %" PRb_size "), ", pg, key);
    }
  printf ("\n");
}

void
test_assert_inner_node_equal (const page *actual, const pgno *e_pgs,
                              const b_size *e_keys, const p_size len)
{
  test_assert_int_equal (len, in_get_len (actual));

  for (p_size i = 0; i < len; ++i)
    {
      const pgno pg = in_get_leaf (actual, i);
      const b_size key = in_get_key (actual, i);

      if (pg != e_pgs[i] || e_keys[i] != key)
        {
          i_log_failure ("Inner Nodes were not equal:\n");
          i_log_failure ("Expected:\n");
          in_print_as_arrays (e_pgs, e_keys, len);
          i_log_failure ("Actual:\n");
          in_print (actual);
        }

      test_assert_type_equal (pg, e_pgs[i], pgno, PRpgno);
      test_assert_type_equal (key, e_keys[i], b_size, PRb_size);
    }
}

#endif
