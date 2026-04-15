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

// numstore
#include "paging/pages/page.h"

#include "tlclib/dev/error.h"
#include "tlclib/core/random.h"
#include "numstore/errors.h"
#include "paging/pages/data_list.h"
#include "paging/pages/fsm_page.h"
#include "paging/pages/inner_node.h"
#include "paging/pages/root_node.h"
#include "paging/pages/var_hash_page.h"
#include "paging/pages/var_page.h"
#include "paging/pages/var_tail.h"
#include "test/testing.h"

////////////////////////////////////////////////////////////
// INITIALIZATION

void
page_init_empty (page *p, const enum page_type type)
{
  page_set_type (p, type);
  page_set_page_lsn (p, PGNO_NULL);
  page_set_checksum (p, 0);

  switch (type)
    {
    case PG_DATA_LIST:
      {
        dl_init_empty (p);
        return;
      }
    case PG_INNER_NODE:
      {
        in_init_empty (p);
        return;
      }
    case PG_FREE_SPACE_MAP:
      {
        fsm_init_empty (p);
        return;
      }
    case PG_ROOT_NODE:
      {
        rn_init_empty (p);
        return;
      }
    case PG_VAR_PAGE:
      {
        vp_init_empty (p);
        return;
      }
    case PG_VAR_TAIL:
      {
        vt_init_empty (p);
        return;
      }
    case PG_VAR_HASH_PAGE:
      {
        vh_init_empty (p);
        return;
      }
    }
  UNREACHABLE ();
}

err_t
page_validate_for_db (const page *p, const int flags, error *e)
{
  ASSERT (p);

  if (flags & PG_PERMISSIVE)
    {
      return SUCCESS;
    }

  const pgh header = page_get_type (p);

  if (!(header & flags))
    {
      return error_causef (e, ERR_CORRUPT, "expected page type %d, got %d",
                           flags, header);
    }

  if (!(flags & PG_SKIP_CHECKSUM))
    {
      const u32 actual_checksum = page_get_checksum (p);
      const u32 expected_checksum = page_compute_checksum (p);

      if (actual_checksum != expected_checksum)
        {
          return error_causef (e, ERR_CORRUPT,
                               "Corrupt page. Expected checksum: "
                               "%d Actual Checksum: %d",
                               expected_checksum, actual_checksum);
        }
    }

  if (header == PG_TRASH)
    {
      return SUCCESS;
    }

  switch ((enum page_type)header)
    {
    case PG_DATA_LIST:
      {
        return dl_validate_for_db (p, e);
      }
    case PG_INNER_NODE:
      {
        return in_validate_for_db (p, e);
      }
    case PG_FREE_SPACE_MAP:
      {
        return fsm_validate_for_db (p, e);
      }
    case PG_ROOT_NODE:
      {
        return rn_validate_for_db (p, e);
      }
    case PG_VAR_PAGE:
      {
        return vp_validate_for_db (p, e);
      }
    case PG_VAR_TAIL:
      {
        return vt_validate_for_db (p, e);
      }
    case PG_VAR_HASH_PAGE:
      {
        return vh_validate_for_db (p, e);
      }
    }

  UNREACHABLE ();
}

////////////////////////////////////////////////////////////
// SETTERS

#ifndef NTEST
TEST (TT_UNIT, page_set_get_simple)
{
  page p, q;
  rand_bytes (p.raw, PAGE_SIZE);
  rand_bytes (q.raw, PAGE_SIZE);

  TEST_CASE ("Initialization: each page type")
  {
    page_init_empty (&p, PG_DATA_LIST);
    test_assert_int_equal (page_get_type (&p), PG_DATA_LIST);

    page_init_empty (&p, PG_INNER_NODE);
    test_assert_int_equal (page_get_type (&p), PG_INNER_NODE);

    page_init_empty (&p, PG_FREE_SPACE_MAP);
    test_assert_int_equal (page_get_type (&p), PG_FREE_SPACE_MAP);

    page_init_empty (&p, PG_ROOT_NODE);
    test_assert_int_equal (page_get_type (&p), PG_ROOT_NODE);
  }

  TEST_CASE ("Setters / Getters: checksum + page_lsn + type")
  {
    page_init_empty (&p, PG_DATA_LIST);

    page_set_checksum (&p, 0xDEADBEEF);
    test_assert_int_equal (page_get_checksum (&p), 0xDEADBEEF);

    page_set_page_lsn (&p, 1234);
    test_assert_int_equal (page_get_page_lsn (&p), 1234);

    page_set_type (&p, PG_INNER_NODE);
    test_assert_int_equal (page_get_type (&p), PG_INNER_NODE);
  }

  TEST_CASE ("page_memcpy roundtrip")
  {
    const struct bytes src = { .head = q.raw, .len = PAGE_SIZE };
    page_memcpy (&p, src);
    test_assert_memequal (p.raw, q.raw, PAGE_SIZE);
  }
}
#endif

void
i_log_page (const int log_level, const page *p)
{
  switch ((enum page_type)page_get_type (p))
    {
    case PG_DATA_LIST:
      {
        i_log_dl (log_level, p);
        return;
      }
    case PG_INNER_NODE:
      {
        i_log_in (log_level, p);
        return;
      }
    case PG_FREE_SPACE_MAP:
      {
        i_log_fsm (log_level, p);
        return;
      }
    case PG_ROOT_NODE:
      {
        i_log_rn (log_level, p);
        return;
      }
    case PG_VAR_PAGE:
      {
        i_log_vp (log_level, p);
        return;
      }
    case PG_VAR_TAIL:
      {
        i_log_vt (log_level, p);
        return;
      }
    case PG_VAR_HASH_PAGE:
      {
        i_log_vh (log_level, p);
        return;
      }
    }
  // UNREACHABLE ();
}
