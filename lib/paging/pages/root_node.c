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

#include "paging/pages/root_node.h"

#include "core/hashing.h"
#include "core/random.h"
#include "core/string.h"
#include "intf/logging.h"
#include "numstore/types.h"
#include "paging/pages/page.h"
#include "test/testing.h"

// numstore
DEFINE_DBG_ASSERT (page, rn_page, d, { ASSERT (d); })

////////////////////////////////////////////////////////////
// INITIALIZATION

#ifndef NTEST
TEST (TT_UNIT, rn_init_empty_and_zeroes)
{
  page p;

  rand_bytes (p.raw, PAGE_SIZE);
  page_init_empty (&p, PG_ROOT_NODE);

  // Type set correctly
  test_assert_int_equal (page_get_type (&p), PG_ROOT_NODE);
}
#endif

#ifndef NTEST
TEST (TT_UNIT, rn_init_empty)
{
  page p;

  rand_bytes (p.raw, PAGE_SIZE);
  page_init_empty (&p, PG_ROOT_NODE);

  test_assert_equal (rn_get_first_tmbst (&p), 1);
  test_assert_equal (rn_get_master_lsn (&p), 0);
}
#endif

err_t
rn_validate_for_db (const page *p, error *e)
{
  if (page_get_type (p) != PG_ROOT_NODE)
    {
      return error_causef (e, ERR_CORRUPT, "wrong page type for root node");
    }
  return SUCCESS;
}

#ifndef NTEST
TEST (TT_UNIT, rn_validate_for_db)
{
  error e = error_create ();
  page p;

  TEST_CASE ("Invalid header -> ERR_CORRUPT")
  {
    rand_bytes (p.raw, PAGE_SIZE);
    page_set_type (&p, PG_DATA_LIST);
    test_assert_int_equal (rn_validate_for_db (&p, &e), ERR_CORRUPT);
    e.cause_code = SUCCESS;
  }

  TEST_CASE ("Valid header -> SUCCESS")
  {
    rand_bytes (p.raw, PAGE_SIZE);
    page_init_empty (&p, PG_ROOT_NODE);
    test_assert_int_equal (rn_validate_for_db (&p, &e), SUCCESS);
  }

  TEST_CASE ("Invalid header -> ERR_CORRUPT")
  {
    rand_bytes (p.raw, PAGE_SIZE);
    page_set_type (&p, PG_DATA_LIST);
    test_assert_int_equal (rn_validate_for_db (&p, &e), ERR_CORRUPT);
    e.cause_code = SUCCESS;
  }

  TEST_CASE ("Valid header -> SUCCESS")
  {
    rand_bytes (p.raw, PAGE_SIZE);
    page_init_empty (&p, PG_ROOT_NODE);
    test_assert_int_equal (rn_validate_for_db (&p, &e), SUCCESS);
  }
}
#endif

#ifndef NTEST
TEST (TT_UNIT, rn_set_get_simple)
{
  page p;

  rand_bytes (p.raw, PAGE_SIZE);
  page_init_empty (&p, PG_ROOT_NODE);

  test_assert_type_equal (rn_get_first_tmbst (&p), 1, pgno, PRpgno);
  test_assert_type_equal (rn_get_master_lsn (&p), (lsn)0, lsn, PRlsn);

  rn_set_first_tmbst (&p, 2);
  rn_set_master_lsn (&p, 3);

  test_assert_type_equal (rn_get_first_tmbst (&p), 2, pgno, PRpgno);
  test_assert_type_equal (rn_get_master_lsn (&p), (lsn)3, lsn, PRlsn);
}
#endif

////////////////////////////////////////////////////////////
// UTILS

void
i_log_rn (const int level, const page *rn)
{
  i_log (level, "=== ROOT NODE PAGE START ===\n");

  i_printf (level, "PGNO: %" PRpgno "\n", rn->pg);
  i_printf (level, "FIRST TMBST: %" PRpgno "\n", rn_get_first_tmbst (rn));
  i_printf (level, "MASTER_LSN:  %" PRlsn "\n", rn_get_master_lsn (rn));

  i_log (level, "=== ROOT NODE PAGE END ===\n");
}
