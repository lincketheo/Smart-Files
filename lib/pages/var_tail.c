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

#include "pages/var_tail.h"

#include "pages/page.h"
#include "c_specx.h"

DEFINE_DBG_ASSERT (page, vt_page, v, { ASSERT (v); })

////////////////////////////////////////////////////////////
// INITIALIZATION

#ifndef NTEST
TEST (vt_init_empty)
{
  page p;

  rand_bytes (p.raw, PAGE_SIZE);
  page_init_empty (&p, PG_VAR_TAIL);

  test_assert_equal (vt_get_next (&p), PGNO_NULL);
}
#endif

////////////////////////////////////////////////////////////
// VALIDATION

err_t
vt_validate_for_db (const page *p, error *e)
{
  if (page_get_type (p) != PG_VAR_TAIL)
    {
      return error_causef (e, ERR_CORRUPT,
                           "wrong page type for var tail node");
    }
  return SUCCESS;
}

#ifndef NTEST
TEST (vt_validate)
{
  page sut;
  error e = error_create ();

  TEST_CASE ("Invalid page type")
  {
    page_init_empty (&sut, PG_DATA_LIST);
    test_err_t_check (vt_validate_for_db (&sut, &e), ERR_CORRUPT, &e);
  }
}
#endif

////////////////////////////////////////////////////////////
// UTILS

void
i_log_vt (const int level, const page *vp)
{
  i_log (level, "=== VARIABLE TAIL START ===\n");

  i_printf (level, "PGNO:   %" PRpgno "\n", vp->pg);
  if (vt_get_next (vp) == PGNO_NULL)
    {
      i_printf (level, "NEXT:   NULL\n");
    }
  else
    {
      i_printf (level, "NEXT:   %" PRpgno "\n", vt_get_next (vp));
    }

  i_log (level, "=== VARIABLE TAIL END ===\n");
}
