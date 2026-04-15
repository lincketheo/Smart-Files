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

#include "paging/pages/var_hash_page.h"

#include "core/error.h"
#include "intf/logging.h"

DEFINE_DBG_ASSERT (page, vh_page, d, { ASSERT (d); })

void
vh_init_empty (page *p)
{
  ASSERT (page_get_type (p) == PG_VAR_HASH_PAGE);
  for (p_size i = 0; i < VH_HASH_LEN; ++i)
    {
      vh_set_hash_value (p, i, PGNO_NULL);
    }
}

// Validation
err_t
vh_validate_for_db (const page *p, error *e)
{
  DBG_ASSERT (vh_page, p);

  if (page_get_type (p) != PG_VAR_HASH_PAGE)
    {
      return error_causef (e, ERR_CORRUPT,
                           "wrong page type for var hash page");
    }

  return SUCCESS;
}

// Utils
void
i_log_vh (const int level, const page *vh)
{
  i_log (level, "=== VAR HASH TABLE PAGE START ===\n");

  bool empty = true;

  for (p_size i = 0; i < VH_HASH_LEN; ++i)
    {
      const pgno p = vh_get_hash_value (vh, i);
      if (p != PGNO_NULL)
        {
          empty = false;
          i_printf (level, "[%" PRp_size "]: %" PRpgno "\n", i, p);
        }
    }

  if (empty)
    {
      i_printf (level, "Empty\n");
    }

  i_log (level, "=== VAR HASH TABLE PAGE END ===\n");
}
