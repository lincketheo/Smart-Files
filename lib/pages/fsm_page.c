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

#include "pages/fsm_page.h"

#include "c_specx.h"
#include "pages/page.h"

void
fsm_init_empty (page *in)
{
  ASSERT (page_get_type (in) == PG_FREE_SPACE_MAP);
  memset (fsm_get_bitmap_mut (in), 0, FS_BTMP_SIZE);
  fsm_set_bit (in, 0); // First bit is always set (that's me)
}

err_t
fsm_validate_for_db (const page *hl, error *e)
{
  const pgh header = page_get_type (hl);

  if (header != (pgh)PG_FREE_SPACE_MAP)
    {
      return error_causef (e, ERR_CORRUPT,
                           "expected header: %" PRpgh " but got: %" PRpgh,
                           (pgh)PG_FREE_SPACE_MAP, (pgh)header);
    }

  return SUCCESS;
}

// Utils
void
i_log_fsm (const int level, const page *t)
{
  i_log (level, "=== FREE SPACE PAGE START ===\n");
  for (p_size i = 0; i < FS_BTMP_NPGS; ++i)
    {
      if (fsm_get_bit (t, i))
        {
          i_printf (level, "|%" PRp_size "| -- Occupied\n", i);
        }
    }
  i_log (level, "=== FREE SPACE PAGE END ===\n");
}
