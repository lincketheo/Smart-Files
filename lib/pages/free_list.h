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

#pragma once

#include "pages/page.h"
#include "tombstone.h"

// OFFSETS and _Static_asserts
#define FL_NEXT_OFST PG_COMMN_END

HEADER_FUNC void
frlst_set_next (page *p, pgno pg)
{
  PAGE_SIMPLE_SET_IMPL (p, pg, TS_NEXT_OFST);
}

HEADER_FUNC void
frlst_init_empty (page *frlst)
{
  ASSERT (page_get_type (frlst) == PG_TOMBSTONE);
  frlst_set_next (frlst, PGNO_NULL);
}

HEADER_FUNC pgno
frlst_get_next (const page *p)
{
  PAGE_SIMPLE_GET_IMPL (p, pgno, TS_NEXT_OFST);
}

// Validation
err_t frlst_validate_for_db (const page *hl, error *e);

// Utils
void i_log_frlst (int level, const page *t);
