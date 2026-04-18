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

// numstore
#include "c_specx.h"
#include "pages/page.h"

////////////////////////////////////////////////////////////
/////// VAR PAGE TAIL

// OFFSETS and _Static_asserts
#define VT_NEXT_OFST PG_COMMN_END
#define VT_DATA_OFST ((p_size)(VT_NEXT_OFST + sizeof (pgno)))
#define VT_DATA_LEN (PAGE_SIZE - VT_DATA_OFST)

// Setters
HEADER_FUNC void
vt_set_next (page *p, const pgno pg)
{
  PAGE_SIMPLE_SET_IMPL (p, pg, VT_NEXT_OFST);
}

// Getters
HEADER_FUNC pgno
vt_get_next (const page *p)
{
  PAGE_SIMPLE_GET_IMPL (p, pgno, VT_NEXT_OFST);
}

HEADER_FUNC void
vt_init_empty (page *p)
{
  ASSERT (page_get_type (p) == PG_VAR_TAIL);
  vt_set_next (p, PGNO_NULL);
}

HEADER_FUNC struct bytes
vt_get_bytes (page *p)
{
  return (struct bytes){
    .head = (void *)&p->raw[VT_DATA_OFST],
    .len = PAGE_SIZE - VT_DATA_OFST,
  };
}

HEADER_FUNC struct cbytes
vt_get_bytes_imut (const page *p)
{
  return (struct cbytes){
    .head = (void *)&p->raw[VT_DATA_OFST],
    .len = PAGE_SIZE - VT_DATA_OFST,
  };
}

// Validation
err_t vt_validate_for_db (const page *p, error *e);

// Utils
void i_log_vt (int level, const page *vp);
