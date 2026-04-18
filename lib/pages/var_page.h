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

#include "c_specx.h"
#include "pages/page.h"

////////////////////////////////////////////////////////////
/////// VAR PAGE

// ============ PAGE START
// HEADER
// NEXT     [pgno]      - Next var page in the hash table chain
// OVNEXT   [pgno]      - Next Overflow page for name / serialized type
// VLEN     [2 bytes]   - Length of the name string in bytes
// ROOT     [pgno]      - Root page of the rptree chain
// NBYTES   [b_size]    - Root page of the rptree chain
// VNAME
// VNAME
// VNAME
// ...
// TSTR
// TSTR
// TSTR
// ...
// ============ PAGE END

// OFFSETS and _Static_asserts
#define VP_NEXT_OFST PG_COMMN_END
#define VP_OVNX_OFST ((p_size)(VP_NEXT_OFST + sizeof (pgno)))
#define VP_VLEN_OFST ((p_size)(VP_OVNX_OFST + sizeof (pgno)))
#define VP_ROOT_OFST ((p_size)(VP_VLEN_OFST + sizeof (u16)))
#define VP_NBYT_OFST ((p_size)(VP_ROOT_OFST + sizeof (pgno)))
#define VP_VNME_OFST ((p_size)(VP_NBYT_OFST + sizeof (b_size)))
#define VP_MAX_LEN (PAGE_SIZE - VP_VNME_OFST)

// Initialization
void vp_init_empty (page *p);

// Setters
void vp_set_next (page *p, pgno pg);
void vp_set_ovnext (page *p, pgno pg);
void vp_set_vlen (page *p, u16 vlen);
void vp_set_root (page *p, pgno root);
void vp_set_nbytes (page *p, b_size nbytes);

// Getters
pgno vp_get_next (const page *p);
pgno vp_get_ovnext (const page *p);
u16 vp_get_vlen (const page *p);
pgno vp_get_root (const page *p);
b_size vp_get_nbytes (const page *p);

b_size vp_calc_tofst (const page *p);
bool vp_is_overflow (const page *p);
struct bytes vp_get_bytes (page *p);
struct cbytes vp_get_bytes_imut (const page *p);

// Validation
err_t vp_validate_for_db (const page *p, error *e);

// Utils
void i_log_vp (int level, const page *vp);
