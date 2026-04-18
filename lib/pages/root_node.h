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

/*
 * PG_ROOT_NODE — the database bootstrap page (always at ROOT_PGNO = 0).
 *
 * This is the first page read on every database open.  It holds two fields:
 *
 *   FSTS (first tombstone) — the pgno of the first unused/deleted page in
 *   a tombstone freelist (currently appears to be page 1, seeding FSM
 *   discovery).
 *
 *   MLSN (master LSN) — the LSN of the CKPT_BEGIN record from the most
 *   recent successful fuzzy checkpoint.  On restart, ARIES reads this value
 *   and begins its analysis/redo/undo pass from that point in the WAL.
 *   A value of 0 means no checkpoint has been taken yet; analysis starts
 *   from the beginning of the WAL.
 */

// ============ PAGE START
// HEADER
// FSTS     [pgno]  - First tombstone
// MLSN     [lsn]   - Master lsn
// ============ PAGE END

// OFFSETS and _Static_asserts
#define RN_FSTS_OFST PG_COMMN_END                             // First tombstone
#define RN_MLSN_OFST ((p_size)(RN_FSTS_OFST + sizeof (pgno))) // Master LSN

// Initialization

// Setters
HEADER_FUNC void
rn_set_first_tmbst (page *p, const pgno pg)
{
  PAGE_SIMPLE_SET_IMPL (p, pg, RN_FSTS_OFST);
}

HEADER_FUNC void
rn_set_master_lsn (page *p, const lsn pg)
{
  PAGE_SIMPLE_SET_IMPL (p, pg, RN_MLSN_OFST);
}

HEADER_FUNC void
rn_init_empty (page *rn)
{
  ASSERT (page_get_type (rn) == PG_ROOT_NODE);
  rn_set_first_tmbst (rn, 1);
  rn_set_master_lsn (rn, 0);
}

// Getters
HEADER_FUNC pgno
rn_get_first_tmbst (const page *p)
{
  PAGE_SIMPLE_GET_IMPL (p, pgno, RN_FSTS_OFST);
}

HEADER_FUNC lsn
rn_get_master_lsn (const page *p)
{
  PAGE_SIMPLE_GET_IMPL (p, pgno, RN_MLSN_OFST);
}

// Validation
err_t rn_validate_for_db (const page *p, error *e);

// Utils
void i_log_rn (int level, const page *rn);
