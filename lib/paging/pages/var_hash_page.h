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

#include "core/hashing.h"
#include "core/string.h"
#include "paging/pages/page.h"

/*
 * PG_VAR_HASH_PAGE — root of the variable name hash table.
 *
 * There is exactly one of these pages in every database, at VHASH_PGNO.
 * It contains an array of VH_HASH_LEN pgno slots, each the head of a
 * singly-linked chain of PG_VAR_PAGE nodes.  A variable name is looked up
 * by hashing the name (FNV-1a) modulo VH_HASH_LEN to find the bucket, then
 * walking the PG_VAR_PAGE chain until the name matches or PGNO_NULL is
 * reached.
 *
 * Because all bucket pointers live in a single page, creating or deleting a
 * variable always requires a write to this page.  Collisions are resolved by
 * chaining via vp_get_next/vp_set_next on the PG_VAR_PAGE nodes.
 */

////////////////////////////////////////////////////////////
/////// VAR HASH PAGE

// ============ PAGE START
// HEADER
// HASH0     [pgno]
// HASH1     [pgno]
// ...
// HASHn     [pgno]
// 0         Maybe extra space
// ============ PAGE END

// OFFSETS and _Static_asserts
#define VH_HASH_OFST PG_COMMN_END
#define VH_HASH_LEN ((PAGE_SIZE - VH_HASH_OFST) / sizeof (pgno))

_Static_assert (PAGE_SIZE > VH_HASH_OFST + 10 * sizeof (pgno),
                "Root Page: PAGE_SIZE must be > RN_HASH_OFST plus at least 10 "
                "extra hashes");

// Initialization
void vh_init_empty (page *p);

// Getters
HEADER_FUNC p_size
vh_get_hash_pos (const struct string vname)
{
  return (p_size)fnv1a_hash (vname) % (VH_HASH_LEN);
}

HEADER_FUNC pgno
vh_get_hash_value (const page *p, const p_size pos)
{
  ASSERT (pos < VH_HASH_LEN);
  PAGE_SIMPLE_GET_IMPL (p, pgno, VH_HASH_OFST + pos * sizeof (pgno));
}

// Setters
HEADER_FUNC void
vh_set_hash_value (page *p, const p_size pos, const pgno value)
{
  ASSERT (pos < VH_HASH_LEN);
  PAGE_SIMPLE_SET_IMPL (p, value, VH_HASH_OFST + pos * sizeof (pgno));
}

// Validation
err_t vh_validate_for_db (const page *p, error *e);

// Utils
void i_log_vh (int level, const page *vh);
