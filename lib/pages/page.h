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

#include "numstore.h"
#include "numstore/compile_config.h"
#include "tlclib.h"

#include <string.h>

/*
 * Page Layout
 *
 * Every page in the database file is PAGE_SIZE bytes.  The first bytes of
 * every page form a fixed common header:
 *
 *   [0, 4)              u32 checksum   — CRC of all bytes after the checksum
 *                                        field (bytes [4, PAGE_SIZE)).
 *   [4, 4+sizeof(pgh))  pgh type       — page type tag (enum page_type).
 *   [4+sizeof(pgh), ...) lsn pageLSN   — LSN of the last WAL record that
 *                                        modified this page.
 *
 * Page content begins at PG_COMMN_END.  Each page type interprets the
 * remaining bytes differently.
 *
 * The checksum is written by pgr_flush() just before the page is written to
 * disk.  It is checked by page_validate_for_db() when a page is read into
 * the buffer pool, detecting corruption.
 *
 * PG_TRASH is a sentinel type used as the initial value for read frames of
 * newly allocated pages that have not yet been written.  Any attempt to
 * validate a PG_TRASH frame will fail unless PG_PERMISSIVE is supplied.
 */
enum page_type
{
  // Common page types
  PG_FREE_SPACE_MAP = (1 << 0), // A free space map page
  PG_ROOT_NODE = (1 << 1),      // The first page with db level meta data

  // Rptree page types
  PG_DATA_LIST = (1 << 2),  // r+tree data node
  PG_INNER_NODE = (1 << 3), // r+tree Inner node

  // Variable page types
  PG_VAR_HASH_PAGE
  = (1 << 4),             // A Hash Table for variable names - links to a linked list
  PG_VAR_PAGE = (1 << 5), // A Single link in the hash table linked list
  PG_VAR_TAIL = (1 << 6), // Overflow to a VAR_PAGE
};

#define PG_PERMISSIVE (1 << 7)
#define PG_SKIP_CHECKSUM (1 << 8)
#define PG_TRASH ((u8) ~((u8)0))

// COMMON PAGE HEADER
#define PG_CKSM_OFST ((p_size)0)
#define PG_HEDR_OFST ((p_size)(PG_CKSM_OFST + sizeof (u32)))
#define PG_PLSN_OFST ((p_size)(PG_HEDR_OFST + sizeof (pgh)))
#define PG_COMMN_END ((p_size)(PG_PLSN_OFST + sizeof (lsn)))

#define PG_CKSM_DATA_LEN (PAGE_SIZE - PG_CKSM_OFST)

typedef struct
{
  u8 raw[PAGE_SIZE];
  pgno pg;
} page;

DEFINE_DBG_ASSERT (page, page_base, p, { ASSERT (p); })

// Initialization
void page_init_empty (page *p, enum page_type t);

// Validate
err_t page_validate_for_db (const page *p, int page_types, error *e);

////////////////////////////////////////////////////////////
/////// Utility Macros

#define PAGE_SIMPLE_GET_IMPL(v, type, ofst)           \
  do                                                  \
    {                                                 \
      ASSERT ((ofst) + sizeof (type) < PAGE_SIZE);    \
      type ret;                                       \
      memcpy (&(ret), &(v)->raw[ofst], sizeof (ret)); \
      return ret;                                     \
    }                                                 \
  while (0)

#define PAGE_SIMPLE_SET_IMPL(v, val, ofst)            \
  do                                                  \
    {                                                 \
      ASSERT ((ofst) + sizeof (val) < PAGE_SIZE);     \
      memcpy (&(v)->raw[ofst], &(val), sizeof (val)); \
    }                                                 \
  while (0)

////////////////////////////////////////////////////////////
// GETTERS

HEADER_FUNC u32
page_get_checksum (const page *p)
{
  DBG_ASSERT (page_base, p);
  PAGE_SIMPLE_GET_IMPL (p, u32, PG_CKSM_OFST);
}

HEADER_FUNC u32
page_compute_checksum (const page *p)
{
  DBG_ASSERT (page_base, p);
  const void *data = &p->raw[4];
  u32 checksum = checksum_init ();
  checksum_execute (&checksum, data, PAGE_SIZE - 4);
  return checksum;
}

HEADER_FUNC pgh
page_get_type (const page *p)
{
  PAGE_SIMPLE_GET_IMPL (p, pgh, PG_HEDR_OFST);
}

HEADER_FUNC lsn
page_get_page_lsn (const page *p)
{
  DBG_ASSERT (page_base, p);
  PAGE_SIMPLE_GET_IMPL (p, lsn, PG_PLSN_OFST);
}

////////////////////////////////////////////////////////////
// SETTERS

HEADER_FUNC void
page_set_checksum (page *p, const u32 checksum)
{
  DBG_ASSERT (page_base, p);
  PAGE_SIMPLE_SET_IMPL (p, checksum, PG_CKSM_OFST);
}

HEADER_FUNC void
page_set_type (page *p, const enum page_type t)
{
  DBG_ASSERT (page_base, p);
  const pgh _type = t;
  PAGE_SIMPLE_SET_IMPL (p, _type, PG_HEDR_OFST);
}

HEADER_FUNC void
page_set_page_lsn (page *p, const lsn page_lsn)
{
  DBG_ASSERT (page_base, p);
  PAGE_SIMPLE_SET_IMPL (p, page_lsn, PG_PLSN_OFST);
}

HEADER_FUNC void
page_memcpy (page *dest, const struct bytes src)
{
  DBG_ASSERT (page_base, dest);
  ASSERT (src.len == PAGE_SIZE);
  memcpy (dest->raw, src.head, src.len);
}

// Logging
void i_log_page (int log_level, const page *p);
