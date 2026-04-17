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
#include "pages/page_delegate.h"
#include "c_specx.h"

#include <string.h>

/*
 * page_frame: one slot in the buffer pool.
 *
 * A frame in PHM_S (shared/read) mode is the authoritative on-disk image.
 * The pgno_to_value hash table always points to the read frame.  When a page
 * is upgraded for writing, a second frame is allocated; pgr->wsibling gives
 * its index.  The write frame is flagged PW_X and is invisible to the hash
 * table — callers reach it only through a page_h in PHM_X mode.
 *
 * pin counts outstanding page_h references to this frame.  A pinned frame
 * may not be evicted.  PW_ACCESS is set on every logical access and cleared
 * by the clock sweep, implementing the second-chance eviction policy.
 * PW_DIRTY is set when the read frame's content has been superseded by an
 * in-progress write and cleared once the page is flushed to disk.
 */
struct page_frame
{
  page page;

  // Locked by the pager lock
  u32 pin;
  u32 flags;
  i32 wsibling; // index of the paired write frame, or -1
};

/*
 * page_h: an in-flight reference to a buffer pool page.
 *
 * A page_h is the handle passed between callers and the pager. It is always
 * in one of three modes:
 *
 *   PHM_NONE  - no page is held; pgr and pgw are NULL.
 *   PHM_S     - the page is held for reading; pgr points to the read frame,
 *               pgw is NULL.  page_h_r() returns the read frame's page.
 *   PHM_X     - the page is held for writing; pgr points to the read frame
 *               (containing the pre-update undo image), pgw points to a
 *               shadow write frame (containing the in-progress redo image).
 *               page_h_w() returns the write frame's page.  Only the write
 *               frame should be modified; the read frame is left intact so
 *               that pgr_release() can use it as the undo image in the WAL
 *               UPDATE record.
 *
 * page_h values are owned by the function that acquired them.  Every
 * non-NONE page_h must be released (pgr_release) or cancelled (pgr_cancel)
 * before the owning function returns.  Ownership can be transferred with
 * page_h_xfer_ownership / page_h_xfer_ownership_ptr.
 */
typedef struct
{
  enum
  {
    PHM_NONE,
    PHM_S,
    PHM_X,
  } mode;

  // Read context stuff
  struct
  {
    struct page_frame *pgr;
  };

  // Write context stuff
  struct
  {
    struct page_frame *pgw;
    struct txn *tx;
  };
} page_h;

DEFINE_DBG_ASSERT (page_h, page_h, h, {
  ASSERT (h);

  switch (h->mode)
    {
    case PHM_S:
      {
        ASSERT (h->pgr);
        ASSERT (h->pgw == NULL);
        // ASSERT (h->pgr->wsibling == -1);
        break;
      }
    case PHM_X:
      {
        ASSERT (h->pgr);
        ASSERT (h->pgw);
        ASSERT (h->pgr->wsibling >= 0);
        break;
      }
    case PHM_NONE:
      {
        ASSERT (h->pgr == NULL);
        ASSERT (h->pgw == NULL);
        break;
      }
    }
})

#define page_h_create() \
  (page_h) { .mode = PHM_NONE, .pgr = NULL, .pgw = NULL, }

HEADER_FUNC void
page_h_xfer_ownership_ptr (page_h *dest, page_h *src)
{
  DBG_ASSERT (page_h, dest);
  DBG_ASSERT (page_h, src);
  if (dest->mode != PHM_NONE)
    {
      ASSERT (dest->mode == PHM_NONE);
      UNREACHABLE ();
    }
  *dest = *src;
  src->mode = PHM_NONE;
  src->pgr = NULL;
  src->pgw = NULL;
}

HEADER_FUNC page_h
page_h_xfer_ownership (page_h *h)
{
  DBG_ASSERT (page_h, h);
  const page_h ret = *h;
  h->mode = PHM_NONE;
  h->pgr = NULL;
  h->pgw = NULL;
  return ret;
}

HEADER_FUNC const page *
page_h_r (const page_h *h)
{
  DBG_ASSERT (page_h, h);
  if (h->mode != PHM_S)
    {
      ASSERT (h->mode == PHM_S);
      UNREACHABLE ();
    }
  return &h->pgr->page;
}

HEADER_FUNC page *
page_h_w (const page_h *h)
{
  DBG_ASSERT (page_h, h);
  if (h->mode != PHM_X)
    {
      ASSERT (h->mode == PHM_X);
      UNREACHABLE ();
    }
  return &h->pgw->page;
}

HEADER_FUNC page *
page_h_w_or_null (const page_h *h)
{
  DBG_ASSERT (page_h, h);
  if (h->mode == PHM_NONE)
    {
      return NULL;
    }
  if (h->mode != PHM_X)
    {
      ASSERT (h->mode == PHM_X);
      UNREACHABLE ();
    }
  return &h->pgw->page;
}

HEADER_FUNC const page *
page_h_ro (const page_h *h)
{
  DBG_ASSERT (page_h, h);
  if (h->mode == PHM_X)
    {
      return &h->pgw->page;
    }
  else if (h->mode == PHM_S)
    {
      return &h->pgr->page;
    }

  ASSERT (h->mode != PHM_NONE);
  UNREACHABLE ();
}

HEADER_FUNC const page *
page_h_ro_or_null (const page_h *h)
{
  DBG_ASSERT (page_h, h);
  if (h->mode == PHM_NONE)
    {
      return NULL;
    }
  return page_h_ro (h);
}

HEADER_FUNC pgno
page_h_pgno (const page_h *h)
{
  DBG_ASSERT (page_h, h);
  const page *p = NULL;
  if (h->mode == PHM_X)
    {
      p = &h->pgw->page;
    }
  else if (h->mode == PHM_S)
    {
      p = &h->pgr->page;
    }
  else
    {
      ASSERT (h->mode != PHM_NONE);
      UNREACHABLE ();
    }
  return p->pg;
}

HEADER_FUNC pgno
page_h_pgno_or_null (const page_h *h)
{
  if (h->mode == PHM_NONE)
    {
      return PGNO_NULL;
    }
  return page_h_pgno (h);
}

HEADER_FUNC enum page_type
page_h_type (const page_h *h)
{
  DBG_ASSERT (page_h, h);
  const page *p = NULL;
  if (h->mode == PHM_X)
    {
      p = &h->pgw->page;
    }
  else if (h->mode == PHM_S)
    {
      p = &h->pgr->page;
    }
  else
    {
      ASSERT (h->mode != PHM_NONE);
      UNREACHABLE ();
    }
  return page_get_type (p);
}

HEADER_FUNC struct in_pair
in_pair_from_pgh (const page_h *pg)
{
  if (pg->mode == PHM_NONE)
    {
      return in_pair_empty;
    }
  return (struct in_pair){
    .pg = page_h_pgno (pg),
    .key = dlgt_get_size (page_h_ro (pg)),
  };
}
