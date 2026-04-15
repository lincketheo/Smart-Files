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

#include "paging/pages/data_list.h"
#include "paging/pages/inner_node.h"
#include "paging/pages/page.h"
#include "paging/pages/var_page.h"
#include "paging/pages/var_tail.h"

typedef union
{
  struct dl_data dl;
  struct in_data in;
} in_dl_data;

#define dlgt_move_all_right(src, dest) \
  dlgt_move_right (src, dest, dlgt_get_len (src))
#define dlgt_move_all_left(dest, src) \
  dlgt_move_left (dest, src, dlgt_get_len (src))

HEADER_FUNC pgno
dlgt_get_next (const page *p)
{
  switch (page_get_type (p))
    {
    case PG_INNER_NODE:
      {
        return in_get_next (p);
      }
    case PG_DATA_LIST:
      {
        return dl_get_next (p);
      }
    case PG_VAR_PAGE:
      {
        return vp_get_next (p);
      }
    case PG_VAR_TAIL:
      {
        return vt_get_next (p);
      }
    default:
      {
        UNREACHABLE ();
      }
    }
}

HEADER_FUNC pgno
dlgt_get_ovnext (const page *p)
{
  switch (page_get_type (p))
    {
    case PG_VAR_PAGE:
      {
        return vp_get_ovnext (p);
      }
    case PG_VAR_TAIL:
      {
        return vt_get_next (p);
      }
    default:
      {
        UNREACHABLE ();
      }
    }
}

HEADER_FUNC void
dlgt_set_next (page *p, const pgno n)
{
  switch (page_get_type (p))
    {
    case PG_INNER_NODE:
      {
        in_set_next (p, n);
        break;
      }
    case PG_DATA_LIST:
      {
        dl_set_next (p, n);
        break;
      }
    case PG_VAR_PAGE:
      {
        vp_set_next (p, n);
        break;
      }
    case PG_VAR_TAIL:
      {
        vt_set_next (p, n);
        break;
      }
    default:
      {
        UNREACHABLE ();
      }
    }
}

HEADER_FUNC pgno
dlgt_get_prev (const page *p)
{
  switch (page_get_type (p))
    {
    case PG_INNER_NODE:
      {
        return in_get_prev (p);
      }
    case PG_DATA_LIST:
      {
        return dl_get_prev (p);
      }
    default:
      {
        UNREACHABLE ();
      }
    }
}

HEADER_FUNC void
dlgt_set_prev (page *p, const pgno prev)
{
  switch (page_get_type (p))
    {
    case PG_INNER_NODE:
      {
        in_set_prev (p, prev);
        break;
      }
    case PG_DATA_LIST:
      {
        dl_set_prev (p, prev);
        break;
      }
    default:
      {
        UNREACHABLE ();
      }
    }
}

HEADER_FUNC void
dlgtset_ovnext (page *p, const pgno next)
{
  switch (page_get_type (p))
    {
    case PG_VAR_PAGE:
      {
        vp_set_ovnext (p, next);
        break;
      }
    case PG_VAR_TAIL:
      {
        vt_set_next (p, next);
        break;
      }
    default:
      {
        UNREACHABLE ();
      }
    }
}

HEADER_FUNC p_size
dlgt_get_len (const page *p)
{
  switch (page_get_type (p))
    {
    case PG_INNER_NODE:
      {
        return in_get_len (p);
      }
    case PG_DATA_LIST:
      {
        return dl_used (p);
      }
    default:
      {
        UNREACHABLE ();
      }
    }
}

HEADER_FUNC b_size
dlgt_get_size (const page *p)
{
  switch (page_get_type (p))
    {
    case PG_INNER_NODE:
      {
        return in_get_size (p);
      }
    case PG_DATA_LIST:
      {
        return dl_used (p);
      }
    default:
      {
        UNREACHABLE ();
      }
    }
}

HEADER_FUNC p_size
dlgt_get_max_len (const page *p)
{
  switch (page_get_type (p))
    {
    case PG_INNER_NODE:
      {
        return IN_MAX_KEYS;
      }
    case PG_DATA_LIST:
      {
        return DL_DATA_SIZE;
      }
    default:
      {
        UNREACHABLE ();
      }
    }
}

HEADER_FUNC bool
dlgt_is_root (const page *p)
{
  return dlgt_get_prev (p) == PGNO_NULL && dlgt_get_next (p) == PGNO_NULL;
}

HEADER_FUNC bool
dlgt_is_full (const page *p)
{
  switch (page_get_type (p))
    {
    case PG_INNER_NODE:
      {
        return in_full (p);
      }
    case PG_DATA_LIST:
      {
        return dl_full (p);
      }
    default:
      {
        UNREACHABLE ();
      }
    }
}

HEADER_FUNC struct bytes
dlgt_get_bytes (page *p)
{
  switch (page_get_type (p))
    {
    case PG_VAR_PAGE:
      {
        return vp_get_bytes (p);
      }
    case PG_VAR_TAIL:
      {
        return vt_get_bytes (p);
      }
    default:
      {
        UNREACHABLE ();
      }
    }
}

HEADER_FUNC struct cbytes
dlgt_get_bytes_imut (const page *p)
{
  switch (page_get_type (p))
    {
    case PG_VAR_PAGE:
      {
        return vp_get_bytes_imut (p);
      }
    case PG_VAR_TAIL:
      {
        return vt_get_bytes_imut (p);
      }
    default:
      {
        UNREACHABLE ();
      }
    }
}

HEADER_FUNC void
dlgt_set_data (page *p, const in_dl_data d)
{
  switch (page_get_type (p))
    {
    case PG_INNER_NODE:
      {
        in_set_data (p, d.in);
        break;
      }
    case PG_DATA_LIST:
      {
        dl_set_data (p, d.dl);
        break;
      }
    default:
      {
        UNREACHABLE ();
      }
    }
}

HEADER_FUNC void
dlgt_move_left (page *dest, page *src, const p_size len)
{
  ASSERT (page_get_type (dest) == page_get_type (src));

  switch (page_get_type (src))
    {
    case PG_INNER_NODE:
      {
        in_move_left (dest, src, len);
        break;
      }
    case PG_DATA_LIST:
      {
        dl_move_left (dest, src, len);
        break;
      }
    default:
      {
        UNREACHABLE ();
      }
    }
}

HEADER_FUNC void
dlgt_move_right (page *src, page *dest, const p_size len)
{
  ASSERT (page_get_type (dest) == page_get_type (src));

  switch (page_get_type (src))
    {
    case PG_INNER_NODE:
      {
        in_move_right (src, dest, len);
        break;
      }
    case PG_DATA_LIST:
      {
        dl_move_right (src, dest, len);
        break;
      }
    default:
      {
        UNREACHABLE ();
      }
    }
}

HEADER_FUNC void
dlgt_link (page *left, page *right)
{
  pgno _left = PGNO_NULL;
  pgno _right = PGNO_NULL;

  if (left)
    {
      _left = left->pg;
    }

  if (right)
    {
      _right = right->pg;
    }

  if (left)
    {
      dlgt_set_next (left, _right);
    }

  if (right)
    {
      dlgt_set_prev (right, _left);
    }
}

HEADER_FUNC void
dlgtovlink (page *left, const page *right)
{
  ASSERT (left);
  pgno _right = PGNO_NULL;

  if (right)
    {
      _right = right->pg;
    }

  dlgtset_ovnext (left, _right);
}

HEADER_FUNC bool
dlgt_valid_neighbors (const page *left, const page *right)
{
  pgno lpg = PGNO_NULL;
  pgno rpg = PGNO_NULL;

  if (left)
    {
      lpg = left->pg;
    }

  if (right)
    {
      rpg = right->pg;
    }

  bool ret = true;

  if (left)
    {
      ret = ret && dlgt_get_next (left) == rpg;
    }

  if (right)
    {
      ret = ret && lpg == dlgt_get_prev (right);
    }

  return ret;
}

HEADER_FUNC void
make_valid (page *p)
{
  switch (page_get_type (p))
    {
    case PG_DATA_LIST:
      {
        dl_make_valid (p);
        break;
      }
    case PG_INNER_NODE:
      {
        in_make_valid (p);
        break;
      }
    default:
      {
        return;
      }
    }
}
