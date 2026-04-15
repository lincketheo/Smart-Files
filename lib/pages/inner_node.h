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

/*
 * Inner Node Page (R+Tree index node)
 *
 * The inner node is the indexing layer of the R+Tree.  It maps byte ranges
 * to child pages, enabling O(log N) seeks into the data layer.
 *
 * A node holds up to IN_MAX_KEYS (key, leaf) pairs.  Each key is a b_size
 * representing the number of bytes covered by all data-list pages under the
 * corresponding leaf (child subtree).  A subtree's key is the sum of its
 * leaves' keys.  Given a target byte offset, in_choose_lidx() walks the key
 * array to find the child whose subtree contains that offset.
 *
 * MEMORY LAYOUT (after PG_COMMN_END):
 *   [IN_NEXT_OFST]  pgno next   — right sibling at this inner-node level.
 *   [IN_PREV_OFST]  pgno prev   — left sibling at this inner-node level.
 *   [IN_NLEN_OFST]  p_size len  — number of (key, leaf) pairs currently
 * stored. [IN_LEAF_OFST]  pgno[len]   — child page numbers, packed left to
 * right. [PAGE_SIZE - len*sizeof(b_size)]  b_size[len] — keys, packed right to
 *                                    left (backwards).  The rightmost stored
 *                                    byte is the key of index 0.
 *
 * Storing keys backwards from the end of the page and leaves forward from a
 * fixed offset allows both arrays to grow towards each other, ensuring that
 * any partially-filled node has a predictable layout without wasted space.
 *
 * prev/next links connect inner nodes at the same tree level into a doubly-
 * linked list, which the rebalancing algorithm uses to move keys between
 * adjacent nodes without traversing the tree from the root.  A node with
 * PGNO_NULL for both prev and next is the sole node at its level (i.e. the
 * root of a single-level tree).
 */
struct in_pair
{
  pgno pg;
  b_size key; // number of bytes covered by this child's subtree
};

#define in_pair_from(_pg, _key) \
  (struct in_pair) { .pg = (_pg), .key = (_key), }

#define in_pair_is_empty(o) (o.pg == PGNO_NULL)

struct three_in_pair
{
  struct in_pair prev;
  struct in_pair cur;
  struct in_pair next;
};

#define in_pair_empty \
  (struct in_pair) { .pg = PGNO_NULL }

DEFINE_DBG_ASSERT (page, inner_node, i, { ASSERT (i); })

struct in_data
{
  struct in_pair *nodes;
  u32 len;
};

#define IN_NEXT_OFST PG_COMMN_END
#define IN_PREV_OFST ((p_size)(IN_NEXT_OFST + sizeof (pgno)))
#define IN_NLEN_OFST ((p_size)(IN_PREV_OFST + sizeof (pgno)))
#define IN_LEAF_OFST ((p_size)(IN_NLEN_OFST + sizeof (p_size)))

_Static_assert (
    PAGE_SIZE > IN_LEAF_OFST + 5 * sizeof (b_size) + 6 * sizeof (pgno),
    "Inner Node: PAGE_SIZE must be > IN_LEAF_OFST plus at least 5 keys");

#define IN_MAX_KEYS \
  (p_size) ((PAGE_SIZE - IN_LEAF_OFST) / (sizeof (pgno) + sizeof (b_size)))
#define IN_MIN_KEYS (IN_MAX_KEYS / 2)

_Static_assert (IN_MAX_KEYS > 5, "Inner Node: IN_MAX_KEYS must be > 5");

void in_init_empty (page *in);
err_t in_validate_for_db (const page *in, error *e);
p_size in_page_memcpy_right (pgno *dest, const page *src, p_size ofst);
p_size in_key_memcpy_right (b_size *dest, const page *src, p_size ofst);
void in_push_left (page *in, p_size len);
void in_push_right_permissive (page *in, p_size amnt);
void in_push_left_permissive (page *in, p_size len);
void in_push_all_left (page *in);
void in_cut_left (page *in, p_size end);
void in_data_from_arrays (const struct in_data *dest, const pgno *pgs, const b_size *keys);
void in_set_data (page *p, struct in_data data);
struct in_data in_get_data (const page *p, struct in_pair nodes[IN_MAX_KEYS]);
void in_move_left (page *dest, page *src, p_size len);
void in_move_right (page *src, page *dest, p_size len);
void in_choose_lidx (p_size *idx, b_size *nleft, const page *node, b_size loc);
void i_log_in (int level, const page *in);
void in_make_valid (page *in);

////////////////////////////////////////////////////////////
// /
////////////////////////////////////////////////////////////
// GETTERS

HEADER_FUNC p_size
in_get_len (const page *in)
{
  PAGE_SIMPLE_GET_IMPL (in, p_size, IN_NLEN_OFST);
}

HEADER_FUNC const void *
in_get_backwards_keys_imut (const page *in)
{
  const p_size n = in_get_len (in);
  const p_size nbytes = n * sizeof (b_size);
  ASSERT (nbytes <= PAGE_SIZE);
  if (nbytes > PAGE_SIZE)
    UNREACHABLE_HINT (); // invariant: callers guarantee nbytes fits in page

  if (nbytes == 0)
    {
      return NULL;
    }

  return &in->raw[PAGE_SIZE - nbytes];
}

HEADER_FUNC b_size
in_get_key (const page *in, const p_size idx)
{
  const p_size n = in_get_len (in);
  ASSERT (idx < n);

  const u8 *base = in_get_backwards_keys_imut (in);
  const p_size offset_elems = n - 1 - idx;
  const p_size offset_bytes = offset_elems * sizeof (b_size);

  b_size ret;
  memcpy (&ret, base + offset_bytes, sizeof ret);

  return ret;
}

HEADER_FUNC b_size
in_get_size (const page *in)
{
  b_size ret = 0;
  // TODO - (17) this could be cached

  for (p_size i = 0; i < in_get_len (in); ++i)
    {
      ret += in_get_key (in, i);
    }

  return ret;
}

HEADER_FUNC p_size
in_get_avail (const page *in)
{
  return IN_MAX_KEYS - in_get_len (in);
}

HEADER_FUNC void *
in_get_backwards_keys_mut (page *in)
{
  const p_size n = in_get_len (in);
  const p_size nbytes = n * sizeof (b_size);
  ASSERT (nbytes <= PAGE_SIZE);

  return &in->raw[PAGE_SIZE - nbytes];
}

HEADER_FUNC void *
in_get_backwards_keys_mut_limit (page *in)
{
  const p_size nbytes = IN_MAX_KEYS * sizeof (b_size);
  ASSERT (nbytes <= PAGE_SIZE);
  return &in->raw[PAGE_SIZE - nbytes];
}

HEADER_FUNC void *
in_get_leafs_mut (page *in)
{
  return &in->raw[IN_LEAF_OFST];
}

HEADER_FUNC const void *
in_get_leafs_imut (const page *in)
{
  return &in->raw[IN_LEAF_OFST];
}

#define in_get_right_most_key(in) in_get_key (in, in_get_len (in) - 1)

HEADER_FUNC pgno
in_get_leaf (const page *in, const p_size idx)
{
  const p_size n = in_get_len (in);
  ASSERT (idx < n);

  pgno leaf;

  const u8 *head = in_get_leafs_imut (in);
  memcpy (&leaf, head + idx * sizeof (pgno), sizeof (leaf));

  return leaf;
}

HEADER_FUNC pgno
in_get_next (const page *in)
{
  PAGE_SIMPLE_GET_IMPL (in, pgno, IN_NEXT_OFST);
}

HEADER_FUNC pgno
in_get_prev (const page *in)
{
  PAGE_SIMPLE_GET_IMPL (in, pgno, IN_PREV_OFST);
}

HEADER_FUNC bool
in_is_root (const page *in)
{
  DBG_ASSERT (inner_node, in);
  return in_get_next (in) == PGNO_NULL && in_get_prev (in) == PGNO_NULL;
}

// Shorthands
#define in_get_right_most_key(in) in_get_key (in, in_get_len (in) - 1)
#define in_get_right_most_leaf(in) in_get_leaf (in, in_get_len (in) - 1)
#define in_get_nchildren(in) (in_get_len (in) + 1)
#define in_get_first_leaf(in) in_get_leaf (in, 0)
#define in_get_last_leaf(in) in_get_leaf (in, in_get_len (in) - 1)
#define in_full(in) (in_get_len (in) == IN_MAX_KEYS)

////////////////////////////////////////////////////////////
// /
////////////////////////////////////////////////////////////
// SETTERS

HEADER_FUNC void
in_set_len (page *in, const p_size len)
{
  PAGE_SIMPLE_SET_IMPL (in, len, IN_NLEN_OFST);
}

HEADER_FUNC void
in_set_leaf (page *in, const p_size idx, const pgno pg)
{
  ASSERT (idx < in_get_len (in) + 1);
  PAGE_SIMPLE_SET_IMPL (in, pg, IN_LEAF_OFST + idx * sizeof (pgno));
}

HEADER_FUNC void
in_set_key (page *in, const p_size idx, const b_size key)
{
  const p_size n = in_get_len (in);
  ASSERT (idx < n);

  u8 *base = in_get_backwards_keys_mut (in);
  const p_size offset_elems = n - 1 - idx;
  const p_size offset_bytes = offset_elems * sizeof (b_size);

  memcpy (base + offset_bytes, &key, sizeof key);
}

HEADER_FUNC void
in_set_key_leaf (page *in, const p_size idx, const b_size key, const pgno pg)
{
  in_set_key (in, idx, key);
  in_set_leaf (in, idx, pg);
}

HEADER_FUNC void
in_push_end (page *dest, const b_size key, const pgno pg)
{
  ASSERT (!in_full (dest));
  in_set_len (dest, in_get_len (dest) + 1);
  in_set_key_leaf (dest, in_get_len (dest) - 1, key, pg);
}

HEADER_FUNC void
in_set_leaf_ignore_len (page *in, const p_size idx, const pgno pg)
{
  ASSERT (idx < IN_MAX_KEYS);
  PAGE_SIMPLE_SET_IMPL (in, pg, IN_LEAF_OFST + idx * sizeof (pgno));
}

HEADER_FUNC void
in_set_key_ignore_len (page *in, const p_size idx, const b_size key)
{
  ASSERT (idx < IN_MAX_KEYS);

  u8 *base = in_get_backwards_keys_mut (in);
  const p_size offset_elems = IN_MAX_KEYS - 1 - idx;
  const p_size offset_bytes = offset_elems * sizeof (b_size);

  memcpy (base + offset_bytes, &key, sizeof key);
}

HEADER_FUNC void
in_set_key_leaf_ignore_len (page *in, const p_size idx, const b_size key, const pgno pg)
{
  in_set_key_ignore_len (in, idx, key);
  in_set_leaf_ignore_len (in, idx, pg);
}

HEADER_FUNC void
in_set_prev (page *in, const pgno prev)
{
  PAGE_SIMPLE_SET_IMPL (in, prev, IN_PREV_OFST);
}

HEADER_FUNC void
in_set_next (page *in, const pgno next)
{
  PAGE_SIMPLE_SET_IMPL (in, next, IN_NEXT_OFST);
}

HEADER_FUNC void
in_link (page *left, page *right)
{
  const pgno lpg = left ? left->pg : PGNO_NULL;
  const pgno rpg = right ? right->pg : PGNO_NULL;
  if (left)
    {
      in_set_next (left, rpg);
    }
  if (right)
    {
      in_set_prev (right, lpg);
    }
}
