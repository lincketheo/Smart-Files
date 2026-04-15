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

#include "paging/dpgt/dirty_page_table.h"

#include "core/assert.h"
#include "core/deserializer.h"
#include "core/error.h"
#include "core/hash_table.h"
#include "core/latch.h"
#include "core/random.h"
#include "core/serializer.h"
#include "core/slab_alloc.h"
#include "intf/logging.h"
#include "intf/os/memory.h"

/*
 * Dirty page table entry.
 *
 * rec_lsn is the LSN of the first WAL record that dirtied this page (the
 * Recovery LSN).  The minimum rec_lsn across all DPT entries determines
 * the redo start point during ARIES recovery: any WAL record before that
 * LSN cannot affect a page that is still dirty, so it can be skipped.
 *
 * The latch protects rec_lsn and pg for concurrent access.  hnode embeds
 * the hash-table intrusion pointer; pg also doubles as the hash key.
 */
struct dpg_entry
{
  lsn rec_lsn;
  pgno pg;
  struct hnode node;
  latch l;
};

#define DPGT_SERIAL_UNIT (sizeof (pgno) + sizeof (lsn))

static void
dpge_key_init (struct dpg_entry *dest, const pgno pg)
{
  dest->pg = pg;
  latch_init (&dest->l);
  hnode_init (&dest->node, pg);
}

static void
dpge_init (struct dpg_entry *dest, const pgno pg, const lsn rec_lsn)
{
  dest->pg = pg;
  dest->rec_lsn = rec_lsn;
  latch_init (&dest->l);
  hnode_init (&dest->node, pg);
}

static bool
dpge_equals (const struct hnode *left, const struct hnode *right)
{
  // Might have passed the exact same ref as exists in the htable
  if (left == right)
    {
      return true;
    }

  // Otherwise, passed a key with just relevant information
  else
    {
      struct dpg_entry *_left = container_of (left, struct dpg_entry, node);
      struct dpg_entry *_right = container_of (right, struct dpg_entry, node);

      latch_lock (&_left->l);
      latch_lock (&_right->l);

      bool ret = _left->pg == _right->pg;

      latch_unlock (&_right->l);
      latch_unlock (&_left->l);

      return ret;
    }
}

DEFINE_DBG_ASSERT (struct dpg_table, dirty_pg_table, d, { ASSERT (d); })

// Lifecycle
struct dpg_table *
dpgt_open (error *e)
{
  struct dpg_table *dest = i_malloc (1, sizeof *dest, e);
  if (dest == NULL)
    {
      goto failed;
    }
  slab_alloc_init (&dest->alloc, sizeof (struct dpg_entry), 1000);

  dest->t = htable_create (512, e);
  if (dest->t == NULL)
    {
      goto dest_failed;
    }

  latch_init (&dest->l);

  return dest;

dest_failed:
  i_free (dest);
failed:
  return NULL;
}

void
dpgt_close (struct dpg_table *t)
{
  DBG_ASSERT (dirty_pg_table, t);
  latch_lock (&t->l);
  slab_alloc_destroy (&t->alloc);
  htable_free (t->t);
  latch_unlock (&t->l);
  i_free (t);
}

static void
i_log_dpge_in_dpgt (struct hnode *node, void *_log_level)
{
  const int *log_level = _log_level;

  struct dpg_entry *entry = container_of (node, struct dpg_entry, node);

  latch_lock (&entry->l);
  i_printf (*log_level, "|pg = %10" PRpgno " rec_lsn = %10" PRlsn "|\n",
            entry->pg, entry->rec_lsn);
  latch_unlock (&entry->l);
}

void
i_log_dpgt (int log_level, struct dpg_table *dpt)
{
  latch_lock (&dpt->l);

  i_log (log_level,
         "================ Dirty Page Table START ================\n");
  htable_foreach (dpt->t, i_log_dpge_in_dpgt, &log_level);
  i_log (log_level,
         "================ Dirty Page Table END ================\n");

  latch_unlock (&dpt->l);
}

struct merge_ctx
{
  struct dpg_table *dest;
  error *e;
};

static void
merge_dpge (const pgno pg, const lsn rec_lsn, void *vctx)
{
  const struct merge_ctx *ctx = vctx;

  if (ctx->e->cause_code)
    {
      return;
    }

  if (dpgt_add (ctx->dest, pg, rec_lsn, ctx->e))
    {
      return;
    }
}

err_t
dpgt_merge_into (struct dpg_table *dest, struct dpg_table *src, error *e)
{
  struct merge_ctx ctx = {
    .dest = dest,
    .e = e,
  };

  latch_lock (&src->l);
  dpgt_foreach (src, merge_dpge, &ctx);
  latch_unlock (&src->l);

  return ctx.e->cause_code;
}

static void
dpge_max (pgno pg, const lsn rec_lsn, void *ctx)
{
  lsn *min = ctx;

  if (rec_lsn < *min)
    {
      *min = rec_lsn;
    }
}

lsn
dpgt_min_rec_lsn (struct dpg_table *d)
{
  ASSERT (dpgt_get_size (d) > 0);
  lsn min = (lsn)-1;

  latch_lock (&d->l);
  dpgt_foreach (d, dpge_max, &min);
  latch_unlock (&d->l);

  return min;
}

struct foreach_ctx
{
  void (*action) (pgno pg, lsn rec_lsn, void *ctx);
  void *ctx;
};

static void
hnode_foreach (struct hnode *node, void *ctx)
{
  const struct foreach_ctx *_ctx = ctx;
  struct dpg_entry *entry = container_of (node, struct dpg_entry, node);

  latch_lock (&entry->l);

  pgno pg = entry->pg;
  lsn l = entry->rec_lsn;

  latch_unlock (&entry->l);

  _ctx->action (pg, l, _ctx->ctx);
}

void
dpgt_foreach (const struct dpg_table *t,
              void (*action) (pgno pg, lsn rec_lsn, void *ctx), void *ctx)
{
  struct foreach_ctx _ctx = {
    .action = action,
    .ctx = ctx,
  };
  htable_foreach (t->t, hnode_foreach, &_ctx);
}

u32
dpgt_get_size (const struct dpg_table *d)
{
  return htable_size (d->t);
}

bool
dpgt_exists (const struct dpg_table *t, const pgno pg)
{
  struct dpg_entry entry;
  dpge_key_init (&entry, pg);

  struct hnode **ret = htable_lookup (t->t, &entry.node, dpge_equals);

  return ret != NULL;
}

err_t
dpgt_add (struct dpg_table *t, const pgno pg, const lsn rec_lsn, error *e)
{
  DBG_ASSERT (dirty_pg_table, t);

  latch_lock (&t->l);

  struct dpg_entry *v = slab_alloc_alloc (&t->alloc, e);
  if (v == NULL)
    {
      goto theend;
    }

  dpge_init (v, pg, rec_lsn);

  htable_insert (t->t, &v->node);

theend:
  latch_unlock (&t->l);
  return error_trace (e);
}

bool
dpgt_get (lsn *dest, struct dpg_table *t, const pgno pg)
{
  DBG_ASSERT (dirty_pg_table, t);

  struct dpg_entry key;
  dpge_key_init (&key, pg);

  latch_lock (&t->l);

  struct hnode **node = htable_lookup (t->t, &key.node, dpge_equals);
  if (node)
    {
      *dest = container_of (*node, struct dpg_entry, node)->rec_lsn;
    }

  latch_unlock (&t->l);

  return node != NULL;
}

void
dpgt_get_expect (lsn *dest, struct dpg_table *t, const pgno pg)
{
  DBG_ASSERT (dirty_pg_table, t);

  struct dpg_entry key;
  dpge_key_init (&key, pg);

  latch_lock (&t->l);

  struct hnode **node = htable_lookup (t->t, &key.node, dpge_equals);
  ASSERT (node != NULL);

  *dest = container_of (*node, struct dpg_entry, node)->rec_lsn;

  latch_unlock (&t->l);
}

void
dpgt_remove (bool *exists, struct dpg_table *t, const pgno pg)
{
  DBG_ASSERT (dirty_pg_table, t);

  struct dpg_entry key;
  dpge_key_init (&key, pg);

  latch_lock (&t->l);

  struct hnode **node = htable_lookup (t->t, &key.node, dpge_equals);

  if (node == NULL)
    {
      *exists = false;
      goto theend;
    }

  *exists = true;

  htable_delete (t->t, node);

theend:
  latch_unlock (&t->l);
}

void
dpgt_remove_expect (struct dpg_table *t, const pgno pg)
{
  DBG_ASSERT (dirty_pg_table, t);

  struct dpg_entry key;
  dpge_key_init (&key, pg);

  latch_lock (&t->l);

  struct hnode **node = htable_lookup (t->t, &key.node, dpge_equals);
  ASSERT (node != NULL);
  htable_delete (t->t, node);

  latch_unlock (&t->l);
}

void
dpgt_update (struct dpg_table *t, const pgno pg, const lsn new_rec_lsn)
{
  struct dpg_entry key;
  dpge_key_init (&key, pg);

  latch_lock (&t->l);
  {
    DBG_ASSERT (dirty_pg_table, t);

    struct hnode **node = htable_lookup (t->t, &key.node, dpge_equals);
    ASSERT (node != NULL);
    struct dpg_entry *entry = container_of (*node, struct dpg_entry, node);

    latch_lock (&entry->l);
    entry->rec_lsn = new_rec_lsn;
    latch_unlock (&entry->l);
  }

  latch_unlock (&t->l);
}

u32
dpgt_get_serialize_size (const struct dpg_table *t)
{
  return htable_size (t->t) * DPGT_SERIAL_UNIT;
}

struct dpge_serialize_ctx
{
  struct serializer s;
};

static void
hnode_foreach_serialize (struct hnode *node, void *ctx)
{
  struct dpge_serialize_ctx *_ctx = ctx;

  struct dpg_entry *entry = container_of (node, struct dpg_entry, node);

  pgno pg;
  lsn rec_lsn;

  latch_lock (&entry->l);

  pg = entry->pg;
  rec_lsn = entry->rec_lsn;

  latch_unlock (&entry->l);

  srlizr_write_expect (&_ctx->s, &pg, sizeof (pg));
  srlizr_write_expect (&_ctx->s, &rec_lsn, sizeof (rec_lsn));
}

u32
dpgt_serialize (u8 *dest, const u32 dlen, const struct dpg_table *t)
{
  struct dpge_serialize_ctx ctx = {
    .s = srlizr_create (dest, dlen),
  };

  htable_foreach (t->t, hnode_foreach_serialize, &ctx);

  return ctx.s.dlen;
}

struct dpg_table *
dpgt_deserialize (const u8 *src, const u32 slen, error *e)
{
  struct dpg_table *dest = dpgt_open (e);
  if (dest == NULL)
    {
      goto failed;
    }

  if (slen == 0)
    {
      return dest;
    }

  struct deserializer d = dsrlizr_create (src, slen);

  ASSERT (slen % DPGT_SERIAL_UNIT == 0);
  const u32 tlen = slen / DPGT_SERIAL_UNIT;

  for (u32 i = 0; i < tlen; ++i)
    {
      pgno pg = 0;
      lsn rec_lsn = 0;

      dsrlizr_read_expect (&pg, sizeof (pg), &d);
      dsrlizr_read_expect (&rec_lsn, sizeof (rec_lsn), &d);

      if (dpgt_add (dest, pg, rec_lsn, e))
        {
          goto dest_failed;
        }
    }

  return dest;

dest_failed:
  dpgt_close (dest);
failed:
  return NULL;
}

u32
dpgtlen_from_serialized (const u32 slen)
{
  ASSERT (slen % DPGT_SERIAL_UNIT == 0);
  return slen / DPGT_SERIAL_UNIT;
}

struct dpgt_eq_ctx
{
  struct dpg_table *other;
  bool ret;
};

static void
dpgt_eq_foreach (struct hnode *node, void *_ctx)
{
  struct dpgt_eq_ctx *ctx = _ctx;
  if (ctx->ret == false)
    {
      return;
    }

  struct dpg_entry *entry = container_of (node, struct dpg_entry, node);
  struct dpg_entry candidate;

  latch_lock (&entry->l);
  {
    dpge_key_init (&candidate, entry->pg);

    struct hnode **other_node
        = htable_lookup (ctx->other->t, &candidate.node, dpge_equals);

    if (other_node == NULL)
      {
        ctx->ret = false;
        goto theend;
      }

    struct dpg_entry *other
        = container_of (*other_node, struct dpg_entry, node);

    latch_lock (&other->l);
    {
      ASSERT (other->pg == entry->pg);
      ctx->ret = other->rec_lsn == entry->rec_lsn;
    }
    latch_unlock (&other->l);
  }

theend:
  latch_unlock (&entry->l);
}

bool
dpgt_equal (struct dpg_table *left, struct dpg_table *right)
{
  bool ret = false;

  latch_lock (&left->l);
  latch_lock (&right->l);

  if (htable_size (left->t) != htable_size (right->t))
    {
      goto theend;
    }

  struct dpgt_eq_ctx ctx = {
    .other = right,
    .ret = true,
  };
  htable_foreach (left->t, dpgt_eq_foreach, &ctx);
  ret = ctx.ret;

theend:
  latch_unlock (&right->l);
  latch_unlock (&left->l);

  return ret;
}

err_t
dpgt_rand_populate (struct dpg_table *t, error *e)
{
  const u32 len = htable_size (t->t);

  pgno pg = 0;
  lsn l = 0;

  for (u32 i = 0; i < 100 - len;
       ++i, pg += randu32r (1, 100), l += randu32r (1, 100))
    {
      if (dpgt_add (t, pg, l, e))
        {
          goto theend;
        }
    }

theend:
  return error_trace (e);
}

void
dpgt_crash (struct dpg_table *t)
{
  DBG_ASSERT (dirty_pg_table, t);
  htable_free (t->t);
  slab_alloc_destroy (&t->alloc);
  i_free (t);
}
