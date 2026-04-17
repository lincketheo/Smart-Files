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

#include "wal/wal.h"

#include "dpgt/dirty_page_table.h"
#include "c_specx.h"
#include "c_specx_dev.h"
#include "txns/txn_table.h"
#include "wal/wal_istream.h"
#include "wal/wal_ostream.h"
#include "wal/wal_rec_hdr.h"

#include <string.h>

/*
 * Concrete WAL backed by a single append-only OS file.
 *
 * The os_wal base must be the first member so that a struct wal * can be
 * freely cast to struct os_wal * and back, as required by the vtable
 * dispatch pattern.
 */
struct wal
{
  struct os_wal base; /* MUST be first */

  // The file that's open
  const char *fname;

  // Input and / or output streams
  struct wal_ostream *ostream;
  struct wal_istream *istream;

  // Headers to read and write
  struct wal_rec_hdr_read rhdr;
  struct wal_rec_hdr_write whdr;

  latch latch;
};

DEFINE_DBG_ASSERT (struct wal, wal, w, { ASSERT (w); })

/* ------------------------------------------------------------------ */
/* os_wal vtable implementations                                        */
/* ------------------------------------------------------------------ */

static bool
wal_is_recoverable_impl (const struct os_wal *self)
{
  (void)self;
  return true;
}

static err_t
wal_close_impl (struct os_wal *self, error *e)
{
  return wal_close ((struct wal *)self, e);
}

static err_t
wal_reset_impl (struct os_wal *self, error *e)
{
  return wal_reset ((struct wal *)self, e);
}

static struct os_wal *
wal_delete_and_reopen_impl (struct os_wal *self, error *e)
{
  return (struct os_wal *)wal_delete_and_reopen ((struct wal *)self, e);
}

static err_t
wal_flush_to_impl (struct os_wal *self, const lsn l, error *e)
{
  return wal_flush_to ((struct wal *)self, l, e);
}

static err_t
wal_flush_all_impl (struct os_wal *self, error *e)
{
  return wal_flush_all ((struct wal *)self, e);
}

static struct wal_rec_hdr_read *
wal_read_next_impl (struct os_wal *self, lsn *read_lsn, error *e)
{
  return wal_read_next ((struct wal *)self, read_lsn, e);
}

static struct wal_rec_hdr_read *
wal_read_entry_impl (struct os_wal *self, const lsn id, error *e)
{
  return wal_read_entry ((struct wal *)self, id, e);
}

static slsn
wal_append_begin_log_impl (struct os_wal *self, const txid tid, error *e)
{
  return wal_append_begin_log ((struct wal *)self, tid, e);
}

static slsn
wal_append_commit_log_impl (struct os_wal *self, const txid tid, const lsn prev, error *e)
{
  return wal_append_commit_log ((struct wal *)self, tid, prev, e);
}

static slsn
wal_append_end_log_impl (struct os_wal *self, const txid tid, const lsn prev, error *e)
{
  return wal_append_end_log ((struct wal *)self, tid, prev, e);
}

static slsn
wal_append_ckpt_begin_impl (struct os_wal *self, error *e)
{
  return wal_append_ckpt_begin ((struct wal *)self, e);
}

static slsn
wal_append_ckpt_end_impl (struct os_wal *self, struct txn_table *att,
                          struct dpg_table *dpt, error *e)
{
  return wal_append_ckpt_end ((struct wal *)self, att, dpt, e);
}

static slsn
wal_append_update_log_impl (struct os_wal *self,
                            const struct wal_update_write update, error *e)
{
  return wal_append_update_log ((struct wal *)self, update, e);
}

static slsn
wal_append_clr_log_impl (struct os_wal *self, const struct wal_clr_write clr,
                         error *e)
{
  return wal_append_clr_log ((struct wal *)self, clr, e);
}

static slsn
wal_append_log_impl (struct os_wal *self, struct wal_rec_hdr_write *hdr,
                     error *e)
{
  return wal_append_log ((struct wal *)self, hdr, e);
}

static err_t
wal_crash_impl (struct os_wal *self, error *e)
{
  return wal_crash ((struct wal *)self, e);
}

static const struct os_wal_vtable wal_vtable = {
  .is_recoverable = wal_is_recoverable_impl,
  .close = wal_close_impl,
  .reset = wal_reset_impl,
  .delete_and_reopen = wal_delete_and_reopen_impl,
  .flush_to = wal_flush_to_impl,
  .flush_all = wal_flush_all_impl,
  .read_next = wal_read_next_impl,
  .read_entry = wal_read_entry_impl,
  .append_begin_log = wal_append_begin_log_impl,
  .append_commit_log = wal_append_commit_log_impl,
  .append_end_log = wal_append_end_log_impl,
  .append_ckpt_begin = wal_append_ckpt_begin_impl,
  .append_ckpt_end = wal_append_ckpt_end_impl,
  .append_update_log = wal_append_update_log_impl,
  .append_clr_log = wal_append_clr_log_impl,
  .append_log = wal_append_log_impl,
  .crash_fn = wal_crash_impl,
};

/* ------------------------------------------------------------------ */
/* Lifecycle                                                            */
/* ------------------------------------------------------------------ */

struct wal *
wal_open (const char *fname, error *e)
{
  struct wal *dest = i_malloc (1, sizeof *dest, e);
  if (dest == NULL)
    {
      return NULL;
    }

  dest->base.vtable = &wal_vtable;
  dest->ostream = NULL;
  dest->istream = NULL;
  dest->fname = fname;
  latch_init (&dest->latch);

  dest->ostream = walos_open (dest->fname, e);
  if (dest->ostream == NULL)
    {
      i_free (dest);
      return NULL;
    }

  dest->istream = walis_open (dest->fname, e);
  if (dest->istream == NULL)
    {
      i_free (dest);
      walos_close (dest->ostream, e);
      return NULL;
    }

  DBG_ASSERT (wal, dest);

  return dest;
}

struct os_wal *
wal_open_os (const char *fname, error *e)
{
  return (struct os_wal *)wal_open (fname, e);
}

err_t
wal_reset (struct wal *w, error *e)
{
  wal_flush_all (w, e);
  walos_close (w->ostream, e);
  walis_close (w->istream, e);

  i_remove_quiet (w->fname, e);

  w->ostream = walos_open (w->fname, e);
  if (w->ostream == NULL)
    {
      return e->cause_code;
    }

  w->istream = walis_open (w->fname, e);
  if (w->istream == NULL)
    {
      walos_close (w->ostream, e);
      return e->cause_code;
    }

  return error_trace (e);
}

err_t
wal_close (struct wal *w, error *e)
{
  wal_flush_all (w, e);
  walos_close (w->ostream, e);
  walis_close (w->istream, e);

  i_free (w);

  return error_trace (e);
}

struct wal *
wal_delete_and_reopen (struct wal *w, error *e)
{
  const char *fname = w->fname;
  if (wal_close (w, e))
    {
      return NULL;
    }

  if (i_remove_quiet (fname, e))
    {
      return NULL;
    }

  return wal_open (fname, e);
}

////////////////////////////////////////////////////////////
// Static write helpers (latch held by caller)

static err_t
wal_write_begin (const struct wal *w, const struct wal_rec_hdr_write *r, error *e)
{
  ASSERT (r->type == WL_BEGIN);

  ASSERT (w->ostream);

  u32 checksum = checksum_init ();
  const wlh t = r->type;
  WRAP (walos_write_all (w->ostream, &checksum, &t, sizeof (wlh), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->begin.tid, sizeof (txid),
                         e));
  WRAP (walos_write_all (w->ostream, NULL, &checksum, sizeof (u32), e));

  return SUCCESS;
}

static err_t
wal_write_commit (const struct wal *w, const struct wal_rec_hdr_write *r, error *e)
{
  ASSERT (r->type == WL_COMMIT);

  ASSERT (w->ostream);

  u32 checksum = checksum_init ();
  const wlh t = r->type;
  WRAP (walos_write_all (w->ostream, &checksum, &t, sizeof (wlh), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->commit.tid, sizeof (txid),
                         e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->commit.prev, sizeof (lsn),
                         e));
  WRAP (walos_write_all (w->ostream, NULL, &checksum, sizeof (u32), e));

  return SUCCESS;
}

static err_t
wal_write_end (const struct wal *w, const struct wal_rec_hdr_write *r, error *e)
{
  ASSERT (r->type == WL_END);

  ASSERT (w->ostream);

  u32 checksum = checksum_init ();
  const wlh t = r->type;
  WRAP (walos_write_all (w->ostream, &checksum, &t, sizeof (wlh), e));
  WRAP (
      walos_write_all (w->ostream, &checksum, &r->end.tid, sizeof (txid), e));
  WRAP (
      walos_write_all (w->ostream, &checksum, &r->end.prev, sizeof (lsn), e));
  WRAP (walos_write_all (w->ostream, NULL, &checksum, sizeof (u32), e));

  return SUCCESS;
}

static err_t
wal_write_physical_update (const struct wal *w, const struct wal_rec_hdr_write *r,
                           error *e)
{
  ASSERT (w->ostream);

  u32 checksum = checksum_init ();
  const wlh t = (wlh)r->type;
  const wlh ut = (wlh)r->update.type;
  WRAP (walos_write_all (w->ostream, &checksum, &t, sizeof (wlh), e));
  WRAP (walos_write_all (w->ostream, &checksum, &ut, sizeof (wlh), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->update.tid, sizeof (txid),
                         e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->update.prev, sizeof (lsn),
                         e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->update.phys.pg,
                         sizeof (pgno), e));
  WRAP (walos_write_all (w->ostream, &checksum, r->update.phys.undo, PAGE_SIZE,
                         e));
  WRAP (walos_write_all (w->ostream, &checksum, r->update.phys.redo, PAGE_SIZE,
                         e));
  WRAP (walos_write_all (w->ostream, NULL, &checksum, sizeof (u32), e));

  return SUCCESS;
}

static err_t
wal_write_fsm_update (const struct wal *w, const struct wal_rec_hdr_write *r,
                      error *e)
{
  ASSERT (w->ostream);

  u32 checksum = checksum_init ();
  const wlh t = (wlh)r->type;
  const wlh ut = (wlh)r->update.type;
  WRAP (walos_write_all (w->ostream, &checksum, &t, sizeof (wlh), e));
  WRAP (walos_write_all (w->ostream, &checksum, &ut, sizeof (wlh), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->update.tid, sizeof (txid),
                         e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->update.prev, sizeof (lsn),
                         e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->update.fsm.pg,
                         sizeof (pgno), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->update.fsm.undo,
                         sizeof (u8), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->update.fsm.redo,
                         sizeof (u8), e));
  WRAP (walos_write_all (w->ostream, NULL, &checksum, sizeof (u32), e));

  return SUCCESS;
}

static err_t
wal_write_file_extend_update (const struct wal *w, const struct wal_rec_hdr_write *r,
                              error *e)
{
  ASSERT (w->ostream);

  u32 checksum = checksum_init ();
  const wlh t = (wlh)r->type;
  const wlh ut = (wlh)r->update.type;
  WRAP (walos_write_all (w->ostream, &checksum, &t, sizeof (wlh), e));
  WRAP (walos_write_all (w->ostream, &checksum, &ut, sizeof (wlh), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->update.tid, sizeof (txid),
                         e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->update.prev, sizeof (lsn),
                         e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->update.fext.undo,
                         sizeof (pgno), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->update.fext.redo,
                         sizeof (pgno), e));
  WRAP (walos_write_all (w->ostream, NULL, &checksum, sizeof (u32), e));

  return SUCCESS;
}

static err_t
wal_write_physical_clr (const struct wal *w, const struct wal_rec_hdr_write *r,
                        error *e)
{
  ASSERT (r->type == WL_CLR);
  ASSERT (r->clr.undo_next != 0);

  ASSERT (w->ostream);

  u32 checksum = checksum_init ();
  const wlh t = r->type;
  const wlh ut = r->clr.type;
  WRAP (walos_write_all (w->ostream, &checksum, &t, sizeof (wlh), e));
  WRAP (walos_write_all (w->ostream, &checksum, &ut, sizeof (wlh), e));
  WRAP (
      walos_write_all (w->ostream, &checksum, &r->clr.tid, sizeof (txid), e));
  WRAP (
      walos_write_all (w->ostream, &checksum, &r->clr.prev, sizeof (lsn), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->clr.phys.pg, sizeof (pgno),
                         e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->clr.undo_next,
                         sizeof (lsn), e));
  WRAP (
      walos_write_all (w->ostream, &checksum, r->clr.phys.redo, PAGE_SIZE, e));
  WRAP (walos_write_all (w->ostream, NULL, &checksum, sizeof (u32), e));

  return SUCCESS;
}

static err_t
wal_write_fsm_clr (const struct wal *w, const struct wal_rec_hdr_write *r, error *e)
{
  ASSERT (r->type == WL_CLR);
  ASSERT (r->clr.undo_next != 0);

  ASSERT (w->ostream);

  u32 checksum = checksum_init ();
  const wlh t = r->type;
  const wlh ut = r->clr.type;
  WRAP (walos_write_all (w->ostream, &checksum, &t, sizeof (wlh), e));
  WRAP (walos_write_all (w->ostream, &checksum, &ut, sizeof (wlh), e));
  WRAP (
      walos_write_all (w->ostream, &checksum, &r->clr.tid, sizeof (txid), e));
  WRAP (
      walos_write_all (w->ostream, &checksum, &r->clr.prev, sizeof (lsn), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->clr.fsm.pg, sizeof (pgno),
                         e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->clr.undo_next,
                         sizeof (lsn), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->clr.fsm.redo, sizeof (u8),
                         e));
  WRAP (walos_write_all (w->ostream, NULL, &checksum, sizeof (u32), e));

  return SUCCESS;
}

static err_t
wal_write_dummy_clr (const struct wal *w, const struct wal_rec_hdr_write *r,
                     error *e)
{
  ASSERT (r->type == WL_CLR);

  ASSERT (w->ostream);

  u32 checksum = checksum_init ();
  const wlh t = r->type;
  const wlh ut = r->clr.type;
  WRAP (walos_write_all (w->ostream, &checksum, &t, sizeof (wlh), e));
  WRAP (walos_write_all (w->ostream, &checksum, &ut, sizeof (wlh), e));
  WRAP (
      walos_write_all (w->ostream, &checksum, &r->clr.tid, sizeof (txid), e));
  WRAP (
      walos_write_all (w->ostream, &checksum, &r->clr.prev, sizeof (lsn), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->clr.undo_next,
                         sizeof (lsn), e));
  WRAP (walos_write_all (w->ostream, NULL, &checksum, sizeof (u32), e));

  return SUCCESS;
}

static err_t
wal_write_ckpt_begin (const struct wal *w, const struct wal_rec_hdr_write *r,
                      error *e)
{
  ASSERT (r->type == WL_CKPT_BEGIN);

  ASSERT (w->ostream);

  u32 checksum = checksum_init ();
  const wlh t = r->type;
  WRAP (walos_write_all (w->ostream, &checksum, &t, sizeof (wlh), e));
  WRAP (walos_write_all (w->ostream, NULL, &checksum, sizeof (u32), e));

  return SUCCESS;
}

static err_t
wal_write_ckpt_end (const struct wal *w, const struct wal_rec_hdr_write *r, error *e)
{
  ASSERT (r->type == WL_CKPT_END);

  void *att_serialized = NULL;
  void *dpgt_serialized = NULL;
  u32 attsize = 0;
  u32 dptsize = 0;

  // Serialize ATT / DPT under their own locks (caller holds w->latch)
  {
    txnt_freeze_active_txns_for_serialization (r->ckpt_end.att);
    latch_lock (&r->ckpt_end.dpt->l);

    attsize = txnt_get_serialize_size (r->ckpt_end.att);
    if (attsize > 0)
      {
        att_serialized = i_malloc (attsize, 1, e);
        if (att_serialized == NULL)
          {
            latch_unlock (&r->ckpt_end.dpt->l);
            txnt_unfreeze (r->ckpt_end.att);
            return error_trace (e);
          }
        txnt_serialize (att_serialized, attsize, r->ckpt_end.att);
      }

    dptsize = dpgt_get_serialize_size (r->ckpt_end.dpt);
    if (dptsize > 0)
      {
        dpgt_serialized = i_malloc (dptsize, 1, e);
        if (dpgt_serialized == NULL)
          {
            latch_unlock (&r->ckpt_end.dpt->l);
            txnt_unfreeze (r->ckpt_end.att);
            i_free (att_serialized);
            return error_trace (e);
          }
        dpgt_serialize (dpgt_serialized, dptsize, r->ckpt_end.dpt);
      }

    latch_unlock (&r->ckpt_end.dpt->l);
    txnt_unfreeze (r->ckpt_end.att);
  }

  // Write WAL (w->latch already held)
  {
    ASSERT (w->ostream);

    u32 checksum = checksum_init ();
    const wlh t = r->type;

    if (unlikely ((walos_write_all (w->ostream, &checksum, &t, sizeof (wlh), e)) < SUCCESS))
      {
        goto theend;
      }
    if (unlikely ((walos_write_all (w->ostream, &checksum, &attsize, sizeof (attsize), e)) < SUCCESS))
      {
        goto theend;
      }
    if (unlikely ((walos_write_all (w->ostream, &checksum, &dptsize, sizeof (dptsize), e)) < SUCCESS))
      {
        goto theend;
      }

    if (attsize > 0)
      {
        if (unlikely ((walos_write_all (w->ostream, &checksum, att_serialized, attsize, e)) < SUCCESS))
          {
            goto theend;
          }
      }
    if (dptsize > 0)
      {
        if (unlikely ((walos_write_all (w->ostream, &checksum, dpgt_serialized, dptsize, e)) < SUCCESS))
          {
            goto theend;
          }
      }

    if (unlikely ((walos_write_all (w->ostream, NULL, &checksum, sizeof (u32), e)) < SUCCESS))
      {
        goto theend;
      }
  }

theend:
  i_cfree (att_serialized);
  i_cfree (dpgt_serialized);
  return error_trace (e);
}

// Dispatch write, return lsn before record. Latch held by caller.
static slsn
wal_write_locked (struct wal *w, error *e)
{
  ASSERT (w->ostream);

  const lsn ret = walos_get_next_lsn (w->ostream);

  switch (w->whdr.type)
    {
    case WL_BEGIN:
      WRAP (wal_write_begin (w, &w->whdr, e));
      break;
    case WL_COMMIT:
      WRAP (wal_write_commit (w, &w->whdr, e));
      break;
    case WL_END:
      WRAP (wal_write_end (w, &w->whdr, e));
      break;
    case WL_UPDATE:
      switch (w->whdr.update.type)
        {
        case WUP_PHYSICAL:
          WRAP (wal_write_physical_update (w, &w->whdr, e));
          break;
        case WUP_FSM:
          WRAP (wal_write_fsm_update (w, &w->whdr, e));
          break;
        case WUP_FEXT:
          WRAP (wal_write_file_extend_update (w, &w->whdr, e));
          break;
        }
      break;
    case WL_CLR:
      switch (w->whdr.clr.type)
        {
        case WCLR_PHYSICAL:
          WRAP (wal_write_physical_clr (w, &w->whdr, e));
          break;
        case WCLR_FSM:
          WRAP (wal_write_fsm_clr (w, &w->whdr, e));
          break;
        case WCLR_DUMMY:
          WRAP (wal_write_dummy_clr (w, &w->whdr, e));
          break;
        }
      break;
    case WL_CKPT_BEGIN:
      WRAP (wal_write_ckpt_begin (w, &w->whdr, e));
      break;
    case WL_CKPT_END:
      WRAP (wal_write_ckpt_end (w, &w->whdr, e));
      break;
    case WL_EOF:
      UNREACHABLE ();
    }

  return ret;
}

////////////////////////////////////////////////////////////
// Append Primitives

slsn
wal_append_begin_log (struct wal *w, const txid tid, error *e)
{
  latch_lock (&w->latch);
  DBG_ASSERT (wal, w);
  w->whdr.type = WL_BEGIN;
  w->whdr.begin = (struct wal_begin){ .tid = tid };
  const slsn result = wal_write_locked (w, e);
  latch_unlock (&w->latch);
  return result;
}

slsn
wal_append_commit_log (struct wal *w, const txid tid, const lsn prev, error *e)
{
  latch_lock (&w->latch);
  DBG_ASSERT (wal, w);
  w->whdr.type = WL_COMMIT;
  w->whdr.commit = (struct wal_commit){ .tid = tid, .prev = prev };
  const slsn result = wal_write_locked (w, e);
  latch_unlock (&w->latch);
  return result;
}

slsn
wal_append_end_log (struct wal *w, const txid tid, const lsn prev, error *e)
{
  latch_lock (&w->latch);
  DBG_ASSERT (wal, w);
  w->whdr.type = WL_END;
  w->whdr.end = (struct wal_end){ .tid = tid, .prev = prev };
  const slsn result = wal_write_locked (w, e);
  latch_unlock (&w->latch);
  return result;
}

slsn
wal_append_update_log (struct wal *w, const struct wal_update_write update, error *e)
{
  latch_lock (&w->latch);
  DBG_ASSERT (wal, w);
  w->whdr.type = WL_UPDATE;
  w->whdr.update = update;
  const slsn result = wal_write_locked (w, e);
  latch_unlock (&w->latch);
  return result;
}

slsn
wal_append_clr_log (struct wal *w, const struct wal_clr_write clr, error *e)
{
  latch_lock (&w->latch);
  DBG_ASSERT (wal, w);
  w->whdr.type = WL_CLR;
  w->whdr.clr = clr;
  const slsn result = wal_write_locked (w, e);
  latch_unlock (&w->latch);
  return result;
}

slsn
wal_append_ckpt_begin (struct wal *w, error *e)
{
  latch_lock (&w->latch);
  DBG_ASSERT (wal, w);
  w->whdr.type = WL_CKPT_BEGIN;
  const slsn result = wal_write_locked (w, e);
  latch_unlock (&w->latch);
  return result;
}

slsn
wal_append_ckpt_end (struct wal *w, struct txn_table *att,
                     struct dpg_table *dpt, error *e)
{
  latch_lock (&w->latch);
  DBG_ASSERT (wal, w);
  w->whdr.type = WL_CKPT_END;
  w->whdr.ckpt_end = (struct wal_ckpt_end_write){ .att = att, .dpt = dpt };
  const slsn result = wal_write_locked (w, e);
  latch_unlock (&w->latch);
  return result;
}

slsn
wal_append_log (struct wal *w, const struct wal_rec_hdr_write *whdr, error *e)
{
  switch (whdr->type)
    {
    case WL_BEGIN:
      return wal_append_begin_log (w, whdr->begin.tid, e);
    case WL_COMMIT:
      return wal_append_commit_log (w, whdr->commit.tid, whdr->commit.prev, e);
    case WL_END:
      return wal_append_end_log (w, whdr->end.tid, whdr->end.prev, e);
    case WL_UPDATE:
      return wal_append_update_log (w, whdr->update, e);
    case WL_CLR:
      return wal_append_clr_log (w, whdr->clr, e);
    case WL_CKPT_BEGIN:
      return wal_append_ckpt_begin (w, e);
    case WL_CKPT_END:
      return wal_append_ckpt_end (w, whdr->ckpt_end.att, whdr->ckpt_end.dpt,
                                  e);
    case WL_EOF:
      UNREACHABLE ();
    }

  UNREACHABLE ();
}

////////////////////////////////////////////////////////////
// Flush

err_t
wal_flush_to (const struct wal *w, const lsn l, error *e)
{
  DBG_ASSERT (wal, w);
  ASSERT (w->ostream);
  return walos_flush_to (w->ostream, l, e);
}

err_t
wal_flush_all (const struct wal *w, error *e)
{
  DBG_ASSERT (wal, w);
  ASSERT (w->ostream);
  return walos_flush_all (w->ostream, e);
}

////////////////////////////////////////////////////////////
// Static read helpers (latch held by caller)

static int
wal_read_ckpt_end_full (const struct wal *w, u32 *checksum, void **buf, error *e)
{
  ASSERT (buf);

  *buf = NULL;
  const wlh type = WL_CKPT_END;
  u32 attsize;
  u32 dptsize;

  {
    bool iseof;
    u32 sizes[2];
    if (walis_read_all (w->istream, &iseof, NULL, checksum, sizes,
                        sizeof (sizes), e))
      {
        return error_trace (e);
      }
    if (iseof)
      {
        return WL_EOF;
      }
    attsize = sizes[0];
    dptsize = sizes[1];
  }

  const u32 size
      = sizeof (type) + 2 * sizeof (u32) + attsize + dptsize + sizeof (u32);

  void *_buf = i_malloc (size, 1, e);
  if (_buf == NULL)
    {
      return error_trace (e);
    }

  u8 *head = _buf;

  memcpy (head, &type, sizeof (wlh));
  head += sizeof (wlh);
  memcpy (head, &attsize, sizeof (u32));
  head += sizeof (u32);
  memcpy (head, &dptsize, sizeof (u32));
  head += sizeof (u32);

  if (attsize + dptsize > 0)
    {
      bool iseof;
      if (walis_read_all (w->istream, &iseof, NULL, checksum, head,
                          attsize + dptsize, e))
        {
          i_free (_buf);
          return error_trace (e);
        }
      if (iseof)
        {
          i_free (_buf);
          return WL_EOF;
        }
    }
  head += attsize + dptsize;

  {
    bool iseof;
    if (walis_read_all (w->istream, &iseof, NULL, NULL, head, sizeof (u32), e))
      {
        i_free (_buf);
        return error_trace (e);
      }
    if (iseof)
      {
        i_free (_buf);
        return WL_EOF;
      }
  }

  u32 actual_crc;
  memcpy (&actual_crc, head, sizeof (u32));
  if (*checksum != actual_crc)
    {
      i_free (_buf);
      return error_causef (e, ERR_CORRUPT, "Invalid CRC");
    }

  *buf = _buf;
  return SUCCESS;
}

static int
wal_read_full (const struct wal *w, u32 *checksum, const wlh type, const wlh second_type,
               u8 *buf, const u32 total_len, error *e)
{
  ASSERT (total_len >= sizeof (wlh) + sizeof (u32));

  u8 *head = buf;

  memcpy (head, &type, sizeof (wlh));
  head += sizeof (wlh);

  if (second_type != WLH_NULL)
    {
      memcpy (head, &second_type, sizeof (wlh));
      head += sizeof (wlh);
    }

  {
    const u32 toread = total_len - (head - buf) - sizeof (u32);
    if (toread > 0)
      {
        bool iseof;
        WRAP (walis_read_all (w->istream, &iseof, NULL, checksum, head, toread,
                              e));
        if (iseof)
          {
            return WL_EOF;
          }
      }

    head += toread;
    bool iseof;
    WRAP (walis_read_all (w->istream, &iseof, NULL, NULL, head, sizeof (u32),
                          e));
    if (iseof)
      {
        return WL_EOF;
      }
  }

  u32 actual_crc;
  memcpy (&actual_crc, buf + total_len - sizeof (u32), sizeof (u32));
  if (*checksum != actual_crc)
    {
      return error_causef (e, ERR_CORRUPT, "Invalid CRC");
    }

  return SUCCESS;
}

static err_t
wal_read_physical_update (struct wal *w, u32 *checksum,
                          struct wal_rec_hdr_read *r, error *e)
{
  ASSERT (r->type == WL_UPDATE);
  u8 buf[WL_UPDATE_LEN];
  const int ret = wal_read_full (w, checksum, r->type, r->update.type, buf,
                                 WL_UPDATE_LEN, e);
  WRAP (ret);
  if (ret == WL_EOF)
    {
      r->type = WL_EOF;
      return SUCCESS;
    }
  ASSERT (ret == SUCCESS);
  walf_decode_physical_update (r, buf);
  return SUCCESS;
}

static err_t
wal_read_fsm_update (struct wal *w, u32 *checksum, struct wal_rec_hdr_read *r,
                     error *e)
{
  ASSERT (r->type == WL_UPDATE);
  u8 buf[WL_FSM_UPDATE_LEN];
  const int ret = wal_read_full (w, checksum, r->type, r->update.type, buf,
                                 WL_FSM_UPDATE_LEN, e);
  WRAP (ret);
  if (ret == WL_EOF)
    {
      r->type = WL_EOF;
      return SUCCESS;
    }
  ASSERT (ret == SUCCESS);
  walf_decode_fsm_update (r, buf);
  return SUCCESS;
}

static err_t
wal_read_file_extend_update (struct wal *w, u32 *checksum,
                             struct wal_rec_hdr_read *r, error *e)
{
  ASSERT (r->type == WL_UPDATE);
  u8 buf[WL_FILE_EXT_LEN];
  const int ret = wal_read_full (w, checksum, r->type, r->update.type, buf,
                                 WL_FILE_EXT_LEN, e);
  WRAP (ret);
  if (ret == WL_EOF)
    {
      r->type = WL_EOF;
      return SUCCESS;
    }
  ASSERT (ret == SUCCESS);
  walf_decode_file_extend_update (r, buf);
  return SUCCESS;
}

static err_t
wal_read_physical_clr (struct wal *w, u32 *checksum,
                       struct wal_rec_hdr_read *r, error *e)
{
  ASSERT (r->type == WL_CLR);
  u8 buf[WL_CLR_LEN];
  const int ret
      = wal_read_full (w, checksum, r->type, r->clr.type, buf, WL_CLR_LEN, e);
  WRAP (ret);
  if (ret == WL_EOF)
    {
      r->type = WL_EOF;
      return SUCCESS;
    }
  ASSERT (ret == SUCCESS);
  walf_decode_physical_clr (r, buf);
  return SUCCESS;
}

static err_t
wal_read_fsm_clr (struct wal *w, u32 *checksum, struct wal_rec_hdr_read *r,
                  error *e)
{
  ASSERT (r->type == WL_CLR);
  u8 buf[WL_FSM_CLR_LEN];
  const int ret = wal_read_full (w, checksum, r->type, r->clr.type, buf,
                                 WL_FSM_CLR_LEN, e);
  WRAP (ret);
  if (ret == WL_EOF)
    {
      r->type = WL_EOF;
      return SUCCESS;
    }
  ASSERT (ret == SUCCESS);
  walf_decode_fsm_clr (r, buf);
  return SUCCESS;
}

static err_t
wal_read_dummy_clr (struct wal *w, u32 *checksum, struct wal_rec_hdr_read *r,
                    error *e)
{
  ASSERT (r->type == WL_CLR);
  u8 buf[WL_DUMMY_CLR_LEN];
  const int ret = wal_read_full (w, checksum, r->type, r->clr.type, buf,
                                 WL_DUMMY_CLR_LEN, e);
  WRAP (ret);
  if (ret == WL_EOF)
    {
      r->type = WL_EOF;
      return SUCCESS;
    }
  ASSERT (ret == SUCCESS);
  walf_decode_dummy_clr (r, buf);
  return SUCCESS;
}

static err_t
wal_read_begin (struct wal *w, u32 *checksum, struct wal_rec_hdr_read *r,
                error *e)
{
  ASSERT (r->type == WL_BEGIN);
  u8 buf[WL_BEGIN_LEN];
  const int ret
      = wal_read_full (w, checksum, r->type, WLH_NULL, buf, WL_BEGIN_LEN, e);
  WRAP (ret);
  if (ret == WL_EOF)
    {
      r->type = WL_EOF;
      return SUCCESS;
    }
  ASSERT (ret == SUCCESS);
  walf_decode_begin (r, buf);
  return SUCCESS;
}

static err_t
wal_read_commit (struct wal *w, u32 *checksum, struct wal_rec_hdr_read *r,
                 error *e)
{
  ASSERT (r->type == WL_COMMIT);
  u8 buf[WL_COMMIT_LEN];
  const int ret
      = wal_read_full (w, checksum, r->type, WLH_NULL, buf, WL_COMMIT_LEN, e);
  WRAP (ret);
  if (ret == WL_EOF)
    {
      r->type = WL_EOF;
      return SUCCESS;
    }
  ASSERT (ret == SUCCESS);
  walf_decode_commit (r, buf);
  return SUCCESS;
}

static err_t
wal_read_end (struct wal *w, u32 *checksum, struct wal_rec_hdr_read *r,
              error *e)
{
  ASSERT (r->type == WL_END);
  u8 buf[WL_END_LEN];
  const int ret = wal_read_full (w, checksum, r->type, WLH_NULL, buf, WL_END_LEN, e);
  WRAP (ret);
  if (ret == WL_EOF)
    {
      r->type = WL_EOF;
      return SUCCESS;
    }
  ASSERT (ret == SUCCESS);
  walf_decode_end (r, buf);
  return SUCCESS;
}

static err_t
wal_read_ckpt_begin (struct wal *w, u32 *checksum, struct wal_rec_hdr_read *r,
                     error *e)
{
  ASSERT (r->type == WL_CKPT_BEGIN);
  u8 buf[WL_CKPT_BEGIN_LEN];
  const int ret = wal_read_full (w, checksum, r->type, WLH_NULL, buf,
                                 WL_CKPT_BEGIN_LEN, e);
  WRAP (ret);
  if (ret == WL_EOF)
    {
      r->type = WL_EOF;
      return SUCCESS;
    }
  ASSERT (ret == SUCCESS);
  return SUCCESS;
}

static err_t
wal_read_ckpt_end (struct wal *w, u32 *checksum, struct wal_rec_hdr_read *r,
                   error *e)
{
  ASSERT (r->type == WL_CKPT_BEGIN || r->type == WL_CKPT_END);
  void *buf;
  const int ret = wal_read_ckpt_end_full (w, checksum, &buf, e);
  WRAP (ret);
  if (ret == WL_EOF)
    {
      r->type = WL_EOF;
      ASSERT (buf == NULL);
      return SUCCESS;
    }
  ASSERT (ret == SUCCESS);
  ASSERT (buf);
  if (walf_decode_ckpt_end (r, buf, e))
    {
      i_free (buf);
      return error_trace (e);
    }
  i_free (buf);
  return SUCCESS;
}

static err_t
wal_read_sequential (struct wal *w, struct wal_rec_hdr_read *dest, lsn *rlsn,
                     error *e)
{
  u32 checksum = checksum_init ();
  wlh t;
  bool iseof;

  walis_mark_start_log (w->istream);

  WRAP (
      walis_read_all (w->istream, &iseof, rlsn, &checksum, &t, sizeof (t), e));
  if (iseof)
    {
      dest->type = WL_EOF;
      return SUCCESS;
    }

  dest->type = -1;

  switch (t)
    {
    case WL_UPDATE:
      {
        dest->type = t;
        dest->update.type = -1;
        WRAP (walis_read_all (w->istream, &iseof, rlsn, &checksum, &t,
                              sizeof (t), e));
        if (iseof)
          {
            dest->type = WL_EOF;
            return SUCCESS;
          }
        switch (t)
          {
          case WUP_PHYSICAL:
            dest->update.type = t;
            WRAP (wal_read_physical_update (w, &checksum, dest, e));
            break;
          case WUP_FEXT:
            dest->update.type = t;
            WRAP (wal_read_file_extend_update (w, &checksum, dest, e));
            break;
          case WUP_FSM:
            dest->update.type = t;
            WRAP (wal_read_fsm_update (w, &checksum, dest, e));
            break;
          }
        if ((int)dest->update.type == -1)
          {
            dest->type = -1;
          }
        break;
      }
    case WL_CLR:
      {
        dest->type = t;
        dest->clr.type = -1;
        WRAP (walis_read_all (w->istream, &iseof, rlsn, &checksum, &t,
                              sizeof (t), e));
        if (iseof)
          {
            dest->type = WL_EOF;
            return SUCCESS;
          }
        switch (t)
          {
          case WCLR_PHYSICAL:
            dest->clr.type = t;
            WRAP (wal_read_physical_clr (w, &checksum, dest, e));
            break;
          case WCLR_FSM:
            dest->clr.type = t;
            WRAP (wal_read_fsm_clr (w, &checksum, dest, e));
            break;
          case WCLR_DUMMY:
            dest->clr.type = t;
            WRAP (wal_read_dummy_clr (w, &checksum, dest, e));
            break;
          }
        if ((int)dest->clr.type == -1)
          {
            dest->type = -1;
          }
        break;
      }
    case WL_BEGIN:
      dest->type = t;
      WRAP (wal_read_begin (w, &checksum, dest, e));
      break;
    case WL_COMMIT:
      dest->type = t;
      WRAP (wal_read_commit (w, &checksum, dest, e));
      break;
    case WL_END:
      dest->type = t;
      WRAP (wal_read_end (w, &checksum, dest, e));
      break;
    case WL_CKPT_BEGIN:
      dest->type = t;
      WRAP (wal_read_ckpt_begin (w, &checksum, dest, e));
      break;
    case WL_CKPT_END:
      dest->type = t;
      WRAP (wal_read_ckpt_end (w, &checksum, dest, e));
      break;
    }

  if ((int)dest->type == -1)
    {
      return error_causef (e, ERR_CORRUPT, "Invalid wal header type");
    }

  walis_mark_end_log (w->istream);
  i_log_wal_rec_hdr_read (LOG_TRACE, dest);

  return SUCCESS;
}

////////////////////////////////////////////////////////////
// Read Primitives

struct wal_rec_hdr_read *
wal_read_next (struct wal *w, lsn *rlsn, error *e)
{
  latch_lock (&w->latch);
  DBG_ASSERT (wal, w);

  ASSERT (w->istream);
  if (wal_read_sequential (w, &w->rhdr, rlsn, e))
    {
      latch_unlock (&w->latch);
      return NULL;
    }

  latch_unlock (&w->latch);
  return &w->rhdr;
}

struct wal_rec_hdr_read *
wal_read_entry (struct wal *w, const lsn id, error *e)
{
  latch_lock (&w->latch);
  DBG_ASSERT (wal, w);

  ASSERT (w->istream);
  if (walis_seek (w->istream, id, e))
    {
      latch_unlock (&w->latch);
      return NULL;
    }

  lsn rlsn;
  if (wal_read_sequential (w, &w->rhdr, &rlsn, e))
    {
      latch_unlock (&w->latch);
      return NULL;
    }

  latch_unlock (&w->latch);
  return &w->rhdr;
}

////////////////////////////////////////////////////////////
// Crash

err_t
wal_crash (struct wal *w, error *e)
{
  DBG_ASSERT (wal, w);

  walos_close (w->ostream, e);
  walis_close (w->istream, e);
  i_free (w);

  return SUCCESS;
}
