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

#include "c_specx.h"
#include "c_specx/ds/string.h"
#include "c_specx/intf/logging.h"
#include "c_specx_dev.h"
#include "dpgt/dirty_page_table.h"
#include "txns/txn_table.h"
#include "wal/wal_istream.h"
#include "wal/wal_ostream.h"
#include "wal/wal_rec_hdr.h"

#include <string.h>

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
wal_append_update_log_impl (struct os_wal *self, const struct wal_update_write update, error *e)
{
  return wal_append_update_log ((struct wal *)self, update, e);
}

static slsn
wal_append_clr_log_impl (struct os_wal *self, const struct wal_clr_write clr, error *e)
{
  return wal_append_clr_log ((struct wal *)self, clr, e);
}

static slsn
wal_append_log_impl (struct os_wal *self, struct wal_rec_hdr_write *hdr, error *e)
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
  .append_update_log = wal_append_update_log_impl,
  .append_clr_log = wal_append_clr_log_impl,
  .append_log = wal_append_log_impl,
  .crash_fn = wal_crash_impl,
};

static struct wal *
wal_open_internal (const char *fname, bool copy_fname, error *e)
{
  struct wal *dest = i_malloc (1, sizeof *dest, e);
  if (dest == NULL)
    {
      return NULL;
    }
  if (copy_fname)
    {
      dest->iown_fname = true;
      if (string_copy (&dest->fname, strfcstr (fname), e))
        {
          i_free (dest);
          return NULL;
        }
    }
  else
    {
      dest->iown_fname = false;
      dest->fname = strfcstr (fname);
    }

  dest->base.vtable = &wal_vtable;
  dest->ostream = NULL;
  dest->istream = NULL;
  latch_init (&dest->latch);

  dest->ostream = walos_open (dest->fname.data, e);
  if (dest->ostream == NULL)
    {
      i_free (dest);
      return NULL;
    }

  dest->istream = walis_open (dest->fname.data, e);
  if (dest->istream == NULL)
    {
      walos_close (dest->ostream, e);
      i_free (dest);
      return NULL;
    }

  DBG_ASSERT (wal, dest);

  return dest;
}

struct wal *
wal_open (const char *fname, error *e)
{
  return wal_open_internal (fname, true, e);
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

  i_remove_quiet (w->fname.data, e);

  w->ostream = walos_open (w->fname.data, e);
  if (w->ostream == NULL)
    {
      return e->cause_code;
    }

  w->istream = walis_open (w->fname.data, e);
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

  if (w->fname.data)
    {
      i_free ((void *)w->fname.data);
    }

  i_free (w);

  return error_trace (e);
}

struct wal *
wal_delete_and_reopen (struct wal *w, error *e)
{
  struct string fname = w->fname;
  w->fname.data = NULL;

  if (wal_close (w, e))
    {
      return NULL;
    }

  if (i_remove_quiet (fname.data, e))
    {
      return NULL;
    }

  return wal_open_internal (fname.data, false, e);
}

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

err_t
wal_crash (struct wal *w, error *e)
{
  DBG_ASSERT (wal, w);

  walos_close (w->ostream, e);
  walis_close (w->istream, e);
  if (w->iown_fname && w->fname.data)
    {
      i_free ((void *)w->fname.data);
    }
  i_free (w);

  return SUCCESS;
}
