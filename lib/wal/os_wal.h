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

#include "c_specx_dev.h"
#include "dpgt/dirty_page_table.h"
#include "txns/txn_table.h"
#include "wal/wal_rec_hdr.h"

struct os_wal;

struct os_wal_vtable
{
  // Is this WAL implementation capable of crash recovery?
  bool (*is_recoverable) (const struct os_wal *self);

  // Lifecycle
  err_t (*close) (struct os_wal *self, error *e);
  err_t (*reset) (struct os_wal *self, error *e);

  /*
   * Delete the underlying WAL storage and open a fresh one in its place.
   * Ownership of self is transferred; the returned pointer must be used
   * from this point on (may differ from self).
   */
  struct os_wal *(*delete_and_reopen) (struct os_wal *self, error *e);

  // Flushing
  err_t (*flush_to) (struct os_wal *self, lsn l, error *e);
  err_t (*flush_all) (struct os_wal *self, error *e);

  // Sequential / random reads
  struct wal_rec_hdr_read *(*read_next) (struct os_wal *self, lsn *read_lsn, error *e);
  struct wal_rec_hdr_read *(*read_entry) (struct os_wal *self, lsn id, error *e);

  // Append helpers
  slsn (*append_begin_log) (struct os_wal *self, txid tid, error *e);
  slsn (*append_commit_log) (struct os_wal *self, txid tid, lsn prev, error *e);
  slsn (*append_end_log) (struct os_wal *self, txid tid, lsn prev, error *e);
  slsn (*append_ckpt_begin) (struct os_wal *self, error *e);
  slsn (*append_ckpt_end) (struct os_wal *self, struct txn_table *att, struct dpg_table *dpt, error *e);
  slsn (*append_update_log) (struct os_wal *self, struct wal_update_write update, error *e);
  slsn (*append_clr_log) (struct os_wal *self, struct wal_clr_write clr, error *e);
  slsn (*append_log) (struct os_wal *self, struct wal_rec_hdr_write *hdr, error *e);

  // Crash
  err_t (*crash_fn) (struct os_wal *self, error *e);
};

struct os_wal
{
  const struct os_wal_vtable *vtable;
};

HEADER_FUNC bool
oswal_is_recoverable (const struct os_wal *w)
{
  return w->vtable->is_recoverable (w);
}

HEADER_FUNC err_t
oswal_close (struct os_wal *w, error *e)
{
  return w->vtable->close (w, e);
}

HEADER_FUNC err_t
oswal_reset (struct os_wal *w, error *e)
{
  return w->vtable->reset (w, e);
}

HEADER_FUNC struct os_wal *
oswal_delete_and_reopen (struct os_wal *w, error *e)
{
  return w->vtable->delete_and_reopen (w, e);
}

HEADER_FUNC err_t
oswal_flush_to (struct os_wal *w, const lsn l, error *e)
{
  return w->vtable->flush_to (w, l, e);
}

HEADER_FUNC err_t
oswal_flush_all (struct os_wal *w, error *e)
{
  return w->vtable->flush_all (w, e);
}

HEADER_FUNC struct wal_rec_hdr_read *
oswal_read_next (struct os_wal *w, lsn *read_lsn, error *e)
{
  return w->vtable->read_next (w, read_lsn, e);
}

HEADER_FUNC struct wal_rec_hdr_read *
oswal_read_entry (struct os_wal *w, const lsn id, error *e)
{
  return w->vtable->read_entry (w, id, e);
}

HEADER_FUNC slsn
oswal_append_begin_log (struct os_wal *w, const txid tid, error *e)
{
  return w->vtable->append_begin_log (w, tid, e);
}

HEADER_FUNC slsn
oswal_append_commit_log (struct os_wal *w, const txid tid, const lsn prev, error *e)
{
  return w->vtable->append_commit_log (w, tid, prev, e);
}

HEADER_FUNC slsn
oswal_append_end_log (struct os_wal *w, const txid tid, const lsn prev, error *e)
{
  return w->vtable->append_end_log (w, tid, prev, e);
}

HEADER_FUNC slsn
oswal_append_ckpt_begin (struct os_wal *w, error *e)
{
  return w->vtable->append_ckpt_begin (w, e);
}

HEADER_FUNC slsn
oswal_append_ckpt_end (struct os_wal *w, struct txn_table *att, struct dpg_table *dpt, error *e)
{
  return w->vtable->append_ckpt_end (w, att, dpt, e);
}

HEADER_FUNC slsn
oswal_append_update_log (struct os_wal *w, const struct wal_update_write update, error *e)
{
  return w->vtable->append_update_log (w, update, e);
}

HEADER_FUNC slsn
oswal_append_clr_log (struct os_wal *w, const struct wal_clr_write clr, error *e)
{
  return w->vtable->append_clr_log (w, clr, e);
}

HEADER_FUNC slsn
oswal_append_log (struct os_wal *w, struct wal_rec_hdr_write *hdr, error *e)
{
  return w->vtable->append_log (w, hdr, e);
}

HEADER_FUNC err_t
oswal_crash (struct os_wal *w, error *e)
{
  return w->vtable->crash_fn (w, e);
}
