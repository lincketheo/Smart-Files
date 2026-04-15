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

#include "paging/dpgt/dirty_page_table.h"
#include "paging/txns/txn_table.h"
#include "paging/wal/os_wal.h"
#include "paging/wal/wal_rec_hdr.h"

struct wal;

// Lifecycle
struct wal *wal_open (const char *fname, error *e);

/*
 * Abstract-type constructor — returns the embedded os_wal base pointer.
 * Equivalent to (struct os_wal *)wal_open(fname, e) but keeps the cast
 * in one place.
 */
struct os_wal *wal_open_os (const char *fname, error *e);
err_t wal_reset (struct wal *dest, error *e);
err_t wal_close (struct wal *w, error *e);
struct wal *wal_delete_and_reopen (struct wal *w, error *e);

/**
 * Flushes the wal to a certain lsn
 *
 * under the hood this is just the same as flush all
 * with an assert that [l] has been written already.
 */
err_t wal_flush_to (const struct wal *w, lsn l, error *e);
err_t wal_flush_all (const struct wal *w, error *e);

/**
 * Reading
 *
 * read_next reads in a sequential way
 * one log at a time.
 *
 * read_entry reads in a random access way
 * be careful with this one - it has
 * high cache miss rates
 */
struct wal_rec_hdr_read *wal_read_next (struct wal *w, lsn *read_lsn, error *e);
struct wal_rec_hdr_read *wal_read_entry (struct wal *w, lsn id, error *e);

slsn wal_append_begin_log (struct wal *w, txid tid, error *e);
slsn wal_append_commit_log (struct wal *w, txid tid, lsn prev, error *e);
slsn wal_append_end_log (struct wal *w, txid tid, lsn prev, error *e);
slsn wal_append_ckpt_begin (struct wal *w, error *e);
slsn wal_append_ckpt_end (
    struct wal *w,
    struct txn_table *att,
    struct dpg_table *dpt,
    error *e);
slsn wal_append_update_log (
    struct wal *w,
    struct wal_update_write update,
    error *e);
slsn wal_append_clr_log (struct wal *w, struct wal_clr_write clr, error *e);

/**
 * Just append an arbitrary log
 * this is used mostly for testing
 */
slsn wal_append_log (struct wal *w, const struct wal_rec_hdr_write *hdr, error *e);

err_t wal_crash (struct wal *w, error *e);
