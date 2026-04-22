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

#include "c_specx.h"
#include "dpgt/dirty_page_table.h"
#include "pager.h"
#include "txns/txn_table.h"
#include "wal/wal_rec_hdr.h"

struct aries_ctx
{
  /**
   * At the end of the analysis phase,
   * this is the minimum recovery lsn
   *
   * It's the minimum page we need to read first in
   * the restart phase on recovery
   */
  lsn redo_lsn;

  /**
   * We keep track of the maximum transaction id that
   * we see in the database in order to pick up where we left
   * off
   */
  txid max_tid;

  /**
   * These are the reconstruction of the active
   * transaction table and the dirty page table
   * while we run recovery.
   *
   * They are ephemral and will be destroyed at the
   * end of recovery. Then the pager will create
   * them again because we're in a clean state
   */
  struct txn_table *txt;
  struct dpg_table *dpt;

  /**
   * While we scan through the log, we'll
   * be adding transactions to the transaction table
   * and we need a place to allocate / put those transactions
   * (normally we do it on the stack)
   */
  struct dbl_buffer txn_ptrs;
  struct slab_alloc alloc;
};

err_t aries_ctx_create (struct aries_ctx *dest, error *e);
void aries_ctx_free (struct aries_ctx *ctx);
struct txn *aries_ctx_txn_alloc (struct aries_ctx *ctx, error *e);

err_t pgr_restart (struct pager *p, struct aries_ctx *ctx, error *e);
err_t pgr_restart_analysis (struct pager *p, struct aries_ctx *ctx, error *e);
err_t pgr_restart_redo (struct pager *p, struct aries_ctx *ctx, error *e);
err_t pgr_restart_undo (struct pager *p, struct aries_ctx *ctx, error *e);
