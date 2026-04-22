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

#include "aries.h"
#include "c_specx.h"
#include "pager.h"

////////////////////////////////////////////////////////////
// RESTART (Figure 9)

/*
 * Entry point for ARIES crash recovery.
 *
 * Sets PGR_ISRESTARTING for the duration of recovery so that pgr_flush()
 * skips the WAL-before-page flush (the WAL is already ahead of any page
 * being replayed).  Runs the three phases in order and frees the aries_ctx
 * on completion, whether or not an error occurred.
 */
err_t
pgr_restart (struct pager *p, struct aries_ctx *ctx, error *e)
{
  err_t ret = SUCCESS;
  p->flags |= PGR_ISRESTARTING;

  // ANALYSIS
  if (pgr_restart_analysis (p, ctx, e))
    {
      goto theend;
    }

  // REDO
  if (pgr_restart_redo (p, ctx, e))
    {
      goto theend;
    }

  // UNDO
  if (pgr_restart_undo (p, ctx, e))
    {
      goto theend;
    }

  // This is a good time to do a checkpoint
  // pgr_deletion_blocking_checkpoint (p, e);

theend:
  aries_ctx_free (ctx);
  p->flags &= ~PGR_ISRESTARTING;

  return error_trace (e);
}
