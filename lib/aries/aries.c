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
#include "txns/txn.h"
#include "txns/txn_table.h"

err_t
aries_ctx_create (struct aries_ctx *dest, const lsn master_lsn, error *e)
{
  dest->master_lsn = master_lsn;
  dest->max_tid = 0;
  slab_alloc_init (&dest->alloc, sizeof (struct txn), 1000);

  dest->txt = txnt_open (e);
  if (dest->txt == NULL)
    {
      goto failed;
    }

  dest->dpt = dpgt_open (e);
  if (dest->dpt == NULL)
    {
      goto txt_failed;
    }

  if (dblb_create (&dest->txn_ptrs, sizeof (struct txn *), 100, e))
    {
      goto dpt_failed;
    }

  return SUCCESS;

dpt_failed:
  dpgt_close (dest->dpt);
txt_failed:
  txnt_close (dest->txt);
failed:
  slab_alloc_destroy (&dest->alloc);

  return error_trace (e);
}

void
aries_ctx_free (struct aries_ctx *ctx)
{
  ASSERT (ctx);
  slab_alloc_destroy (&ctx->alloc);
  txnt_close (ctx->txt);
  dpgt_close (ctx->dpt);
  dblb_free (&ctx->txn_ptrs);
}
