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

#include "c_specx.h"
#include "c_specx_dev.h"
#include "dpgt/dirty_page_table.h"
#include "txns/txn_table.h"
#include "wal/wal.h"
#include "wal/wal_istream.h"
#include "wal/wal_ostream.h"
#include "wal/wal_rec_hdr.h"

#include <string.h>

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
wal_append_log (struct wal *w, const struct wal_rec_hdr_write *whdr, error *e)
{
  switch (whdr->type)
    {
    case WL_BEGIN:
      {
        return wal_append_begin_log (w, whdr->begin.tid, e);
      }
    case WL_COMMIT:
      {
        return wal_append_commit_log (w, whdr->commit.tid, whdr->commit.prev, e);
      }
    case WL_END:
      {
        return wal_append_end_log (w, whdr->end.tid, whdr->end.prev, e);
      }
    case WL_UPDATE:
      {
        return wal_append_update_log (w, whdr->update, e);
      }
    case WL_CLR:
      {
        return wal_append_clr_log (w, whdr->clr, e);
      }
    case WL_EOF:
      UNREACHABLE ();
    }

  UNREACHABLE ();
}
