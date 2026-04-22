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

static err_t
wal_write_begin (const struct wal *w, const struct wal_rec_hdr_write *r, error *e)
{
  ASSERT (r->type == WL_BEGIN);

  ASSERT (w->ostream);

  u32 checksum = checksum_init ();
  const wlh t = r->type;
  WRAP (walos_write_all (w->ostream, &checksum, &t, sizeof (wlh), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->begin.tid, sizeof (txid), e));
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
  WRAP (walos_write_all (w->ostream, &checksum, &r->commit.tid, sizeof (txid), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->commit.prev, sizeof (lsn), e));
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
  WRAP (walos_write_all (w->ostream, &checksum, &r->end.tid, sizeof (txid), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->end.prev, sizeof (lsn), e));
  WRAP (walos_write_all (w->ostream, NULL, &checksum, sizeof (u32), e));

  return SUCCESS;
}

static err_t
wal_write_physical_update (const struct wal *w, const struct wal_rec_hdr_write *r, error *e)
{
  ASSERT (w->ostream);

  u32 checksum = checksum_init ();
  const wlh t = (wlh)r->type;
  const wlh ut = (wlh)r->update.type;
  WRAP (walos_write_all (w->ostream, &checksum, &t, sizeof (wlh), e));
  WRAP (walos_write_all (w->ostream, &checksum, &ut, sizeof (wlh), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->update.tid, sizeof (txid), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->update.prev, sizeof (lsn), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->update.phys.pg, sizeof (pgno), e));
  WRAP (walos_write_all (w->ostream, &checksum, r->update.phys.undo, PAGE_SIZE, e));
  WRAP (walos_write_all (w->ostream, &checksum, r->update.phys.redo, PAGE_SIZE, e));
  WRAP (walos_write_all (w->ostream, NULL, &checksum, sizeof (u32), e));

  return SUCCESS;
}

static err_t
wal_write_fsm_update (const struct wal *w, const struct wal_rec_hdr_write *r, error *e)
{
  ASSERT (w->ostream);

  u32 checksum = checksum_init ();
  const wlh t = (wlh)r->type;
  const wlh ut = (wlh)r->update.type;
  WRAP (walos_write_all (w->ostream, &checksum, &t, sizeof (wlh), e));
  WRAP (walos_write_all (w->ostream, &checksum, &ut, sizeof (wlh), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->update.tid, sizeof (txid), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->update.prev, sizeof (lsn), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->update.fsm.pg, sizeof (pgno), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->update.fsm.bit, sizeof (p_size), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->update.fsm.undo, sizeof (u8), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->update.fsm.redo, sizeof (u8), e));
  WRAP (walos_write_all (w->ostream, NULL, &checksum, sizeof (u32), e));

  return SUCCESS;
}

static err_t
wal_write_file_extend_update (const struct wal *w, const struct wal_rec_hdr_write *r, error *e)
{
  ASSERT (w->ostream);

  u32 checksum = checksum_init ();
  const wlh t = (wlh)r->type;
  const wlh ut = (wlh)r->update.type;
  WRAP (walos_write_all (w->ostream, &checksum, &t, sizeof (wlh), e));
  WRAP (walos_write_all (w->ostream, &checksum, &ut, sizeof (wlh), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->update.tid, sizeof (txid), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->update.prev, sizeof (lsn), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->update.fext.undo, sizeof (pgno), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->update.fext.redo, sizeof (pgno), e));
  WRAP (walos_write_all (w->ostream, NULL, &checksum, sizeof (u32), e));

  return SUCCESS;
}

static err_t
wal_write_physical_clr (const struct wal *w, const struct wal_rec_hdr_write *r, error *e)
{
  ASSERT (r->type == WL_CLR);

  ASSERT (w->ostream);

  u32 checksum = checksum_init ();
  const wlh t = r->type;
  const wlh ut = r->clr.type;
  WRAP (walos_write_all (w->ostream, &checksum, &t, sizeof (wlh), e));
  WRAP (walos_write_all (w->ostream, &checksum, &ut, sizeof (wlh), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->clr.tid, sizeof (txid), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->clr.prev, sizeof (lsn), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->clr.phys.pg, sizeof (pgno), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->clr.undo_next, sizeof (lsn), e));
  WRAP (walos_write_all (w->ostream, &checksum, r->clr.phys.redo, PAGE_SIZE, e));
  WRAP (walos_write_all (w->ostream, NULL, &checksum, sizeof (u32), e));

  return SUCCESS;
}

static err_t
wal_write_fsm_clr (const struct wal *w, const struct wal_rec_hdr_write *r, error *e)
{
  ASSERT (r->type == WL_CLR);

  ASSERT (w->ostream);

  u32 checksum = checksum_init ();
  const wlh t = r->type;
  const wlh ut = r->clr.type;
  WRAP (walos_write_all (w->ostream, &checksum, &t, sizeof (wlh), e));
  WRAP (walos_write_all (w->ostream, &checksum, &ut, sizeof (wlh), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->clr.tid, sizeof (txid), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->clr.prev, sizeof (lsn), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->clr.fsm.pg, sizeof (pgno), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->clr.undo_next, sizeof (lsn), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->clr.fsm.bit, sizeof (p_size), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->clr.fsm.redo, sizeof (u8), e));
  WRAP (walos_write_all (w->ostream, NULL, &checksum, sizeof (u32), e));

  return SUCCESS;
}

static err_t
wal_write_dummy_clr (const struct wal *w, const struct wal_rec_hdr_write *r, error *e)
{
  ASSERT (r->type == WL_CLR);

  ASSERT (w->ostream);

  u32 checksum = checksum_init ();
  const wlh t = r->type;
  const wlh ut = r->clr.type;
  WRAP (walos_write_all (w->ostream, &checksum, &t, sizeof (wlh), e));
  WRAP (walos_write_all (w->ostream, &checksum, &ut, sizeof (wlh), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->clr.tid, sizeof (txid), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->clr.prev, sizeof (lsn), e));
  WRAP (walos_write_all (w->ostream, &checksum, &r->clr.undo_next, sizeof (lsn), e));
  WRAP (walos_write_all (w->ostream, NULL, &checksum, sizeof (u32), e));

  return SUCCESS;
}

slsn
wal_write_locked (struct wal *w, error *e)
{
  ASSERT (w->ostream);

  const lsn ret = walos_get_next_lsn (w->ostream);

  switch (w->whdr.type)
    {
    case WL_BEGIN:
      {
        WRAP (wal_write_begin (w, &w->whdr, e));
        break;
      }
    case WL_COMMIT:
      {
        WRAP (wal_write_commit (w, &w->whdr, e));
        break;
      }
    case WL_END:
      {
        WRAP (wal_write_end (w, &w->whdr, e));
        break;
      }
    case WL_UPDATE:
      {
        switch (w->whdr.update.type)
          {
          case WUP_PHYSICAL:
            {
              WRAP (wal_write_physical_update (w, &w->whdr, e));
              break;
            }
          case WUP_FSM:
            {
              WRAP (wal_write_fsm_update (w, &w->whdr, e));
              break;
            }
          case WUP_FEXT:
            {
              WRAP (wal_write_file_extend_update (w, &w->whdr, e));
              break;
            }
          }
        break;
      }
    case WL_CLR:
      {
        switch (w->whdr.clr.type)
          {
          case WCLR_PHYSICAL:
            {
              WRAP (wal_write_physical_clr (w, &w->whdr, e));
              break;
            }
          case WCLR_FSM:
            {
              WRAP (wal_write_fsm_clr (w, &w->whdr, e));
              break;
            }
          case WCLR_DUMMY:
            {
              WRAP (wal_write_dummy_clr (w, &w->whdr, e));
              break;
            }
          }
        break;
      }
    case WL_EOF:
      {
        UNREACHABLE ();
      }
    }

  return ret;
}
