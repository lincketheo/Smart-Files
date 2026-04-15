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
#include "pager.h"
#include "pages/fsm_page.h"
#include "pages/page.h"
#include "tlclib.h"
#include "wal/wal_rec_hdr.h"

static err_t
pgr_apply_physical_redo (struct pager *p, page_h *ph, pgno pg, const u8 *redo,
                         const struct aries_ctx *ctx, error *e)
{
  memcpy (page_h_w (ph)->raw, redo, PAGE_SIZE);
  page_set_page_lsn (page_h_w (ph), ctx->redo_lsn);
  return pgr_release (p, ph, PG_PERMISSIVE, e);
}

static err_t
pgr_apply_fsm_redo (struct pager *p, page_h *ph, const pgno pg, const u8 redo,
                    const struct aries_ctx *ctx, error *e)
{
  if (redo)
    {
      fsm_set_bit (page_h_w (ph), pgtoidx (pg));
    }
  else
    {
      fsm_clr_bit (page_h_w (ph), pgtoidx (pg));
    }
  page_set_page_lsn (page_h_w (ph), ctx->redo_lsn);
  return pgr_release (p, ph, PG_PERMISSIVE, e);
}

err_t
pgr_apply_redo (struct pager *p, struct wal_rec_hdr_read *log_rec,
                struct aries_ctx *ctx, error *e)
{
  ASSERT (log_rec->type == WL_UPDATE || log_rec->type == WL_CLR);

  pgno affected_pg = 0;

  /*
   * Filter out record types that have no page-level redo effect.
   * WUP_FEXT: file extension is re-done by replaying the size metadata,
   * not by writing page data - nothing to apply here.
   * WCLR_DUMMY: a dummy CLR is a no-op compensation record with no redo
   * image.
   */
  switch (log_rec->type)
    {
    case WL_UPDATE:
      {
        switch (log_rec->update.type)
          {
          case WUP_PHYSICAL:
            {
              affected_pg = log_rec->update.phys.pg;
              break;
            }
          case WUP_FSM:
            {
              affected_pg = pgtofsm (log_rec->update.fsm.pg);
              break;
            }
          case WUP_FEXT:
            {
              return SUCCESS;
            }
          }
        break;
      }
    case WL_CLR:
      {
        switch (log_rec->clr.type)
          {
          case WCLR_PHYSICAL:
            {
              affected_pg = log_rec->clr.phys.pg;
              break;
            }
          case WCLR_FSM:
            {
              affected_pg = pgtofsm (log_rec->clr.fsm.pg);
              break;
            }
          case WCLR_DUMMY:
            {
              return SUCCESS;
            }
          }
        break;
      }
    default:
      {
        UNREACHABLE ();
      }
    }

  page_h ph = page_h_create ();
  lsn rec_lsn;

  /**
   * If the page isn't in the dirty page table, the buffer manager
   * already flushed it to disk cleanly before the crash. The on-disk
   * copy is at least as up-to-date as the checkpoint, so this log
   * record's effect is already durable
   */
  if (!dpgt_get (&rec_lsn, ctx->dpt, affected_pg))
    {
      return SUCCESS;
    }

  /**
   * rec_lsn is the LSN at which this page first became dirty after the
   * last checkpoint. If redo_lsn < rec_lsn, this log record was written
   * before the page was even dirtied - it cannot possibly be an update
   * we need to re-apply to this page.
   */
  if (ctx->redo_lsn < rec_lsn)
    {
      return SUCCESS;
    }

  if (pgr_get_writable (&ph, NULL, PG_PERMISSIVE, affected_pg, p, e))
    {
      goto failed;
    }

  /**
   * The page's stored LSN records the last log record that was applied
   * to it before it was written to disk. If that LSN is already >= our
   * redo_lsn, this update was already written into the page image on
   * disk - applying it again would be a double-apply, so we skip.
   */
  if (page_get_page_lsn (page_h_ro (&ph)) >= ctx->redo_lsn)
    {
      if (pgr_release (p, &ph, PG_PERMISSIVE, e))
        {
          goto failed;
        }
      return SUCCESS;
    }

  // Actually apply the redo image to the page.
  switch (log_rec->type)
    {
    case WL_UPDATE:
      {
        switch (log_rec->update.type)
          {
          case WUP_PHYSICAL:
            {
              struct physical_read_update *up = &log_rec->update.phys;
              if (pgr_apply_physical_redo (p, &ph, up->pg, up->redo, ctx, e))
                {
                  goto failed;
                }
              return SUCCESS;
            }
          case WUP_FSM:
            {
              const struct fsm_update up = log_rec->update.fsm;
              if (pgr_apply_fsm_redo (p, &ph, up.pg, up.redo, ctx, e))
                {
                  goto failed;
                }
              return SUCCESS;
            }
          case WUP_FEXT:
            {
              UNREACHABLE ();
            }
          }
        break;
      }
    case WL_CLR:
      {
        switch (log_rec->clr.type)
          {
          case WCLR_PHYSICAL:
            {
              struct physical_read_clr *up = &log_rec->clr.phys;
              if (pgr_apply_physical_redo (p, &ph, up->pg, up->redo, ctx, e))
                {
                  goto failed;
                }
              return SUCCESS;
            }
          case WCLR_FSM:
            {
              const struct fsm_clr up = log_rec->clr.fsm;
              if (pgr_apply_fsm_redo (p, &ph, up.pg, up.redo, ctx, e))
                {
                  goto failed;
                }
              return SUCCESS;
            }
          case WCLR_DUMMY:
            {
              UNREACHABLE ();
            }
          }
        break;
      }
    default:
      {
        UNREACHABLE ();
      }
    }

  UNREACHABLE ();

failed:
  pgr_cancel_if_exists (p, &ph);
  return e->cause_code;
}
