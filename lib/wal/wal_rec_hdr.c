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

#include "wal/wal_rec_hdr.h"

#include "c_specx.h"
#include "c_specx/dev/assert.h"
#include "dpgt/dirty_page_table.h"
#include "pages/fsm_page.h"
#include "pages/page.h"
#include "txns/txn_table.h"

void
wal_rec_hdr_read_random (struct wal_rec_hdr_read *dest, struct alloc *alloc)
{
  dest->type = randu32r (WL_BEGIN, WL_CLR);
  switch (dest->type)
    {
    case WL_BEGIN:
      {
        dest->begin.tid = randu32 ();
        break;
      }
    case WL_COMMIT:
      {
        dest->commit.tid = randu32 ();
        dest->commit.prev = randu32 ();
        break;
      }
    case WL_END:
      {
        dest->end.tid = randu32 ();
        dest->end.prev = randu32 ();
        break;
      }
    case WL_UPDATE:
      {
        dest->update.type = randu32r (WUP_PHYSICAL, WUP_FEXT);
        dest->update.tid = randu32 ();
        dest->update.prev = randu32 ();
        switch (dest->update.type)
          {
          case WUP_PHYSICAL:
            {
              dest->update.phys.pg = randu32 ();
              rand_bytes (dest->update.phys.undo, PAGE_SIZE);
              rand_bytes (dest->update.phys.redo, PAGE_SIZE);
              break;
            }
          case WUP_FSM:
            {
              dest->update.fsm.pg = randu32 ();
              dest->update.fsm.undo = randu8 ();
              dest->update.fsm.redo = randu8 ();
              break;
            }
          case WUP_FEXT:
            {
              dest->update.fext.undo = randu32 ();
              dest->update.fext.redo = randu32 ();
              break;
            }
          }
        break;
      }
    case WL_CLR:
      {
        dest->clr.type = randu32r (WCLR_PHYSICAL, WCLR_DUMMY);
        dest->clr.tid = randu32 ();
        dest->clr.prev = randu32 ();
        dest->clr.undo_next = randu32 ();
        switch (dest->clr.type)
          {
          case WCLR_PHYSICAL:
            {
              dest->clr.phys.pg = randu32 ();
              rand_bytes (dest->clr.phys.redo, PAGE_SIZE);
              break;
            }
          case WCLR_FSM:
            {
              dest->clr.fsm.pg = randu32 ();
              dest->clr.fsm.redo = randu8 ();
              break;
            }
          case WCLR_DUMMY:
            {
              break;
            }
          }
        break;
      }
    case WL_EOF:
      {
        ASSERT (false);
      }
    }
}

const char *
wal_rec_hdr_type_tostr (const enum wal_rec_hdr_type type)
{
  switch (type)
    {
    case WL_UPDATE:
      {
        return "WL_UPDATE";
      }
    case WL_CLR:
      {
        return "WL_CLR";
      }
    case WL_BEGIN:
      {
        return "WL_BEGIN";
      }
    case WL_COMMIT:
      {
        return "WL_COMMIT";
      }
    case WL_END:
      {
        return "WL_END";
      }
    case WL_EOF:
      {
        return "WL_EOF";
      }
    }

  UNREACHABLE ();
}

struct wal_rec_hdr_write
wrhw_from_wrhr (struct wal_rec_hdr_read *src)
{
  switch (src->type)
    {
    case WL_BEGIN:
      {
        return (struct wal_rec_hdr_write){
          .type = WL_BEGIN,
          .begin = src->begin,
        };
      }
    case WL_COMMIT:
      {
        return (struct wal_rec_hdr_write){
          .type = WL_COMMIT,
          .commit = src->commit,
        };
      }
    case WL_END:
      {
        return (struct wal_rec_hdr_write){
          .type = WL_END,
          .end = src->end,
        };
      }
    case WL_UPDATE:
      {
        switch (src->update.type)
          {
          case WUP_PHYSICAL:
            {
              return (struct wal_rec_hdr_write){
                .type = WL_UPDATE,
                .update = {
                    .type = WUP_PHYSICAL,
                    .tid = src->update.tid,
                    .prev = src->update.prev,
                    .phys = (struct physical_write_update){
                        .pg = src->update.phys.pg,
                        .redo = src->update.phys.redo,
                        .undo = src->update.phys.undo,
                    },
                },
              };
            }
          case WUP_FSM:
            {
              return (struct wal_rec_hdr_write){
                .type = WL_UPDATE,
                .update = {
                    .type = WUP_FSM,
                    .tid = src->update.tid,
                    .prev = src->update.prev,
                    .fsm = src->update.fsm,
                },
              };
            }
          case WUP_FEXT:
            {
              return (struct wal_rec_hdr_write){
                .type = WL_UPDATE,
                .update = {
                    .type = WUP_FEXT,
                    .tid = src->update.tid,
                    .prev = src->update.prev,
                    .fext = src->update.fext,
                },
              };
            }
          }
        break;
      }
    case WL_CLR:
      {
        switch (src->clr.type)
          {
          case WCLR_PHYSICAL:
            {
              return (struct wal_rec_hdr_write){
                .type = WL_CLR,
                .clr = {
                    .type = WCLR_PHYSICAL,
                    .tid = src->clr.tid,
                    .prev = src->clr.prev,
                    .undo_next = src->clr.undo_next,
                    .phys = (struct physical_write_clr){
                        .pg = src->clr.phys.pg,
                        .redo = src->clr.phys.redo,
                    },
                },
              };
            }
          case WCLR_FSM:
            {
              return (struct wal_rec_hdr_write){
                .type = WL_CLR,
                .clr = {
                    .type = WCLR_FSM,
                    .tid = src->clr.tid,
                    .prev = src->clr.prev,
                    .undo_next = src->clr.undo_next,
                    .fsm = src->clr.fsm,
                },
              };
            }
          case WCLR_DUMMY:
            {
              return (struct wal_rec_hdr_write){
                .type = WL_CLR,
                .clr = {
                    .type = WCLR_DUMMY,
                    .tid = src->clr.tid,
                    .prev = src->clr.prev,
                    .undo_next = src->clr.undo_next,
                },
              };
            }
          }
        break;
      }
    case WL_EOF:
      {
        UNREACHABLE ();
      }
    }
  UNREACHABLE ();
}

stxid
wrh_get_tid (const struct wal_rec_hdr_read *h)
{
  switch (h->type)
    {
    case WL_BEGIN:
      {
        return h->begin.tid;
      }
    case WL_COMMIT:
      {
        return h->commit.tid;
      }
    case WL_END:
      {
        return h->end.tid;
      }
    case WL_UPDATE:
      {
        return h->update.tid;
      }
    case WL_CLR:
      {
        return h->clr.tid;
      }
    case WL_EOF:
      {
        UNREACHABLE ();
      }
    }
  UNREACHABLE ();
}

slsn
wrh_get_prev_lsn (const struct wal_rec_hdr_read *h)
{
  switch (h->type)
    {
    case WL_BEGIN:
      {
        return 0;
      }
    case WL_COMMIT:
      {
        return h->commit.prev;
      }
    case WL_END:
      {
        return h->end.prev;
      }
    case WL_UPDATE:
      {
        return h->update.prev;
      }
    case WL_CLR:
      {
        return h->clr.prev;
      }
    case WL_EOF:
      {
        UNREACHABLE ();
      }
    }
  UNREACHABLE ();
}

bool
wrh_is_undoable (const struct wal_rec_hdr_read *h)
{
  switch (h->type)
    {
    case WL_BEGIN:
      {
        return false;
      }
    case WL_COMMIT:
      {
        return false;
      }
    case WL_END:
      {
        return false;
      }
    case WL_UPDATE:
      {
        switch (h->update.type)
          {
          case WUP_PHYSICAL:
            {
              return true;
            }
          case WUP_FSM:
            {
              return true;
            }
          case WUP_FEXT:
            {
              return false;
            }
          }
        UNREACHABLE ();
      }
    case WL_CLR:
      {
        return false;
      }
    case WL_EOF:
      {
        UNREACHABLE ();
      }
    }
  UNREACHABLE ();
}

bool
wrh_is_redoable (const struct wal_rec_hdr_read *h)
{
  switch (h->type)
    {
    case WL_BEGIN:
      {
        return false;
      }
    case WL_COMMIT:
      {
        return false;
      }
    case WL_END:
      {
        return false;
      }
    case WL_UPDATE:
      {
        switch (h->update.type)
          {
          case WUP_PHYSICAL:
            {
              return true;
            }
          case WUP_FSM:
            {
              return true;
            }
          case WUP_FEXT:
            {
              return false;
            }
          }
        UNREACHABLE ();
      }
    case WL_CLR:
      {
        switch (h->clr.type)
          {
          case WCLR_PHYSICAL:
            {
              return true;
            }
          case WCLR_FSM:
            {
              return true;
            }
          case WCLR_DUMMY:
            {
              return false;
            }
          }
        UNREACHABLE ();
      }
    case WL_EOF:
      {
        UNREACHABLE ();
      }
    }
  UNREACHABLE ();
}

pgno
wrh_get_affected_pg (const struct wal_rec_hdr_read *h)
{
  switch (h->type)
    {
    case WL_BEGIN:
      {
        UNREACHABLE ();
      }
    case WL_COMMIT:
      {
        UNREACHABLE ();
      }
    case WL_END:
      {
        UNREACHABLE ();
      }
    case WL_UPDATE:
      {
        switch (h->update.type)
          {
          case WUP_PHYSICAL:
            {
              return h->update.phys.pg;
            }
          case WUP_FSM:
            {
              return h->update.fsm.pg;
            }
          case WUP_FEXT:
            {
              UNREACHABLE ();
            }
          }
        UNREACHABLE ();
      }
    case WL_CLR:
      {
        switch (h->clr.type)
          {
          case WCLR_PHYSICAL:
            {
              return h->clr.phys.pg;
            }
          case WCLR_FSM:
            {
              return h->clr.fsm.pg;
            }
          case WCLR_DUMMY:
            {
              UNREACHABLE ();
            }
          }
        UNREACHABLE ();
      }
    case WL_EOF:
      {
        UNREACHABLE ();
      }
    }
  UNREACHABLE ();
}

#ifndef NTEST
bool
wal_rec_hdr_read_equal (const struct wal_rec_hdr_read *left, const struct wal_rec_hdr_read *right)
{
  if (left->type != right->type)
    {
      return false;
    }

  bool match = true;

  switch (left->type)
    {
    case WL_UPDATE:
      {
        if (left->update.type != right->update.type)
          {
            return false;
          }

        match = match && left->update.tid == right->update.tid;
        match = match && left->update.prev == right->update.prev;

        switch (left->update.type)
          {
          case WUP_FSM:
            {
              match = match && left->update.fsm.pg == right->update.fsm.pg;
              match = match && left->update.fsm.undo == right->update.fsm.undo;
              match = match && left->update.fsm.redo == right->update.fsm.redo;
              break;
            }
          case WUP_PHYSICAL:
            {
              match = match && left->update.phys.pg == right->update.phys.pg;
              match = match
                      && memcmp (left->update.phys.undo,
                                 right->update.phys.undo, PAGE_SIZE)
                             == 0;
              match = match
                      && memcmp (left->update.phys.redo,
                                 right->update.phys.redo, PAGE_SIZE)
                             == 0;
              break;
            }
          case WUP_FEXT:
            {
              match
                  = match && left->update.fext.undo == right->update.fext.undo;
              match
                  = match && left->update.fext.redo == right->update.fext.redo;
              break;
            }
          }
        break;
      }

    case WL_CLR:
      {
        if (left->clr.type != right->clr.type)
          {
            return false;
          }

        match = match && left->clr.tid == right->clr.tid;
        match = match && left->clr.prev == right->clr.prev;
        match = match && left->clr.undo_next == right->clr.undo_next;

        switch (left->clr.type)
          {
          case WCLR_PHYSICAL:
            {
              match = match && left->clr.phys.pg == right->clr.phys.pg;
              match = match
                      && memcmp (left->clr.phys.redo, right->clr.phys.redo,
                                 PAGE_SIZE)
                             == 0;
              break;
            }
          case WCLR_FSM:
            {
              match = match && left->clr.fsm.pg == right->clr.fsm.pg;
              match = match && left->clr.fsm.redo == right->clr.fsm.redo;
              break;
            }
          case WCLR_DUMMY:
            {
              break;
            }
          }

        break;
      }

    case WL_BEGIN:
      {
        match = match && left->begin.tid == right->begin.tid;
        break;
      }

    case WL_END:
      {
        match = match && left->end.tid == right->end.tid;
        match = match && left->end.prev == right->end.prev;
        break;
      }

    case WL_COMMIT:
      {
        match = match && left->commit.tid == right->commit.tid;
        match = match && left->commit.prev == right->commit.prev;
        break;
      }

    case WL_EOF:
      {
        return true;
      }
    }

  return match;
}
#endif

static void
i_log_pysical_update (const int log_level, const struct wal_rec_hdr_read *r)
{
  i_log (log_level, "------------------ %s:\n",
         wal_rec_hdr_type_tostr (r->type));
  i_printf (log_level, "TID: %" PRtxid "\n", r->update.tid);
  i_printf (log_level, "PREV: %" PRpgno "\n", r->update.prev);
  i_printf (log_level, "PG: %" PRpgno "\n", r->update.phys.pg);
  page temp;

  i_printf (log_level, "UNDO: ");
  for (int i = 0; i < 10; ++i)
    {
      i_printf (log_level, "%X ", r->update.phys.undo[i]);
    }
  i_printf (log_level, "... ");
  for (u32 i = PAGE_SIZE - 10; i < PAGE_SIZE; ++i)
    {
      i_printf (log_level, "%X ", r->update.phys.undo[i]);
    }
  i_printf (log_level, "\n");
  memcpy (temp.raw, r->update.phys.undo, PAGE_SIZE);
  temp.pg = r->update.phys.pg;
  i_log_page (log_level, &temp);

  i_printf (log_level, "REDO: ");
  for (u32 i = 0; i < 10; ++i)
    {
      i_printf (log_level, "%X ", r->update.phys.redo[i]);
    }
  i_printf (log_level, "... ");
  for (u32 i = PAGE_SIZE - 10; i < PAGE_SIZE; ++i)
    {
      i_printf (log_level, "%X ", r->update.phys.redo[i]);
    }
  i_printf (log_level, "\n");
  memcpy (temp.raw, r->update.phys.redo, PAGE_SIZE);
  temp.pg = r->update.phys.pg;
  i_log_page (log_level, &temp);
  i_log (log_level, "------------------------------------\n");
}

static void
i_log_fsm_update (const int log_level, const struct wal_rec_hdr_read *r)
{
  i_log (log_level, "------------------ %s:\n",
         wal_rec_hdr_type_tostr (r->type));
  i_printf (log_level, "TID: %" PRtxid "\n", r->update.tid);
  i_printf (log_level, "PREV: %" PRpgno "\n", r->update.prev);
  i_printf (log_level, "PG: %" PRpgno "\n", r->update.fsm.pg);
  i_printf (log_level, "UNDO: %d\n", r->update.fsm.undo);
  i_printf (log_level, "REDO: %d\n", r->update.fsm.redo);
  i_log (log_level, "------------------------------------\n");
}

static void
i_log_file_extend_update (const int log_level, const struct wal_rec_hdr_read *r)
{
  i_log (log_level, "------------------ %s:\n",
         wal_rec_hdr_type_tostr (r->type));
  i_printf (log_level, "TID: %" PRtxid "\n", r->update.tid);
  i_printf (log_level, "PREV: %" PRpgno "\n", r->update.prev);
  i_printf (log_level, "UNDO: %" PRpgno "\n", r->update.fext.undo);
  i_printf (log_level, "REDO: %" PRpgno "\n", r->update.fext.redo);
  i_log (log_level, "------------------------------------\n");
}

static void
i_log_physical_clr (const int log_level, const struct wal_rec_hdr_read *r)
{
  i_log (log_level, "------------------ %s:\n",
         wal_rec_hdr_type_tostr (r->type));
  i_printf (log_level, "TID: %" PRtxid "\n", r->clr.tid);
  i_printf (log_level, "PREV: %" PRpgno "\n", r->clr.prev);
  i_printf (log_level, "PG: %" PRpgno "\n", r->clr.phys.pg);
  i_printf (log_level, "UNDO_NEXT: %" PRpgno "\n", r->clr.undo_next);
  i_printf (log_level, "REDO: ");
  for (u32 i = 0; i < 10; ++i)
    {
      i_printf (log_level, "%X ", r->clr.phys.redo[i]);
    }
  i_printf (log_level, "... ");
  for (u32 i = PAGE_SIZE - 10; i < PAGE_SIZE; ++i)
    {
      i_printf (log_level, "%X ", r->clr.phys.redo[i]);
    }
  i_printf (log_level, "\n");
  i_log (log_level, "------------------------------------\n");
}

static void
i_log_fsm_clr (const int log_level, const struct wal_rec_hdr_read *r)
{
  i_log (log_level, "------------------ %s:\n",
         wal_rec_hdr_type_tostr (r->type));
  i_printf (log_level, "TID: %" PRtxid "\n", r->clr.tid);
  i_printf (log_level, "PREV: %" PRpgno "\n", r->clr.prev);
  i_printf (log_level, "PG: %" PRpgno "\n", r->clr.fsm.pg);
  i_printf (log_level, "UNDO_NEXT: %" PRpgno "\n", r->clr.undo_next);
  i_printf (log_level, "REDO: %d\n", r->clr.fsm.redo);
  i_log (log_level, "------------------------------------\n");
}

static void
i_log_dummy_clr (const int log_level, const struct wal_rec_hdr_read *r)
{
  i_log (log_level, "------------------ %s:\n",
         wal_rec_hdr_type_tostr (r->type));
  i_printf (log_level, "TID: %" PRtxid "\n", r->clr.tid);
  i_printf (log_level, "PREV: %" PRpgno "\n", r->clr.prev);
  i_printf (log_level, "UNDO_NEXT: %" PRpgno "\n", r->clr.undo_next);
  i_log (log_level, "------------------------------------\n");
}

static void
i_log_begin (const int log_level, const struct wal_rec_hdr_read *r)
{
  i_log (log_level, "------------------ %s:\n",
         wal_rec_hdr_type_tostr (r->type));
  i_printf (log_level, "HEADER: %d\n", r->type);
  i_printf (log_level, "TID: %" PRtxid "\n", r->begin.tid);
  i_log (log_level, "------------------------------------\n");
}

static void
i_log_commit (const int log_level, const struct wal_rec_hdr_read *r)
{
  i_log (log_level, "------------------ %s:\n",
         wal_rec_hdr_type_tostr (r->type));
  i_printf (log_level, "HEADER: %d\n", r->type);
  i_printf (log_level, "TID: %" PRtxid "\n", r->commit.tid);
  i_printf (log_level, "PREV: %" PRlsn "\n", r->commit.prev);
  i_log (log_level, "------------------------------------\n");
}

static void
i_log_end (const int log_level, const struct wal_rec_hdr_read *r)
{
  i_log (log_level, "------------------ %s:\n",
         wal_rec_hdr_type_tostr (r->type));
  i_printf (log_level, "HEADER: %d\n", r->type);
  i_printf (log_level, "TID: %" PRtxid "\n", r->end.tid);
  i_printf (log_level, "PREV: %" PRlsn "\n", r->end.prev);
  i_log (log_level, "------------------------------------\n");
}

void
i_log_wal_rec_hdr_read (const int log_level, struct wal_rec_hdr_read *r)
{
  switch (r->type)
    {
    case WL_UPDATE:
      {
        switch (r->update.type)
          {
          case WUP_PHYSICAL:
            {
              i_log_pysical_update (log_level, r);
              break;
            }
          case WUP_FSM:
            {
              i_log_fsm_update (log_level, r);
              break;
            }
          case WUP_FEXT:
            {
              i_log_file_extend_update (log_level, r);
              break;
            }
          }
        break;
      }

    case WL_CLR:
      {
        switch (r->clr.type)
          {
          case WCLR_PHYSICAL:
            {
              i_log_physical_clr (log_level, r);
              break;
            }
          case WCLR_FSM:
            {
              i_log_fsm_clr (log_level, r);
              break;
            }
          case WCLR_DUMMY:
            {
              i_log_dummy_clr (log_level, r);
              break;
            }
          }
        break;
      }

    case WL_BEGIN:
      {
        i_log_begin (log_level, r);
        break;
      }

    case WL_COMMIT:
      {
        i_log_commit (log_level, r);
        break;
      }
    case WL_END:
      {
        i_log_end (log_level, r);
        break;
      }

    case WL_EOF:
      {
        i_log (log_level, "WL_EOF\n");
        break;
      }
    }
}

void
i_print_wal_rec_hdr_read_light (const int log_level, const struct wal_rec_hdr_read *r,
                                const lsn l)
{
  switch (r->type)
    {
    case WL_UPDATE:
      switch (r->update.type)
        {
        case WUP_PHYSICAL:
          {
            i_printf (log_level,
                      "%15" PRlsn "  UPDATE PHYS  "
                      "[ txid = %8" PRtxid ", pg   = %8" PRpgno
                      "                   "
                      "                   "
                      "   ] --> %" PRlsn "\n",
                      l, r->update.tid, r->update.phys.pg, r->update.prev);
            break;
          }

        case WUP_FSM:
          {
            i_printf (log_level,
                      "%15" PRlsn "  UPDATE FSM   "
                      "[ txid = %8" PRtxid ", pg   = %8" PRpgno
                      ", undo = 0x%02x, redo = "
                      "0x%02x               ] "
                      "--> %" PRlsn "\n",
                      l, r->update.tid, r->update.fsm.pg,
                      (unsigned)r->update.fsm.undo,
                      (unsigned)r->update.fsm.redo, r->update.prev);
            break;
          }

        case WUP_FEXT:
          {
            i_printf (log_level,
                      "%15" PRlsn "  UPDATE FEXT  "
                      "[ txid = %8" PRtxid ", undo_pgs = %8" PRpgno
                      ", redo_pgs = %8" PRpgno "                ] --> "
                      "%" PRlsn "\n",
                      l, r->update.tid, r->update.fext.undo,
                      r->update.fext.redo, r->update.prev);
            break;
          }
        }
      break;

    case WL_CLR:
      switch (r->clr.type)
        {
        case WCLR_PHYSICAL:
          {
            i_printf (log_level,
                      "%15" PRlsn "  CLR PHYS     "
                      "[ txid = %8" PRtxid ", pg   = %8" PRpgno
                      ", undoNxt = %15" PRlsn "              ] --> "
                      "%" PRlsn "\n",
                      l, r->clr.tid, r->clr.phys.pg, r->clr.undo_next,
                      r->clr.prev);
            break;
          }

        case WCLR_FSM:
          {
            i_printf (log_level,
                      "%15" PRlsn "  CLR FSM      "
                      "[ txid = %8" PRtxid ", pg   = %8" PRpgno
                      ", redo = 0x%02x, undoNxt "
                      "= %15" PRlsn " ] --> %" PRlsn "\n",
                      l, r->clr.tid, r->clr.fsm.pg, (unsigned)r->clr.fsm.redo,
                      r->clr.undo_next, r->clr.prev);
            break;
          }

        case WCLR_DUMMY:
          {
            i_printf (log_level,
                      "%15" PRlsn "  CLR DUMMY    "
                      "[ txid = %8" PRtxid ", undoNxt = %15" PRlsn
                      "                         "
                      "      ] --> %" PRlsn "\n",
                      l, r->clr.tid, r->clr.undo_next, r->clr.prev);
            break;
          }
        }
      break;

    case WL_BEGIN:
      {
        i_printf (log_level,
                  "%15" PRlsn "  BEGIN        "
                  "[ txid = %8" PRtxid "                                   "
                  "                       ]\n",
                  l, r->begin.tid);
        break;
      }

    case WL_COMMIT:
      {
        i_printf (log_level,
                  "%15" PRlsn "  COMMIT       "
                  "[ txid = %8" PRtxid "                                      "
                  "                    ] --> %" PRlsn "\n",
                  l, r->commit.tid, r->commit.prev);
        break;
      }

    case WL_END:
      {
        i_printf (log_level,
                  "%15" PRlsn "  END          "
                  "[ txid = %8" PRtxid "                                      "
                  "                    ] --> %" PRlsn "\n",
                  l, r->end.tid, r->end.prev);
        break;
      }

    case WL_EOF:
      {
        i_printf (log_level, "%15" PRlsn "  WL_EOF\n", l);
        break;
      }
    }
}

void
wrh_undo (struct wal_rec_hdr_read *h, page_h *ph)
{
  switch (h->type)
    {
    case WL_BEGIN:
      {
        UNREACHABLE ();
      }
    case WL_COMMIT:
      {
        UNREACHABLE ();
      }
    case WL_END:
      {
        UNREACHABLE ();
      }
    case WL_UPDATE:
      {
        switch (h->update.type)
          {
          case WUP_PHYSICAL:
            {
              memcpy (page_h_w (ph), h->update.phys.undo, PAGE_SIZE);
              return;
            }
          case WUP_FSM:
            {
              if (h->update.fsm.undo)
                {
                  fsm_set_bit (page_h_w (ph), h->update.fsm.bit);
                }
              else
                {
                  fsm_clr_bit (page_h_w (ph), h->update.fsm.bit);
                }
              return;
            }
          case WUP_FEXT:
            {
              UNREACHABLE ();
            }
          }
        UNREACHABLE ();
      }
    case WL_CLR:
      {
        switch (h->clr.type)
          {
          case WCLR_PHYSICAL:
            {
              UNREACHABLE ();
            }
          case WCLR_FSM:
            {
              UNREACHABLE ();
            }
          case WCLR_DUMMY:
            {
              UNREACHABLE ();
            }
          }
        UNREACHABLE ();
      }
    case WL_EOF:
      {
        UNREACHABLE ();
      }
    }
  UNREACHABLE ();
}

void
wrh_redo (struct wal_rec_hdr_read *h, page_h *ph)
{
  switch (h->type)
    {
    case WL_BEGIN:
      {
        UNREACHABLE ();
      }
    case WL_COMMIT:
      {
        UNREACHABLE ();
      }
    case WL_END:
      {
        UNREACHABLE ();
      }
    case WL_UPDATE:
      {
        switch (h->update.type)
          {
          case WUP_PHYSICAL:
            {
              memcpy (page_h_w (ph), h->update.phys.redo, PAGE_SIZE);
              return;
            }
          case WUP_FSM:
            {
              if (h->update.fsm.redo)
                {
                  fsm_set_bit (page_h_w (ph), h->update.fsm.bit);
                }
              else
                {
                  fsm_clr_bit (page_h_w (ph), h->update.fsm.bit);
                }
              return;
            }
          case WUP_FEXT:
            {
              UNREACHABLE ();
            }
          }
        UNREACHABLE ();
      }
    case WL_CLR:
      {
        switch (h->clr.type)
          {
          case WCLR_PHYSICAL:
            {
              memcpy (page_h_w (ph), h->clr.phys.redo, PAGE_SIZE);
              return;
            }
          case WCLR_FSM:
            {
              if (h->clr.fsm.redo)
                {
                  fsm_set_bit (page_h_w (ph), h->clr.fsm.bit);
                }
              else
                {
                  fsm_clr_bit (page_h_w (ph), h->clr.fsm.bit);
                }
              return;
            }
          case WCLR_DUMMY:
            {
              UNREACHABLE ();
            }
          }
        UNREACHABLE ();
      }
    case WL_EOF:
      {
        UNREACHABLE ();
      }
    }
  UNREACHABLE ();
}
