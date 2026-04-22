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
#include "c_specx/dev/assert.h"
#include "dpgt/dirty_page_table.h"
#include "pages/fsm_page.h"
#include "pages/page.h"
#include "txns/txn_table.h"
#include "wal/wal_rec_hdr.h"

void
walf_decode_physical_update (struct wal_rec_hdr_read *r, const u8 buf[WL_UPDATE_LEN])
{
  ASSERT (r->type == WL_UPDATE);

  u32 head = 2 * sizeof (wlh);

  // TID
  memcpy (&r->update.tid, buf + head, sizeof (r->update.tid));
  head += sizeof (r->update.tid);

  // PREV
  memcpy (&r->update.prev, buf + head, sizeof (r->update.prev));
  head += sizeof (r->update.prev);

  // PG
  memcpy (&r->update.phys.pg, buf + head, sizeof (r->update.phys.pg));
  head += sizeof (r->update.phys.pg);

  // UNDO
  memcpy (r->update.phys.undo, buf + head, PAGE_SIZE);
  head += PAGE_SIZE;

  // REDO
  memcpy (r->update.phys.redo, buf + head, PAGE_SIZE);
}

void
walf_decode_fsm_update (struct wal_rec_hdr_read *r, const u8 buf[WL_FSM_UPDATE_LEN])
{
  ASSERT (r->type == WL_UPDATE);
  ASSERT (r->update.type == WUP_FSM);

  u32 head = 2 * sizeof (wlh);

  // TID
  memcpy (&r->update.tid, buf + head, sizeof (r->update.tid));
  head += sizeof (r->update.tid);

  // PREV
  memcpy (&r->update.prev, buf + head, sizeof (r->update.prev));
  head += sizeof (r->update.prev);

  // PG
  memcpy (&r->update.fsm.pg, buf + head, sizeof (r->update.fsm.pg));
  head += sizeof (r->update.fsm.pg);

  // BIT
  memcpy (&r->update.fsm.bit, buf + head, sizeof (r->update.fsm.bit));
  head += sizeof (r->update.fsm.bit);

  // UNDO
  memcpy (&r->update.fsm.undo, buf + head, sizeof (r->update.fsm.undo));
  head += sizeof (r->update.fsm.undo);

  // REDO
  memcpy (&r->update.fsm.redo, buf + head, sizeof (r->update.fsm.redo));
}

void
walf_decode_file_extend_update (struct wal_rec_hdr_read *r,
                                const u8 buf[WL_FILE_EXT_LEN])
{
  ASSERT (r->type == WL_UPDATE);
  ASSERT (r->update.type == WUP_FEXT);

  u32 head = 2 * sizeof (wlh);

  // TID
  memcpy (&r->update.tid, buf + head, sizeof (r->update.tid));
  head += sizeof (r->update.tid);

  // PREV
  memcpy (&r->update.prev, buf + head, sizeof (r->update.prev));
  head += sizeof (r->update.prev);

  // UNDO
  memcpy (&r->update.fext.undo, buf + head, sizeof (r->update.fext.undo));
  head += sizeof (r->update.fext.undo);

  // REDO
  memcpy (&r->update.fext.redo, buf + head, sizeof (r->update.fext.redo));
}

void
walf_decode_physical_clr (struct wal_rec_hdr_read *r, const u8 buf[WL_CLR_LEN])
{
  ASSERT (r->type == WL_CLR);
  ASSERT (r->clr.type == WCLR_PHYSICAL);

  u32 head = 2 * sizeof (wlh);

  // TID
  memcpy (&r->clr.tid, buf + head, sizeof (r->clr.tid));
  head += sizeof (r->clr.tid);

  // PREV
  memcpy (&r->clr.prev, buf + head, sizeof (r->clr.prev));
  head += sizeof (r->clr.prev);

  // PG
  memcpy (&r->clr.phys.pg, buf + head, sizeof (r->clr.phys.pg));
  head += sizeof (r->clr.phys.pg);

  // UNDO_NEXT
  memcpy (&r->clr.undo_next, buf + head, sizeof (r->clr.undo_next));
  head += sizeof (r->clr.undo_next);

  // REDO
  memcpy (r->clr.phys.redo, buf + head, PAGE_SIZE);
}

void
walf_decode_fsm_clr (struct wal_rec_hdr_read *r, const u8 buf[WL_FSM_CLR_LEN])
{
  ASSERT (r->type == WL_CLR);
  ASSERT (r->clr.type == WCLR_FSM);

  u32 head = 2 * sizeof (wlh);

  // TID
  memcpy (&r->clr.tid, buf + head, sizeof (r->clr.tid));
  head += sizeof (r->clr.tid);

  // PREV
  memcpy (&r->clr.prev, buf + head, sizeof (r->clr.prev));
  head += sizeof (r->clr.prev);

  // PG
  memcpy (&r->clr.fsm.pg, buf + head, sizeof (r->clr.fsm.pg));
  head += sizeof (r->clr.fsm.pg);

  // UNDO_NEXT
  memcpy (&r->clr.undo_next, buf + head, sizeof (r->clr.undo_next));
  head += sizeof (r->clr.undo_next);

  // BIT
  memcpy (&r->clr.fsm.bit, buf + head, sizeof (r->clr.fsm.bit));
  head += sizeof (r->clr.fsm.bit);

  // REDO
  memcpy (&r->clr.fsm.redo, buf + head, sizeof (r->clr.fsm.redo));
}

void
walf_decode_dummy_clr (struct wal_rec_hdr_read *r,
                       const u8 buf[WL_DUMMY_CLR_LEN])
{
  ASSERT (r->type == WL_CLR);
  ASSERT (r->clr.type == WCLR_DUMMY);

  u32 head = 2 * sizeof (wlh);

  // TID
  memcpy (&r->clr.tid, buf + head, sizeof (r->clr.tid));
  head += sizeof (r->clr.tid);

  // PREV
  memcpy (&r->clr.prev, buf + head, sizeof (r->clr.prev));
  head += sizeof (r->clr.prev);

  // UNDO_NEXT
  memcpy (&r->clr.undo_next, buf + head, sizeof (r->clr.undo_next));
  head += sizeof (r->clr.undo_next);
}

void
walf_decode_begin (struct wal_rec_hdr_read *r, const u8 buf[WL_BEGIN_LEN])
{
  ASSERT (r->type == WL_BEGIN);

  u32 head = sizeof (wlh);

  // TID
  memcpy (&r->begin.tid, buf + head, sizeof (r->begin.tid));
}

void
walf_decode_commit (struct wal_rec_hdr_read *r, const u8 buf[WL_COMMIT_LEN])
{
  ASSERT (r->type == WL_COMMIT);

  u32 head = sizeof (wlh);

  // TID
  memcpy (&r->commit.tid, buf + head, sizeof (r->commit.tid));
  head += sizeof (r->commit.tid);

  // PREV
  memcpy (&r->commit.prev, buf + head, sizeof (r->commit.prev));
}

void
walf_decode_end (struct wal_rec_hdr_read *r, const u8 buf[WL_END_LEN])
{
  ASSERT (r->type == WL_END);

  u32 head = sizeof (wlh);

  // TID
  memcpy (&r->end.tid, buf + head, sizeof (r->end.tid));
  head += sizeof (r->end.tid);

  // PREV
  memcpy (&r->end.prev, buf + head, sizeof (r->end.prev));
}
