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

#include "compile_config.h"
#include "dpgt/dirty_page_table.h"
#include "pager/page_h.h"
#include "txns/txn_table.h"

////////////////////////////////////////////////////////////
/// Update Logs

struct wal_update_read
{
  enum wal_update_type
  {
    WUP_PHYSICAL = 1,
    WUP_FSM = 2,
    WUP_FEXT = 3,
  } type;

  txid tid; // Transaction id this log is associated with
  lsn prev; // Previous lsn

  union
  {
    struct physical_read_update
    {
      pgno pg;
      u8 undo[PAGE_SIZE];
      u8 redo[PAGE_SIZE];
    } phys;

    struct fsm_update
    {
      pgno pg;
      p_size bit;
      u8 undo;
      u8 redo;
    } fsm;

    struct file_ext
    {
      pgno undo;
      pgno redo;
    } fext;
  };
};

struct wal_update_write
{
  enum wal_update_type type;

  txid tid;
  lsn prev;

  union
  {
    struct physical_write_update
    {
      pgno pg;
      u8 *undo;
      u8 *redo;
    } phys;

    struct fsm_update fsm;

    struct file_ext fext;
  };
};

////////////////////////////////////////////////////////////
/// Other Records

struct wal_begin
{
  txid tid;
};

struct wal_commit
{
  txid tid;
  lsn prev;
};

struct wal_end
{
  txid tid;
  lsn prev;
};

struct wal_clr_read
{
  enum wal_clr_type
  {
    WCLR_PHYSICAL = 1,
    WCLR_FSM = 2,
    WCLR_DUMMY = 3,
  } type;

  txid tid;
  lsn prev;
  pgno pg;
  lsn undo_next;

  union
  {
    struct physical_read_clr
    {
      pgno pg;
      u8 redo[PAGE_SIZE];
    } phys;

    struct fsm_clr
    {
      pgno pg;
      p_size bit;
      u8 redo;
    } fsm;
  };
};

struct wal_clr_write
{
  enum wal_clr_type type;

  txid tid;
  lsn prev;
  lsn undo_next;

  union
  {
    struct physical_write_clr
    {
      pgno pg;
      u8 *redo;
    } phys;

    struct fsm_clr fsm;
  };
};

enum wal_rec_hdr_type
{
  WL_BEGIN = 1,
  WL_COMMIT = 2,
  WL_END = 3,
  WL_UPDATE = 4,
  WL_CLR = 5,
  WL_EOF = 6,
};

struct wal_rec_hdr_read
{
  enum wal_rec_hdr_type type;

  union
  {
    struct wal_update_read update;
    struct wal_begin begin;
    struct wal_commit commit;
    struct wal_end end;
    struct wal_clr_read clr;
  };
};

struct wal_rec_hdr_write
{
  enum wal_rec_hdr_type type;

  union
  {
    struct wal_update_write update;
    struct wal_begin begin;
    struct wal_commit commit;
    struct wal_end end;
    struct wal_clr_write clr;
  };
};

void wal_rec_hdr_read_random (struct wal_rec_hdr_read *dest, struct alloc *alloc);
const char *wal_rec_hdr_type_tostr (enum wal_rec_hdr_type type);
struct wal_rec_hdr_write wrhw_from_wrhr (struct wal_rec_hdr_read *src);

// Size of BEGIN entry
#define WL_BEGIN_LEN              \
  (sizeof (wlh) +  /* header */   \
   sizeof (txid) + /* txid */     \
   sizeof (u32)    /* checksum */ \
  )

// Size of COMMIT entry
#define WL_COMMIT_LEN             \
  (sizeof (wlh) +  /* header */   \
   sizeof (txid) + /* txid */     \
   sizeof (lsn) +  /* prev */     \
   sizeof (u32)    /* checksum */ \
  )

// Size of END entry
#define WL_END_LEN                \
  (sizeof (wlh) +  /* header */   \
   sizeof (txid) + /* txid */     \
   sizeof (lsn) +  /* prev */     \
   sizeof (u32)    /* checksum */ \
  )

// Size of physical UPDATE entry
#define WL_UPDATE_LEN                \
  (2 * sizeof (wlh) + /* header */   \
   sizeof (txid) +    /* txid */     \
   sizeof (lsn) +     /* prev */     \
   sizeof (pgno) +    /* pg */       \
   PAGE_SIZE +        /* undo */     \
   PAGE_SIZE +        /* redo */     \
   sizeof (u32)       /* checksum */ \
  )

// Size of FSM UPDATE entry
#define WL_FSM_UPDATE_LEN            \
  (2 * sizeof (wlh) + /* header */   \
   sizeof (txid) +    /* txid */     \
   sizeof (lsn) +     /* prev */     \
   sizeof (pgno) +    /* pg */       \
   sizeof (p_size) +  /* bit */      \
   sizeof (u8) +      /* undo */     \
   sizeof (u8) +      /* redo */     \
   sizeof (u32)       /* checksum */ \
  )

// Size of FILE EXTENT UPDATE entry
#define WL_FILE_EXT_LEN              \
  (2 * sizeof (wlh) + /* header */   \
   sizeof (txid) +    /* txid */     \
   sizeof (lsn) +     /* prev */     \
   sizeof (pgno) +    /* undo */     \
   sizeof (pgno) +    /* redo */     \
   sizeof (u32)       /* checksum */ \
  )

// Size of physical CLR entry
#define WL_CLR_LEN                    \
  (2 * sizeof (wlh) + /* header */    \
   sizeof (txid) +    /* txid */      \
   sizeof (lsn) +     /* prev */      \
   sizeof (pgno) +    /* pg */        \
   sizeof (lsn) +     /* undo_next */ \
   PAGE_SIZE +        /* redo */      \
   sizeof (u32)       /* checksum */  \
  )

// Size of FSM CLR entry
#define WL_FSM_CLR_LEN                \
  (2 * sizeof (wlh) + /* header */    \
   sizeof (txid) +    /* txid */      \
   sizeof (lsn) +     /* prev */      \
   sizeof (pgno) +    /* pg */        \
   sizeof (lsn) +     /* undo_next */ \
   sizeof (p_size) +  /* bit */       \
   sizeof (u8) +      /* redo */      \
   sizeof (u32)       /* checksum */  \
  )
// Size of DUMMY_CLR entry
#define WL_DUMMY_CLR_LEN (2 * sizeof (wlh) + sizeof (txid) + sizeof (lsn) + sizeof (lsn) + sizeof (u32))

// Utils
stxid wrh_get_tid (const struct wal_rec_hdr_read *h);
slsn wrh_get_prev_lsn (const struct wal_rec_hdr_read *h);
bool wrh_is_undoable (const struct wal_rec_hdr_read *h);
bool wrh_is_redoable (const struct wal_rec_hdr_read *h);
pgno wrh_get_affected_pg (const struct wal_rec_hdr_read *h);
void i_log_wal_rec_hdr_read (int log_level, struct wal_rec_hdr_read *r);
void i_print_wal_rec_hdr_read_light (int log_level, const struct wal_rec_hdr_read *w, lsn l);
void wrh_undo (struct wal_rec_hdr_read *h, page_h *ph);
void wrh_redo (struct wal_rec_hdr_read *h, page_h *ph);

// DECODE
void walf_decode_physical_update (struct wal_rec_hdr_read *r, const u8 buf[WL_UPDATE_LEN]);
void walf_decode_fsm_update (struct wal_rec_hdr_read *r, const u8 buf[WL_FSM_UPDATE_LEN]);
void walf_decode_file_extend_update (struct wal_rec_hdr_read *r, const u8 buf[WL_FILE_EXT_LEN]);
void walf_decode_physical_clr (struct wal_rec_hdr_read *r, const u8 buf[WL_CLR_LEN]);
void walf_decode_fsm_clr (struct wal_rec_hdr_read *r, const u8 buf[WL_FSM_CLR_LEN]);
void walf_decode_dummy_clr (struct wal_rec_hdr_read *r, const u8 buf[WL_DUMMY_CLR_LEN]);
void walf_decode_begin (struct wal_rec_hdr_read *r, const u8 buf[WL_BEGIN_LEN]);
void walf_decode_commit (struct wal_rec_hdr_read *r, const u8 buf[WL_COMMIT_LEN]);
void walf_decode_end (struct wal_rec_hdr_read *r, const u8 buf[WL_END_LEN]);

#ifndef NTEST
bool wal_rec_hdr_read_equal (const struct wal_rec_hdr_read *left, const struct wal_rec_hdr_read *right);
#endif
