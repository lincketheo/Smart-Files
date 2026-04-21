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

  txid tid;
  lsn prev;

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

struct wal_ckpt_end_read
{
  struct txn_table *att;
  struct dpg_table *dpt;
  struct txn *txn_bank;
};

struct wal_ckpt_end_write
{
  struct txn_table *att;
  struct dpg_table *dpt;
};

enum wal_rec_hdr_type
{
  WL_BEGIN = 1,
  WL_COMMIT = 2,
  WL_END = 3,
  WL_UPDATE = 4,
  WL_CLR = 5,
  WL_CKPT_BEGIN = 6,
  WL_CKPT_END = 7,
  WL_EOF = 8,
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
    struct wal_ckpt_end_read ckpt_end;
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
    struct wal_ckpt_end_write ckpt_end;
  };
};

err_t wal_rec_hdr_read_random (struct wal_rec_hdr_read *dest, struct alloc *alloc, error *e);
const char *wal_rec_hdr_type_tostr (enum wal_rec_hdr_type type);
struct wal_rec_hdr_write wrhw_from_wrhr (struct wal_rec_hdr_read *src);

// BEGIN (only tid, no prev)
#define WL_BEGIN_LEN (sizeof (wlh) + sizeof (txid) + sizeof (u32))

#define WL_COMMIT_LEN \
  (sizeof (wlh) + sizeof (txid) + sizeof (lsn) + sizeof (u32))

#define WL_END_LEN (sizeof (wlh) + sizeof (txid) + sizeof (lsn) + sizeof (u32))

// Size of Update Entry
#define WL_UPDATE_LEN                                              \
  (2 * sizeof (wlh) + sizeof (txid) + sizeof (lsn) + sizeof (pgno) \
   + PAGE_SIZE + PAGE_SIZE + sizeof (u32))

// Size of Update Entry
#define WL_FSM_UPDATE_LEN                                          \
  (2 * sizeof (wlh) + sizeof (txid) + sizeof (lsn) + sizeof (pgno) \
   + 2 * sizeof (u8) + sizeof (u32))

// Size of Update Entry
#define WL_FILE_EXT_LEN                                                \
  (2 * sizeof (wlh) + sizeof (txid) + sizeof (lsn) + 2 * sizeof (pgno) \
   + sizeof (u32))

// Size of CLR entry
#define WL_CLR_LEN                                                 \
  (2 * sizeof (wlh) + sizeof (txid) + sizeof (lsn) + sizeof (pgno) \
   + sizeof (lsn) + PAGE_SIZE + sizeof (u32))

// Size of FSM_CLR entry
#define WL_FSM_CLR_LEN                                             \
  (2 * sizeof (wlh) + sizeof (txid) + sizeof (lsn) + sizeof (pgno) \
   + sizeof (lsn) + sizeof (u8) + sizeof (u32))

// Size of DUMMY_CLR entry
#define WL_DUMMY_CLR_LEN                                          \
  (2 * sizeof (wlh) + sizeof (txid) + sizeof (lsn) + sizeof (lsn) \
   + sizeof (u32))

#define WL_CKPT_BEGIN_LEN (sizeof (wlh) + sizeof (u32))

#define WL_CKPT_END_MAX_LEN \
  (sizeof (wlh) + MAX_TXNT_SRL_SIZE + MAX_DPGT_SRL_SIZE + sizeof (u32))

stxid wrh_get_tid (const struct wal_rec_hdr_read *h);
slsn wrh_get_prev_lsn (const struct wal_rec_hdr_read *h);
void i_log_wal_rec_hdr_read (int log_level, struct wal_rec_hdr_read *r);
void i_print_wal_rec_hdr_read_light (int log_level, const struct wal_rec_hdr_read *w,
                                     lsn l);

// DECODE
void walf_decode_physical_update (struct wal_rec_hdr_read *r,
                                  const u8 buf[WL_UPDATE_LEN]);
void walf_decode_fsm_update (struct wal_rec_hdr_read *r,
                             const u8 buf[WL_FSM_UPDATE_LEN]);
void walf_decode_file_extend_update (struct wal_rec_hdr_read *r,
                                     const u8 buf[WL_FILE_EXT_LEN]);
void walf_decode_physical_clr (struct wal_rec_hdr_read *r,
                               const u8 buf[WL_CLR_LEN]);
void walf_decode_fsm_clr (struct wal_rec_hdr_read *r,
                          const u8 buf[WL_FSM_CLR_LEN]);
void walf_decode_dummy_clr (struct wal_rec_hdr_read *r,
                            const u8 buf[WL_DUMMY_CLR_LEN]);
void walf_decode_begin (struct wal_rec_hdr_read *r,
                        const u8 buf[WL_BEGIN_LEN]);
void walf_decode_commit (struct wal_rec_hdr_read *r,
                         const u8 buf[WL_COMMIT_LEN]);
void walf_decode_end (struct wal_rec_hdr_read *r, const u8 buf[WL_END_LEN]);
void walf_decode_ckpt_begin (const struct wal_rec_hdr_read *r,
                             const u8 buf[WL_CKPT_BEGIN_LEN]);
err_t walf_decode_ckpt_end (struct wal_rec_hdr_read *r, const u8 *buf,
                            error *e);

#ifndef NTEST
bool wal_rec_hdr_read_equal (const struct wal_rec_hdr_read *left,
                             const struct wal_rec_hdr_read *right);
#endif
