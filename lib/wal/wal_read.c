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

static int
wal_read_full (
    const struct wal *w,
    u32 *checksum,
    const wlh type,
    const wlh second_type,
    u8 *buf,
    const u32 total_len,
    error *e)
{
  ASSERT (total_len >= sizeof (wlh) + sizeof (u32));

  u8 *head = buf;

  memcpy (head, &type, sizeof (wlh));
  head += sizeof (wlh);

  if (second_type != WLH_NULL)
    {
      memcpy (head, &second_type, sizeof (wlh));
      head += sizeof (wlh);
    }

  {
    const u32 toread = total_len - (head - buf) - sizeof (u32);
    if (toread > 0)
      {
        bool iseof;
        WRAP (walis_read_all (w->istream, &iseof, NULL, checksum, head, toread,
                              e));
        if (iseof)
          {
            return WL_EOF;
          }
      }

    head += toread;
    bool iseof;
    WRAP (walis_read_all (w->istream, &iseof, NULL, NULL, head, sizeof (u32),
                          e));
    if (iseof)
      {
        return WL_EOF;
      }
  }

  u32 actual_crc;
  memcpy (&actual_crc, buf + total_len - sizeof (u32), sizeof (u32));
  if (*checksum != actual_crc)
    {
      return error_causef (e, ERR_CORRUPT, "Invalid CRC");
    }

  return SUCCESS;
}

static err_t
wal_read_physical_update (struct wal *w, u32 *checksum,
                          struct wal_rec_hdr_read *r, error *e)
{
  ASSERT (r->type == WL_UPDATE);
  u8 buf[WL_UPDATE_LEN];
  const int ret = wal_read_full (w, checksum, r->type, r->update.type, buf,
                                 WL_UPDATE_LEN, e);
  WRAP (ret);
  if (ret == WL_EOF)
    {
      r->type = WL_EOF;
      return SUCCESS;
    }
  ASSERT (ret == SUCCESS);
  walf_decode_physical_update (r, buf);
  return SUCCESS;
}

static err_t
wal_read_fsm_update (struct wal *w, u32 *checksum, struct wal_rec_hdr_read *r,
                     error *e)
{
  ASSERT (r->type == WL_UPDATE);
  u8 buf[WL_FSM_UPDATE_LEN];
  const int ret = wal_read_full (w, checksum, r->type, r->update.type, buf, WL_FSM_UPDATE_LEN, e);
  WRAP (ret);
  if (ret == WL_EOF)
    {
      r->type = WL_EOF;
      return SUCCESS;
    }
  ASSERT (ret == SUCCESS);
  walf_decode_fsm_update (r, buf);
  return SUCCESS;
}

static err_t
wal_read_file_extend_update (struct wal *w, u32 *checksum,
                             struct wal_rec_hdr_read *r, error *e)
{
  ASSERT (r->type == WL_UPDATE);
  u8 buf[WL_FILE_EXT_LEN];
  const int ret = wal_read_full (w, checksum, r->type, r->update.type, buf,
                                 WL_FILE_EXT_LEN, e);
  WRAP (ret);
  if (ret == WL_EOF)
    {
      r->type = WL_EOF;
      return SUCCESS;
    }
  ASSERT (ret == SUCCESS);
  walf_decode_file_extend_update (r, buf);
  return SUCCESS;
}

static err_t
wal_read_physical_clr (struct wal *w, u32 *checksum,
                       struct wal_rec_hdr_read *r, error *e)
{
  ASSERT (r->type == WL_CLR);
  u8 buf[WL_CLR_LEN];
  const int ret
      = wal_read_full (w, checksum, r->type, r->clr.type, buf, WL_CLR_LEN, e);
  WRAP (ret);
  if (ret == WL_EOF)
    {
      r->type = WL_EOF;
      return SUCCESS;
    }
  ASSERT (ret == SUCCESS);
  walf_decode_physical_clr (r, buf);
  return SUCCESS;
}

static err_t
wal_read_fsm_clr (struct wal *w, u32 *checksum, struct wal_rec_hdr_read *r,
                  error *e)
{
  ASSERT (r->type == WL_CLR);
  u8 buf[WL_FSM_CLR_LEN];
  const int ret = wal_read_full (w, checksum, r->type, r->clr.type, buf,
                                 WL_FSM_CLR_LEN, e);
  WRAP (ret);
  if (ret == WL_EOF)
    {
      r->type = WL_EOF;
      return SUCCESS;
    }
  ASSERT (ret == SUCCESS);
  walf_decode_fsm_clr (r, buf);
  return SUCCESS;
}

static err_t
wal_read_dummy_clr (struct wal *w, u32 *checksum, struct wal_rec_hdr_read *r,
                    error *e)
{
  ASSERT (r->type == WL_CLR);
  u8 buf[WL_DUMMY_CLR_LEN];
  const int ret = wal_read_full (w, checksum, r->type, r->clr.type, buf,
                                 WL_DUMMY_CLR_LEN, e);
  WRAP (ret);
  if (ret == WL_EOF)
    {
      r->type = WL_EOF;
      return SUCCESS;
    }
  ASSERT (ret == SUCCESS);
  walf_decode_dummy_clr (r, buf);
  return SUCCESS;
}

static err_t
wal_read_begin (struct wal *w, u32 *checksum, struct wal_rec_hdr_read *r,
                error *e)
{
  ASSERT (r->type == WL_BEGIN);
  u8 buf[WL_BEGIN_LEN];
  const int ret
      = wal_read_full (w, checksum, r->type, WLH_NULL, buf, WL_BEGIN_LEN, e);
  WRAP (ret);
  if (ret == WL_EOF)
    {
      r->type = WL_EOF;
      return SUCCESS;
    }
  ASSERT (ret == SUCCESS);
  walf_decode_begin (r, buf);
  return SUCCESS;
}

static err_t
wal_read_commit (struct wal *w, u32 *checksum, struct wal_rec_hdr_read *r,
                 error *e)
{
  ASSERT (r->type == WL_COMMIT);
  u8 buf[WL_COMMIT_LEN];
  const int ret = wal_read_full (w, checksum, r->type, WLH_NULL, buf, WL_COMMIT_LEN, e);
  WRAP (ret);
  if (ret == WL_EOF)
    {
      r->type = WL_EOF;
      return SUCCESS;
    }
  ASSERT (ret == SUCCESS);
  walf_decode_commit (r, buf);
  return SUCCESS;
}

static err_t
wal_read_end (struct wal *w, u32 *checksum, struct wal_rec_hdr_read *r,
              error *e)
{
  ASSERT (r->type == WL_END);
  u8 buf[WL_END_LEN];
  const int ret = wal_read_full (w, checksum, r->type, WLH_NULL, buf, WL_END_LEN, e);
  WRAP (ret);
  if (ret == WL_EOF)
    {
      r->type = WL_EOF;
      return SUCCESS;
    }
  ASSERT (ret == SUCCESS);
  walf_decode_end (r, buf);
  return SUCCESS;
}

static err_t
wal_read_sequential (struct wal *w, struct wal_rec_hdr_read *dest, lsn *rlsn, error *e)
{
  u32 checksum = checksum_init ();
  wlh t;
  bool iseof;

  walis_mark_start_log (w->istream);

  WRAP (
      walis_read_all (w->istream, &iseof, rlsn, &checksum, &t, sizeof (t), e));
  if (iseof)
    {
      dest->type = WL_EOF;
      return SUCCESS;
    }

  dest->type = -1;

  switch (t)
    {
    case WL_UPDATE:
      {
        dest->type = t;
        dest->update.type = -1;
        WRAP (walis_read_all (w->istream, &iseof, rlsn, &checksum, &t, sizeof (t), e));
        if (iseof)
          {
            dest->type = WL_EOF;
            return SUCCESS;
          }
        switch (t)
          {
          case WUP_PHYSICAL:
            dest->update.type = t;
            WRAP (wal_read_physical_update (w, &checksum, dest, e));
            break;
          case WUP_FEXT:
            dest->update.type = t;
            WRAP (wal_read_file_extend_update (w, &checksum, dest, e));
            break;
          case WUP_FSM:
            dest->update.type = t;
            WRAP (wal_read_fsm_update (w, &checksum, dest, e));
            break;
          }
        if ((int)dest->update.type == -1)
          {
            dest->type = -1;
          }
        break;
      }
    case WL_CLR:
      {
        dest->type = t;
        dest->clr.type = -1;
        WRAP (walis_read_all (w->istream, &iseof, rlsn, &checksum, &t,
                              sizeof (t), e));
        if (iseof)
          {
            dest->type = WL_EOF;
            return SUCCESS;
          }
        switch (t)
          {
          case WCLR_PHYSICAL:
            dest->clr.type = t;
            WRAP (wal_read_physical_clr (w, &checksum, dest, e));
            break;
          case WCLR_FSM:
            dest->clr.type = t;
            WRAP (wal_read_fsm_clr (w, &checksum, dest, e));
            break;
          case WCLR_DUMMY:
            dest->clr.type = t;
            WRAP (wal_read_dummy_clr (w, &checksum, dest, e));
            break;
          }
        if ((int)dest->clr.type == -1)
          {
            dest->type = -1;
          }
        break;
      }
    case WL_BEGIN:
      {
        dest->type = t;
        WRAP (wal_read_begin (w, &checksum, dest, e));
        break;
      }
    case WL_COMMIT:
      {
        dest->type = t;
        WRAP (wal_read_commit (w, &checksum, dest, e));
        break;
      }
    case WL_END:
      {
        dest->type = t;
        WRAP (wal_read_end (w, &checksum, dest, e));
        break;
      }
    }

  if ((int)dest->type == -1)
    {
      return error_causef (e, ERR_CORRUPT, "Invalid wal header type");
    }

  walis_mark_end_log (w->istream);
  i_log_wal_rec_hdr_read (LOG_TRACE, dest);

  return SUCCESS;
}

struct wal_rec_hdr_read *
wal_read_next (struct wal *w, lsn *rlsn, error *e)
{
  latch_lock (&w->latch);
  DBG_ASSERT (wal, w);

  ASSERT (w->istream);
  if (wal_read_sequential (w, &w->rhdr, rlsn, e))
    {
      latch_unlock (&w->latch);
      return NULL;
    }

  latch_unlock (&w->latch);
  return &w->rhdr;
}

struct wal_rec_hdr_read *
wal_read_entry (struct wal *w, const lsn id, error *e)
{
  latch_lock (&w->latch);
  DBG_ASSERT (wal, w);

  ASSERT (w->istream);
  if (walis_seek (w->istream, id, e))
    {
      latch_unlock (&w->latch);
      return NULL;
    }

  lsn rlsn;
  if (wal_read_sequential (w, &w->rhdr, &rlsn, e))
    {
      latch_unlock (&w->latch);
      return NULL;
    }

  latch_unlock (&w->latch);
  return &w->rhdr;
}
