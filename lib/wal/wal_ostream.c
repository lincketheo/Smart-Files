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

#include "wal/wal_ostream.h"

#include "c_specx.h"
#include "smfile.h"

DEFINE_DBG_ASSERT (struct wal_ostream, wal_ostream, w, { ASSERT (w); })

/*
 * Background flush thread for the WAL output stream.
 *
 * Waits on write_cond while flush_pending is false and shutdown is false.
 * When signalled, it drains the "other" buffer (the full buffer swapped in
 * by walos_flush_impl) to disk with a single write + fsync, then advances
 * flushed_lsn and broadcasts write_done_cond so waiters in walos_flush_to
 * can unblock.
 *
 * I/O is done outside the write_lock to avoid holding it during disk
 * operations; write_lock is re-acquired after I/O to update state.
 * Panics on write or fsync failure — WAL write errors are unrecoverable.
 */
static void *
walos_flush_thread (void *ctx)
{
  struct wal_ostream *w = ctx;

  i_mutex_lock (&w->write_lock);
  while (true)
    {
      while (!w->flush_pending && !w->shutdown)
        {
          i_cond_wait (&w->write_cond, &w->write_lock);
        }

      if (w->shutdown)
        {
          break;
        }

      const u32 towrite = cbuffer_len (w->other);
      i_mutex_unlock (&w->write_lock);

      // I/O outside the lock
      error e = error_create ();
      if (cbuffer_write_to_file_1_expect (&w->fd, w->other, towrite, &e))
        {
          panic ("Wal write failed");
        }
      cbuffer_write_to_file_2 (w->other, towrite);

      if (i_fsync (&w->fd, &e))
        {
          panic ("Wal fsync failed");
        }

      i_mutex_lock (&w->write_lock);
      w->flushed_lsn += towrite;
      w->flush_pending = false;
      i_cond_broadcast (&w->write_done_cond);
    }

  i_mutex_unlock (&w->write_lock);
  return NULL;
}

////////////////////////////////////////////////////////////
/// Lifecycle

/*
 * Open the WAL output stream, initializing a double-buffered async writer.
 *
 * The ostream maintains two circular buffers (buffer1, buffer2).  Callers
 * write into the active buffer (w->buffer) under a latch.  When a flush is
 * requested, the active and other buffers are swapped and the background
 * thread drains the "other" buffer to disk.  This allows new records to be
 * buffered into the fresh active buffer while the disk write is in flight.
 *
 * flushed_lsn is initialised to the current file length so that LSN values
 * (byte offsets into the WAL file) are correct from the first append.
 */
struct wal_ostream *
walos_open (const char *fname, error *e)
{
  struct wal_ostream *ret = i_malloc (1, sizeof *ret, e);
  if (ret == NULL)
    {
      return NULL;
    }

  if (i_open_w (&ret->fd, fname, e))
    {
      goto err_free;
    }

  const i64 len = i_seek (&ret->fd, 0, I_SEEK_END, e);
  if (len < 0)
    {
      goto err_close;
    }

  latch_init (&ret->l);

  ret->buffer1 = cbuffer_create (ret->_buffer1, sizeof (ret->_buffer1));
  ret->buffer2 = cbuffer_create (ret->_buffer2, sizeof (ret->_buffer2));
  ret->buffer = &ret->buffer1;
  ret->other = &ret->buffer2;

  ret->flushed_lsn = len;
  ret->flush_pending = false;
  ret->shutdown = false;

  if (i_mutex_create (&ret->write_lock, e))
    {
      goto err_close;
    }

  if (i_cond_create (&ret->write_cond, e))
    {
      goto err_mutex;
    }

  if (i_cond_create (&ret->write_done_cond, e))
    {
      goto err_cond1;
    }

  if (i_thread_create (&ret->writer_thread, walos_flush_thread, ret, e))
    {
      goto err_cond2;
    }

  DBG_ASSERT (wal_ostream, ret);
  return ret;

err_cond2:
  i_cond_free (&ret->write_done_cond);
err_cond1:
  i_cond_free (&ret->write_cond);
err_mutex:
  i_mutex_free (&ret->write_lock);
err_close:
  i_close (&ret->fd, e);
err_free:
  i_free (ret);
  return NULL;
}

/// Shutdown the writer thread. Caller must not access w->write_lock after
/// this.
static void
walos_shutdown_thread (struct wal_ostream *w, error *e)
{
  i_mutex_lock (&w->write_lock);
  w->shutdown = true;
  i_cond_signal (&w->write_cond);
  i_mutex_unlock (&w->write_lock);

  i_thread_join (&w->writer_thread, e);

  i_mutex_free (&w->write_lock);
  i_cond_free (&w->write_cond);
  i_cond_free (&w->write_done_cond);
}

err_t
walos_close (struct wal_ostream *w, error *e)
{
  DBG_ASSERT (wal_ostream, w);

  // Flush everything, then tear down
  walos_flush_all (w, e);

  // Wait for in-flight flush to land before shutting down
  i_mutex_lock (&w->write_lock);
  while (w->flush_pending)
    {
      i_cond_wait (&w->write_done_cond, &w->write_lock);
    }
  i_mutex_unlock (&w->write_lock);

  walos_shutdown_thread (w, e);
  i_close (&w->fd, e);
  i_free (w);
  return error_trace (e);
}

err_t
walos_crash (struct wal_ostream *w, error *e)
{
  DBG_ASSERT (wal_ostream, w);

  walos_shutdown_thread (w, e);
  i_close (&w->fd, e);
  i_free (w);
  return error_trace (e);
}

/*
 * Core flush implementation: swap buffers and optionally wait for completion.
 *
 * Must be called with write_lock held (or from walos_flush_to which holds
 * the latch).  If l is already below flushed_lsn, the request is a no-op.
 * Otherwise, the active and other buffers are swapped, flush_pending is set,
 * and the flush thread is signalled.  If wait==true, the caller blocks on
 * write_done_cond until flush_pending clears.
 */
static err_t
walos_flush_impl (struct wal_ostream *w, const lsn l, bool wait, error *e)
{
  i_mutex_lock (&w->write_lock);

  // Wait for previous flush to release `other`
  while (w->flush_pending)
    {
      i_cond_wait (&w->write_done_cond, &w->write_lock);
    }

  // State is consistent here: other is empty, flushed_lsn is current
  if (l <= w->flushed_lsn)
    {
      i_mutex_unlock (&w->write_lock);
      return SUCCESS;
    }

  ASSERT (l <= w->flushed_lsn + cbuffer_len (w->buffer));

  if (cbuffer_len (w->buffer) > 0)
    {
      struct cbuffer *temp = w->buffer;
      w->buffer = w->other;
      w->other = temp;
      w->flush_pending = true;
      i_cond_signal (&w->write_cond);

      if (wait)
        {
          while (w->flush_pending)
            {
              i_cond_wait (&w->write_done_cond, &w->write_lock);
            }
        }
    }

  i_mutex_unlock (&w->write_lock);
  return error_trace (e);
}

err_t
walos_flush_to (struct wal_ostream *w, const lsn l, error *e)
{
  latch_lock (&w->l);

  // Already durable
  if (l <= w->flushed_lsn)
    {
      latch_unlock (&w->l);
      return SUCCESS;
    }

  const err_t ret = walos_flush_impl (w, l, true, e);

  latch_unlock (&w->l);
  return ret;
}

err_t
walos_flush_all (struct wal_ostream *w, error *e)
{
  DBG_ASSERT (wal_ostream, w);

  latch_lock (&w->l);
  const err_t ret = walos_flush_impl (w, w->flushed_lsn + cbuffer_len (w->buffer),
                                      true, e);
  latch_unlock (&w->l);

  return ret;
}

////////////////////////////////////////////////////////////
/// Write

/*
 * Append len bytes to the WAL active buffer, updating a running checksum.
 *
 * If the active buffer fills mid-write, walos_flush_impl is called in
 * non-waiting mode to swap buffers and allow new data in; this may be called
 * multiple times for very large records.  The function holds the latch for
 * the entire write so records from concurrent callers never interleave.
 */
err_t
walos_write_all (struct wal_ostream *w, u32 *checksum, const void *data,
                 const u32 len, error *e)
{
  DBG_ASSERT (wal_ostream, w);

  if (checksum)
    {
      checksum_execute (checksum, data, len);
    }

  u32 written = 0;
  const u8 *src = data;

  latch_lock (&w->l);

  while (written < len)
    {
      if (cbuffer_avail (w->buffer) == 0)
        {
          // Active buffer full; swap to the other buffer and wake
          // the flush thread
          if (walos_flush_impl (w, w->flushed_lsn + cbuffer_len (w->buffer),
                                false, e))
            {
              latch_unlock (&w->l);
              return error_trace (e);
            }
        }

      // Copy as much as the buffer can accept in one shot
      const u32 towrite = MIN (len - written, cbuffer_avail (w->buffer));
      ASSERT (towrite > 0);
      cbuffer_write_expect (src + written, 1, towrite, w->buffer);
      written += towrite;
    }

  latch_unlock (&w->l);
  return SUCCESS;
}

lsn
walos_get_next_lsn (const struct wal_ostream *w)
{
  return w->flushed_lsn + cbuffer_len (w->buffer);
}
