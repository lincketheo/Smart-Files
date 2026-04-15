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

#include "core/cbuffer.h"
#include "core/latch.h"
#include "numstore/compile_config.h"

struct wal_ostream
{
  i_file fd;
  latch l;
  lsn flushed_lsn;

  // Background writer thread
  bool flush_pending;
  bool shutdown;
  i_thread writer_thread;
  i_cond write_cond;
  i_cond write_done_cond;
  i_mutex write_lock;

  struct cbuffer *buffer;
  struct cbuffer *other;

  struct cbuffer buffer1;
  u8 _buffer1[WAL_BUFFER_CAP];

  struct cbuffer buffer2;
  u8 _buffer2[WAL_BUFFER_CAP];
};

// Lifecycle
struct wal_ostream *walos_open (const char *fname, error *e);
err_t walos_close (struct wal_ostream *w, error *e);

// Flush
err_t walos_flush_to (struct wal_ostream *w, lsn l, error *e);
err_t walos_flush_all (struct wal_ostream *w, error *e);

// Write
err_t walos_write_all (struct wal_ostream *w, u32 *checksum, const void *data,
                       u32 len, error *e);
lsn walos_get_next_lsn (const struct wal_ostream *w);
err_t walos_truncate (struct wal_ostream *w, u64 howmuch, error *e);

err_t walos_crash (struct wal_ostream *w, error *e);
