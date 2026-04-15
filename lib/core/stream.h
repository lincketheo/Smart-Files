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

#include "numstore/errors.h"
#include "numstore/stdtypes.h"

#include <stdatomic.h>

/*
 * A polymorphic byte-oriented I/O interface used throughout NumStore.
 *
 * A stream wraps a (pull, push, close) vtable
 * The `done` flag signals end-of-data; a stream sets it via
 * stream_finish() when it has no more bytes to produce or accept.  Callers
 * test it with stream_isdone() to decide when to stop reading or writing.
 *
 *   stream_bread(dest, size, n, src)  — pull up to n elements of [size] bytes
 *                                       from src into the dest buffer.
 *   stream_bwrite(buf, size, n, dest) — push n elements of [size] bytes from
 *                                       buf into dest.
 *   stream_read(dest, size, n, src)   — stream-to-stream copy.
 *
 * All three return the number of elements transferred (>= 0) or a negative
 * error code.  A return value smaller than n does not indicate an error;
 * the caller should check stream_isdone() to distinguish short-read from
 * error.
 *
 * Concrete stream implementations included here:
 *   stream_ibuf  — pulls from a fixed const byte buffer (read source).
 *   stream_obuf  — pushes into a fixed mutable byte buffer (write sink).
 *   stream_sink  — discards all bytes written to it (null sink).
 *   stream_opsink — applies a callback to each element pushed.
 *   stream_limit — wraps another stream and enforces a byte limit.
 */
struct stream;

typedef i32 (*stream_pull_fn) (struct stream *s, void *ctx, void *buf,
                               u32 size, u32 n, error *e);
typedef i32 (*stream_push_fn) (struct stream *s, void *ctx, const void *buf,
                               u32 size, u32 n, error *e);
typedef void (*stream_close_fn) (void *ctx);

struct stream_ops
{
  stream_pull_fn pull;
  stream_push_fn push;
  stream_close_fn close;
};

struct stream
{
  const struct stream_ops *ops;
  void *ctx;
  atomic_int done;
};

void stream_init (struct stream *s, const struct stream_ops *ops, void *ctx);
void stream_close (const struct stream *s);

void stream_finish (struct stream *s);
bool stream_isdone (const struct stream *s);

i32 stream_read (struct stream *dest, u32 size, u32 n, struct stream *src,
                 error *e);
i32 stream_bread (void *dest, u32 size, u32 n, struct stream *src, error *e);
i32 stream_bwrite (const void *buf, u32 size, u32 n, struct stream *dest,
                   error *e);

////////////////////////////////////////////////////////////
/// Special Streams

// Buffer streams

struct stream_ibuf_ctx
{
  const u8 *buf;
  u32 size;
  u32 pos;
};

struct stream_obuf_ctx
{
  u8 *buf;
  u32 cap;
  u32 pos;
};

void stream_ibuf_init (struct stream *s, struct stream_ibuf_ctx *ctx,
                       const void *buf, u32 size);
void stream_obuf_init (struct stream *s, struct stream_obuf_ctx *ctx,
                       void *buf, u32 cap);

// Sink stream

void stream_sink_init (struct stream *s);

// Sized Operator

typedef void (*byte_op) (void *buffer);

struct stream_opsink_ctx
{
  byte_op op;
  void *buf;
  u32 size;
  u32 pos;
};

void stream_opsink_init (struct stream *s, struct stream_opsink_ctx *ctx,
                         byte_op op, void *buf, u32 size);

// A stream that limits the number of bytes from an existing stream
struct stream_limit_ctx
{
  struct stream *underlying;
  b_size limit;
  b_size consumed;
};

void stream_limit_init (struct stream *s, struct stream_limit_ctx *ctx,
                        struct stream *src, b_size limit);
