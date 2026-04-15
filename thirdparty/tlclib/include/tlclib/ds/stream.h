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

#include "tlclib/dev/error.h"
#include "tlclib/dev/stdtypes.h"

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
 *   stream_ibuf    — pulls from a fixed const byte buffer (read source).
 *   stream_obuf    — pushes into a fixed mutable byte buffer (write sink).
 *   stream_sink    — discards all bytes written to it (null sink).
 *   stream_opsink  — applies a callback to each element pushed.
 *   stream_limit   — wraps another stream and enforces a byte limit.
 */
struct stream;

/// Function pointer type for pulling bytes out of a stream into a caller buffer
typedef i32 (*stream_pull_fn) (
    struct stream *s, ///< The stream being read
    void *ctx,        ///< Implementation-defined context
    void *buf,        ///< Destination buffer to receive the data
    u32 size,         ///< Size of each element in bytes
    u32 n,            ///< Maximum number of elements to pull
    error *e);        ///< The error object

/// Function pointer type for pushing bytes from a caller buffer into a stream
typedef i32 (*stream_push_fn) (
    struct stream *s, ///< The stream being written
    void *ctx,        ///< Implementation-defined context
    const void *buf,  ///< Source buffer containing the data to push
    u32 size,         ///< Size of each element in bytes
    u32 n,            ///< Number of elements to push
    error *e);        ///< The error object

/// Function pointer type for releasing any resources held by a stream implementation
typedef void (*stream_close_fn) (void *ctx); ///< Implementation-defined context

/// Vtable of operations backing a stream
struct stream_ops
{
  stream_pull_fn pull;   ///< Pull bytes out of the stream (may be NULL for write-only streams)
  stream_push_fn push;   ///< Push bytes into the stream (may be NULL for read-only streams)
  stream_close_fn close; ///< Release resources held by the stream (may be NULL)
};

/// A polymorphic byte-oriented I/O stream
struct stream
{
  const struct stream_ops *ops; ///< Vtable of stream operations
  void *ctx;                    ///< Opaque context passed to every vtable call
  atomic_int done;              ///< Non-zero once the stream has no more data to produce or accept
};

/// Initializes a stream with a given vtable and context
void stream_init (
    struct stream *s,             ///< Stream to initialize
    const struct stream_ops *ops, ///< Vtable to attach
    void *ctx);                   ///< Opaque context to attach

/// Calls the stream's close function and releases any implementation resources
void stream_close (const struct stream *s); ///< Stream to close

/// Marks a stream as done, signaling to callers that no more data will be produced or accepted
void stream_finish (struct stream *s); ///< Stream to mark done

/// Returns true if the stream has been marked done via stream_finish()
bool stream_isdone (const struct stream *s); ///< Stream to test

/// Copies up to n elements of size bytes from src to dest via their stream interfaces
///
/// Returns the number of elements transferred (>= 0) or a negative error code.
/// A return smaller than n is not an error; check stream_isdone() on src to distinguish.
i32 stream_read (
    struct stream *dest, ///< Destination stream to push into
    u32 size,            ///< Size of each element in bytes
    u32 n,               ///< Maximum number of elements to transfer
    struct stream *src,  ///< Source stream to pull from
    error *e);           ///< The error object

/// Pulls up to n elements of size bytes from src into a raw buffer
///
/// Returns the number of elements transferred (>= 0) or a negative error code.
/// A return smaller than n is not an error; check stream_isdone() on src to distinguish.
i32 stream_bread (
    void *dest,         ///< Destination buffer to receive the data
    u32 size,           ///< Size of each element in bytes
    u32 n,              ///< Maximum number of elements to pull
    struct stream *src, ///< Source stream to pull from
    error *e);          ///< The error object

/// Pushes n elements of size bytes from a raw buffer into dest
///
/// Returns the number of elements transferred (>= 0) or a negative error code.
i32 stream_bwrite (
    const void *buf,     ///< Source buffer containing the data to push
    u32 size,            ///< Size of each element in bytes
    u32 n,               ///< Number of elements to push
    struct stream *dest, ///< Destination stream to push into
    error *e);           ///< The error object

////////////////////////////////////////////////////////////
/// Special Streams

/// Context for a read-only stream backed by a fixed const byte buffer
struct stream_ibuf_ctx
{
  const u8 *buf; ///< Pointer to the source buffer
  u32 size;      ///< Total number of bytes in buf
  u32 pos;       ///< Current read position in bytes
};

/// Context for a write-only stream that writes into a fixed mutable byte buffer
struct stream_obuf_ctx
{
  u8 *buf; ///< Pointer to the destination buffer
  u32 cap; ///< Total capacity of buf in bytes
  u32 pos; ///< Current write position in bytes
};

/// Initializes a read-only stream that pulls from a fixed const byte buffer
void stream_ibuf_init (
    struct stream *s,            ///< Stream to initialize
    struct stream_ibuf_ctx *ctx, ///< Context to initialize and attach
    const void *buf,             ///< Source buffer to read from
    u32 size);                   ///< Number of bytes in buf

/// Initializes a write-only stream that pushes into a fixed mutable byte buffer
void stream_obuf_init (
    struct stream *s,            ///< Stream to initialize
    struct stream_obuf_ctx *ctx, ///< Context to initialize and attach
    void *buf,                   ///< Destination buffer to write into
    u32 cap);                    ///< Capacity of buf in bytes

/// Initializes a null sink stream that discards all bytes written to it
void stream_sink_init (struct stream *s); ///< Stream to initialize

/// Callback type invoked on each element pushed into an opsink stream
typedef void (*byte_op) (void *buffer); ///< Pointer to the element being processed

/// Context for a stream that applies a callback to each element pushed into it
struct stream_opsink_ctx
{
  byte_op op; ///< Callback invoked on each complete element
  void *buf;  ///< Staging buffer used to accumulate one element before invoking op
  u32 size;   ///< Size of each element in bytes
  u32 pos;    ///< Current write position within the staging buffer
};

/// Initializes a stream that applies op to each complete element of size bytes pushed into it
void stream_opsink_init (
    struct stream *s,              ///< Stream to initialize
    struct stream_opsink_ctx *ctx, ///< Context to initialize and attach
    byte_op op,                    ///< Callback to invoke on each element
    void *buf,                     ///< Staging buffer of at least size bytes
    u32 size);                     ///< Size of each element in bytes

/// Context for a stream that forwards to an underlying stream up to a byte limit
struct stream_limit_ctx
{
  struct stream *underlying; ///< The stream being wrapped
  u64 limit;                 ///< Maximum number of bytes to forward
  u64 consumed;              ///< Number of bytes forwarded so far
};

/// Initializes a stream that forwards at most limit bytes from src before marking itself done
void stream_limit_init (
    struct stream *s,             ///< Stream to initialize
    struct stream_limit_ctx *ctx, ///< Context to initialize and attach
    struct stream *src,           ///< Underlying stream to wrap
    u64 limit);                   ///< Maximum number of bytes to forward
