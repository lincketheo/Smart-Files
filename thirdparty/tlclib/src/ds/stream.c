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

#include "tlclib/ds/stream.h"

#include "tlclib/dev/assert.h"
#include "tlclib/dev/error.h"

#include <string.h>

void
stream_init (struct stream *s, const struct stream_ops *ops, void *ctx)
{
  s->ops = ops;
  s->ctx = ctx;
  s->done = false;
}

void
stream_finish (struct stream *s)
{
  atomic_store_explicit (&s->done, true, memory_order_release);
}

bool
stream_isdone (const struct stream *s)
{
  return atomic_load_explicit (&s->done, memory_order_acquire);
}

void
stream_close (const struct stream *s)
{
  if (s->ops->close)
    {
      s->ops->close (s->ctx);
    }
}

i32 stream_read (struct stream *dest, u32 size, u32 n, struct stream *src,
                 error *e);

i32
stream_bread (void *dest, const u32 size, const u32 n, struct stream *src,
              error *e)
{
  return src->ops->pull (src, src->ctx, dest, size, n, e);
}

i32
stream_bwrite (const void *buf, const u32 size, const u32 n,
               struct stream *dest, error *e)
{
  return dest->ops->push (dest, dest->ctx, buf, size, n, e);
}

i32
stream_read (struct stream *dest, const u32 size, const u32 n,
             struct stream *src, error *e)
{
  ASSERT (dest->ops->push);
  ASSERT (src->ops->pull);

  u8 buf[4096];
  u32 total = 0;
  u32 remaining = n;
  u32 batch_max = sizeof (buf) / size;

  if (batch_max == 0)
    {
      batch_max = 1;
    }

  while (remaining > 0)
    {
      const u32 batch = remaining < batch_max ? remaining : batch_max;

      const i32 got = src->ops->pull (src, src->ctx, buf, size, batch, e);
      if (got < 0)
        {
          return got;
        }
      if (got == 0)
        {
          break;
        }

      u32 pushed = 0;
      while (pushed < (u32)got)
        {
          const i32 w = dest->ops->push (dest, dest->ctx, buf + (pushed * size),
                                         size, got - pushed, e);
          if (w < 0)
            {
              return w;
            }
          if (w == 0)
            {
              break;
            }
          pushed += w;
        }

      total += pushed;
      remaining -= pushed;

      if (pushed < (u32)got)
        {
          break;
        }
    }

  return total;
}

static i32
stream_ibuf_pull (struct stream *s, void *vctx, void *dest,
                  const u32 size, const u32 n, error *e)
{
  struct stream_ibuf_ctx *ctx = (struct stream_ibuf_ctx *)vctx;

  const u32 avail = ctx->size - ctx->pos;
  const u32 want = size * n;

  const u32 next = MIN (avail, want);

  if (next == 0)
    {
      if (avail == 0)
        {
          stream_finish (s);
        }
      return 0;
    }

  memcpy (dest, ctx->buf + ctx->pos, next);
  ctx->pos += next;

  return next / size;
}

static i32
stream_obuf_push (struct stream *s, void *vctx, const void *src,
                  const u32 size, const u32 n, error *e)
{
  struct stream_obuf_ctx *ctx = (struct stream_obuf_ctx *)vctx;

  const u32 avail = ctx->cap - ctx->pos;
  const u32 want = size * n;

  const u32 next = MIN (avail, want);

  if (next == 0)
    {
      if (avail == 0)
        {
          stream_finish (s);
        }
      return 0;
    }

  memcpy (ctx->buf + ctx->pos, src, next);
  ctx->pos += next;

  return next / size;
}

static const struct stream_ops stream_ibuf_ops = {
  .pull = stream_ibuf_pull,
  .push = NULL,
  .close = NULL,
};

static const struct stream_ops stream_obuf_ops = {
  .pull = NULL,
  .push = stream_obuf_push,
  .close = NULL,
};

void
stream_ibuf_init (struct stream *s, struct stream_ibuf_ctx *ctx,
                  const void *buf, const u32 size)
{
  ctx->buf = (const u8 *)buf;
  ctx->size = size;
  ctx->pos = 0;
  stream_init (s, &stream_ibuf_ops, ctx);
}

void
stream_obuf_init (struct stream *s, struct stream_obuf_ctx *ctx, void *buf,
                  const u32 cap)
{
  ctx->buf = buf;
  ctx->cap = cap;
  ctx->pos = 0;
  stream_init (s, &stream_obuf_ops, ctx);
}

static i32
stream_sink_push (struct stream *s, void *vctx, const void *src,
                  u32 size, const u32 n, error *e)
{
  return n;
}

static const struct stream_ops stream_sink_ops = {
  .pull = NULL,
  .push = stream_sink_push,
  .close = NULL,
};

void
stream_sink_init (struct stream *s)
{
  stream_init (s, &stream_sink_ops, NULL);
}

static i32
stream_opsink_push (struct stream *s, void *vctx, const void *src,
                    const u32 size, const u32 n, error *e)
{
  struct stream_opsink_ctx *ctx = (struct stream_opsink_ctx *)vctx;

  const u32 avail = ctx->size - ctx->pos;
  const u32 want = size * n;

  const u32 next = MIN (avail, want);

  if (next == 0)
    {
      return 0;
    }

  memcpy ((u8 *)ctx->buf + ctx->pos, src, next);
  ctx->pos += next;

  if (ctx->pos == ctx->size)
    {
      ctx->op (ctx->buf);
      ctx->pos = 0;
    }

  return next / size;
}

static const struct stream_ops stream_opsink_ops = {
  .pull = NULL,
  .push = stream_opsink_push,
  .close = NULL,
};

void
stream_opsink_init (struct stream *s, struct stream_opsink_ctx *ctx,
                    const byte_op op, void *buf, const u32 size)
{
  ctx->buf = buf, ctx->op = op, ctx->size = size;
  ctx->pos = 0;
  stream_init (s, &stream_opsink_ops, ctx);
}
static i32
stream_limit_pull (struct stream *s, void *vctx, void *buf,
                   const u32 size, const u32 n, error *e)
{
  struct stream_limit_ctx *ctx = (struct stream_limit_ctx *)vctx;

  const u64 remaining = ctx->limit - ctx->consumed;
  if (remaining == 0)
    {
      stream_finish (s);
      return 0;
    }

  const u32 capped_n = (u32)(MIN ((u64)(size * n), remaining) / size);
  if (capped_n == 0)
    {
      stream_finish (s);
      return 0;
    }

  const i32 got = stream_bread (buf, size, capped_n, ctx->underlying, e);
  if (got < 0)
    {
      return got;
    }

  ctx->consumed += (u64)got * size;

  if (ctx->consumed >= ctx->limit || stream_isdone (ctx->underlying))
    {
      stream_finish (s);
    }

  return got;
}

static i32
stream_limit_push (struct stream *s, void *vctx, const void *buf,
                   const u32 size, const u32 n, error *e)
{
  struct stream_limit_ctx *ctx = (struct stream_limit_ctx *)vctx;

  const u64 remaining = ctx->limit - ctx->consumed;
  if (remaining == 0)
    {
      stream_finish (s);
      return 0;
    }

  const u32 capped_n = (u32)(MIN ((u64)(size * n), remaining) / size);
  if (capped_n == 0)
    {
      stream_finish (s);
      return 0;
    }

  const i32 put = stream_bwrite (buf, size, capped_n, ctx->underlying, e);
  if (put < 0)
    {
      return put;
    }

  ctx->consumed += (u64)put * size;

  if (ctx->consumed >= ctx->limit || stream_isdone (ctx->underlying))
    {
      stream_finish (s);
    }

  return put;
}

static const struct stream_ops stream_limit_ops = {
  .pull = stream_limit_pull,
  .push = stream_limit_push,
  .close = NULL,
};

void
stream_limit_init (struct stream *s, struct stream_limit_ctx *ctx,
                   struct stream *underlying, const u64 limit)
{
  ctx->underlying = underlying;
  ctx->limit = limit;
  ctx->consumed = 0;
  stream_init (s, &stream_limit_ops, ctx);
}
