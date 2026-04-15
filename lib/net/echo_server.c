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

#include "net/echo_server.h"

#include "intf/os.h"
#include "intf/os/socket.h"

#include <string.h>

static u32
decode_prefix (const u8 *p)
{
  u32 val;
  memcpy (&val, p, sizeof (u32));
  return i_ntohl (val);
}

static void
set_prefix (void *dest, const u32 src)
{
  const u32 val = i_htonl (src);
  memcpy (dest, &val, sizeof (u32));
}

err_t
echo_conn_func (const void *echo_ctx, struct connection *conn, error *e)
{
  // Just created a new connection
  if (conn->rx_cap == 0 && conn->tx_cap == 0)
    {
      conn->rx_buf = i_malloc (4, 1, e);
      if (!conn->rx_buf)
        return error_trace (e);
      conn->rx_cap = 4;
      conn->rx_len = 0;
      return SUCCESS;
    }

  // Finished sending tx
  if (conn->tx_cap > 0 && conn->tx_sent == conn->tx_cap)
    {
      conn->tx_cap = 0;
      conn->tx_sent = 0;
      return SUCCESS;
    }

  // Haven't finished receiving yet
  if (conn->rx_len < conn->rx_cap)
    return SUCCESS;

  // Just finished receiving prefix — extend rx buf
  if (conn->rx_cap == 4)
    {
      const u32 msg_len = decode_prefix (conn->rx_buf);
      if (msg_len == 0)
        {
          conn->rx_len = 0;
          return SUCCESS;
        }
      const u32 total = 4 + msg_len;
      void *buf = i_realloc_right (conn->rx_buf, conn->rx_cap, total, 1, e);
      if (!buf)
        return error_trace (e);
      conn->rx_buf = buf;
      conn->rx_cap = total;
      return SUCCESS;
    }

  // Done consuming — do work
  ASSERT (conn->rx_len == conn->rx_cap);
  {
    const struct echo_context *ctx = echo_ctx;
    const u32 plen = (u32)strlen (ctx->prefix);
    const u32 slen = (u32)strlen (ctx->suffix);
    const u32 mlen = decode_prefix (conn->rx_buf);
    const u32 msg_len = mlen + plen + slen;
    const u32 frame_len = msg_len + 4;

    conn->tx_buf = i_malloc (frame_len, 1, e);
    if (!conn->tx_buf)
      return error_trace (e);

    set_prefix (conn->tx_buf, msg_len);

    u32 head = 4;
    memcpy ((u8 *)conn->tx_buf + head, ctx->prefix, plen);
    head += plen;
    memcpy ((u8 *)conn->tx_buf + head, (u8 *)conn->rx_buf + 4, mlen);
    head += mlen;
    memcpy ((u8 *)conn->tx_buf + head, ctx->suffix, slen);
    head += slen;
    ASSERT (head == frame_len);

    conn->tx_cap = frame_len;
    conn->tx_sent = 0;

    printf ("[>] sock %d  %u bytes\n", (int)conn->sock.fd, frame_len - 4);

    const u32 old_rx_cap = frame_len - plen - slen;
    void *buf = i_realloc_right (conn->rx_buf, old_rx_cap, 4, 1, e);
    if (!buf)
      return error_trace (e);
    conn->rx_buf = buf;
    conn->rx_cap = 4;
    conn->rx_len = 0;
    return SUCCESS;
  }
}

struct connection *
echo_conn_alloc (const void *ctx, error *e)
{
  (void)ctx;
  return i_malloc (1, sizeof (struct connection), e);
}

void
echo_conn_free (const void *ctx, struct connection *conn)
{
  (void)ctx;
  i_free (conn);
}
