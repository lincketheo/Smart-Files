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

#include "net/client.h"

#include "core/error.h"
#include "intf/os.h"
#include "intf/os/socket.h"

#include <string.h>

static err_t
read_exact (i_socket *s, void *buf, const u32 count, error *e)
{
  u8 *p = buf;
  u32 total = 0;

  while (total < count)
    {
      const i64 n = i_socket_recv (s, p + total, count - total, e);
      if (n < 0)
        return error_trace (e);
      if (n == 0)
        return error_causef (e, ERR_IO, "read_exact: peer closed connection");
      total += (u32)n;
    }

  return SUCCESS;
}

static err_t
write_exact (i_socket *s, const void *buf, const u32 count, error *e)
{
  const u8 *p = buf;
  u32 total = 0;

  while (total < count)
    {
      const i64 n = i_socket_send (s, p + total, count - total, e);
      if (n < 0)
        return error_trace (e);
      if (n == 0)
        return error_causef (e, ERR_IO, "write_exact: peer closed connection");
      total += (u32)n;
    }

  return SUCCESS;
}

err_t
client_connect (struct client *dest, const char *host, const u16 port, error *e)
{
  WRAP (i_socket_create (&dest->sock, e));
  WRAP (i_socket_connect (&dest->sock, host, port, e));
  return SUCCESS;
}

err_t
client_write_all (struct client *c, const void *src, const u16 len, error *e)
{
  return write_exact (&c->sock, src, (u32)len, e);
}

err_t
client_write_all_size_prefixed (struct client *c, const void *msg, const u16 len,
                                error *e)
{
  const u32 wire = i_htonl ((u32)len);
  WRAP (write_exact (&c->sock, &wire, 4, e));
  WRAP (write_exact (&c->sock, msg, (u32)len, e));
  return SUCCESS;
}

err_t
client_read_all (struct client *c, void *dest, const u16 len, error *e)
{
  return read_exact (&c->sock, dest, (u32)len, e);
}

i32
client_read_all_size_prefixed (struct client *c, void *dest, const u16 len, error *e)
{
  u32 wire = 0;
  WRAP (read_exact (&c->sock, &wire, 4, e));

  const u32 msg_len = i_ntohl (wire);
  if (msg_len > (u32)len)
    return error_causef (e, ERR_IO, "read: message too large (%u > %u)",
                         msg_len, (u32)len);

  WRAP (read_exact (&c->sock, dest, msg_len, e));
  return (i32)msg_len;
}

err_t
client_disconnect (struct client *c, error *e)
{
  return i_socket_close (&c->sock, e);
}
