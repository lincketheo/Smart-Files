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

#include "tlclib/intf/os/socket.h"

#include "sys/poll.h"
#include "tlclib/dev/assert.h"
#include "tlclib/dev/error.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

err_t
i_socket_create (i_socket *s, error *e)
{
  ASSERT (s);
  s->fd = socket (AF_INET, SOCK_STREAM, 0);
  if (s->fd == I_INVALID_SOCKET)
    return error_causef (e, ERR_IO, "socket: %s", strerror (errno));
  return SUCCESS;
}

err_t
i_socket_close (i_socket *s, error *e)
{
  ASSERT (s);
  if (close (s->fd) < 0)
    return error_causef (e, ERR_IO, "close: %s", strerror (errno));
  s->fd = I_INVALID_SOCKET;
  return SUCCESS;
}

err_t
i_socket_set_reuseaddr (i_socket *s, error *e)
{
  ASSERT (s);
  const int opt = 1;
  if (setsockopt (s->fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof (opt)) < 0)
    return error_causef (e, ERR_IO, "setsockopt: %s", strerror (errno));
  return SUCCESS;
}

err_t
i_socket_connect (i_socket *s, const char *host, u16 port, error *e)
{
  ASSERT (s);
  ASSERT (host);

  struct sockaddr_in addr = { 0 };
  addr.sin_family = AF_INET;
  addr.sin_port = htons (port);

  if (inet_pton (AF_INET, host, &addr.sin_addr) <= 0)
    return error_causef (e, ERR_IO, "inet_pton: invalid address '%s'", host);

  if (connect (s->fd, (struct sockaddr *)&addr, sizeof (addr)) < 0)
    return error_causef (e, ERR_IO, "connect: %s", strerror (errno));

  return SUCCESS;
}

err_t
i_socket_bind (i_socket *s, const int port, error *e)
{
  ASSERT (s);
  struct sockaddr_in addr = { .sin_family = AF_INET,
                              .sin_port = htons ((uint16_t)port),
                              .sin_addr.s_addr = INADDR_ANY };
  if (bind (s->fd, (struct sockaddr *)&addr, sizeof (addr)) < 0)
    return error_causef (e, ERR_IO, "bind: %s", strerror (errno));
  return SUCCESS;
}

err_t
i_socket_listen (i_socket *s, error *e)
{
  ASSERT (s);
  if (listen (s->fd, SOMAXCONN) < 0)
    return error_causef (e, ERR_IO, "listen: %s", strerror (errno));
  return SUCCESS;
}

err_t
i_socket_accept (i_socket *s, i_socket *dest, char *ip_out,
                 const int ip_out_len, int *port_out, error *e)
{
  ASSERT (s);
  ASSERT (dest);

  struct sockaddr_in peer;
  socklen_t peer_len = sizeof (peer);

  dest->fd = accept (s->fd, (struct sockaddr *)&peer, &peer_len);
  if (dest->fd == I_INVALID_SOCKET)
    return error_causef (e, ERR_IO, "accept: %s", strerror (errno));

  if (ip_out)
    inet_ntop (AF_INET, &peer.sin_addr, ip_out, (socklen_t)ip_out_len);
  if (port_out)
    *port_out = ntohs (peer.sin_port);

  return SUCCESS;
}

i64
i_socket_recv (i_socket *s, void *buf, const u32 len, error *e)
{
  ASSERT (s);
  ASSERT (buf);

  const ssize_t n = read (s->fd, buf, len);
  if (n < 0)
    return error_causef (e, ERR_IO, "recv: %s", strerror (errno));
  return (i64)n;
}

i64
i_socket_send (i_socket *s, const void *buf, const u32 len, error *e)
{
  ASSERT (s);
  ASSERT (buf);

  const ssize_t n = write (s->fd, buf, len);
  if (n < 0)
    return error_causef (e, ERR_IO, "send: %s", strerror (errno));
  return (i64)n;
}

int
i_poll (i_pollfd *fds, const u32 nfds, const int timeout_ms, error *e)
{
  const int ret = poll (fds, (nfds_t)nfds, timeout_ms);
  if (ret < 0)
    {
      if (errno == EINTR)
        return 0;
      return error_causef (e, ERR_IO, "poll: %s", strerror (errno));
    }
  return ret;
}

u32
i_htonl (u32 host)
{
  return htonl (host);
}
u32
i_ntohl (u32 net)
{
  return ntohl (net);
}
u16
i_htons (u16 host)
{
  return htons (host);
}
u16
i_ntohs (u16 net)
{
  return ntohs (net);
}
