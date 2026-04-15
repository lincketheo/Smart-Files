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

#define WIN32_LEAN_AND_MEAN
#include "tlclib/winsock2.h"
#include "tlclib/ws2tcpip.h"

#ifdef _MSC_VER
#pragma comment(lib, "ws2_32.lib")
#endif

#include "tlclib/dev/assert.h"
#include "tlclib/error.h"
#include "tlclib/intf/logging.h"
#include "tlclib/intf/os/socket.h"

#include <stdio.h>
#include <stdlib.h>

////////////////////////////////////////////////////////////
// Helpers

static char *
wsa_strerror (int code, char *buf, int buflen)
{
  FormatMessageA (FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
                  NULL, (DWORD)code, 0, buf, (DWORD)buflen, NULL);
  return buf;
}

#define WSA_BUF 256
#define WSA_ERR(buf) wsa_strerror (WSAGetLastError (), buf, sizeof (buf))

////////////////////////////////////////////////////////////
// WSA lifecycle

/**
static void
wsa_cleanup (void)
{
  WSACleanup ();
}

// should run at start
static void
wsa_init (void)
{
  WSADATA wsa;
  int     ret = WSAStartup (MAKEWORD (2, 2), &wsa);
  if (ret != 0)
    {
      char buf[WSA_BUF];
      wsa_strerror (ret, buf, sizeof (buf));
      fprintf (stderr, "WSAStartup failed: %s\n", buf);
      abort ();
    }
  atexit (wsa_cleanup);
}
*/

////////////////////////////////////////////////////////////
// Socket

err_t
i_socket_create (i_socket *s, error *e)
{
  ASSERT (s);
  s->fd = socket (AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (s->fd == INVALID_SOCKET)
    {
      char buf[WSA_BUF];
      return error_causef (e, ERR_IO, "socket: %s", WSA_ERR (buf));
    }
  return SUCCESS;
}

err_t
i_socket_close (i_socket *s, error *e)
{
  ASSERT (s);
  if (closesocket (s->fd) == SOCKET_ERROR)
    {
      char buf[WSA_BUF];
      return error_causef (e, ERR_IO, "closesocket: %s", WSA_ERR (buf));
    }
  s->fd = INVALID_SOCKET;
  return SUCCESS;
}

err_t
i_socket_set_reuseaddr (i_socket *s, error *e)
{
  ASSERT (s);
  int opt = 1;
  if (setsockopt (s->fd, SOL_SOCKET, SO_REUSEADDR, (const char *)&opt,
                  sizeof (opt))
      == SOCKET_ERROR)
    {
      char buf[WSA_BUF];
      return error_causef (e, ERR_IO, "setsockopt: %s", WSA_ERR (buf));
    }
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
    {
      char buf[WSA_BUF];
      return error_causef (e, ERR_IO, "inet_pton: invalid address '%s' (%s)", host,
                           WSA_ERR (buf));
    }

  if (connect (s->fd, (struct sockaddr *)&addr, sizeof (addr)) == SOCKET_ERROR)
    {
      char buf[WSA_BUF];
      return error_causef (e, ERR_IO, "connect: %s", WSA_ERR (buf));
    }

  return SUCCESS;
}

err_t
i_socket_bind (i_socket *s, int port, error *e)
{
  ASSERT (s);
  struct sockaddr_in addr = { 0 };
  addr.sin_family = AF_INET;
  addr.sin_port = htons ((u_short)port);
  addr.sin_addr.s_addr = INADDR_ANY;

  if (bind (s->fd, (struct sockaddr *)&addr, sizeof (addr)) == SOCKET_ERROR)
    {
      char buf[WSA_BUF];
      return error_causef (e, ERR_IO, "bind: %s", WSA_ERR (buf));
    }
  return SUCCESS;
}

err_t
i_socket_listen (i_socket *s, error *e)
{
  ASSERT (s);
  if (listen (s->fd, SOMAXCONN) == SOCKET_ERROR)
    {
      char buf[WSA_BUF];
      return error_causef (e, ERR_IO, "listen: %s", WSA_ERR (buf));
    }
  return SUCCESS;
}

err_t
i_socket_accept (i_socket *s, i_socket *dest, char *ip_out, int ip_out_len,
                 int *port_out, error *e)
{
  ASSERT (s);
  ASSERT (dest);

  struct sockaddr_in peer = { 0 };
  int peer_len = sizeof (peer);

  dest->fd = accept (s->fd, (struct sockaddr *)&peer, &peer_len);
  if (dest->fd == INVALID_SOCKET)
    {
      char buf[WSA_BUF];
      return error_causef (e, ERR_IO, "accept: %s", WSA_ERR (buf));
    }

  if (ip_out)
    inet_ntop (AF_INET, &peer.sin_addr, ip_out, (socklen_t)ip_out_len);
  if (port_out)
    *port_out = ntohs (peer.sin_port);

  return SUCCESS;
}

i64
i_socket_recv (i_socket *s, void *buf, u32 len, error *e)
{
  ASSERT (s);
  ASSERT (buf);

  int n = recv (s->fd, (char *)buf, (int)len, 0);
  if (n == SOCKET_ERROR)
    {
      char ebuf[WSA_BUF];
      return error_causef (e, ERR_IO, "recv: %s", WSA_ERR (ebuf));
    }
  return (i64)n;
}

i64
i_socket_send (i_socket *s, const void *buf, u32 len, error *e)
{
  ASSERT (s);
  ASSERT (buf);

  int n = send (s->fd, (const char *)buf, (int)len, 0);
  if (n == SOCKET_ERROR)
    {
      char ebuf[WSA_BUF];
      return error_causef (e, ERR_IO, "send: %s", WSA_ERR (ebuf));
    }
  return (i64)n;
}

int
i_poll (i_pollfd *fds, u32 nfds, int timeout_ms, error *e)
{
  int ret = WSAPoll (fds, (ULONG)nfds, timeout_ms);
  if (ret == SOCKET_ERROR)
    {
      char buf[WSA_BUF];
      return error_causef (e, ERR_IO, "WSAPoll: %s", WSA_ERR (buf));
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
