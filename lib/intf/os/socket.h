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

#include "core/error.h"
#include "core/system.h"

#if defined(_WIN32)
#define WIN32_LEAN_AND_MEAN
#include "winsock2.h"
#include "ws2tcpip.h"
typedef SOCKET i_sock_fd;
typedef WSAPOLLFD i_pollfd;
#define I_INVALID_SOCKET INVALID_SOCKET
#else
#include "sys/poll.h"
typedef int i_sock_fd;
typedef struct pollfd i_pollfd;
#define I_INVALID_SOCKET (-1)
#endif

////////////////////////////////////////////////////////////
// Socket

typedef struct
{
  i_sock_fd fd;
} i_socket;

err_t i_socket_create (i_socket *s, error *e);
err_t i_socket_close (i_socket *s, error *e);
err_t i_socket_connect (i_socket *s, const char *host, u16 port, error *e);
err_t i_socket_bind (i_socket *s, int port, error *e);
err_t i_socket_listen (i_socket *s, error *e);
err_t i_socket_accept (i_socket *s, i_socket *dest, char *ip_out,
                       int ip_out_len, int *port_out, error *e);
err_t i_socket_set_reuseaddr (i_socket *s, error *e);

i64 i_socket_recv (i_socket *s, void *buf, u32 len, error *e);
i64 i_socket_send (i_socket *s, const void *buf, u32 len, error *e);

////////////////////////////////////////////////////////////
// Poll

int i_poll (i_pollfd *fds, u32 nfds, int timeout_ms, error *e);

////////////////////////////////////////////////////////////
// Byte order

u32 i_htonl (u32 host);
u32 i_ntohl (u32 net);
u16 i_htons (u16 host);
u16 i_ntohs (u16 net);
