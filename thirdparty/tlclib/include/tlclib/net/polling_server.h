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

#include "tlclib/concurrency/latch.h"
#include "tlclib/dev/error.h"
#include "tlclib/intf/os/socket.h"

struct connection
{
  void *rx_buf;
  u32 rx_cap;
  u32 rx_len;
  void *tx_buf;
  u32 tx_cap;
  u32 tx_sent;
  i_socket sock;
  latch l;
};

struct conn_actions
{
  struct connection *(*conn_alloc) (void *ctx, error *e);
  err_t (*conn_func) (void *ctx, struct connection *conn, error *e);
  void (*conn_free) (void *ctx, struct connection *conn);
};

struct polling_server
{
  i_pollfd *fds;
  struct connection **conns;
  u32 len;
  u32 cap;
  volatile int running;
  struct conn_actions actions;
  void *ctx;
  i_socket server;
};

err_t pserv_open (
    struct polling_server *ps,
    int port,
    struct conn_actions actions,
    void *ctx,
    error *e);

int pserv_execute (struct polling_server *ps, error *e);

err_t pserv_close (struct polling_server *ps, error *e);
