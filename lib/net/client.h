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
#include "intf/os/socket.h"

struct client
{
  i_socket sock;
};

err_t client_connect (struct client *dest, const char *host, u16 port,
                      error *e);
err_t client_write_all (struct client *c, const void *src, u16 len, error *e);
err_t client_write_all_size_prefixed (struct client *c, const void *msg,
                                      u16 len, error *e);
err_t client_read_all (struct client *c, void *dest, u16 len, error *e);
i32 client_read_all_size_prefixed (struct client *c, void *dest, u16 len,
                                   error *e);
err_t client_disconnect (struct client *c, error *e);
