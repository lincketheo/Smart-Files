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

#include "net/polling_server.h"

#include "core/assert.h"
#include "core/error.h"
#include "core/latch.h"
#include "core/slab_alloc.h"
#include "intf/os.h"
#include "intf/os/socket.h"

#include <stdio.h>
#include <string.h>

err_t
pserv_open (struct polling_server *ps, const int port, const struct conn_actions actions,
            void *ctx, error *e)
{
  ps->actions = actions;
  ps->ctx = ctx;
  ps->cap = 101;

  ps->fds = i_malloc (ps->cap, sizeof *ps->fds, e);
  if (!ps->fds)
    return error_trace (e);

  ps->conns = i_malloc (ps->cap, sizeof (struct connection *), e);
  if (!ps->conns)
    {
      i_free (ps->fds);
      return error_trace (e);
    }
  ps->conns[0] = NULL;

  WRAP (i_socket_create (&ps->server, e));
  WRAP (i_socket_set_reuseaddr (&ps->server, e));
  WRAP (i_socket_bind (&ps->server, port, e));
  WRAP (i_socket_listen (&ps->server, e));

  ps->fds[0].fd = ps->server.fd;
  ps->fds[0].events = POLLIN;
  ps->fds[0].revents = 0;
  ps->len = 1;
  ps->running = 1;

  return SUCCESS;
}

static err_t
grow_if_needed (struct polling_server *s, error *e)
{
  if (s->len < s->cap)
    return SUCCESS;

  const u32 new_cap = s->cap * 2;

  i_pollfd *fds = i_realloc_right (s->fds, s->cap, new_cap,
                                   sizeof (i_pollfd), e);
  if (!fds)
    return error_trace (e);
  s->fds = fds;

  struct connection **conns = i_realloc_right (
      s->conns, s->cap, new_cap, sizeof (struct connection *), e);
  if (!conns)
    return error_trace (e);

  s->conns = conns;
  s->cap = new_cap;
  return SUCCESS;
}

static err_t
pserv_execute_conns (const struct polling_server *s, error *e)
{
  for (u32 i = 1; i < s->len; ++i)
    {
      struct connection *conn = s->conns[i];
      ASSERT (conn);

      if (s->actions.conn_func (s->ctx, conn, e))
        return error_trace (e);

      s->fds[i].events = 0;

      latch_lock (&conn->l);
      bool reading = conn->rx_cap > 0;
      bool writing = conn->tx_cap > 0;
      bool done_reading = conn->rx_len == conn->rx_cap;
      bool done_writing = conn->tx_sent == conn->tx_cap;
      latch_unlock (&conn->l);

      if (reading && !done_reading)
        s->fds[i].events |= POLLIN;
      if (writing && !done_writing)
        s->fds[i].events |= POLLOUT;
    }
  return SUCCESS;
}

static err_t
handle_accept (struct polling_server *s, error *e)
{
  struct connection *conn = s->actions.conn_alloc (s->ctx, e);
  if (!conn)
    return error_trace (e);

  memset (conn, 0, sizeof *conn);
  latch_init (&conn->l);

  char ip[64];
  int port = 0;

  if (i_socket_accept (&s->server, &conn->sock, ip, sizeof (ip), &port, e))
    {
      s->actions.conn_free (s->ctx, conn);
      return error_trace (e);
    }

  if (grow_if_needed (s, e))
    {
      i_socket_close (&conn->sock, e);
      s->actions.conn_free (s->ctx, conn);
      return error_trace (e);
    }

  printf ("[+] connected %s:%d  (fd %d clients)\n", ip, port, s->len - 1);

  s->fds[s->len].fd = conn->sock.fd;
  s->fds[s->len].events = 0;
  s->fds[s->len].revents = 0;
  s->conns[s->len] = conn;
  s->len++;

  return SUCCESS;
}

static err_t
remove_client (struct polling_server *s, const u32 i, error *e)
{
  struct connection *conn = s->conns[i];

  latch_lock (&conn->l);
  i_free (conn->rx_buf);
  i_free (conn->tx_buf);
  latch_unlock (&conn->l);

  WRAP (i_socket_close (&conn->sock, e));

  s->actions.conn_free (s->ctx, conn);
  s->fds[i] = s->fds[s->len - 1];
  s->conns[i] = s->conns[s->len - 1];
  s->len--;

  return SUCCESS;
}

static err_t
handle_read (struct connection *conn, error *e)
{
  latch_lock (&conn->l);

  ASSERT (conn->rx_cap > conn->rx_len);
  const i64 n = i_socket_recv (&conn->sock,
                               (char *)conn->rx_buf + conn->rx_len,
                               conn->rx_cap - conn->rx_len, e);

  if (n < 0)
    goto theend;

  if (n == 0)
    {
      error_causef (e, ERR_IO, "recv: peer closed connection");
      goto theend;
    }

  conn->rx_len += (u32)n;

theend:
  latch_unlock (&conn->l);
  return error_trace (e);
}

static err_t
handle_write (struct connection *conn, error *e)
{
  latch_lock (&conn->l);

  ASSERT (conn->tx_cap > conn->tx_sent);
  const i64 n = i_socket_send (&conn->sock,
                               (const char *)conn->tx_buf + conn->tx_sent,
                               conn->tx_cap - conn->tx_sent, e);
  if (n > 0)
    conn->tx_sent += (u32)n;

  latch_unlock (&conn->l);
  return SUCCESS;
}

int
pserv_execute (struct polling_server *ps, error *e)
{
  if (pserv_execute_conns (ps, e))
    return error_trace (e);

  const int ret = i_poll (ps->fds, ps->len, 1000, e);
  if (ret < 0)
    return error_trace (e);
  if (ret == 0)
    return ps->running;

  for (int i = (int)ps->len - 1; i >= 0; i--)
    {
      const int ev = ps->fds[i].revents;
      if (!ev)
        continue;

      if (i == 0)
        {
          ASSERT (ev & POLLIN);
          if (handle_accept (ps, e))
            return error_trace (e);
        }
      else if (ev & (POLLERR | POLLHUP))
        {
          printf ("[-] fd %d hangup/error (0x%x)\n", i, ev);
          if (remove_client (ps, (u32)i, e))
            return error_trace (e);
        }
      else
        {
          struct connection *conn = ps->conns[i];
          ASSERT (conn);

          if (ev & POLLIN)
            {
              if (handle_read (conn, e))
                if (remove_client (ps, (u32)i, e))
                  return error_trace (e);
            }
          else if (ev & POLLOUT)
            {
              handle_write (conn, e);
            }
        }
    }

  return ps->running;
}

err_t
pserv_close (struct polling_server *ps, error *e)
{
  for (u32 i = 1; i < ps->len; i++)
    {
      latch_lock (&ps->conns[i]->l);
      i_free (ps->conns[i]->rx_buf);
      i_free (ps->conns[i]->tx_buf);
      latch_unlock (&ps->conns[i]->l);
      i_socket_close (&ps->conns[i]->sock, e);
      ps->actions.conn_free (ps->ctx, ps->conns[i]);
    }

  i_socket_close (&ps->server, e);

  i_free (ps->conns);
  i_free (ps->fds);

  ps->conns = NULL;
  ps->fds = NULL;
  ps->len = 0;
  ps->cap = 0;

  return error_trace (e);
}
