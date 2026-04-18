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

#include "c_specx.h"
#include "smfile.h"

struct os_pager;

struct os_pager_vtable
{
  err_t (*close) (struct os_pager *self, error *e);
  err_t (*reset) (struct os_pager *self, error *e);
  p_size (*get_npages) (const struct os_pager *self);
  err_t (*extend) (struct os_pager *self, pgno dest, error *e);
  err_t (*read) (struct os_pager *self, u8 *dest, pgno pg, error *e);
  err_t (*write) (struct os_pager *self, const u8 *src, pgno pg, error *e);
  err_t (*crash_fn) (struct os_pager *self, error *e);
};

struct os_pager
{
  const struct os_pager_vtable *vtable;
};

HEADER_FUNC err_t
ospgr_close (struct os_pager *p, error *e)
{
  return p->vtable->close (p, e);
}

HEADER_FUNC err_t
ospgr_reset (struct os_pager *p, error *e)
{
  return p->vtable->reset (p, e);
}

HEADER_FUNC p_size
ospgr_get_npages (const struct os_pager *p)
{
  return p->vtable->get_npages (p);
}

HEADER_FUNC err_t
ospgr_extend (struct os_pager *p, const pgno dest, error *e)
{
  return p->vtable->extend (p, dest, e);
}

HEADER_FUNC err_t
ospgr_read (struct os_pager *p, u8 *dest, const pgno pg, error *e)
{
  return p->vtable->read (p, dest, pg, e);
}

HEADER_FUNC err_t
ospgr_write (struct os_pager *p, const u8 *src, const pgno pg, error *e)
{
  return p->vtable->write (p, src, pg, e);
}

HEADER_FUNC err_t
ospgr_crash (struct os_pager *p, error *e)
{
  return p->vtable->crash_fn (p, e);
}
