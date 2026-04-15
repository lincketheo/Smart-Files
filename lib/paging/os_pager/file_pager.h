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

/*
 * file_pager — concrete os_pager backed by a single OS file
 *
 * file_pager embeds struct os_pager as its first member so it can be used
 * anywhere an struct os_pager * is expected via a simple pointer cast.
 *
 * Callers that need a generic handle should call:
 *
 *   struct os_pager *fp = fpgr_open_os(path, e);
 *
 * Callers that need the concrete type (e.g. tests probing internal state)
 * may use fpgr_open() which returns struct file_pager *.
 */

#include "numstore/errors.h"
#include "paging/os_pager/os_pager.h"

struct file_pager;

/* Concrete-type constructor — returns struct file_pager *. */
struct file_pager *fpgr_open (const char *dbname, error *e);

/*
 * Abstract-type constructor — returns the embedded os_pager base pointer.
 * Equivalent to (struct os_pager *)fpgr_open(dbname, e) but keeps the
 * cast in one place.
 */
struct os_pager *fpgr_open_os (const char *dbname, error *e);

err_t fpgr_close (struct file_pager *f, error *e);
err_t fpgr_reset (struct file_pager *f, error *e);

p_size fpgr_get_npages (const struct file_pager *fp);
err_t fpgr_extend (struct file_pager *p, pgno pgno, error *e);
err_t fpgr_read (struct file_pager *p, u8 *dest, pgno pgno, error *e);
err_t fpgr_write (struct file_pager *p, const u8 *src, pgno pgno, error *e);

err_t fpgr_crash (struct file_pager *p, error *e);
