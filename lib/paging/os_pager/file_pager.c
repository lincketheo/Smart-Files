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

#include "paging/os_pager/file_pager.h"

#include "core/assert.h"
#include "core/error.h"
#include "intf/os/file_system.h"
#include "intf/os/memory.h"
#include "numstore/compile_config.h"
#include "test/testing.h"

#include <string.h>

/*
 * Concrete page-I/O implementation backed by a single OS file.
 *
 * The os_pager base must be the first member so that a struct file_pager *
 * can be freely cast to struct os_pager * and back, as required by the
 * vtable dispatch pattern.
 */
struct file_pager
{
  struct os_pager base;
  pgno npages;
  i_file f;
};

DEFINE_DBG_ASSERT (struct file_pager, file_pager, p, { ASSERT (p); })

static err_t
fp_close_impl (struct os_pager *self, error *e)
{
  struct file_pager *fp = (struct file_pager *)self;
  return fpgr_close (fp, e);
}

static err_t
fp_reset_impl (struct os_pager *self, error *e)
{
  struct file_pager *fp = (struct file_pager *)self;
  return fpgr_reset (fp, e);
}

static p_size
fp_get_npages_impl (const struct os_pager *self)
{
  const struct file_pager *fp = (const struct file_pager *)self;
  return fpgr_get_npages (fp);
}

static err_t
fp_extend_impl (struct os_pager *self, const pgno dest, error *e)
{
  struct file_pager *fp = (struct file_pager *)self;
  return fpgr_extend (fp, dest, e);
}

static err_t
fp_read_impl (struct os_pager *self, u8 *dest, const pgno pg, error *e)
{
  struct file_pager *fp = (struct file_pager *)self;
  return fpgr_read (fp, dest, pg, e);
}

static err_t
fp_write_impl (struct os_pager *self, const u8 *src, const pgno pg, error *e)
{
  struct file_pager *fp = (struct file_pager *)self;
  return fpgr_write (fp, src, pg, e);
}

static err_t
fp_crash_impl (struct os_pager *self, error *e)
{
  struct file_pager *fp = (struct file_pager *)self;
  return fpgr_crash (fp, e);
}

static const struct os_pager_vtable fp_vtable = {
  .close = fp_close_impl,
  .reset = fp_reset_impl,
  .get_npages = fp_get_npages_impl,
  .extend = fp_extend_impl,
  .read = fp_read_impl,
  .write = fp_write_impl,
  .crash_fn = fp_crash_impl,
};

struct file_pager *
fpgr_open (const char *dbname, error *e)
{
  struct file_pager *dest = i_malloc (1, sizeof *dest, e);
  if (dest == NULL)
    {
      return NULL;
    }

  dest->base.vtable = &fp_vtable;

  if (i_open_rw (&dest->f, dbname, e))
    {
      goto failed;
    }

  const i64 size = i_file_size (&dest->f, e);
  if (size < 0)
    {
      goto fp_failed;
    }

  if (size % PAGE_SIZE != 0)
    {
      error_causef (e, ERR_CORRUPT,
                    "file size %" PRId64
                    " is not a multiple of PAGE_SIZE (%" PRp_size ")",
                    size, PAGE_SIZE);
      goto fp_failed;
    }

  dest->npages = size / PAGE_SIZE;

  DBG_ASSERT (file_pager, dest);

  return dest;

fp_failed:
  i_close (&dest->f, e);
  i_free (dest);

failed:
  return NULL;
}

struct os_pager *
fpgr_open_os (const char *dbname, error *e)
{
  return (struct os_pager *)fpgr_open (dbname, e);
}

#ifndef NTEST
TEST (TT_UNIT, fpgr_open)
{
  error e = error_create ();
  _Static_assert (PAGE_SIZE > 2,
                  "PAGE_SIZE should be > 2 for file_pager test");

  i_file fp = { 0 };
  i_open_rw (&fp, "test.db", &e);

  // edge case: file shorter than header
  test_fail_if (i_truncate (&fp, PAGE_SIZE - 1, &e));
  struct file_pager *pager = fpgr_open ("test.db", &e);
  test_err_t_check (e.cause_code, ERR_CORRUPT, &e);

  // edge case: file size = half a page
  test_fail_if (i_truncate (&fp, PAGE_SIZE / 2, &e));
  pager = fpgr_open ("test.db", &e);
  test_err_t_check (e.cause_code, ERR_CORRUPT, &e);

  // happy path: file exactly header size, zero pages
  test_fail_if (i_truncate (&fp, 0, &e));
  pager = fpgr_open ("test.db", &e);
  test_assert_int_equal ((int)pager->npages, 0);
  test_fail_if (fpgr_close (pager, &e));

  // happy path: file exactly header size, more pages
  test_fail_if (i_truncate (&fp, 3 * PAGE_SIZE, &e));
  pager = fpgr_open ("test.db", &e);
  test_assert_equal (pager->npages, 3);
  test_fail_if (fpgr_close (pager, &e));

  // There were 2 refs to file - close it here too
  test_fail_if (i_close (&fp, &e));
  test_fail_if (i_unlink ("test.db", &e));
}
#endif

err_t
fpgr_close (struct file_pager *f, error *e)
{
  DBG_ASSERT (file_pager, f);
  i_close (&f->f, e);
  i_free (f);
  return error_trace (e);
}

err_t
fpgr_reset (struct file_pager *f, error *e)
{
  DBG_ASSERT (file_pager, f);
  WRAP (i_truncate (&f->f, 0, e));
  f->npages = 0;
  return error_trace (e);
}

p_size
fpgr_get_npages (const struct file_pager *fp)
{
  DBG_ASSERT (file_pager, fp);
  return fp->npages;
}

err_t
fpgr_extend (struct file_pager *p, const pgno dest, error *e)
{
  DBG_ASSERT (file_pager, p);
  ASSERT (dest);

  if (dest < p->npages)
    {
      return SUCCESS;
    }

  if (i_truncate (&p->f, PAGE_SIZE * (dest), e))
    {
      goto failed;
    }

  p->npages = dest;

  return SUCCESS;

failed:
  return error_trace (e);
}

#ifndef NTEST
TEST (TT_UNIT, fpgr_new)
{
  i_file fp = { 0 };
  error e = error_create ();
  test_fail_if (i_open_rw (&fp, "test.db", &e));

  test_fail_if (i_truncate (&fp, 0, &e));

  struct file_pager *pager = fpgr_open ("test.db", &e);

  // Create a new page
  test_fail_if (fpgr_extend (pager, 1, &e));
  test_assert_int_equal (pager->npages, 1);
  test_assert_int_equal (i_file_size (&fp, &e), PAGE_SIZE * pager->npages);

  // Add two more pages and do the same thing
  test_fail_if (fpgr_extend (pager, 2, &e));
  test_assert_int_equal (pager->npages, 2);
  test_assert_int_equal (i_file_size (&fp, &e), PAGE_SIZE * pager->npages);

  test_fail_if (fpgr_extend (pager, 3, &e));
  test_assert_int_equal (pager->npages, 3);
  test_assert_int_equal (i_file_size (&fp, &e), PAGE_SIZE * pager->npages);

  test_fail_if (fpgr_close (pager, &e));

  // There were 2 refs to file - close it here too
  test_fail_if (i_close (&fp, &e));
  test_fail_if (i_unlink ("test.db", &e));
}
#endif

err_t
fpgr_read (struct file_pager *p, u8 *dest, const pgno pg, error *e)
{
  DBG_ASSERT (file_pager, p);
  ASSERT (dest);

  if (pg >= p->npages)
    {
      return error_causef (
          e, ERR_PG_OUT_OF_RANGE,
          "page %" PRpgno " out of range (npages=%" PRpgno ")", pg, p->npages);
    }

  // Read all from file
  const i64 nread = i_pread_all (&p->f, dest, PAGE_SIZE, pg * PAGE_SIZE, e);

  if (nread == 0)
    {
      return error_causef (e, ERR_CORRUPT,
                           "pread returned 0 bytes at page %" PRpgno, pg);
    }

  if (nread < 0)
    {
      return error_trace (e);
    }

  return SUCCESS;
}

err_t
fpgr_write (struct file_pager *p, const u8 *src, const pgno pg, error *e)
{
  DBG_ASSERT (file_pager, p);
  ASSERT (src);
  ASSERT (pg < p->npages);

  if (i_pwrite_all (&p->f, src, PAGE_SIZE, pg * PAGE_SIZE, e))
    {
      goto failed;
    }

  return SUCCESS;

failed:
  return error_trace (e);
}

#ifndef NTEST
TEST (TT_UNIT, fpgr_read_write)
{
  // The raw page bytes
  u8 _page[PAGE_SIZE];

  // Create a temporary file
  i_file fp = { 0 };
  error e = error_create ();
  test_fail_if (i_open_rw (&fp, "test.db", &e));

  // File should be size 0
  test_fail_if (i_truncate (&fp, 0, &e));

  // Open a new pager
  struct file_pager *pager = fpgr_open ("test.db", &e);
  test_assert_int_equal (e.cause_code, SUCCESS);
  // happy path: new page, write, then read back
  test_fail_if (fpgr_extend (pager, 2, &e));

  // Write 0 : PAGE_SIZE to each byte (overflow fine, it's just data)
  for (u32 i = 0; i < PAGE_SIZE; i++)
    {
      _page[i] = (u8)i;
    }
  // Write it out
  test_fail_if (fpgr_write (pager, _page, 1, &e));

  // Scramble page so we can read it back in
  memset (_page, 0xFF, PAGE_SIZE);
  test_fail_if (fpgr_read (pager, _page, 1, &e));

  // Iterate and check that it matches what we expect
  for (u32 i = 0; i < PAGE_SIZE; i++)
    {
      test_assert_int_equal (_page[i], (u8)i);
    }

  // There's 2 refs to this file, close the other one
  test_fail_if (fpgr_close (pager, &e));
  test_fail_if (i_close (&fp, &e));
  test_fail_if (i_unlink ("test.db", &e));
}
#endif

err_t
fpgr_crash (struct file_pager *p, error *e)
{
  DBG_ASSERT (file_pager, p);
  i_close (&p->f, e);
  i_free (p);
  return error_trace (e);
}
