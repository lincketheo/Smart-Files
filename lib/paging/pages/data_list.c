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

#include "paging/pages/data_list.h"

#include "core/assert.h"
#include "core/cbuffer.h"
#include "core/error.h"
#include "core/math.h"
#include "core/random.h"
#include "intf/logging.h"
#include "numstore/types.h"
#include "paging/pages/page.h"
#include "test/testing.h"

// numstore
// Initialization

err_t
dl_validate_for_db (const page *d, error *e)
{
  DBG_ASSERT (data_list, d);

  const enum page_type type = page_get_type (d);

  if (type != (u8)PG_DATA_LIST)
    {
      return error_causef (e, ERR_CORRUPT,
                           "expected header: %" PRpgh " but got: %" PRpgh,
                           (pgh)PG_DATA_LIST, (pgh)type);
    }

  const p_size used = dl_used (d);

  if (used > DL_DATA_SIZE)
    {
      return error_causef (e, ERR_CORRUPT,
                           "used %" PRp_size " > max %" PRp_size
                           " (page_size=%" PRp_size ")",
                           used, DL_DATA_SIZE, PAGE_SIZE);
    }

  // Root check
  if (dl_is_root (d))
    {
      if (dl_used (d) == 0)
        {
          return error_causef (e, ERR_CORRUPT,
                               "Root node must have at least 1 "
                               "element");
        }
    }
  else
    {
      if (dl_used (d) < DL_DATA_SIZE / 2)
        {
          return error_causef (e, ERR_CORRUPT,
                               "non-root data list node below "
                               "half capacity");
        }
    }

  return SUCCESS;
}

#ifndef NTEST
TEST (TT_UNIT, dl_validate)
{
  page sut;
  error e = error_create ();

  // Initialized page is base valid
  TEST_CASE ("Initialized page base - no changes is valid")
  {
    page_init_empty (&sut, PG_DATA_LIST);
    test_err_t_check (dl_validate_for_db (&sut, &e), ERR_CORRUPT, &e);
  }

  // Corrupt page header
  TEST_CASE ("Corrupt page header")
  {
    page_init_empty (&sut, PG_DATA_LIST);
    page_set_type (&sut, PG_INNER_NODE);
    test_err_t_check (dl_validate_for_db (&sut, &e), ERR_CORRUPT, &e);
  }

  TEST_CASE ("Root page size checks")
  {
    page_init_empty (&sut, PG_DATA_LIST);

    // Hack the page size
    // Can't do this b/c of ASSERT - but it's still possible on
    // corruption dl_set_used(&sut, DL_DATA_SIZE + 1);
    p_size size = DL_DATA_SIZE + 1;
    PAGE_SIMPLE_SET_IMPL (&sut, size, DL_BLEN_OFST);
    test_err_t_check (dl_validate_for_db (&sut, &e), ERR_CORRUPT, &e);

    // Max size is valid
    dl_set_used (&sut, DL_DATA_SIZE);
    test_err_t_check (dl_validate_for_db (&sut, &e), SUCCESS, &e);

    // 0 size is invalid
    dl_set_used (&sut, 0);
    test_err_t_check (dl_validate_for_db (&sut, &e), ERR_CORRUPT, &e);

    // in between size is valid
    dl_set_used (&sut, DL_DATA_SIZE / 2);
    test_err_t_check (dl_validate_for_db (&sut, &e), SUCCESS, &e);

    // in between size is valid for root
    dl_set_used (&sut, DL_DATA_SIZE / 2 - 1);
    test_err_t_check (dl_validate_for_db (&sut, &e), SUCCESS, &e);
  }

  TEST_CASE ("Non root page size checks")
  {
    page_init_empty (&sut, PG_DATA_LIST);
    dl_set_next (&sut, 5);

    // Hack the page size
    // Can't do this b/c of ASSERT - but it's still possible on
    // corruption dl_set_used(&sut, DL_DATA_SIZE + 1);
    p_size size = DL_DATA_SIZE + 1;
    PAGE_SIMPLE_SET_IMPL (&sut, size, DL_BLEN_OFST);
    test_err_t_check (dl_validate_for_db (&sut, &e), ERR_CORRUPT, &e);

    // Max size is valid
    dl_set_used (&sut, DL_DATA_SIZE);
    test_err_t_check (dl_validate_for_db (&sut, &e), SUCCESS, &e);

    // 0 size is invalid
    dl_set_used (&sut, 0);
    test_err_t_check (dl_validate_for_db (&sut, &e), ERR_CORRUPT, &e);

    // in between size is valid
    dl_set_used (&sut, DL_DATA_SIZE / 2);
    test_err_t_check (dl_validate_for_db (&sut, &e), SUCCESS, &e);

    // in between size is valid for root
    dl_set_used (&sut, DL_DATA_SIZE / 2 - 1);
    test_err_t_check (dl_validate_for_db (&sut, &e), ERR_CORRUPT, &e);
  }
}
#endif

// Getters

#ifndef NTEST
TEST (TT_UNIT, dl_set_get)
{
  page p;
  rand_bytes (p.raw, PAGE_SIZE);

  // Randomize array to pretend random

  // Start
  TEST_CASE ("Initial page values")
  {
    page_init_empty (&p, PG_DATA_LIST);
    test_assert_type_equal (dl_get_next (&p), PGNO_NULL, pgno, PRpgno);
    test_assert_type_equal (dl_get_prev (&p), PGNO_NULL, pgno, PRpgno);
    test_assert_int_equal (dl_used (&p), 0);
    test_assert_int_equal (dl_avail (&p), DL_DATA_SIZE);
  }

  // Set / Get
  TEST_CASE ("Getters / Setters work")
  {
    page_init_empty (&p, PG_DATA_LIST);
    dl_set_next (&p, 11);
    dl_set_prev (&p, 12);
    dl_set_used (&p, 13);
    dl_set_byte (&p, 10, 5);

    test_assert_type_equal (dl_get_next (&p), 11, pgno, PRpgno);
    test_assert_type_equal (dl_get_prev (&p), 12, pgno, PRpgno);
    test_assert_int_equal (dl_used (&p), 13);
    test_assert_int_equal (dl_avail (&p), DL_DATA_SIZE - 13);
    test_assert_int_equal (dl_get_byte (&p, 10), 5);
  }
}
#endif

p_size
dl_read (const page *d, u8 *dest, const p_size offset, const p_size nbytes)
{
  DBG_ASSERT (data_list, d);
  ASSERT (nbytes > 0);

  const p_size dlen = dl_used (d);
  const u8 *base = dl_get_data (d);

  ASSERT (offset <= dlen);

  if (offset == dlen)
    {
      return 0;
    }

  const p_size avail = dlen - offset;
  const p_size toread = MIN (avail, nbytes);

  if (toread > 0 && dest)
    {
      memcpy (dest, base + offset, toread);
    }

  return toread;
}

#ifndef NTEST
TEST (TT_UNIT, dl_read)
{
  page p;
  u8 buf[DL_DATA_SIZE];
  u8 src[DL_DATA_SIZE];

  TEST_CASE ("Read on an empty page")
  {
    rand_bytes (p.raw, PAGE_SIZE);
    page_init_empty (&p, PG_DATA_LIST);

    test_assert_int_equal (dl_read (&p, buf, 0, 1), 0);
    test_assert_int_equal (dl_read (&p, buf, 0, DL_DATA_SIZE), 0);
  }

  TEST_CASE ("Random data")
  {
    rand_bytes (src, DL_DATA_SIZE);

    page_init_empty (&p, PG_DATA_LIST);
    memcpy (dl_get_data (&p), src, DL_DATA_SIZE);
    dl_set_used (&p, DL_DATA_SIZE);
  }
  TEST_CASE ("full read")
  {
    memset (buf, 0, sizeof buf);
    const p_size got = dl_read (&p, buf, 0, DL_DATA_SIZE);
    test_assert_int_equal (got, DL_DATA_SIZE);
    test_assert_memequal (buf, src, DL_DATA_SIZE);
  }

  TEST_CASE ("partial read (first 16 bytes)")
  {
    memset (buf, 0, sizeof buf);
    const p_size got = dl_read (&p, buf, 0, 16);
    test_assert_int_equal (got, 16);
    test_assert_memequal (buf, src, 16);
  }

  TEST_CASE ("offset into middle")
  {
    const p_size off = 10;
    const p_size n = 20;
    memset (buf, 0, sizeof buf);
    const p_size got = dl_read (&p, buf, off, n);
    test_assert_int_equal (got, n);
    test_assert_memequal (buf, src + off, n);
  }

  TEST_CASE ("request past end")
  {
    const p_size off = DL_DATA_SIZE - 8;
    memset (buf, 0, sizeof buf);
    const p_size got = dl_read (&p, buf, off, 32);
    test_assert_int_equal (got, 8);
    test_assert_memequal (buf, src + off, 8);
  }

  TEST_CASE ("offset == used (should return 0)")
  {
    const p_size off = DL_DATA_SIZE;
    const p_size got = dl_read (&p, buf, off, 1);
    test_assert_int_equal (got, 0);
  }
}
#endif

p_size
dl_read_into_cbuffer (const page *d, struct cbuffer *dest, const p_size offset,
                      const p_size b)
{
  ASSERT (b > 0);

  const p_size dlen = dl_used (d);
  const u8 *base = dl_get_data (d);

  ASSERT (offset <= dlen);

  if (offset == dlen)
    {
      return 0;
    }

  const p_size avail = dlen - offset;
  p_size toread = MIN (avail, b);

  if (toread > 0 && dest)
    {
      toread = cbuffer_write (base + offset, 1, toread, dest);
    }

  return toread;
}

p_size
dl_read_out_into_cbuffer (page *d, struct cbuffer *dest, const p_size offset,
                          const p_size b)
{
  ASSERT (b > 0);

  const p_size dlen = dl_used (d);
  u8 *base = dl_get_data (d);

  ASSERT (offset <= dlen);

  if (offset == dlen)
    {
      return 0;
    }

  const p_size avail = dlen - offset;
  p_size toread = MIN (avail, b);

  if (toread > 0 && dest)
    {
      toread = cbuffer_write (base + offset, 1, toread, dest);
    }

  // [ ++++++++++ __________ ++++++ ]
  //            ^           ^
  //            ofst     ofst + toread
  //                        [       ] = dlen - (ofst + toread)
  const p_size remain = dlen - toread - offset;
  memmove (base + offset, base + offset + toread, remain);
  dl_set_used (d, offset + remain);

  return toread;
}

void
dl_read_expect (const page *d, u8 *dest, const p_size offset, const p_size nbytes)
{
  const p_size read = dl_read (d, dest, offset, nbytes);
  ASSERT (read == nbytes);
}

p_size
dl_read_out_from (page *d, u8 *dest, const p_size offset)
{
  DBG_ASSERT (data_list, d);
  ASSERT (dest);

  p_size dlen = dl_used (d);

  ASSERT (offset <= dlen);

  if (offset == dlen)
    {
      return 0;
    }

  const u8 *head = dl_get_data (d);

  head += offset;
  dlen -= offset;

  if (dlen > 0)
    {
      memcpy (dest, head, dlen);
      dl_set_used (d, dl_used (d) - dlen);
    }

  return dlen;
}

#ifndef NTEST
TEST (TT_UNIT, dl_read_out_from)
{
  page dl;
  page_init_empty (&dl, PG_DATA_LIST);

  u8 dest[DL_DATA_SIZE];
  u8 alldata[DL_DATA_SIZE];
  u8 somedata[DL_DATA_SIZE / 2];
  arr_range (alldata);
  arr_range (somedata);

  TEST_CASE ("Empty read")
  {
    test_assert_int_equal (dl_used (&dl), 0);
    p_size ret = dl_read_out_from (&dl, dest, 0);
    test_assert_int_equal (ret, 0);
    test_assert_int_equal (dl_used (&dl), 0);
  }

  TEST_CASE ("Read all from 0")
  {
    dl_append (&dl, somedata, DL_DATA_SIZE / 2);
    p_size ret = dl_read_out_from (&dl, dest, 0);
    test_assert_int_equal (ret, DL_DATA_SIZE / 2);
    test_assert_int_equal (dl_used (&dl), 0);

    for (p_size i = 0; i < DL_DATA_SIZE / 2; ++i)
      {
        test_assert_int_equal (dest[i], somedata[i]);
      }
    dl_set_used (&dl, 0);
  }

  TEST_CASE ("Read some from middle")
  {
    _Static_assert (DL_DATA_SIZE / 2 > 1,
                    "DL_DATA_SIZE is too small. Increase page size");

    dl_append (&dl, somedata, DL_DATA_SIZE / 2);
    p_size ret = dl_read_out_from (&dl, dest, 1);
    test_assert_int_equal (ret, DL_DATA_SIZE / 2 - 1);
    test_assert_int_equal (dl_used (&dl), 1);

    for (p_size i = 0; i < DL_DATA_SIZE / 2 - 1; ++i)
      {
        test_assert_int_equal (dest[i], somedata[i + 1]);
      }
    for (p_size i = 0; i < 1; ++i)
      {
        test_assert_int_equal (dl_get_byte (&dl, i), i);
      }
    dl_set_used (&dl, 0);
  }

  TEST_CASE ("Read some later in the middle")
  {
    _Static_assert (DL_DATA_SIZE / 2 > 10,
                    "DL_DATA_SIZE is too small. Increase page size");

    dl_append (&dl, somedata, DL_DATA_SIZE / 2);
    p_size ret = dl_read_out_from (&dl, dest, 10);
    test_assert_int_equal (ret, DL_DATA_SIZE / 2 - 10);
    test_assert_int_equal (dl_used (&dl), 10);

    for (p_size i = 0; i < DL_DATA_SIZE / 2 - 10; ++i)
      {
        test_assert_int_equal (dest[i], somedata[i + 10]);
      }
    for (p_size i = 0; i < 10; ++i)
      {
        test_assert_int_equal (dl_get_byte (&dl, i), i);
      }
    dl_set_used (&dl, 0);
  }

  TEST_CASE ("Read some from the end")
  {
    _Static_assert (DL_DATA_SIZE / 2 > 10,
                    "DL_DATA_SIZE is too small. Increase page size");

    dl_append (&dl, somedata, DL_DATA_SIZE / 2);
    p_size ret = dl_read_out_from (&dl, dest, DL_DATA_SIZE / 2);
    test_assert_int_equal (ret, 0);
    test_assert_int_equal (dl_used (&dl), DL_DATA_SIZE / 2);

    for (p_size i = 0; i < DL_DATA_SIZE / 2; ++i)
      {
        test_assert_int_equal (dl_get_byte (&dl, i), somedata[i]);
      }
    dl_set_used (&dl, 0);
  }

  TEST_CASE ("Read full middle")
  {
    _Static_assert (DL_DATA_SIZE > 1,
                    "DL_DATA_SIZE is too small. Increase page size");

    dl_append (&dl, alldata, DL_DATA_SIZE);
    p_size ret = dl_read_out_from (&dl, dest, 1);
    test_assert_int_equal (ret, DL_DATA_SIZE - 1);
    test_assert_int_equal (dl_used (&dl), 1);

    for (p_size i = 0; i < DL_DATA_SIZE - 1; ++i)
      {
        test_assert_int_equal (dest[i], alldata[i + 1]);
      }
    for (p_size i = 0; i < 1; ++i)
      {
        test_assert_int_equal (dl_get_byte (&dl, i), alldata[i]);
      }
    dl_set_used (&dl, 0);
  }

  TEST_CASE ("Read full later middle")
  {
    _Static_assert (DL_DATA_SIZE > 10,
                    "DL_DATA_SIZE is too small. Increase page size");

    dl_append (&dl, alldata, DL_DATA_SIZE);
    p_size ret = dl_read_out_from (&dl, dest, 10);
    test_assert_int_equal (ret, DL_DATA_SIZE - 10);
    test_assert_int_equal (dl_used (&dl), 10);

    for (p_size i = 0; i < DL_DATA_SIZE - 10; ++i)
      {
        test_assert_int_equal (dest[i], alldata[i + 10]);
      }
    for (p_size i = 0; i < 10; ++i)
      {
        test_assert_int_equal (dl_get_byte (&dl, i), alldata[i]);
      }
    dl_set_used (&dl, 0);
  }

  TEST_CASE ("Read full end")
  {
    dl_append (&dl, alldata, DL_DATA_SIZE);
    p_size ret = dl_read_out_from (&dl, dest, DL_DATA_SIZE);
    test_assert_int_equal (ret, 0);
    test_assert_int_equal (dl_used (&dl), DL_DATA_SIZE);

    for (p_size i = 0; i < DL_DATA_SIZE; ++i)
      {
        test_assert_int_equal (dl_get_byte (&dl, i), alldata[i]);
      }
    dl_set_used (&dl, 0);
  }
}
#endif

p_size
dl_append (page *d, const u8 *src, const p_size nbytes)
{
  DBG_ASSERT (data_list, d);
  ASSERT (src);
  ASSERT (nbytes > 0);

  const p_size avail = DL_DATA_SIZE - dl_used (d);
  const p_size next = MIN (avail, nbytes);

  u8 *data = dl_get_data (d);

  if (next > 0)
    {
      u8 *tail = data + dl_used (d);

      memcpy (tail, src, next);

      dl_set_used (d, dl_used (d) + next);
    }

  DBG_ASSERT (data_list, d);
  return next;
}

void
dl_append_from_cbuffer (page *d, struct cbuffer *src, const p_size amnt)
{
  DBG_ASSERT (data_list, d);
  ASSERT (amnt > 0);
  ASSERT (src);
  ASSERT (amnt <= DL_DATA_SIZE - dl_used (d));
  ASSERT (amnt <= cbuffer_len (src));

  u8 *data = dl_get_data (d);
  u8 *tail = data + dl_used (d);
  cbuffer_read_expect (tail, 1, amnt, src);
  dl_set_used (d, dl_used (d) + amnt);

  DBG_ASSERT (data_list, d);
}

#ifndef NTEST
TEST (TT_UNIT, dl_append)
{
  page p;
  u8 src[DL_DATA_SIZE];

  rand_bytes (src, sizeof src);

  TEST_CASE ("empty page, append small data")
  {
    rand_bytes (p.raw, PAGE_SIZE);
    page_init_empty (&p, PG_DATA_LIST);

    const p_size n = 16;
    const p_size got = dl_append (&p, src, n);
    test_assert_int_equal (got, n);
    test_assert_int_equal (dl_used (&p), n);
    test_assert_memequal (dl_get_data (&p), src, n);
  }

  TEST_CASE ("multiple appends accumulate")
  {
    rand_bytes (p.raw, PAGE_SIZE);
    page_init_empty (&p, PG_DATA_LIST);

    const p_size n1 = 8;
    const p_size n2 = 12;

    const p_size got1 = dl_append (&p, src, n1);
    const p_size got2 = dl_append (&p, src + n1, n2);

    test_assert_int_equal (got1, n1);
    test_assert_int_equal (got2, n2);
    test_assert_int_equal (dl_used (&p), n1 + n2);
    test_assert_memequal (dl_get_data (&p), src, n1 + n2);
  }

  TEST_CASE ("append more than available → clipped")
  {
    rand_bytes (p.raw, PAGE_SIZE);
    page_init_empty (&p, PG_DATA_LIST);

    const p_size big = DL_DATA_SIZE + 100;
    const p_size got = dl_append (&p, src, big);

    test_assert_int_equal (got, DL_DATA_SIZE);
    test_assert_int_equal (dl_used (&p), DL_DATA_SIZE);
    test_assert_memequal (dl_get_data (&p), src, DL_DATA_SIZE);
  }

  TEST_CASE ("no space left → append returns 0")
  {
    rand_bytes (p.raw, PAGE_SIZE);
    page_init_empty (&p, PG_DATA_LIST);

    // fill page fully
    dl_set_used (&p, DL_DATA_SIZE);

    const p_size got = dl_append (&p, src, 32);
    test_assert_int_equal (got, 0);
    test_assert_int_equal (dl_used (&p), DL_DATA_SIZE);
  }
}
#endif

p_size
dl_write (const page *d, const u8 *src, const p_size offset, const p_size nbytes)
{
  DBG_ASSERT (data_list, d);
  ASSERT (nbytes > 0);

  const p_size dlen = dl_used (d);
  u8 *base = dl_get_data (d);

  ASSERT (offset <= dlen);

  if (offset == dlen)
    {
      return 0;
    }

  const p_size avail = dlen - offset;
  const p_size toread = MIN (avail, nbytes);

  if (toread > 0 && src)
    {
      memcpy (base + offset, src, toread);
    }

  DBG_ASSERT (data_list, d);
  return toread;
}

#ifndef NTEST
TEST (TT_UNIT, dl_write)
{
  page p;
  u8 src[DL_DATA_SIZE];

  rand_bytes (src, sizeof src);

  // Fill page fully with known data
  {
    rand_bytes (p.raw, PAGE_SIZE);
    page_init_empty (&p, PG_DATA_LIST);

    memcpy (dl_get_data (&p), src, DL_DATA_SIZE);
    dl_set_used (&p, DL_DATA_SIZE);
  }

  TEST_CASE ("overwrite beginning")
  {
    u8 newdata[8];
    rand_bytes (newdata, sizeof newdata);

    const p_size got = dl_write (&p, newdata, 0, sizeof newdata);
    test_assert_int_equal (got, sizeof newdata);
    test_assert_memequal (dl_get_data (&p), newdata, sizeof newdata);
    test_assert_memequal ((u8 *)dl_get_data (&p) + sizeof newdata,
                          src + sizeof newdata, DL_DATA_SIZE - sizeof newdata);
  }

  TEST_CASE ("overwrite middle")
  {
    u8 newdata[16];
    rand_bytes (newdata, sizeof newdata);

    const p_size off = 32;
    const p_size got = dl_write (&p, newdata, off, sizeof newdata);
    test_assert_int_equal (got, sizeof newdata);
    test_assert_memequal ((u8 *)dl_get_data (&p) + off, newdata,
                          sizeof newdata);
  }

  TEST_CASE ("overwrite near end, clipped")
  {
    u8 newdata[32];
    rand_bytes (newdata, sizeof newdata);

    const p_size off = DL_DATA_SIZE - 10;
    const p_size got = dl_write (&p, newdata, off, sizeof newdata);
    test_assert_int_equal (got, 10); // only 10 bytes available
    test_assert_memequal ((u8 *)dl_get_data (&p) + off, newdata, got);
  }

  TEST_CASE ("offset == used → no write")
  {
    u8 newdata[8];
    rand_bytes (newdata, sizeof newdata);

    const p_size off = DL_DATA_SIZE;
    const p_size got = dl_write (&p, newdata, off, sizeof newdata);
    test_assert_int_equal (got, 0);
  }
}
#endif

p_size
dl_write_from_buffer (const page *d, struct cbuffer *src, const p_size offset,
                      const p_size nbytes)
{
  ASSERT (nbytes > 0);

  const p_size dlen = dl_used (d);
  u8 *base = dl_get_data (d);

  ASSERT (offset <= dlen);

  if (offset == dlen)
    {
      return 0;
    }

  const p_size avail = dlen - offset;
  p_size toread = MIN (avail, nbytes);

  if (toread > 0 && src)
    {
      toread = cbuffer_read (base + offset, 1, toread, src);
    }

  return toread;
}

p_size
dl_memset_from_buffer (page *d, struct cbuffer *src, const p_size nbytes)
{
  ASSERT (nbytes > 0);
  ASSERT (nbytes < DL_DATA_SIZE);

  const p_size read = cbuffer_read (dl_get_data (d), 1, nbytes, src);
  dl_set_used (d, read);

  return read;
}

void
dl_memset_from_buffer_expect (page *d, struct cbuffer *src, const p_size nbytes)
{
  const p_size read = dl_memset_from_buffer (d, src, nbytes);
  ASSERT (read == nbytes);
}

void
dl_memset (page *d, const u8 *buf, const p_size len)
{
  DBG_ASSERT (data_list, d);
  ASSERT (len <= DL_DATA_SIZE);

  memmove (dl_get_data (d), buf, len);
  dl_set_used (d, len);
}

#ifndef NTEST
TEST (TT_UNIT, dl_memset)
{
  page p;
  u8 buf1[64];
  u8 buf2[128];

  rand_bytes (buf1, sizeof buf1);
  rand_bytes (buf2, sizeof buf2);

  TEST_CASE ("basic fill")
  {
    rand_bytes (p.raw, PAGE_SIZE);
    page_init_empty (&p, PG_DATA_LIST);

    dl_memset (&p, buf1, sizeof buf1);

    test_assert_int_equal (dl_used (&p), sizeof buf1);
    test_assert_memequal (dl_get_data (&p), buf1, sizeof buf1);
  }

  TEST_CASE ("overwrite existing data")
  {
    dl_memset (&p, buf2, sizeof buf2);

    test_assert_int_equal (dl_used (&p), sizeof buf2);
    test_assert_memequal (dl_get_data (&p), buf2, sizeof buf2);
  }

  TEST_CASE ("len = 0 (valid, empties page)")
  {
    dl_memset (&p, buf1, 0);

    test_assert_int_equal (dl_used (&p), 0);
  }

  TEST_CASE ("len = DL_DATA_SIZE (fills entire available space)")
  {
    u8 full[DL_DATA_SIZE];
    rand_bytes (full, sizeof full);

    dl_memset (&p, full, DL_DATA_SIZE);

    test_assert_int_equal (dl_used (&p), DL_DATA_SIZE);
    test_assert_memequal (dl_get_data (&p), full, DL_DATA_SIZE);
  }
}
#endif

void
dl_set_data (page *p, const struct dl_data d)
{
  ASSERT (d.blen <= DL_DATA_SIZE);
  dl_memset (p, d.data, d.blen);
}

// Data Movement
void
dl_move_left (page *dest, page *src, const p_size len)
{
  DBG_ASSERT (data_list, dest);
  DBG_ASSERT (data_list, src);

  ASSERT (len <= dl_avail (dest));

  i_log_trace ("%" PRp_size " %" PRp_size "\n", dl_used (src), dl_used (dest));

  const p_size actual = MIN (len, dl_used (src));
  if (actual > 0)
    {
      dl_append (dest, dl_get_data (src), actual);
      dl_memset (src, (u8 *)dl_get_data (src) + actual,
                 dl_used (src) - actual);
    }

  i_log_trace ("%" PRp_size " %" PRp_size "\n", dl_used (src), dl_used (dest));
}

#ifndef NTEST
TEST (TT_UNIT, dl_move_left)
{
  page src, dest;
  u8 srcbuf[64];
  u8 destbuf[64];

  rand_bytes (srcbuf, sizeof srcbuf);
  rand_bytes (destbuf, sizeof destbuf);

  TEST_CASE ("simple move")
  {
    rand_bytes (src.raw, PAGE_SIZE);
    rand_bytes (dest.raw, PAGE_SIZE);

    page_init_empty (&src, PG_DATA_LIST);
    page_init_empty (&dest, PG_DATA_LIST);

    dl_memset (&src, srcbuf, sizeof srcbuf);

    const p_size orig_used_src = dl_used (&src);
    const p_size orig_used_dest = dl_used (&dest);

    dl_move_left (&dest, &src, 32);

    test_assert_int_equal (dl_used (&dest), orig_used_dest + 32);
    test_assert_int_equal (dl_used (&src), orig_used_src - 32);
    test_assert_memequal (dl_get_data (&dest), srcbuf, 32);
  }

  TEST_CASE ("request more than src has")
  {
    rand_bytes (src.raw, PAGE_SIZE);
    rand_bytes (dest.raw, PAGE_SIZE);

    page_init_empty (&src, PG_DATA_LIST);
    page_init_empty (&dest, PG_DATA_LIST);

    dl_memset (&src, srcbuf, 20);

    const p_size got_src_before = dl_used (&src);
    const p_size got_dest_before = dl_used (&dest);

    dl_move_left (&dest, &src, 64); // only 20 available

    test_assert_int_equal (dl_used (&dest), got_dest_before + 20);
    test_assert_int_equal (dl_used (&src), got_src_before - 20);
  }

  TEST_CASE ("request when src empty")
  {
    rand_bytes (src.raw, PAGE_SIZE);
    rand_bytes (dest.raw, PAGE_SIZE);

    page_init_empty (&src, PG_DATA_LIST);
    page_init_empty (&dest, PG_DATA_LIST);

    dl_move_left (&dest, &src, 10);

    test_assert_int_equal (dl_used (&dest), 0);
    test_assert_int_equal (dl_used (&src), 0);
  }
}
#endif

void
dl_shift_right (page *d, const p_size len)
{
  DBG_ASSERT (data_list, d);
  ASSERT (len <= dl_avail (d));

  const p_size used = dl_used (d);

  // Shift existing bytes to the right
  memmove ((u8 *)dl_get_data (d) + len, dl_get_data (d), used);

  // Account for the new space
  dl_set_used (d, used + len);
}

#ifndef NTEST
TEST (TT_UNIT, dl_shift_right)
{
  page p;
  u8 src[32];
  u8 gapfill[4];

  rand_bytes (src, sizeof src);
  rand_bytes (gapfill, sizeof gapfill);

  TEST_CASE ("basic shift")
  {
    rand_bytes (p.raw, PAGE_SIZE);
    page_init_empty (&p, PG_DATA_LIST);

    // load 32 bytes
    dl_memset (&p, src, sizeof src);

    // shift right by 4
    dl_shift_right (&p, 4);

    // used should increase
    test_assert_int_equal (dl_used (&p), sizeof src + 4);

    // original data should now be at offset 4
    test_assert_memequal ((u8 *)dl_get_data (&p) + 4, src, sizeof src);

    // gap is writable
    memcpy (dl_get_data (&p), gapfill, sizeof gapfill);
    test_assert_memequal (dl_get_data (&p), gapfill, sizeof gapfill);
  }

  TEST_CASE ("zero shift is a no-op")
  {
    rand_bytes (p.raw, PAGE_SIZE);
    page_init_empty (&p, PG_DATA_LIST);

    dl_memset (&p, src, sizeof src);

    const p_size before = dl_used (&p);
    dl_shift_right (&p, 0);

    test_assert_int_equal (dl_used (&p), before);
    test_assert_memequal (dl_get_data (&p), src, sizeof src);
  }

  TEST_CASE ("maximum legal shift")
  {
    rand_bytes (p.raw, PAGE_SIZE);
    page_init_empty (&p, PG_DATA_LIST);

    // fill small data
    dl_memset (&p, src, 8);

    const p_size avail = dl_avail (&p);
    dl_shift_right (&p, avail);

    // should now be completely full
    test_assert_int_equal (dl_used (&p), DL_DATA_SIZE);
    test_assert_memequal ((u8 *)dl_get_data (&p) + avail, src, 8);
  }
}
#endif

void
dl_move_right (page *src, page *dest, const p_size len)
{
  DBG_ASSERT (data_list, dest);
  DBG_ASSERT (data_list, src);

  ASSERT (len <= dl_avail (dest));

  const p_size actual = MIN (len, dl_used (src));
  if (actual == 0)
    {
      return;
    }

  const p_size ofst = dl_used (src) - actual;

  // Make space in dest
  dl_shift_right (dest, actual);

  // Copy the chunk from the *end* of src into the new space in dest
  memcpy (dl_get_data (dest), (u8 *)dl_get_data (src) + ofst, actual);

  // Shrink src
  dl_set_used (src, dl_used (src) - actual);
}

#ifndef NTEST
TEST (TT_UNIT, dl_move_right)
{
  page src, dest;
  u8 srcbuf[64];
  u8 destbuf[32];

  rand_bytes (srcbuf, sizeof srcbuf);
  rand_bytes (destbuf, sizeof destbuf);

  TEST_CASE ("basic move")
  {
    rand_bytes (src.raw, PAGE_SIZE);
    rand_bytes (dest.raw, PAGE_SIZE);

    page_init_empty (&src, PG_DATA_LIST);
    page_init_empty (&dest, PG_DATA_LIST);

    dl_memset (&src, srcbuf, sizeof srcbuf);
    dl_memset (&dest, destbuf, sizeof destbuf);

    const p_size src_before = dl_used (&src);
    const p_size dest_before = dl_used (&dest);

    dl_move_right (&src, &dest, 16);

    // src shrinks, dest grows
    test_assert_int_equal (dl_used (&src), src_before - 16);
    test_assert_int_equal (dl_used (&dest), dest_before + 16);

    // the 16 bytes moved should equal the last 16 of srcbuf
    test_assert_memequal (dl_get_data (&dest), srcbuf + (sizeof srcbuf - 16),
                          16);
  }

  TEST_CASE ("request more than src has → only available moves")
  {
    rand_bytes (src.raw, PAGE_SIZE);
    rand_bytes (dest.raw, PAGE_SIZE);

    page_init_empty (&src, PG_DATA_LIST);
    page_init_empty (&dest, PG_DATA_LIST);

    dl_memset (&src, srcbuf, 20);
    dl_memset (&dest, destbuf, 8);

    const p_size src_before = dl_used (&src);
    const p_size dest_before = dl_used (&dest);

    dl_move_right (&src, &dest, 64); // only 20 available

    test_assert_int_equal (dl_used (&src), 0);
    test_assert_int_equal (dl_used (&dest), dest_before + src_before);

    // check last 20 of srcbuf landed at start of dest
    test_assert_memequal (dl_get_data (&dest), srcbuf, src_before);
  }

  TEST_CASE ("src empty → no-op")
  {
    rand_bytes (src.raw, PAGE_SIZE);
    rand_bytes (dest.raw, PAGE_SIZE);

    page_init_empty (&src, PG_DATA_LIST);
    page_init_empty (&dest, PG_DATA_LIST);

    dl_memset (&dest, destbuf, 12);

    const p_size src_before = dl_used (&src);
    const p_size dest_before = dl_used (&dest);

    dl_move_right (&src, &dest, 10);

    test_assert_int_equal (dl_used (&src), src_before);
    test_assert_int_equal (dl_used (&dest), dest_before);
    test_assert_memequal (dl_get_data (&dest), destbuf, 12);
  }
}
#endif

void
i_log_dl (const int level, const page *d)
{
  i_log (level, "=== DATA LIST PAGE START ===\n");

  i_printf (level, "PGNO: %" PRpgno "\n", d->pg);
  if (dl_get_next (d) == PGNO_NULL)
    {
      i_printf (level, "NEXT: NULL\n");
    }
  else
    {
      i_printf (level, "NEXT: %" PRpgno "\n", dl_get_next (d));
    }
  if (dl_get_prev (d) == PGNO_NULL)
    {
      i_printf (level, "PREV: NULL\n");
    }
  else
    {
      i_printf (level, "PREV: %" PRpgno "\n", dl_get_prev (d));
    }
  i_printf (level, "BLEN: %u\n", dl_used (d));

  i_log (level, "=== DATA LIST PAGE END ===\n");
}

void
dl_make_valid (page *d)
{
  dl_set_used (d, DL_DATA_SIZE);
}
