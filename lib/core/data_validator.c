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

#include "core/data_validator.h"

#include "core/error.h"
#include "core/math.h"
#include "core/random.h"
#include "intf/os/memory.h"
#include "numstore/errors.h"

#include <string.h>

static err_t
dvalidtr_light_validate (const struct dvalidtr *d, error *e)
{
  const i64 sut_len = d->sut.functions.getlen (d->sut.ctx, e);
  if (sut_len < 0)
    {
      return error_trace (e);
    }

  const i64 ref_len = d->ref.functions.getlen (d->ref.ctx, e);
  if (ref_len < 0)
    {
      return error_trace (e);
    }

  if (sut_len != ref_len)
    {
      return error_causef (e, ERR_CORRUPT,
                           "length mismatch: ref=%" PRb_size " sut=%" PRb_size
                           " (delta=%" PRb_size ")",
                           ref_len, sut_len, (i64)sut_len - (i64)ref_len);
    }

  return SUCCESS;
}

static err_t
dvalidtr_read (const struct dvalidtr *d, const struct stride str, const u32 size, void *_dest,
               error *e)
{
  void *ref = i_malloc (str.nelems, size, e);
  void *dest = _dest;

  if (ref == NULL)
    {
      goto theend;
    }

  if (_dest == NULL)
    {
      dest = i_malloc (str.nelems, size, e);
      if (dest == NULL)
        {
          goto theend;
        }
    }

  // Read from the ref
  const i64 ref_read = d->ref.functions.read (d->ref.ctx, str, size, ref, e);
  if (ref_read < 0)
    {
      error_causef (e, ERR_CORRUPT,
                    "ref read failed (start=%" PRb_size " stride=%" PRb_size
                    " nelems=%" PRb_size " size=%u)",
                    str.start, str.stride, str.nelems, size);
      goto theend;
    }

  // Read from the system under test
  const i64 sut_read = d->sut.functions.read (d->sut.ctx, str, size, dest, e);
  if (sut_read < 0)
    {
      error_causef (e, ERR_CORRUPT,
                    "sut read failed (start=%" PRb_size " stride=%" PRb_size
                    " nelems=%" PRb_size " size=%u)",
                    str.start, str.stride, str.nelems, size);
      goto theend;
    }

  if (ref_read != sut_read)
    {
      error_causef (e, ERR_CORRUPT,
                    "read count mismatch: ref=%" PRb_size " sut=%" PRb_size
                    " (start=%" PRb_size " stride=%" PRb_size
                    " nelems=%" PRb_size " size=%u)",
                    ref_read, sut_read, str.start, str.stride, str.nelems,
                    size);
      goto theend;
    }

  if (memcmp (dest, ref, (u64)sut_read * size) != 0)
    {
      error_causef (e, ERR_CORRUPT,
                    "read data mismatch: ref/sut diverged"
                    " (start=%" PRb_size " stride=%" PRb_size
                    " nelems=%" PRb_size " size=%u nread=%" PRb_size ")",
                    str.start, str.stride, str.nelems, size, sut_read);
      goto theend;
    }

theend:
  if (ref)
    {
      i_free (ref);
    }
  if (_dest == NULL && dest)
    {
      i_free (dest);
    }
  return error_trace (e);
}

static err_t
dvalidtr_insert (struct dvalidtr *d, const u32 ofst, const void *_src, const u32 slen,
                 error *e)
{
  u8 *src = (u8 *)_src;
  if (_src == NULL)
    {
      src = i_malloc (slen, 1, e);
      if (src == NULL)
        {
          goto theend;
        }
      ptr_range (src, slen);
    }

  // Insert into ref
  const sb_size ref_written
      = d->ref.functions.insert (d->ref.ctx, ofst, src, slen, e);
  if (ref_written < 0)
    {
      error_causef (e, ERR_CORRUPT,
                    "insert into ref failed at byte offset "
                    "%u (slen=%u)",
                    ofst, slen);
      goto theend;
    }

  // Insert into system under test
  const sb_size sut_written
      = d->sut.functions.insert (d->sut.ctx, ofst, src, slen, e);
  if (sut_written < 0)
    {
      error_causef (e, ERR_CORRUPT,
                    "insert into sut failed at byte offset "
                    "%u (slen=%u)",
                    ofst, slen);
      goto theend;
    }

  if (ref_written != sut_written)
    {
      error_causef (e, ERR_CORRUPT,
                    "insert count mismatch: ref=%" PRb_size " sut=%" PRb_size,
                    ref_written, sut_written);
      goto theend;
    }

  // Read back what we just inserted (byte-level, so size=1 and
  // nelems=slen)
  const sb_size sut_read = dvalidtr_read (d,
                                          (struct stride){
                                              .start = ofst,
                                              .nelems = slen,
                                              .stride = 1,
                                          },
                                          1, NULL, e);
  if (sut_read < 0)
    {
      error_causef (e, ERR_CORRUPT,
                    "read-back after insert failed (byte "
                    "offset=%u, slen=%u)",
                    ofst, slen);
      goto theend;
    }

  if (dvalidtr_light_validate (d, e))
    {
      error_causef (e, ERR_CORRUPT,
                    "length check failed after insert at "
                    "byte offset %u (slen=%u)",
                    ofst, slen);
      goto theend;
    }

  if (d->isvalid)
    {
      if (d->isvalid (d->sut.ctx, e))
        {
          goto theend;
        }
    }

theend:
  if (_src == NULL)
    {
      i_free (src);
    }
  return error_trace (e);
}

static err_t
dvalidtr_write (struct dvalidtr *d, const struct stride str, const u32 size,
                const void *_src, error *e)
{
  u8 *src = (u8 *)_src;
  if (_src == NULL)
    {
      src = i_malloc (str.nelems, size, e);
      if (src == NULL)
        {
          goto theend;
        }
      ptr_range (src, str.nelems * size);
    }

  const i64 sut_written = d->sut.functions.write (d->sut.ctx, str, size, src, e);
  if (sut_written < 0)
    {
      error_causef (e, ERR_CORRUPT,
                    "write into sut failed (start=%" PRb_size
                    " stride=%" PRb_size " nelems=%" PRb_size " size=%u)",
                    str.start, str.stride, str.nelems, size);
      goto theend;
    }

  const i64 ref_written = d->ref.functions.write (d->ref.ctx, str, size, src, e);
  if (ref_written < 0)
    {
      error_causef (e, ERR_CORRUPT,
                    "write into ref failed (start=%" PRb_size
                    " stride=%" PRb_size " nelems=%" PRb_size " size=%u)",
                    str.start, str.stride, str.nelems, size);
      goto theend;
    }

  if (ref_written != sut_written)
    {
      error_causef (e, ERR_CORRUPT,
                    "write count mismatch: ref=%" PRb_size " sut=%" PRb_size
                    " (start=%" PRb_size " stride=%" PRb_size
                    " nelems=%" PRb_size " size=%u)",
                    ref_written, sut_written, str.start, str.stride,
                    str.nelems, size);
      goto theend;
    }

  // Read back what we just wrote
  if (dvalidtr_read (d, str, size, NULL, e))
    {
      error_causef (e, ERR_CORRUPT,
                    "read-back after write failed (start=%" PRb_size
                    " stride=%" PRb_size " nelems=%" PRb_size " size=%u)",
                    str.start, str.stride, str.nelems, size);
      goto theend;
    }

  if (d->isvalid)
    {
      if (d->isvalid (d->sut.ctx, e))
        {
          goto theend;
        }
    }

theend:
  if (_src == NULL && src)
    {
      i_free (src);
    }
  return error_trace (e);
}

static err_t
dvalidtr_remove (struct dvalidtr *d, const struct stride str, const u32 size, void *_dest,
                 error *e)
{
  void *ref = i_malloc (str.nelems, size, e);
  void *dest = _dest;

  if (ref == NULL)
    {
      goto theend;
    }

  if (_dest == NULL)
    {
      dest = i_malloc (str.nelems, size, e);
      if (dest == NULL)
        {
          goto theend;
        }
    }

  // Remove from the ref
  const i64 ref_read = d->ref.functions.remove (d->ref.ctx, str, size, ref, e);
  if (ref_read < 0)
    {
      error_causef (e, ERR_CORRUPT,
                    "remove from ref failed (start=%" PRb_size
                    " stride=%" PRb_size " nelems=%" PRb_size " size=%u)",
                    str.start, str.stride, str.nelems, size);
      goto theend;
    }

  // Remove from the system under test
  const i64 sut_read = d->sut.functions.remove (d->sut.ctx, str, size, dest, e);
  if (sut_read < 0)
    {
      error_causef (e, ERR_CORRUPT,
                    "remove from sut failed (start=%" PRb_size
                    " stride=%" PRb_size " nelems=%" PRb_size " size=%u)",
                    str.start, str.stride, str.nelems, size);
      goto theend;
    }

  if (ref_read != sut_read)
    {
      error_causef (e, ERR_CORRUPT,
                    "remove count mismatch: ref=%" PRb_size " sut=%" PRb_size
                    " (start=%" PRb_size " stride=%" PRb_size
                    " nelems=%" PRb_size " size=%u)",
                    ref_read, sut_read, str.start, str.stride, str.nelems,
                    size);
      goto theend;
    }

  if (memcmp (dest, ref, (u64)sut_read * size) != 0)
    {
      error_causef (e, ERR_CORRUPT,
                    "remove data mismatch: ref/sut diverged"
                    " (start=%" PRb_size " stride=%" PRb_size
                    " nelems=%" PRb_size " size=%u nremoved=%" PRb_size ")",
                    str.start, str.stride, str.nelems, size, sut_read);
      goto theend;
    }

  if (dvalidtr_light_validate (d, e))
    {
      error_causef (e, ERR_CORRUPT,
                    "length check failed after remove "
                    "(start=%" PRb_size "stride=%" PRb_size
                    " nelems=%" PRb_size " size=%u)",
                    str.start, str.stride, str.nelems, size);
      goto theend;
    }

  if (d->isvalid)
    {
      if (d->isvalid (d->sut.ctx, e))
        {
          goto theend;
        }
    }

theend:
  if (ref)
    {
      i_free (ref);
    }
  if (_dest == NULL && dest)
    {
      i_free (dest);
    }
  return error_trace (e);
}

static err_t
dvalidtr_getlen (const struct dvalidtr *d, error *e)
{
  return d->ref.functions.getlen (d->ref.ctx, e);
}

static err_t
dvalidtr_validate (struct dvalidtr *d, error *e)
{
  if (dvalidtr_light_validate (d, e))
    {
      return error_trace (e);
    }

  const i64 len = d->ref.functions.getlen (d->ref.ctx, e);
  if (len < 0)
    {
      return error_trace (e);
    }

  if (dvalidtr_read (d,
                     (struct stride){
                         .start = 0,
                         .stride = 1,
                         .nelems = len,
                     },
                     1, NULL, e))
    {
      error_causef (e, ERR_CORRUPT,
                    "full read validation failed (len=%" PRb_size ")", len);
      return error_trace (e);
    }

  return SUCCESS;
}

err_t
dvalidtr_random_test (struct dvalidtr *d, const u32 size, const u32 niters, const u64 max_insert,
                      error *e)
{

  for (u32 k = 0; k < niters; ++k)
    {
      i64 len = dvalidtr_getlen (d, e);
      if (len < 0)
        {
          return error_trace (e);
        }
      len /= size;

      enum
      {
        C_INSERT,
        C_READ,
        C_REMOVE,
        C_WRITE,
      } choice = randu32r (0, 3);
      if (len == 0)
        {
          choice = C_INSERT;
        }

      const b_size start = len > 0 ? randu32r (0, (u32)len - 1) : 0;
      const b_size stride = randu32r (1, 8);
      const u32 max_n = len > 0 ? ((u32)len - 1 - start) / stride + 1 : 0;
      const b_size nelems = max_n > 0 ? randu32r (1, MIN (max_n, max_insert)) : 0;
      const u32 ninsert = randu32r (1, (u32)max_insert);

      const struct stride str = {
        .start = start,
        .stride = stride,
        .nelems = nelems,
      };

      switch (choice)
        {
        case C_INSERT:
          {
            if (dvalidtr_insert (d, start * size, NULL, ninsert * size, e))
              {
                return error_causef (e, ERR_CORRUPT,
                                     "random "
                                     "test "
                                     "failed "
                                     "on iter "
                                     "%u: "
                                     "insert "
                                     "(len="
                                     "%" PRb_size " start="
                                     "%" PRb_size " ninsert="
                                     "%u "
                                     "size=%u)",
                                     k, len, start, ninsert, size);
              }
            break;
          }
        case C_READ:
          {
            if (dvalidtr_read (d, str, size, NULL, e))
              {
                return error_causef (e, ERR_CORRUPT,
                                     "random "
                                     "test "
                                     "failed "
                                     "on iter "
                                     "%u: read "
                                     "(len="
                                     "%" PRb_size " start="
                                     "%" PRb_size " stride="
                                     "%" PRb_size " nelems="
                                     "%" PRb_size " size=%"
                                     "u)",
                                     k, len, start, stride, nelems, size);
              }
            break;
          }
        case C_REMOVE:
          {
            if (dvalidtr_remove (d, str, size, NULL, e))
              {
                return error_causef (e, ERR_CORRUPT,
                                     "random "
                                     "test "
                                     "failed "
                                     "on iter "
                                     "%u: "
                                     "remove "
                                     "(len="
                                     "%" PRb_size " start="
                                     "%" PRb_size " stride="
                                     "%" PRb_size " nelems="
                                     "%" PRb_size " size=%"
                                     "u)",
                                     k, len, start, stride, nelems, size);
              }
            break;
          }
        case C_WRITE:
          {
            if (dvalidtr_write (d, str, size, NULL, e))
              {
                return error_causef (e, ERR_CORRUPT,
                                     "random "
                                     "test "
                                     "failed "
                                     "on iter "
                                     "%u: "
                                     "write "
                                     "(len="
                                     "%" PRb_size " start="
                                     "%" PRb_size " stride="
                                     "%" PRb_size " nelems="
                                     "%" PRb_size " size=%"
                                     "u)",
                                     k, len, start, stride, nelems, size);
              }
            break;
          }
        }
    }

  if (dvalidtr_validate (d, e))
    {
      return error_causef (e, ERR_CORRUPT,
                           "random test failed to validate data at the end");
    }

  return SUCCESS;
}
