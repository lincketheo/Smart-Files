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

#include "algorithms/smfile/smfile.h"
#include "c_specx/dev/testing.h"
#include "c_specx/ds/data_writer.h"
#include "c_specx/intf/os/memory.h"
#include "pager.h"
#include "smfile.h"
#include "testing/smfile_test_fixture.h"

static err_t
smfile_insert_func (
    void *ctx,
    u32 ofst,
    const void *src,
    u32 slen,
    error *e)
{
  return smfile_pinsert (ctx, "testing", src, ofst, slen);
}

static i64
smfile_read_func (
    void *ctx,
    struct stride str,
    u32 size,
    void *dest,
    error *e)
{
  return smfile_pread (ctx, "testing", dest, size, str.start * size, str.stride * size, str.nelems);
}

static i64
smfile_write_func (
    void *ctx,
    struct stride str,
    u32 size,
    const void *src,
    error *e)
{
  return smfile_pwrite (ctx, "testing", src, size, str.start * size, str.stride * size, str.nelems);
}

static i64
smfile_remove_func (
    void *ctx,
    struct stride str,
    u32 size,
    void *dest,
    error *e)
{
  return smfile_premove (ctx, "testing", dest, size, str.start * size, str.stride * size, str.nelems);
}

static i64
smfile_get_len_func (void *ctx, error *e)
{
  return smfile_psize (ctx, "testing");
}

static const struct data_writer_functions smfile_functions = {
  .insert = smfile_insert_func,
  .read = smfile_read_func,
  .write = smfile_write_func,
  .remove = smfile_remove_func,
  .getlen = smfile_get_len_func,
};

// Data writer fixture
struct data_writer *
smfile_data_writer_open (const char *path)
{
  smfile_t *smf = smfile_open (path);
  if (smf == NULL)
    {
      return NULL;
    }
  struct data_writer *writer = i_malloc (1, sizeof *writer, &smf->e);
  if (writer == NULL)
    {
      smfile_close (smf);
      return NULL;
    }
  writer->ctx = smf;
  writer->functions = smfile_functions;
  return writer;
}

int
smfile_data_writer_close (struct data_writer *w)
{
  int ret = smfile_close (w->ctx);
  i_free (w);
  return ret;
}

#ifndef NTEST
TEST (smfile_data_writer)
{
  error e = error_create ();

  const u32 niters[] = { 100, 100, 100, 100, 100, 100, 1000, 1000, 1000, 1000, 10000 };

  for (u32 i = 0; i < arrlen (niters); ++i)
    {
      i_log_info ("smfile data validator test: %d\n", i);

      struct ext_array ext_arr_1 = ext_array_create ();

      struct data_writer ref;
      ext_array_data_writer (&ref, &ext_arr_1);

      pgr_delete_single_file ("test", &e);
      struct data_writer *sut = smfile_data_writer_open ("test");

      struct dvalidtr d = {
        .sut = *sut,
        .ref = ref,
        .isvalid = NULL,
      };

      test_assert_equal (dvalidtr_random_test (&d, 1, niters[i], 1000, &e), SUCCESS);

      ext_array_free (&ext_arr_1);
      test_assert (smfile_data_writer_close (sut) == 0);
    }
}
#endif
