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
#include "c_specx/ds/data_writer.h"
#include "smfile.h"

// Data writer fixture
struct data_writer *smfile_data_writer_open (const char *path);
int smfile_data_writer_close (struct data_writer *w);

// Specialty fixture for smfile
typedef sb_size (*pinsert_func) (void *ctx, const char *name, const void *src, sb_size bofst, b_size slen);
typedef sb_size (*pwrite_func) (void *ctx, const char *name, const void *src, t_size size, b_size bofst, sb_size stride, b_size nelem);
typedef sb_size (*pread_func) (void *ctx, const char *name, void *dest, t_size size, sb_size bofst, sb_size stride, b_size nelem);
typedef sb_size (*premove_func) (void *ctx, const char *name, void *dest, t_size size, sb_size bofst, sb_size stride, b_size nelem);
typedef int (*delete_func) (void *ctx, const char *name);
typedef sb_size (*psize_func) (void *ctx, const char *name);
typedef int (*crash_func) (void *ctx);
typedef int (*begin_func) (void *ctx);
typedef int (*commit_func) (void *ctx);
typedef int (*rollback_func) (void *ctx);

/// The full set of function pointers that back a data_writer
struct smfile_test_fixture_functions
{
  pinsert_func pinsert;
  pwrite_func pwrite;
  pread_func pread;
  premove_func premove;
  delete_func delete;
  psize_func psize;
  crash_func crash;
  begin_func begin;
  commit_func commit;
  rollback_func rollback;
};

/// A virtual data source/sink pairing a function table with its context
struct smfile_test_fixture
{
  void *ctx;                                      ///< Opaque context passed to every function call
  struct smfile_test_fixture_functions functions; ///< Vtable of data operations
};

struct smfile_test_fixture smfile_test_fixture_open (const char *path);
int smfile_test_fixture_close (struct smfile_test_fixture *f);
