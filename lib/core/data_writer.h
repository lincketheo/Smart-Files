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
#include "core/stride.h"

typedef err_t (*insert_func) (void *ctx, u32 ofst, const void *src, u32 slen,
                              error *e);
typedef i64 (*read_func) (void *ctx, struct stride str, u32 size, void *dest,
                          error *e);
typedef i64 (*write_func) (void *ctx, struct stride str, u32 size,
                           const void *src, error *e);
typedef i64 (*remove_func) (void *ctx, struct stride str, u32 size, void *dest,
                            error *e);
typedef i64 (*get_len_func) (void *ctx, error *e);

struct data_writer_functions
{
  insert_func insert;
  read_func read;
  write_func write;
  remove_func remove;
  get_len_func getlen;
};

struct data_writer
{
  struct data_writer_functions functions;
  void *ctx;
};
