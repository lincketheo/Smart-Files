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

#include "core/data_writer.h"
#include "core/slab_alloc.h"
#include "numstore/types.h"

struct block
{
  struct block *next;
  struct block *prev;
  u32 len;
  u8 data[];
};

struct block_array
{
  struct slab_alloc block_alloc;
  u32 cap_per_node;
  struct block *head;

  u32 tlen;
  u8 tail[];
};

struct block_array *block_array_create (u32 cap_per_node, error *e);
void block_array_free (struct block_array *r);

err_t block_array_insert (struct block_array *r, u32 ofst, const void *src,
                          u32 slen, error *e);
u64 block_array_read (const struct block_array *r, struct stride str, u32 size,
                      void *dest);
u64 block_array_write (const struct block_array *r, struct stride str, u32 size,
                       const void *src);
i64 block_array_remove (struct block_array *r, struct stride str, u32 size,
                        void *dest, error *e);
u64 block_array_getlen (const struct block_array *r);

// Array accessor pattern
void *block_array_get (struct block_array *r, u64 idx);
void block_array_set (struct block_array *r, u64 idx, const void *data,
                      u32 dlen);

void block_array_data_writer (struct data_writer *dest,
                              struct block_array *arr);
