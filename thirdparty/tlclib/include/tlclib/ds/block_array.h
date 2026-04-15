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

#include "data_writer.h"
#include "tlclib/memory/slab_alloc.h"

/**
 * @class block
 * @brief A block is an individual node in a block array that holds
 * data array that is sized [cap_per_node] of it's parent and it has a
 * length that is how full it is
 */
struct block
{
  struct block *next;
  struct block *prev;
  u32 len;
  u8 data[];
};

/**
 * @class block_array
 * @brief A block array is a linked listed of data blocks that
 * represent byte data. Each block has capacity [cap_per_node] and
 * links to their neighbors
 */
struct block_array
{
  struct slab_alloc block_alloc; ///< Allocates blocks
  u32 cap_per_node;
  struct block *head;

  u32 tlen;  ///< length of tail
  u8 tail[]; ///< A temporary buffer used for storing the right half of a block on insert
};

struct block_array *block_array_create (u32 cap_per_node, error *e);
void block_array_free (struct block_array *r);

err_t block_array_insert (
    struct block_array *r,
    u32 ofst,
    const void *src,
    u32 slen,
    error *e);

u64 block_array_read (
    const struct block_array *r,
    struct stride str,
    u32 size,
    void *dest);

u64 block_array_write (
    const struct block_array *r,
    struct stride str,
    u32 size,
    const void *src);

i64 block_array_remove (
    struct block_array *r,
    struct stride str,
    u32 size,
    void *dest,
    error *e);

u64 block_array_getlen (const struct block_array *r);

// Array accessor pattern
void *block_array_get (struct block_array *r, u64 idx);

void block_array_set (
    struct block_array *r,
    u64 idx,
    const void *data,
    u32 dlen);

void block_array_data_writer (
    struct data_writer *dest,
    struct block_array *arr);
