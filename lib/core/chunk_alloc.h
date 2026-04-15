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
#include "core/lalloc.h"
#include "core/latch.h"

struct chunk
{
  struct lalloc alloc;
  struct chunk *next;
  u8 data[];
};

struct chunk_alloc_settings
{
  u32 max_alloc_size;      // Maximum single allocation (0 = unlimited)
  u32 max_total_size;      // Maximum total memory (0 = unlimited)
  float target_chunk_mult; // Fraction of the requested size to allocate
                           // each chunk (> 1)
  u32 min_chunk_size;      // Minimum chunk size
  u32 max_chunk_size;      // Maximum chunk size (0 = unlimited)
  u32 max_chunks;          // Maximum number of chunks (0 = unlimited)
};

struct chunk_alloc
{
  latch latch;
  struct chunk_alloc_settings settings;
  struct chunk *head;
  u32 num_chunks;
  u32 total_allocated;
  u32 total_used;
};

void chunk_alloc_create (struct chunk_alloc *dest,
                         struct chunk_alloc_settings settings);
void chunk_alloc_create_default (struct chunk_alloc *dest);
void chunk_alloc_free_all (
    struct chunk_alloc *ca); // Free's everything and starts from 0
void chunk_alloc_reset_all (
    struct chunk_alloc *ca); // Doesn't free anything - keeps chunks around

// Main Methods
void *chunk_malloc (struct chunk_alloc *ca, u32 req, u32 size, error *e);
void *chunk_calloc (struct chunk_alloc *ca, u32 req, u32 size, error *e);

// Utils
void *chunk_alloc_move_mem (struct chunk_alloc *ca, const void *ptr, u32 size,
                            error *e);
