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

#include "tlclib/concurrency/latch.h"
#include "tlclib/dev/error.h"
#include "tlclib/memory/lalloc.h"

/// A single chunk of memory in a chunk allocator chain
struct chunk
{
  struct lalloc alloc; ///< Base allocator interface for this chunk
  struct chunk *next;  ///< Next chunk in the linked list, or NULL if tail
  u8 data[];           ///< Flexible array of chunk-owned bytes
};

/// Configuration settings for a chunk allocator
struct chunk_alloc_settings
{
  u32 max_alloc_size;      ///< Maximum size of a single allocation in bytes (0 = unlimited)
  u32 max_total_size;      ///< Maximum total memory across all chunks in bytes (0 = unlimited)
  float target_chunk_mult; ///< Multiplier applied to the requested size when sizing a new chunk (must be > 1)
  u32 min_chunk_size;      ///< Minimum size of a newly allocated chunk in bytes
  u32 max_chunk_size;      ///< Maximum size of a newly allocated chunk in bytes (0 = unlimited)
  u32 max_chunks;          ///< Maximum number of chunks that may be allocated (0 = unlimited)
};

/// A chunk-based arena allocator
struct chunk_alloc
{
  latch latch;                          ///< Synchronization latch guarding this allocator
  struct chunk_alloc_settings settings; ///< Configuration settings
  struct chunk *head;                   ///< Head of the chunk linked list
  u32 num_chunks;                       ///< Current number of allocated chunks
  u32 total_allocated;                  ///< Total bytes allocated across all chunks
  u32 total_used;                       ///< Total bytes currently in use
};

/// Initializes a chunk allocator with the given settings
void chunk_alloc_create (
    struct chunk_alloc *dest,              ///< Allocator to initialize
    struct chunk_alloc_settings settings); ///< Settings to apply

/// Initializes a chunk allocator with default settings
void chunk_alloc_create_default (struct chunk_alloc *dest); ///< Allocator to initialize

/// Frees all chunks and resets the allocator to its initial state
void chunk_alloc_free_all (struct chunk_alloc *ca); ///< Target allocator

/// Resets all chunk usage counters without freeing any memory, keeping chunks available for reuse
void chunk_alloc_reset_all (struct chunk_alloc *ca); ///< Target allocator

/// Allocates uninitialized memory from the chunk allocator
void *chunk_malloc (
    struct chunk_alloc *ca, ///< Target allocator
    u32 req,                ///< Requested alignment in bytes
    u32 size,               ///< Number of bytes to allocate
    error *e);              ///< The error object

/// Allocates zero-initialized memory from the chunk allocator
void *chunk_calloc (
    struct chunk_alloc *ca, ///< Target allocator
    u32 req,                ///< Requested alignment in bytes
    u32 size,               ///< Number of bytes to allocate
    error *e);              ///< The error object

/// Copies memory from an external pointer into the chunk allocator and returns a pointer to the copy
void *chunk_alloc_move_mem (
    struct chunk_alloc *ca, ///< Target allocator
    const void *ptr,        ///< Source data to copy
    u32 size,               ///< Number of bytes to copy
    error *e);              ///< The error object
