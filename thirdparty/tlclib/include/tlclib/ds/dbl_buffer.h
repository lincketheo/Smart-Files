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

/// A heap-allocated buffer that doubles in capacity when exhausted
struct dbl_buffer
{
  latch latch;   ///< Synchronization latch guarding this buffer
  void *data;    ///< Pointer to the underlying heap allocation
  u32 size;      ///< Size of each element in bytes
  u32 nelem_cap; ///< Maximum number of elements the current allocation can hold
  u32 nelem;     ///< Number of elements currently in use
};

/// Creates a double buffer with a given element size and initial capacity
err_t dblb_create (
    struct dbl_buffer *dest, ///< Buffer to initialize
    u32 size,                ///< Size of each element in bytes
    u32 initial_cap,         ///< Initial element capacity to allocate
    error *e);               ///< The error object

/// Appends elements to the buffer, doubling capacity if necessary
err_t dblb_append (
    struct dbl_buffer *d, ///< Target buffer
    const void *data,     ///< Source elements to append
    u32 nelem,            ///< Number of elements to append
    error *e);            ///< The error object

/// Ensures the buffer has room for at least nelem additional elements, reallocating if necessary
err_t dblb_ensure_space (
    struct dbl_buffer *d, ///< Target buffer
    u32 nelem,            ///< Number of additional elements to reserve space for
    error *e);            ///< The error object

/// Reserves space for nelem elements at the end of the buffer and returns a pointer to that region
///
/// The returned pointer is valid until the next reallocation. The caller is
/// responsible for writing valid data into the region before further mutations.
void *dblb_append_alloc (
    struct dbl_buffer *d, ///< Target buffer
    u32 nelem,            ///< Number of elements to reserve
    error *e);            ///< The error object

/// Frees all memory owned by the buffer
void dblb_free (struct dbl_buffer *d); ///< Target buffer
