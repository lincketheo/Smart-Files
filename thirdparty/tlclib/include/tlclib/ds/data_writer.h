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

#include "stride.h"
#include "tlclib/dev/error.h"

/// Function pointer type for inserting data at a byte offset
typedef err_t (*insert_func) (
    void *ctx,       ///< Caller-provided context pointer
    u32 ofst,        ///< Byte offset at which to insert
    const void *src, ///< Source data to insert
    u32 slen,        ///< Number of bytes to insert
    error *e);       ///< The error object

/// Function pointer type for reading elements from a strided range
typedef i64 (*read_func) (
    void *ctx,         ///< Caller-provided context pointer
    struct stride str, ///< Stride descriptor defining start, step, and element count
    u32 size,          ///< Size of each element in bytes
    void *dest,        ///< Destination buffer to receive the data
    error *e);         ///< The error object

/// Function pointer type for writing elements into a strided range
typedef i64 (*write_func) (
    void *ctx,         ///< Caller-provided context pointer
    struct stride str, ///< Stride descriptor defining start, step, and element count
    u32 size,          ///< Size of each element in bytes
    const void *src,   ///< Source data to write
    error *e);         ///< The error object

/// Function pointer type for removing elements from a strided range
typedef i64 (*remove_func) (
    void *ctx,         ///< Caller-provided context pointer
    struct stride str, ///< Stride descriptor defining start, step, and element count
    u32 size,          ///< Size of each element in bytes
    void *dest,        ///< Optional destination buffer to capture removed data (NULL to discard)
    error *e);         ///< The error object

/// Function pointer type for querying the total number of bytes in the data source
typedef i64 (*get_len_func) (
    void *ctx, ///< Caller-provided context pointer
    error *e); ///< The error object

/// The full set of function pointers that back a data_writer
struct data_writer_functions
{
  insert_func insert;  ///< Insert bytes at an offset
  read_func read;      ///< Read elements from a strided range
  write_func write;    ///< Overwrite elements in a strided range
  remove_func remove;  ///< Remove elements from a strided range
  get_len_func getlen; ///< Query the total byte length
};

/// A virtual data source/sink pairing a function table with its context
struct data_writer
{
  struct data_writer_functions functions; ///< Vtable of data operations
  void *ctx;                              ///< Opaque context passed to every function call
};
