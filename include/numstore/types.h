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

#include "numstore/stdtypes.h"

/// A resolved, internal stride descriptor for tree operations
struct stride
{
  b_size start;  ///< Byte offset at which to begin
  b_size stride; ///< Bytes to advance between successive elements
  b_size nelems; ///< Number of elements to access
};

/// A user-facing stride descriptor using signed, Python-style slice semantics
struct user_stride
{
  sb_size start; ///< Start index (negative values index from the end)
  sb_size step;  ///< Step between elements (negative not yet supported)
  sb_size stop;  ///< Exclusive stop index (negative values index from the end)
  int present;   ///< Non-zero if this stride was explicitly provided by the user
};

/// A length-prefixed, non-owning string view
struct string
{
  u32 len;          ///< Number of bytes in data (not necessarily null-terminated)
  const char *data; ///< Pointer to the string bytes
};
