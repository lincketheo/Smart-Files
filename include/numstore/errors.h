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

#include <stdio.h>

/**
 * Numstore's error system is two fold. First, there's the error codes.
 * Most functions that return an error (imagine early C days where every function
 * returned an int) return an err_t, which is just an enumerated value of what
 * can go wrong in numstore
 */
typedef enum
{
  SUCCESS = 0,                         ///< Operation completed successfully.
  ERR_IO = -1,                         ///< Generic I/O error (read, write, or fsync failure).
  ERR_CORRUPT = -2,                    ///< Page contents are inconsistent with the expected format; the database file may be damaged.
  ERR_NOMEM = -3,                      ///< Memory allocation failed.
  ERR_ARITH = -4,                      ///< Integer arithmetic overflow detected.
  ERR_PAGER_FULL = -5,                 ///< The buffer pool has no free frames and cannot evict; try committing or reducing working set.
  ERR_PG_OUT_OF_RANGE = -6,            ///< Attempted to access a page number beyond the current database extent.
  ERR_SYNTAX = -7,                     ///< Statement string failed to parse (compile-time syntax error).
  ERR_INTERP = -8,                     ///< Statement parsed successfully but is semantically invalid (compile-time semantic error).
  ERR_RPTREE_PAGE_STACK_OVERFLOW = -9, ///< Internal R+Tree traversal stack overflowed; tree depth exceeds the compiled limit (rare).
  ERR_DUPLICATE_VARIABLE = -10,        ///< ns_create() was called with a name that already exists in the variable hash table.
  ERR_VARIABLE_NE = -11,               ///< The referenced variable does not exist in the hash table.
  ERR_INVALID_ARGUMENT = -12,          ///< A caller-supplied argument is NULL, out of range, or otherwise invalid.
  ERR_DUPLICATE_COMMIT = -13,          ///< ns_commit() was called on a transaction that has already been committed.
} err_t;

typedef struct
{
  err_t cause_code;    ///< Machine-readable error code. @c SUCCESS when no error is pending.
  char cause_msg[256]; ///< Null-terminated human-readable description of the failure.
  u32 cmlen;           ///< Length of @c cause_msg in bytes, excluding the null terminator.
} error;

error error_create (void);

void ns_perror (FILE *output, error *e);
