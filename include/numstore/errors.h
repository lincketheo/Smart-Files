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
 * @defgroup nsdb_errors Error Handling
 * @brief Error codes, error context, and error reporting utilities.
 * @{
 */

/**
 * @brief All error codes returned by numstore operations.
 *
 * Every function that can fail returns an @c err_t or encodes failure as a
 * negative @c sb_size / @c spgno return value. @c SUCCESS (0) is the only
 * non-error value; all failure codes are negative so callers can use a simple
 * `< 0` or `!= SUCCESS` check.
 *
 * Codes are stable across releases — do not test for specific negative
 * integers in application code; use the named constants instead.
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

/**
 * @brief Error context populated by failing operations.
 *
 * Declare one of these on the stack (via error_create()), pass its address to
 * any numstore function, and inspect it when the function reports failure.
 * The struct is intentionally small — it is safe to pass by pointer from any
 * call depth.
 *
 * @c cause_code holds the machine-readable error kind; @c cause_msg holds a
 * human-readable description of the specific failure, including context such
 * as page numbers or variable names where available.
 *
 * @note The @c error is consumed (reset to @c SUCCESS) by ns_perror(). After
 *       consumption the same struct may be reused for subsequent calls.
 *
 * @code
 * error e = error_create();
 * nsdb_t *db = ns_open("data", &e);
 * if (!db) {
 *     ns_perror(stderr, &e);   // prints and resets e
 *     return -1;
 * }
 * @endcode
 */
typedef struct
{
  err_t cause_code;    ///< Machine-readable error code. @c SUCCESS when no error is pending.
  char cause_msg[256]; ///< Null-terminated human-readable description of the failure.
  u32 cmlen;           ///< Length of @c cause_msg in bytes, excluding the null terminator.
} error;

/**
 * @brief Initialise a new @c error struct with no pending error.
 *
 * Returns an @c error with @c cause_code set to @c SUCCESS and @c cause_msg
 * zeroed. Always use this to initialise an @c error rather than a bare
 * @c {0} aggregate — the implementation may perform additional setup in
 * future versions.
 *
 * @return A zero-initialised @c error ready to be passed to numstore functions.
 *
 * @code
 * error e = error_create();
 * @endcode
 */
error error_create (void);

/**
 * @brief Print a human-readable error message to @p output and reset the error.
 *
 * Formats and writes the pending error in @p e to @p output in a style
 * similar to @c perror(3): the cause code name followed by the cause message.
 * After printing, @p e is reset to @c SUCCESS so it can be reused.
 *
 * Passing an @c error whose @c cause_code is already @c SUCCESS is a no-op.
 *
 * @param output  Destination stream (e.g. @c stderr or an open log file).
 *                Must not be NULL.
 * @param e       Error to print. Must not be NULL. Reset to @c SUCCESS on return.
 *
 * @code
 * error e = error_create();
 * if (ns_delete(db, tx, "imu/accel_x", &e) != SUCCESS) {
 *     ns_perror(stderr, &e);
 *     // e.cause_code is now SUCCESS — safe to reuse
 * }
 * @endcode
 */
void ns_perror (FILE *output, error *e);

/** @} */
