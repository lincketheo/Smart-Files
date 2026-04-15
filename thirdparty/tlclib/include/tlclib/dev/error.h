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

#include "stdtypes.h"
#include "tlclib/core/macros.h"
#include "tlclib/intf/os/compiler.h"

#define FPREFIX_STR "%s:%d (%s): \n            "
#define FPREFIX_ARGS file_basename (__FILE__), __LINE__, __func__

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

#include "stdtypes.h"

#include <stdio.h>

typedef int err_t;

#define SUCCESS 0               ///< Operation completed successfully.
#define ERR_IO -1               ///< Generic I/O error (read, write, or fsync failure).
#define ERR_NOMEM -2            ///< Memory allocation failed.
#define ERR_ARITH -3            ///< Integer arithmetic overflow detected.
#define ERR_CORRUPT -4          ///< Corrupted data - user might've tampered with something
#define ERR_INVALID_ARGUMENT -5 ///< User provided an invalid argument

typedef struct
{
  err_t cause_code;    ///< Machine-readable error code. @c SUCCESS when no error is pending.
  char cause_msg[256]; ///< Null-terminated human-readable description of the failure.
  u32 cmlen;           ///< Length of @c cause_msg in bytes, excluding the null terminator.
} error;

error error_create (void);
void err_t_perror (FILE *output, error *e);
err_t error_causef (error *e, err_t c, const char *fmt, ...) PRINTF_ATTR (3, 4);
err_t error_change_causef (error *e, err_t c, const char *fmt, ...) PRINTF_ATTR (3, 4);
err_t error_change_causef_from (
    error *e,
    err_t from,
    err_t to,
    const char *fmt, ...) PRINTF_ATTR (4, 5);
void error_log_consume (error *e);
bool error_equal (const error *left, const error *right);

NORETURN void error_fatal (const char *fmt, ...);

////////////////////////////////////////////////////////////
/// Macro Wrappers

#define error_trace(e) (e)->cause_code < 0 ? error_causef (e, (e)->cause_code, __func__) : (e)->cause_code

/*
** WRAP(expr) — evaluate expr, and if it signals failure return immediately.
**
** Equivalent to:
**   if (unlikely(expr < SUCCESS)) { return error_trace(e); }
**
** The error context variable must be named 'e' in the enclosing scope.
** This is a statement macro — it must be followed by a semicolon.
*/
#define WRAP(expr)                     \
  do                                   \
    {                                  \
      if (unlikely ((expr) < SUCCESS)) \
        {                              \
          return error_trace (e);      \
        }                              \
    }                                  \
  while (0)

#define WRAP_GOTO(expr, label)         \
  do                                   \
    {                                  \
      if (unlikely ((expr) < SUCCESS)) \
        {                              \
          goto label;                  \
        }                              \
    }                                  \
  while (0)
