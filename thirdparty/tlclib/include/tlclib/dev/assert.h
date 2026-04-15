/// Copyright [yyyy] [name of copyright owner]
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
#include "tlclib/intf/os.h"
#include "tlclib/intf/os/compiler.h"

#define crash()               \
  do                          \
    {                         \
      *(volatile int *)0 = 1; \
      UNREACHABLE_HINT ();    \
    }                         \
  while (0)

#define UNIMPLEMENTED() UNREACHABLE ()

#define UNREACHABLE() \
  do                  \
    {                 \
      crash ();       \
    }                 \
  while (0)

#ifndef NDEBUG
#include "signatures.h"
#include "tlclib/intf/logging.h"

////////////////////////////////////////////////////////////
/// PANIC
#define panic(msg)                      \
  do                                    \
    {                                   \
      i_log_error ("PANIC! %s\n", msg); \
      i_log_flush ();                   \
      crash ();                         \
    }                                   \
  while (0)

////////////////////////////////////////////////////////////
/// ASSERT
#define ASSERT(expr)                                                  \
  do                                                                  \
    {                                                                 \
      if (!(expr))                                                    \
        {                                                             \
          i_log_assert ("%s failed at %s:%d (%s)\n", #expr, __FILE__, \
                        __LINE__, __func__);                          \
          i_log_flush ();                                             \
          crash ();                                                   \
        }                                                             \
    }                                                                 \
  while (0)

#define DEFINE_DBG_ASSERT(type, name, var, body) \
  HEADER_FUNC void name##_assert__ (const type *var) body

#define DBG_ASSERT(name, expr) name##_assert__ (expr)

#else

#include "tlclib/dev/signatures.h"
#include "tlclib/macros.h"

// Throw a compile time error to discourage various debug macros
#define NOT_FOR_PRODUCTION() // typedef char
                             // PANIC_in_release_mode_is_not_allowed[-1]

// Release doesn't allow these two
#define panic(msg) NOT_FOR_PRODUCTION ()
#define ASSERT(expr)

#define DEFINE_DBG_ASSERT(type, name, var, body) \
  HEADER_FUNC void name##_assert (const type *var) { (void)var; }

#define DBG_ASSERT(name, expr) ((void)0)
#endif
