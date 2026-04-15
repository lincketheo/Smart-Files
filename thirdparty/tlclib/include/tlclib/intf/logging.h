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

#include "tlclib/intf/os/compiler.h"

////////////////////////////////////////////////////////////
// TERMINAL SPECIALS

#if ANSI_COLORS
#define BLACK "\033[0;30m"
#define RED "\033[0;31m"
#define GREEN "\033[0;32m"
#define YELLOW "\033[0;33m"
#define BLUE "\033[0;34m"
#define MAGENTA "\033[0;35m"
#define CYAN "\033[0;36m"
#define WHITE "\033[0;37m"
#define BOLD_BLACK "\033[1;30m"
#define BOLD_RED "\033[1;31m"
#define BOLD_GREEN "\033[1;32m"
#define BOLD_YELLOW "\033[1;33m"
#define BOLD_BLUE "\033[1;34m"
#define BOLD_MAGENTA "\033[1;35m"
#define BOLD_CYAN "\033[1;36m"
#define BOLD_WHITE "\033[1;37m"
#define RESET "\033[0m"
#else
// ANSI escape codes not supported — use empty strings.
#define BLACK ""
#define RED ""
#define GREEN ""
#define YELLOW ""
#define BLUE ""
#define MAGENTA ""
#define CYAN ""
#define WHITE ""
#define BOLD_BLACK ""
#define BOLD_RED ""
#define BOLD_GREEN ""
#define BOLD_YELLOW ""
#define BOLD_BLUE ""
#define BOLD_MAGENTA ""
#define BOLD_CYAN ""
#define BOLD_WHITE ""
#define RESET ""
#endif

////////////////////////////////////////////////////////////
// LOGGING LEVELS

#define LOG_NONE 0
#define LOG_ERROR 1
#define LOG_WARN 2
#define LOG_INFO 3
#define LOG_DEBUG 4
#define LOG_TRACE 5

void i_log_internal (const char *prefix, const char *color,
                     const char *fmt, ...) PRINTF_ATTR (3, 4);

void i_log_flush (void);

#ifndef I_LOG_LEVEL
#define I_LOG_LEVEL LOG_DEBUG
#endif

////////////////////////////////////////////////////////////
// LOGGING WRAPPERS

#if defined(NLOGGING)
#define SHOULD_LOG_AT(lvl) 0
#else
#define SHOULD_LOG_AT(lvl) ((I_LOG_LEVEL) >= (lvl))
#endif

#if SHOULD_LOG_AT(LOG_TRACE)
#define i_log_trace(...) i_log_internal ("TRACE", BOLD_WHITE, __VA_ARGS__)
#define i_printf_trace(...) fprintf (stderr, __VA_ARGS__);
#else
#define i_log_trace(...) ((void)0)
#define i_printf_trace(...) ((void)0)
#endif

#if SHOULD_LOG_AT(LOG_DEBUG)
#define i_log_debug(...) i_log_internal ("DEBUG", BLUE, __VA_ARGS__)
#define i_printf_debug(...) fprintf (stderr, __VA_ARGS__)
#else
#define i_log_debug(...) ((void)0)
#define i_printf_debug(...) ((void)0)
#endif

#if SHOULD_LOG_AT(LOG_INFO)
#define i_log_info(...) i_log_internal ("INFO", GREEN, __VA_ARGS__)
#define i_printf_info(...) fprintf (stderr, __VA_ARGS__)
#else
#define i_log_info(...) ((void)0)
#define i_printf_info(...) ((void)0)
#endif

#if SHOULD_LOG_AT(LOG_WARN)
#define i_log_warn(...) i_log_internal ("WARN", YELLOW, __VA_ARGS__)
#define i_printf_warn(...) fprintf (stderr, __VA_ARGS__)
#else
#define i_log_warn(...) ((void)0)
#define i_printf_warn(...) ((void)0)
#endif

#if SHOULD_LOG_AT(LOG_ERROR)
#define i_log_error(...) i_log_internal ("ERROR", RED, __VA_ARGS__)
#define i_printf_error(...) fprintf (stderr, __VA_ARGS__)
#else
#define i_log_error(...) ((void)0)
#define i_printf_error(...) ((void)0)
#endif

// These are always logged
#ifndef NLOGGING
#define i_log_assert(...) i_log_internal ("ASSERT", RED, __VA_ARGS__)
#define i_log_failure(...) i_log_internal ("FAILURE", BOLD_RED, __VA_ARGS__)
#define i_log_passed(...) i_log_internal ("PASSED", BOLD_GREEN, __VA_ARGS__)
#else
#define i_log_assert(...) i_log_internal ("ASSERT", RED, __VA_ARGS__)
#define i_log_failure(...) ((void)0)
#define i_log_passed(...) ((void)0)
#endif

// Programatically choose the log level
#define i_log(lvl, ...)              \
  do                                 \
    {                                \
      if ((lvl) == LOG_TRACE)        \
        {                            \
          i_log_trace (__VA_ARGS__); \
        }                            \
      else if ((lvl) == LOG_DEBUG)   \
        {                            \
          i_log_debug (__VA_ARGS__); \
        }                            \
      else if ((lvl) == LOG_INFO)    \
        {                            \
          i_log_info (__VA_ARGS__);  \
        }                            \
      else if ((lvl) == LOG_WARN)    \
        {                            \
          i_log_warn (__VA_ARGS__);  \
        }                            \
      else if ((lvl) == LOG_ERROR)   \
        {                            \
          i_log_error (__VA_ARGS__); \
        }                            \
      else if ((lvl) == LOG_NONE)    \
        {                            \
        }                            \
      else                           \
        {                            \
          UNREACHABLE ();            \
        }                            \
    }                                \
  while (0)

// Print instead of log at a certain logging level
#define i_printf(lvl, ...)              \
  do                                    \
    {                                   \
      if ((lvl) == LOG_TRACE)           \
        {                               \
          i_printf_trace (__VA_ARGS__); \
        }                               \
      else if ((lvl) == LOG_DEBUG)      \
        {                               \
          i_printf_debug (__VA_ARGS__); \
        }                               \
      else if ((lvl) == LOG_INFO)       \
        {                               \
          i_printf_info (__VA_ARGS__);  \
        }                               \
      else if ((lvl) == LOG_WARN)       \
        {                               \
          i_printf_warn (__VA_ARGS__);  \
        }                               \
      else if ((lvl) == LOG_ERROR)      \
        {                               \
          i_printf_error (__VA_ARGS__); \
        }                               \
      else if ((lvl) == LOG_NONE)       \
        {                               \
        }                               \
      else                              \
        {                               \
          UNREACHABLE ();               \
        }                               \
    }                                   \
  while (0)
