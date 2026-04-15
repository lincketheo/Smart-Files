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

// Branch-prediction hints
#if defined(_MSC_VER)
#define likely(x) (x)
#define unlikely(x) (x)
#else
#define likely(x) __builtin_expect (!!(x), 1)
#define unlikely(x) __builtin_expect (!!(x), 0)
#endif

// Unreachable hint (optimizer / static-analysis)
#if defined(_MSC_VER)
#define UNREACHABLE_HINT() __assume (0)
#else
#define UNREACHABLE_HINT() __builtin_unreachable ()
#endif

// NORETURN — marks a function that never returns
#if defined(_MSC_VER)
#define NORETURN __declspec (noreturn)
#else
#define NORETURN __attribute__ ((noreturn))
#endif

// PRINTF_ATTR(fmt_pos, va_pos) — printf-format annotation ───────────
// Positions are 1-based and follow the same convention as the GCC attribute
// (counting any implicit self/object argument).
#if defined(_MSC_VER)
#define PRINTF_ATTR(fmt_pos, va_pos)
#else
#define PRINTF_ATTR(fmt_pos, va_pos) \
  __attribute__ ((format (printf, fmt_pos, va_pos)))
#endif

// ── MAYBE_UNUSED — suppress "unused entity" warnings ──────────────────
#if defined(_MSC_VER)
#define MAYBE_UNUSED
#else
#define MAYBE_UNUSED __attribute__ ((unused))
#endif

// ── ANSI_COLORS — 1 if ANSI escape sequences work in terminal output ──
// Old Windows consoles do not support them without extra setup.
#if defined(_WIN32)
#define ANSI_COLORS 0
#else
#define ANSI_COLORS 1
#endif

// ── HAS_BUILTIN_OVERFLOW — 1 if __builtin_{add,sub,mul}_overflow exist ─
#if defined(_MSC_VER)
#define HAS_BUILTIN_OVERFLOW 0
#else
#define HAS_BUILTIN_OVERFLOW 1
#endif
