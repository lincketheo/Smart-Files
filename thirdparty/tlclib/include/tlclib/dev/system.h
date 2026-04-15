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

// Platform Macros:
// PLATFORM_WINDOWS       - Windows (any version)
// PLATFORM_LINUX         - Linux (excluding Android)
// PLATFORM_ANDROID       - Android
// PLATFORM_MAC           - macOS
// PLATFORM_IOS           - iOS (including simulator)
// PLATFORM_BSD           - BSD variants (FreeBSD, OpenBSD, NetBSD, DragonFly)
// PLATFORM_EMSCRIPTEN    - Emscripten (WebAssembly)
// PLATFORM_UNIX          - Any Unix-like system
// PLATFORM_POSIX         - Any POSIX-compliant system
// Usage:
// #include "tlclib/platform.h"
// #if PLATFORM_WINDOWS
// // Windows-specific code
// #elif PLATFORM_LINUX
// // Linux-specific code
// #elif PLATFORM_MAC
// // macOS-specific code
// #endif

#define PLATFORM_WINDOWS 0
#define PLATFORM_LINUX 0
#define PLATFORM_ANDROID 0
#define PLATFORM_MAC 0
#define PLATFORM_IOS 0
#define PLATFORM_BSD 0
#define PLATFORM_EMSCRIPTEN 0
#define PLATFORM_UNIX 0
#define PLATFORM_POSIX 0

// Detect Emscripten first (sets its own __unix__ macro)
#ifdef __EMSCRIPTEN__
#undef PLATFORM_EMSCRIPTEN
#define PLATFORM_EMSCRIPTEN 1
#undef PLATFORM_POSIX
#define PLATFORM_POSIX 1
#endif

// Detect Windows
#if defined(_WIN32) || defined(_WIN64) || defined(__WINDOWS__)
#undef PLATFORM_WINDOWS
#define PLATFORM_WINDOWS 1
#endif

// Detect Android before Linux (Android defines __linux__)
#if defined(__ANDROID__)
#undef PLATFORM_ANDROID
#define PLATFORM_ANDROID 1
#undef PLATFORM_LINUX
#define PLATFORM_LINUX 0
#undef PLATFORM_UNIX
#define PLATFORM_UNIX 1
#undef PLATFORM_POSIX
#define PLATFORM_POSIX 1
#endif

// Detect Linux (excluding Android)
#if defined(__linux__) && !defined(__ANDROID__)
#undef PLATFORM_LINUX
#define PLATFORM_LINUX 1
#undef PLATFORM_UNIX
#define PLATFORM_UNIX 1
#undef PLATFORM_POSIX
#define PLATFORM_POSIX 1
#endif

// Detect macOS
#if defined(__APPLE__) && defined(__MACH__)
#include "TargetConditionals.h"
#include "signatures.h"

#if TARGET_OS_IPHONE || TARGET_IPHONE_SIMULATOR
// iOS (device or simulator)
#undef PLATFORM_IOS
#define PLATFORM_IOS 1
#elif TARGET_OS_MAC
// macOS
#undef PLATFORM_MAC
#define PLATFORM_MAC 1
#endif

#undef PLATFORM_UNIX
#define PLATFORM_UNIX 1
#undef PLATFORM_POSIX
#define PLATFORM_POSIX 1
#endif

// Detect BSD variants
#if defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__) \
    || defined(__DragonFly__) || defined(__bsdi__)
#undef PLATFORM_BSD
#define PLATFORM_BSD 1
#undef PLATFORM_UNIX
#define PLATFORM_UNIX 1
#undef PLATFORM_POSIX
#define PLATFORM_POSIX 1
#endif

// Fallback Unix detection for other Unix systems
#if !PLATFORM_UNIX && (defined(__unix__) || defined(__unix))
#undef PLATFORM_UNIX
#define PLATFORM_UNIX 1
#undef PLATFORM_POSIX
#define PLATFORM_POSIX 1
#endif

// Sanity check - exactly one primary platform should be detected (or none for
// unknown)
#if (PLATFORM_WINDOWS + PLATFORM_LINUX + PLATFORM_ANDROID + PLATFORM_MAC \
     + PLATFORM_IOS + PLATFORM_BSD + PLATFORM_EMSCRIPTEN)                \
    > 1
#warning "Multiple platforms detected - check your build configuration"
#endif

// Convenience macros for common checks
#define PLATFORM_APPLE (PLATFORM_MAC || PLATFORM_IOS)
#define PLATFORM_MOBILE (PLATFORM_ANDROID || PLATFORM_IOS)
#define PLATFORM_DESKTOP \
  (PLATFORM_WINDOWS || PLATFORM_LINUX || PLATFORM_MAC || PLATFORM_BSD)

static inline const char *
platformstr (void)
{
  if (PLATFORM_WINDOWS)
    {
      return "Windows";
    }
  else if (PLATFORM_LINUX)
    {
      return "Linux";
    }
  else if (PLATFORM_ANDROID)
    {
      return "Android";
    }
  else if (PLATFORM_MAC)
    {
      return "macOS";
    }
  else if (PLATFORM_IOS)
    {
      return "iOS";
    }
  else if (PLATFORM_BSD)
    {
      return "BSD";
    }
  else if (PLATFORM_EMSCRIPTEN)
    {
      return "Emscripten/WebAssembly";
    }

  return 0;
}
