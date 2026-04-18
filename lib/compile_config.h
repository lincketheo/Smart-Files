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

////////////////////////////////////////////////////////////
// VERSION

#define NS_VERSION_MAJOR 0
#define NS_VERSION_MINOR 0
#define NS_VERSION_PATCH 1
#define NS_VERSION_STRING "0.0.1"

////////////////////////////////////////////////////////////
// TUNING

#define PAGE_SIZE ((p_size)4096)
#define MEMORY_PAGE_LEN ((u32)4096)
#define MAX_VSTR 8192
#define MAX_TSTR 8192
#define WAL_BUFFER_CAP 1048576

////////////////////////////////////////////////////////////
// PLATFORM

#define NS_PLATFORM_LINUX 1
#define NS_PLATFORM_MACOS 0
#define NS_PLATFORM_WINDOWS 0
#define NS_PLATFORM_NAME "Linux"
#define NS_PLATFORM_VERSION "6.1.0-44-amd64"

////////////////////////////////////////////////////////////
// ARCHITECTURE

#define NS_ARCH_X86_64 1
#define NS_ARCH_ARM64 0
#define NS_ARCH_32BIT 0
#define NS_ARCH_NAME "x86_64"
#define NS_POINTER_SIZE 8
#define NS_POINTER_BITS 64

////////////////////////////////////////////////////////////
// ENDIANNESS

#define NS_LITTLE_ENDIAN 1
#define NS_BIG_ENDIAN 0

////////////////////////////////////////////////////////////
// COMPILER

#define NS_COMPILER_GCC 1
#define NS_COMPILER_CLANG 0
#define NS_COMPILER_MSVC 0
#define NS_COMPILER_ID "GNU"
#define NS_COMPILER_VERSION "12.2.0"

////////////////////////////////////////////////////////////
// CPU

#define NS_CPU_COUNT 8

////////////////////////////////////////////////////////////
// BUILD

#define NS_BUILD_DEBUG 1
#define NS_BUILD_RELEASE 0
