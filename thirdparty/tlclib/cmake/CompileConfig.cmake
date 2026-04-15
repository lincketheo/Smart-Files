# This file has all the in code constants to define and auto generates include/numstore/compile_config

# Platform 
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(PLATFORM_LINUX 1)
    set(PLATFORM_MACOS 0)
    set(PLATFORM_WINDOWS 0)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(PLATFORM_LINUX 0)
    set(PLATFORM_MACOS 1)
    set(PLATFORM_WINDOWS 0)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Windows")
    set(PLATFORM_LINUX 0)
    set(PLATFORM_MACOS 0)
    set(PLATFORM_WINDOWS 1)
else()
    set(PLATFORM_LINUX 0)
    set(PLATFORM_MACOS 0)
    set(PLATFORM_WINDOWS 0)
endif()

set(PLATFORM_NAME "${CMAKE_SYSTEM_NAME}")
set(PLATFORM_VERSION "${CMAKE_SYSTEM_VERSION}")

# Architecture 
if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|AMD64)$")
    set(ARCH_X86_64  1)
    set(ARCH_ARM64   0)
    set(ARCH_32BIT   0)
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64|arm64)$")
    set(ARCH_X86_64  0)
    set(ARCH_ARM64   1)
    set(ARCH_32BIT   0)
else()
    set(ARCH_X86_64  0)
    set(ARCH_ARM64   0)
    set(ARCH_32BIT   1)
endif()

set(ARCH_NAME "${CMAKE_SYSTEM_PROCESSOR}")

# Endianness 
include(TestBigEndian)
test_big_endian(BIG_ENDIAN_DETECTED)
if(BIG_ENDIAN_DETECTED)
    set(BIG_ENDIAN    1)
    set(LITTLE_ENDIAN 0)
else()
    set(BIG_ENDIAN    0)
    set(LITTLE_ENDIAN 1)
endif()

# Compiler 
set(COMPILER_ID      "${CMAKE_C_COMPILER_ID}")
set(COMPILER_VERSION "${CMAKE_C_COMPILER_VERSION}")

if(CMAKE_C_COMPILER_ID STREQUAL "GNU")
    set(COMPILER_GCC   1)
    set(COMPILER_CLANG 0)
    set(COMPILER_MSVC  0)
elseif(CMAKE_C_COMPILER_ID MATCHES "Clang")
    set(COMPILER_GCC   0)
    set(COMPILER_CLANG 1)
    set(COMPILER_MSVC  0)
elseif(CMAKE_C_COMPILER_ID STREQUAL "MSVC")
    set(COMPILER_GCC   0)
    set(COMPILER_CLANG 0)
    set(COMPILER_MSVC  1)
else()
    set(COMPILER_GCC   0)
    set(COMPILER_CLANG 0)
    set(COMPILER_MSVC  0)
endif()

# Pointer / word size 
set(POINTER_SIZE "${CMAKE_SIZEOF_VOID_P}")   # 4 or 8
math(EXPR POINTER_BITS "${CMAKE_SIZEOF_VOID_P} * 8")

# CPU count 
include(ProcessorCount)
ProcessorCount(CPU_COUNT)
if(CPU_COUNT EQUAL 0)
    set(CPU_COUNT 1)  # fallback if detection fails
endif()

# Build type 
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(BUILD_DEBUG   1)
    set(BUILD_RELEASE 0)
else()
    set(BUILD_DEBUG   0)
    set(BUILD_RELEASE 1)
endif()

set(VERSION_MAJOR "${PROJECT_VERSION_MAJOR}")
set(VERSION_MINOR "${PROJECT_VERSION_MINOR}")
set(VERSION_PATCH "${PROJECT_VERSION_PATCH}")
set(VERSION_STRING "${PROJECT_VERSION}")
