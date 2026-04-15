# This file has all the in code constants to define and auto generates include/numstore/compile_config

# Application specific constants
set(NS_PAGE_SIZE        4096    CACHE STRING "Page size")
set(NS_MEMORY_PAGE_LEN  4096    CACHE STRING "Number of buffer pool slots")
set(NS_MAX_VSTR         8192    CACHE STRING "Maximum variable name length")
set(NS_MAX_TSTR         8192    CACHE STRING "Maximum serialized type string length")
set(NS_WAL_BUFFER_CAP   1048576 CACHE STRING "WAL buffer capacity in bytes")

# Platform 
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(NS_PLATFORM_LINUX 1)
    set(NS_PLATFORM_MACOS 0)
    set(NS_PLATFORM_WINDOWS 0)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(NS_PLATFORM_LINUX 0)
    set(NS_PLATFORM_MACOS 1)
    set(NS_PLATFORM_WINDOWS 0)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Windows")
    set(NS_PLATFORM_LINUX 0)
    set(NS_PLATFORM_MACOS 0)
    set(NS_PLATFORM_WINDOWS 1)
else()
    set(NS_PLATFORM_LINUX 0)
    set(NS_PLATFORM_MACOS 0)
    set(NS_PLATFORM_WINDOWS 0)
endif()

set(NS_PLATFORM_NAME "${CMAKE_SYSTEM_NAME}")
set(NS_PLATFORM_VERSION "${CMAKE_SYSTEM_VERSION}")

# Architecture 
if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|AMD64)$")
    set(NS_ARCH_X86_64  1)
    set(NS_ARCH_ARM64   0)
    set(NS_ARCH_32BIT   0)
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64|arm64)$")
    set(NS_ARCH_X86_64  0)
    set(NS_ARCH_ARM64   1)
    set(NS_ARCH_32BIT   0)
else()
    set(NS_ARCH_X86_64  0)
    set(NS_ARCH_ARM64   0)
    set(NS_ARCH_32BIT   1)
endif()

set(NS_ARCH_NAME "${CMAKE_SYSTEM_PROCESSOR}")

# Endianness 
include(TestBigEndian)
test_big_endian(NS_BIG_ENDIAN_DETECTED)
if(NS_BIG_ENDIAN_DETECTED)
    set(NS_BIG_ENDIAN    1)
    set(NS_LITTLE_ENDIAN 0)
else()
    set(NS_BIG_ENDIAN    0)
    set(NS_LITTLE_ENDIAN 1)
endif()

# Compiler 
set(NS_COMPILER_ID      "${CMAKE_C_COMPILER_ID}")
set(NS_COMPILER_VERSION "${CMAKE_C_COMPILER_VERSION}")

if(CMAKE_C_COMPILER_ID STREQUAL "GNU")
    set(NS_COMPILER_GCC   1)
    set(NS_COMPILER_CLANG 0)
    set(NS_COMPILER_MSVC  0)
elseif(CMAKE_C_COMPILER_ID MATCHES "Clang")
    set(NS_COMPILER_GCC   0)
    set(NS_COMPILER_CLANG 1)
    set(NS_COMPILER_MSVC  0)
elseif(CMAKE_C_COMPILER_ID STREQUAL "MSVC")
    set(NS_COMPILER_GCC   0)
    set(NS_COMPILER_CLANG 0)
    set(NS_COMPILER_MSVC  1)
else()
    set(NS_COMPILER_GCC   0)
    set(NS_COMPILER_CLANG 0)
    set(NS_COMPILER_MSVC  0)
endif()

# Pointer / word size 
set(NS_POINTER_SIZE "${CMAKE_SIZEOF_VOID_P}")   # 4 or 8
math(EXPR NS_POINTER_BITS "${CMAKE_SIZEOF_VOID_P} * 8")

# CPU count 
include(ProcessorCount)
ProcessorCount(NS_CPU_COUNT)
if(NS_CPU_COUNT EQUAL 0)
    set(NS_CPU_COUNT 1)  # fallback if detection fails
endif()

# Build type 
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(NS_BUILD_DEBUG   1)
    set(NS_BUILD_RELEASE 0)
else()
    set(NS_BUILD_DEBUG   0)
    set(NS_BUILD_RELEASE 1)
endif()

set(NS_VERSION_MAJOR "${PROJECT_VERSION_MAJOR}")
set(NS_VERSION_MINOR "${PROJECT_VERSION_MINOR}")
set(NS_VERSION_PATCH "${PROJECT_VERSION_PATCH}")
set(NS_VERSION_STRING "${PROJECT_VERSION}")

# Generate 
configure_file(
    "${CMAKE_SOURCE_DIR}/include/numstore/compile_config.h.in"
    "${CMAKE_BINARY_DIR}/include/numstore/compile_config.h"
    @ONLY
)

include_directories("${CMAKE_BINARY_DIR}/include")

install(
    FILES "${CMAKE_BINARY_DIR}/include/numstore/compile_config.h"
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/numstore"
    COMPONENT development
)
