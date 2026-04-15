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

#include <complex.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>

// Unsigned shorthands
typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;

// Signed shorthands
typedef int8_t i8;
typedef int16_t i16;
typedef int32_t i32;
typedef int64_t i64;

// Float shorthands
typedef u16 f16;
typedef float f32;
typedef double f64;
typedef long double f128;

// Complex floats

#ifdef _MSC_VER

typedef _Fcomplex cf32;
typedef _Dcomplex cf64;
typedef _Lcomplex cf128;

#else

typedef u16 cf32[2];
typedef u32 cf64[2];
typedef u64 cf128[2];
// #if defined(__SIZEOF_FLOAT128__)
//     typedef __float128 _Complex cf256;
//  #endif

#endif

typedef i8 ci16[2];
typedef i16 ci32[2];
typedef i32 ci64[2];
typedef i64 ci128[2];

typedef u8 cu16[2];
typedef u16 cu32[2];
typedef u32 cu64[2];
typedef u64 cu128[2];

// Maximum unsigned
#define U8_MAX ((u8) ~(u8)0)
#define U16_MAX ((u16) ~(u16)0)
#define U32_MAX ((u32) ~(u32)0)
#define U64_MAX ((u64) ~(u64)0)

// Maximum signed
#define I8_MAX ((i8)(U8_MAX >> 1))
#define I16_MAX ((i16)(U16_MAX >> 1))
#define I32_MAX ((i32)(U32_MAX >> 1))
#define I64_MAX ((i64)(U64_MAX >> 1))

// Max of both negative and positive
#define I8_ABS_MAX ((u8)(I8_MAX) + 1)
#define I16_ABS_MAX ((u16)(I16_MAX) + 1)
#define I32_ABS_MAX ((u32)(I32_MAX) + 1)
#define I64_ABS_MAX ((u64)(I64_MAX) + 1)

// Minimum signed
#define I8_MIN ((i8)(~I8_MAX))
#define I16_MIN ((i16)(~I16_MAX))
#define I32_MIN ((i32)(~I32_MAX))
#define I64_MIN ((i64)(~I64_MAX))

#define F16_MAX 65504.0f
#define F32_MAX 3.4028235e+38f
#define F64_MAX 1.7976931348623157e+308
#define F128_MAX FLT128_MAX
#define F256_MAX 1.6113e+78913L

#define F16_MIN (-65504.0f)
#define F32_MIN (-3.4028235e+38f)
#define F64_MIN (-1.7976931348623157e+308)
#define F128_MIN FLT128_MIN
#define F256_MIN (-1.6113e+78913L)
