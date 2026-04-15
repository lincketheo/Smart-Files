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

#include "error.h"
#include "signatures.h"

#include <math.h>
#include <stdint.h>

// clang-format off

// =============================================================================
// INTERNAL HELPERS (Simplifies MSVC bounds checking)
// =============================================================================

// Unsigned checks
#define _uadd_ok(a, b, max)      ((a) <= ((max) - (b)))
#define _usub_ok(a, b)           ((a) >= (b))
#define _umul_ok(a, b, max)      ((a) == 0 || (b) <= ((max) / (a)))

// Signed checks
#define _sadd_ok(a, b, min, max) (((b) > 0 && (a) <= ((max) - (b))) || ((b) <= 0 && (a) >= ((min) - (b))))
#define _ssub_ok(a, b, min, max) (((b) < 0 && (a) <= ((max) + (b))) || ((b) >= 0 && (a) >= ((min) + (b))))
#define _smul_ok(a, b, min, max) ((a) == 0 || (b) == 0 || \
                                  ((a) > 0 && (b) > 0 && (a) <= ((max) / (b))) || \
                                  ((a) < 0 && (b) < 0 && (a) >= ((max) / (b))) || \
                                  ((a) > 0 && (b) < 0 && (b) >= ((min) / (a))) || \
                                  ((a) < 0 && (b) > 0 && (a) >= ((min) / (b))))

// Division checks
#define _div_ok(b)               ((b) != 0)
#define _sdiv_ok(a, b, min)      ((b) != 0 && !((a) == (min) && (b) == -1))

// =============================================================================
// UNSIGNED INTEGERS
// =============================================================================

#if HAS_BUILTIN_OVERFLOW // GCC / Clang (Uses fast built-ins)

#define safe_add_u8(dest, a, b)  (!__builtin_add_overflow(a, b, dest))
#define safe_sub_u8(dest, a, b)  (!__builtin_sub_overflow(a, b, dest))
#define safe_mul_u8(dest, a, b)  (!__builtin_mul_overflow(a, b, dest))

#define safe_add_u16(dest, a, b) (!__builtin_add_overflow(a, b, dest))
#define safe_sub_u16(dest, a, b) (!__builtin_sub_overflow(a, b, dest))
#define safe_mul_u16(dest, a, b) (!__builtin_mul_overflow(a, b, dest))

#define safe_add_u32(dest, a, b) (!__builtin_add_overflow(a, b, dest))
#define safe_sub_u32(dest, a, b) (!__builtin_sub_overflow(a, b, dest))
#define safe_mul_u32(dest, a, b) (!__builtin_mul_overflow(a, b, dest))

#define safe_add_u64(dest, a, b) (!__builtin_add_overflow(a, b, dest))
#define safe_sub_u64(dest, a, b) (!__builtin_sub_overflow(a, b, dest))
#define safe_mul_u64(dest, a, b) (!__builtin_mul_overflow(a, b, dest))

#else // compiler without __builtin_overflow / compiler without __builtin_overflow

#define safe_add_u8(dest, a, b)  (_uadd_ok((u8)(a), (u8)(b), UINT8_MAX)  ? (*(dest) = (u8)(a) + (u8)(b), true) : false)
#define safe_sub_u8(dest, a, b)  (_usub_ok((u8)(a), (u8)(b))             ? (*(dest) = (u8)(a) - (u8)(b), true) : false)
#define safe_mul_u8(dest, a, b)  (_umul_ok((u8)(a), (u8)(b), UINT8_MAX)  ? (*(dest) = (u8)(a) * (u8)(b), true) : false)

#define safe_add_u16(dest, a, b) (_uadd_ok((u16)(a), (u16)(b), UINT16_MAX) ? (*(dest) = (u16)(a) + (u16)(b), true) : false)
#define safe_sub_u16(dest, a, b) (_usub_ok((u16)(a), (u16)(b))             ? (*(dest) = (u16)(a) - (u16)(b), true) : false)
#define safe_mul_u16(dest, a, b) (_umul_ok((u16)(a), (u16)(b), UINT16_MAX) ? (*(dest) = (u16)(a) * (u16)(b), true) : false)

#define safe_add_u32(dest, a, b) (_uadd_ok((u32)(a), (u32)(b), UINT32_MAX) ? (*(dest) = (u32)(a) + (u32)(b), true) : false)
#define safe_sub_u32(dest, a, b) (_usub_ok((u32)(a), (u32)(b))             ? (*(dest) = (u32)(a) - (u32)(b), true) : false)
#define safe_mul_u32(dest, a, b) (_umul_ok((u32)(a), (u32)(b), UINT32_MAX) ? (*(dest) = (u32)(a) * (u32)(b), true) : false)

#define safe_add_u64(dest, a, b) (_uadd_ok((u64)(a), (u64)(b), UINT64_MAX) ? (*(dest) = (u64)(a) + (u64)(b), true) : false)
#define safe_sub_u64(dest, a, b) (_usub_ok((u64)(a), (u64)(b))             ? (*(dest) = (u64)(a) - (u64)(b), true) : false)
#define safe_mul_u64(dest, a, b) (_umul_ok((u64)(a), (u64)(b), UINT64_MAX) ? (*(dest) = (u64)(a) * (u64)(b), true) : false)

#endif

// Unsigned Division (Standard across compilers)
#define safe_div_u8(dest, a, b)  (_div_ok(b) ? (*(dest) = (a) / (b), true) : false)
#define safe_div_u16(dest, a, b) (_div_ok(b) ? (*(dest) = (a) / (b), true) : false)
#define safe_div_u32(dest, a, b) (_div_ok(b) ? (*(dest) = (a) / (b), true) : false)
#define safe_div_u64(dest, a, b) (_div_ok(b) ? (*(dest) = (a) / (b), true) : false)

// =============================================================================
// SIGNED INTEGERS
// =============================================================================

#ifndef _MSC_VER // GCC / Clang

#define safe_add_i8(dest, a, b)  (!__builtin_add_overflow(a, b, dest))
#define safe_sub_i8(dest, a, b)  (!__builtin_sub_overflow(a, b, dest))
#define safe_mul_i8(dest, a, b)  (!__builtin_mul_overflow(a, b, dest))

#define safe_add_i16(dest, a, b) (!__builtin_add_overflow(a, b, dest))
#define safe_sub_i16(dest, a, b) (!__builtin_sub_overflow(a, b, dest))
#define safe_mul_i16(dest, a, b) (!__builtin_mul_overflow(a, b, dest))

#define safe_add_i32(dest, a, b) (!__builtin_add_overflow(a, b, dest))
#define safe_sub_i32(dest, a, b) (!__builtin_sub_overflow(a, b, dest))
#define safe_mul_i32(dest, a, b) (!__builtin_mul_overflow(a, b, dest))

#define safe_add_i64(dest, a, b) (!__builtin_add_overflow(a, b, dest))
#define safe_sub_i64(dest, a, b) (!__builtin_sub_overflow(a, b, dest))
#define safe_mul_i64(dest, a, b) (!__builtin_mul_overflow(a, b, dest))

#else // compiler without __builtin_overflow

#define safe_add_i8(dest, a, b)  (_sadd_ok((i8)(a), (i8)(b), INT8_MIN, INT8_MAX) ? (*(dest) = (i8)(a) + (i8)(b), true) : false)
#define safe_sub_i8(dest, a, b)  (_ssub_ok((i8)(a), (i8)(b), INT8_MIN, INT8_MAX) ? (*(dest) = (i8)(a) - (i8)(b), true) : false)
#define safe_mul_i8(dest, a, b)  (_smul_ok((i8)(a), (i8)(b), INT8_MIN, INT8_MAX) ? (*(dest) = (i8)(a) * (i8)(b), true) : false)

#define safe_add_i16(dest, a, b) (_sadd_ok((i16)(a), (i16)(b), INT16_MIN, INT16_MAX) ? (*(dest) = (i16)(a) + (i16)(b), true) : false)
#define safe_sub_i16(dest, a, b) (_ssub_ok((i16)(a), (i16)(b), INT16_MIN, INT16_MAX) ? (*(dest) = (i16)(a) - (i16)(b), true) : false)
#define safe_mul_i16(dest, a, b) (_smul_ok((i16)(a), (i16)(b), INT16_MIN, INT16_MAX) ? (*(dest) = (i16)(a) * (i16)(b), true) : false)

#define safe_add_i32(dest, a, b) (_sadd_ok((i32)(a), (i32)(b), INT32_MIN, INT32_MAX) ? (*(dest) = (i32)(a) + (i32)(b), true) : false)
#define safe_sub_i32(dest, a, b) (_ssub_ok((i32)(a), (i32)(b), INT32_MIN, INT32_MAX) ? (*(dest) = (i32)(a) - (i32)(b), true) : false)
#define safe_mul_i32(dest, a, b) (_smul_ok((i32)(a), (i32)(b), INT32_MIN, INT32_MAX) ? (*(dest) = (i32)(a) * (i32)(b), true) : false)

#define safe_add_i64(dest, a, b) (_sadd_ok((i64)(a), (i64)(b), INT64_MIN, INT64_MAX) ? (*(dest) = (i64)(a) + (i64)(b), true) : false)
#define safe_sub_i64(dest, a, b) (_ssub_ok((i64)(a), (i64)(b), INT64_MIN, INT64_MAX) ? (*(dest) = (i64)(a) - (i64)(b), true) : false)
#define safe_mul_i64(dest, a, b) (_smul_ok((i64)(a), (i64)(b), INT64_MIN, INT64_MAX) ? (*(dest) = (i64)(a) * (i64)(b), true) : false)

#endif

// Signed Division (Standard across compilers)
#define safe_div_i8(dest, a, b)  (_sdiv_ok((i8)(a), (i8)(b), INT8_MIN)  ? (*(dest) = (a) / (b), true) : false)
#define safe_div_i16(dest, a, b) (_sdiv_ok((i16)(a), (i16)(b), INT16_MIN) ? (*(dest) = (a) / (b), true) : false)
#define safe_div_i32(dest, a, b) (_sdiv_ok((i32)(a), (i32)(b), INT32_MIN) ? (*(dest) = (a) / (b), true) : false)
#define safe_div_i64(dest, a, b) (_sdiv_ok((i64)(a), (i64)(b), INT64_MIN) ? (*(dest) = (a) / (b), true) : false)

// =============================================================================
// FLOATING POINT
// =============================================================================

#define safe_add_f32(dest, a, b) ((*(dest) = (a) + (b)), isfinite(*(dest)))
#define safe_sub_f32(dest, a, b) ((*(dest) = (a) - (b)), isfinite(*(dest)))
#define safe_mul_f32(dest, a, b) ((*(dest) = (a) * (b)), isfinite(*(dest)))
#define safe_div_f32(dest, a, b) ((b) != 0.0f ? (*(dest) = (a) / (b), isfinite(*(dest))) : false)

#define safe_add_f64(dest, a, b) ((*(dest) = (a) + (b)), isfinite(*(dest)))
#define safe_sub_f64(dest, a, b) ((*(dest) = (a) - (b)), isfinite(*(dest)))
#define safe_mul_f64(dest, a, b) ((*(dest) = (a) * (b)), isfinite(*(dest)))
#define safe_div_f64(dest, a, b) ((b) != 0.0  ? (*(dest) = (a) / (b), isfinite(*(dest))) : false)

// =============================================================================
// ERROR HANDLING FUNCTIONS
// =============================================================================

HEADER_FUNC err_t safe_add_u64_err(u64 *dest, const u64 a, const u64 b, error *e)
{
    if (!safe_add_u64(dest, a, b)) {
        return error_causef(e, ERR_ARITH, "Overflow");
    }
    return SUCCESS;
}

// clang-format on
