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

#include "tlclib/dev/error.h"

err_t parse_i32_expect (i32 *dest, const char *data, u32 len, error *e);
err_t parse_i64_expect (i64 *dest, const char *data, u32 len, error *e);
err_t parse_f32_expect (f32 *dest, const char *s, u32 len, error *e);
f32 py_mod_f32 (f32 num, f32 denom);
i32 py_mod_i32 (i32 num, i32 denom);
