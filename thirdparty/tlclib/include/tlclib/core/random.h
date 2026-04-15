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

#include "tlclib/ds/string.h"
#include "tlclib/memory/chunk_alloc.h"

void rand_seed (void);
void rand_seed_with (u32 seed);

u8 randu8 (void);
i8 randi8 (void);
u8 randu8r (u8 lower, u8 upper);
i8 randi8r (i8 lower, i8 upper);

u16 randu16 (void);
i16 randi16 (void);
u16 randu16r (u16 lower, u16 upper);
i16 randi16r (i16 lower, i16 upper);

u32 randu32 (void);
i32 randi32 (void);
u32 randu32r (u32 lower, u32 upper);
i32 randi32r (i32 lower, i32 upper);

u64 randu64 (void);
i64 randi64 (void);
u64 randu64r (u64 lower, u64 upper);
i64 randi64r (i64 lower, i64 upper);

err_t rand_str (struct string *dest, struct chunk_alloc *alloc, u32 minlen, u32 maxlen, error *e);

void rand_bytes (void *dest, u32 len);
#define decl_rand_buffer(name, type, len) \
  type name[len];                         \
  rand_bytes (name, sizeof (type) * len);

void shuffle_u32 (u32 *array, u32 len);
