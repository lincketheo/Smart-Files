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

#include "core/chunk_alloc.h"
#include "core/llist.h"
#include "core/signatures.h"
#include "numstore/types.h"

#define STOP_PRESENT (1 << 0)
#define STEP_PRESENT (1 << 1)
#define START_PRESENT (1 << 2)
#define COLON_PRESENT (1 << 3)

#define USER_STRIDE_ALL              \
  ((struct user_stride){ .start = 0, \
                         .step = 1,  \
                         .stop = 0,  \
                         .present = STEP_PRESENT | START_PRESENT })

bool ustride_equal (struct user_stride left, struct user_stride right);
void stride_resolve_expect (struct stride *dest, struct user_stride src,
                            b_size arrlen);
err_t stride_resolve (struct stride *dest, struct user_stride src,
                      b_size arrlen, error *e);

////////////////////////////////////////////////////////////
/// Small Constructors

HEADER_FUNC struct user_stride
ustride_single (const sb_size start)
{
  return (struct user_stride){
    .start = start,
    .present = START_PRESENT,
  };
}

HEADER_FUNC struct user_stride
ustride012 (const sb_size start, const sb_size step, const sb_size stop)
{
  return (struct user_stride){
    .start = start,
    .step = step,
    .stop = stop,
    .present = STOP_PRESENT | STEP_PRESENT | START_PRESENT | COLON_PRESENT,
  };
}

HEADER_FUNC struct user_stride
ustride01 (const sb_size start, const sb_size step)
{
  return (struct user_stride){
    .start = start,
    .step = step,
    .present = STEP_PRESENT | START_PRESENT | COLON_PRESENT,
  };
}

HEADER_FUNC struct user_stride
ustride0 (const sb_size start)
{
  return (struct user_stride){
    .start = start,
    .present = START_PRESENT | COLON_PRESENT,
  };
}

HEADER_FUNC struct user_stride
ustride12 (const sb_size step, const sb_size stop)
{
  return (struct user_stride){
    .step = step,
    .stop = stop,
    .present = STOP_PRESENT | STEP_PRESENT | COLON_PRESENT,
  };
}

HEADER_FUNC struct user_stride
ustride1 (const sb_size step)
{
  return (struct user_stride){
    .step = step,
    .present = STEP_PRESENT | START_PRESENT | COLON_PRESENT,
  };
}

HEADER_FUNC struct user_stride
ustride2 (const sb_size stop)
{
  return (struct user_stride){
    .stop = stop,
    .present = STOP_PRESENT | COLON_PRESENT,
  };
}

HEADER_FUNC struct user_stride
ustride (void)
{
  return (struct user_stride){
    .present = COLON_PRESENT,
  };
}

HEADER_FUNC struct user_stride
usfrms (const struct stride str)
{
  return ustride012 (str.start, str.stride,
                     str.start + str.stride * str.nelems);
}

struct multi_user_stride
{
  struct user_stride *strides;
  u32 len;
};

struct mus_llnode
{
  struct user_stride stride;
  struct llnode link;
};

struct mus_builder
{
  struct llnode *head;
  struct chunk_alloc *temp;
  struct chunk_alloc *persistent;
};

void musb_create (struct mus_builder *dest, struct chunk_alloc *temp,
                  struct chunk_alloc *persistent);

err_t musb_accept_key (struct mus_builder *eb, struct user_stride stride,
                       error *e);
err_t musb_build (struct multi_user_stride *persistent, const struct mus_builder *eb,
                  error *e);
