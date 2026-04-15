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

#include "core/signatures.h"
#include "numstore/stdtypes.h"

struct bstack
{
  void *bytes;
  u32 cap;
  u32 len;
};

// Block until dlen bytes are available
HEADER_FUNC void
bstack_consumer_block (struct bstack *b, u32 dlen)
{
}

HEADER_FUNC void
bstack_producer_block (struct bstack *b, u32 dlen)
{
}

HEADER_FUNC struct bstack
bstack_empty (void *bytes, u32 cap)
{
  return (struct bstack){
    .bytes = bytes,
    .cap = cap,
    .len = 0,
  };
}

HEADER_FUNC struct bstack
bstack_full (void *bytes, u32 cap)
{
  return (struct bstack){
    .bytes = bytes,
    .cap = cap,
    .len = cap,
  };
}

HEADER_FUNC u8 *
bstack_head (struct bstack *b)
{
  return (u8 *)b->bytes + b->len;
}

HEADER_FUNC u32
bstack_remain (struct bstack *b)
{
  return b->cap - b->len;
}

HEADER_FUNC void
bstack_inc (struct bstack *b, u32 bytes)
{
  b->len += bytes;
}
