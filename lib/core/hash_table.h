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

#include "core/assert.h"
#include "core/error.h"
#include "core/latch.h"
#include "core/llist.h"

struct hnode
{
  struct hnode *next;
  u32 hcode;
};

HEADER_FUNC void
hnode_init (struct hnode *dest, const u32 hcode)
{
  dest->hcode = hcode;
  dest->next = NULL;
}

struct htable
{
  u32 cap;
  u32 size;
  latch latch;
  struct hnode *table[];
};

struct htable *htable_create (u32 n, error *e);
void htable_free (struct htable *t);

void htable_insert (struct htable *t, struct hnode *node);
struct hnode **htable_lookup (
    struct htable *t,
    const struct hnode *key,
    bool (*eq) (const struct hnode *, const struct hnode *));
struct hnode *htable_delete (struct htable *t, struct hnode **from);
struct hnode **htable_random (struct htable *t);

// Simple getters
HEADER_FUNC u32
htable_size (const struct htable *t)
{
  return t->size;
}

// Iterator
void htable_foreach (const struct htable *t,
                     void (*action) (struct hnode *v, void *ctx),
                     void *ctx);
