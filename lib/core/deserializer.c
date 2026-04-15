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

#include "core/deserializer.h"

#include "core/assert.h"

#include <string.h>

DEFINE_DBG_ASSERT (struct deserializer, deserializer, s, {
  ASSERT (s);
  ASSERT (s->data);
  ASSERT (s->dlen > 0);
  ASSERT (s->head <= s->dlen);
})

struct deserializer
dsrlizr_create (const u8 *data, const u32 dlen)
{
  struct deserializer ret = (struct deserializer){
    .data = data,
    .head = 0,
    .dlen = dlen,
  };
  latch_init (&ret.latch);
  return ret;
}

bool
dsrlizr_read (void *dest, const u32 dlen, struct deserializer *src)
{
  latch_lock (&src->latch);

  DBG_ASSERT (deserializer, src);

  if (src->head + dlen > src->dlen)
    {
      latch_unlock (&src->latch);
      return false;
    }
  memcpy (dest, src->data + src->head, dlen);
  src->head += dlen;

  DBG_ASSERT (deserializer, src);

  latch_unlock (&src->latch);

  return true;
}
