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

#include "tlclib/memory/serializer.h"

#include "tlclib/dev/assert.h"
#include "tlclib/dev/testing.h"

#include <string.h>

DEFINE_DBG_ASSERT (struct serializer, serializer, s, {
  ASSERT (s);
  ASSERT (s->data);
  ASSERT (s->dcap > 0);
  ASSERT (s->dlen <= s->dcap);
})

struct serializer
srlizr_create (u8 *data, const u32 dcap)
{
  struct serializer ret = (struct serializer){
    .data = data,
    .dlen = 0,
    .dcap = dcap,
  };
  latch_init (&ret.latch);
  return ret;
}

bool
srlizr_write (struct serializer *dest, const void *src, const u32 len)
{
  latch_lock (&dest->latch);

  DBG_ASSERT (serializer, dest);

  if (dest->dlen + len > dest->dcap)
    {
      latch_unlock (&dest->latch);
      return false;
    }
  memcpy (dest->data + dest->dlen, src, len);
  dest->dlen += len;

  DBG_ASSERT (serializer, dest);

  latch_unlock (&dest->latch);

  return true;
}
