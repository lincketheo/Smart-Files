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

#include "c_specx.h"
#include "pages/page.h"

// OFFSETS and _Static_asserts
#define FS_BTMP_OFST PG_COMMN_END

#define FS_BTMP_SIZE (PAGE_SIZE - FS_BTMP_OFST)
#define FS_BTMP_NPGS ((PAGE_SIZE - FS_BTMP_OFST) * 8)

void fsm_init_empty (page *in);

HEADER_FUNC pgno
pgtofsm (const pgno pg)
{
  const pgno ret = pg / FS_BTMP_NPGS;
  return ret * FS_BTMP_NPGS;
}

HEADER_FUNC p_size
pgtoidx (const pgno pg)
{
  return pg % FS_BTMP_NPGS;
}

////////////////////////////////////////////////////////////
/// GETTERS

HEADER_FUNC const void *
fsm_get_bitmap_imut (const page *p)
{
  return (void *)&p->raw[FS_BTMP_OFST];
}

HEADER_FUNC void *
fsm_get_bitmap_mut (page *p)
{
  return (void *)&p->raw[FS_BTMP_OFST];
}

HEADER_FUNC p_size
fsm_get_bit (const page *p, const p_size idx)
{
  return (((const u8 *)fsm_get_bitmap_imut (p))[idx / 8] >> (idx % 8)) & 1;
}

HEADER_FUNC sp_size
fsm_next_freebit (const page *p, const p_size frombit)
{
  const void *data = fsm_get_bitmap_imut (p);
  const p_size frombyte = frombit / 8;

  // TODO - (25) optimize using u64's or u32's
  for (p_size byte_i = frombyte; byte_i < FS_BTMP_SIZE; ++byte_i)
    {
      u8 b = ~((u8 *)data)[byte_i];
      if (b == 0)
        {
          continue;
        }

      if (byte_i == frombyte)
        {
          b &= ~((1 << (frombit % 8)) - 1);
        }

      if (!b)
        {
          continue;
        }

      for (p_size bit = 0; bit < 8; ++bit)
        {
          if ((b >> bit) & 1)
            {
              return byte_i * 8 + bit;
            }
        }
    }

  return -1;
}

////////////////////////////////////////////////////////////
/// SETTERS

HEADER_FUNC void
fsm_set_bit (page *p, const p_size idx)
{
  ASSERT (idx < FS_BTMP_SIZE * 8);
  ((u8 *)fsm_get_bitmap_mut (p))[idx / 8] |= (1 << (idx % 8));
}

HEADER_FUNC void
fsm_clr_bit (page *p, const p_size idx)
{
  ASSERT (idx < PAGE_SIZE * 8);
  ((u8 *)fsm_get_bitmap_mut (p))[idx / 8] &= ~(1 << (idx % 8));
}

// Validation
err_t fsm_validate_for_db (const page *hl, error *e);

// Utils
void i_log_fsm (int level, const page *t);
