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

#include "tlclib/core/checksums.h"

#include "tlclib/dev/assert.h"
#include "tlclib/dev/testing.h"

static u32 _crc32c_tbl[256];
static int _crc32c_inited = 0;

static void
_crc32c_init (void)
{
  for (u32 i = 0; i < 256; ++i)
    {
      u32 c = i;
      for (int k = 0; k < 8; ++k)
        {
          c = (c >> 1) ^ (0x82F63B78u & -((int)(c & 1)));
        }
      _crc32c_tbl[i] = c;
    }
  _crc32c_inited = 1;
}

u32
checksum_init (void)
{
  return 0;
}

void
checksum_execute (u32 *state, const u8 *data, const u32 len)
{
  ASSERT (state);
  ASSERT (data);
  ASSERT (len > 0);

  if (!_crc32c_inited)
    {
      _crc32c_init ();
    }

  u32 c = ~(*state);
  for (u32 i = 0; i < len; ++i)
    {
      c = (c >> 8) ^ _crc32c_tbl[(c ^ data[i]) & 0xFF];
    }
  *state = ~c;
}

#ifndef NTEST
TEST (checksum_execute_simple)
{
  const u8 data[] = { 1, 2, 3, 4 };
  u32 state = checksum_init ();
  checksum_execute (&state, data, 4);

  // Should produce some non-zero checksum

  test_assert (state != 0);
}

TEST (checksum_execute_deterministic)
{
  const u8 data[] = { 5, 10, 15, 20 };
  u32 state1 = checksum_init ();
  u32 state2 = checksum_init ();

  checksum_execute (&state1, data, 4);
  checksum_execute (&state2, data, 4);

  test_assert_equal (state1, state2);
}

TEST (checksum_execute_incremental)
{
  const u8 data[] = { 1, 2, 3, 4, 5, 6 };

  // All at once
  u32 state1 = checksum_init ();
  checksum_execute (&state1, data, 6);

  // Incremental
  u32 state2 = checksum_init ();
  checksum_execute (&state2, data, 3);
  checksum_execute (&state2, data + 3, 3);

  test_assert_equal (state1, state2);
}
#endif
