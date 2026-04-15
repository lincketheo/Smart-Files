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

#include "tlclib/dev/assert.h"
#include "tlclib/ds/ht_models.h"
#include "tlclib/intf/os.h"

#ifndef VTYPE
#define VTYPE int
#endif
#ifndef SUFFIX
#define SUFFIX int
#endif
#ifndef HASH_FUNC
#define HASH_FUNC(v) ((u32)(v))
#endif
#ifndef CMP_FUNC
#define CMP_FUNC(a, b) ((a) == (b))
#endif

#define HENWRAP_T RH__XCAT (hentry_, SUFFIX)
#define HASH_TABLE_T RH__XCAT (hash_table_, SUFFIX)

#define HT_INIT RH__XCAT (ht_init_, SUFFIX)
#define HT_INSERT RH__XCAT (ht_insert_, SUFFIX)
#define HT_INSERT_EXPECT RH__XCAT (ht_insert_expect_, SUFFIX)
#define HT_GET RH__XCAT (ht_get_, SUFFIX)
#define HT_GET_EXPECT RH__XCAT (ht_get_expect_, SUFFIX)
#define HT_DELETE RH__XCAT (ht_delete_, SUFFIX)
#define HT_DELETE_EXPECT RH__XCAT (ht_delete_expect_, SUFFIX)

typedef struct
{
  VTYPE value;  // The value we store
  u32 hash;     // Hash of the value
  u32 dib;      // Distance from initial bucket
  bool present; // Exists or not
} HENWRAP_T;

typedef struct
{
  u32 cap;
  HENWRAP_T *elems;
} HASH_TABLE_T;

HEADER_FUNC void
HT_INIT (HASH_TABLE_T *dest, HENWRAP_T *arr, u32 nelem)
{
  ASSERT (dest);
  ASSERT (arr);

  memset (arr, 0, nelem * sizeof *arr);
  dest->elems = arr;
  dest->cap = nelem;
}

HEADER_FUNC hti_res
HT_INSERT (HASH_TABLE_T *ht, VTYPE value)
{
  ASSERT (ht);
  ASSERT (ht->cap > 0);

  u32 hash = HASH_FUNC (value);
  u32 dibn = 0; // Current distance from initial bucket

  for (u32 i = 0; i < ht->cap; ++i, ++dibn)
    {
      // Mapped index after probing
      u32 _i = (hash + dibn) % ht->cap;

      // If not present, insert
      if (!ht->elems[_i].present)
        {
          ht->elems[_i].value = value;
          ht->elems[_i].hash = hash;
          ht->elems[_i].dib = dibn;
          ht->elems[_i].present = true;
          return HTIR_SUCCESS;
        }

      // Swap (lt means dib != dibn, therefore different
      // values)
      if (ht->elems[_i].dib < dibn)
        {
          VTYPE temp_value = ht->elems[_i].value;
          u32 temp_hash = ht->elems[_i].hash;
          u32 temp_dib = ht->elems[_i].dib;

          ht->elems[_i].value = value;
          ht->elems[_i].hash = hash;
          ht->elems[_i].dib = dibn;

          dibn = temp_dib;
          value = temp_value;
          hash = temp_hash;
        }

      // Compare values for duplicates
      if (ht->elems[_i].hash == hash && CMP_FUNC (ht->elems[_i].value, value))
        {
          return HTIR_EXISTS;
        }
    }

  return HTIR_FULL;
}

HEADER_FUNC void
HT_INSERT_EXPECT (HASH_TABLE_T *ht, VTYPE value)
{
  hti_res ret = HT_INSERT (ht, value);
  ASSERT (ret == HTIR_SUCCESS);
}

HEADER_FUNC hta_res
HT_GET (const HASH_TABLE_T *ht, VTYPE *dest, VTYPE value)
{
  ASSERT (ht);
  ASSERT (ht->cap > 0);

  u32 hash = HASH_FUNC (value);
  u32 dibn = 0;

  for (u32 i = 0; i < ht->cap; ++i, ++dibn)
    {
      // Mapped index after probing
      u32 _i = (hash + i) % ht->cap;

      // If not present, return
      if (!ht->elems[_i].present)
        {
          return HTAR_DOESNT_EXIST;
        }

      // Short cut - DIB invariant is broken
      if (ht->elems[_i].dib < dibn)
        {
          return HTAR_DOESNT_EXIST;
        }

      // Check for value match
      if (ht->elems[_i].hash == hash && CMP_FUNC (ht->elems[_i].value, value))
        {
          if (dest)
            {
              *dest = ht->elems[_i].value;
            }
          return HTAR_SUCCESS;
        }
    }

  return HTAR_DOESNT_EXIST;
}

HEADER_FUNC void
HT_GET_EXPECT (const HASH_TABLE_T *ht, VTYPE *dest, VTYPE value)
{
  hta_res ret = HT_GET (ht, dest, value);
  ASSERT (ret == HTAR_SUCCESS);
}

HEADER_FUNC hta_res
HT_DELETE (HASH_TABLE_T *ht, VTYPE *dest, VTYPE value)
{
  ASSERT (ht);
  ASSERT (ht->cap > 0);

  u32 hash = HASH_FUNC (value);
  u32 dibn = 0;
  u32 i = 0;

  for (i = 0; i < ht->cap; ++i, ++dibn)
    {
      // Mapped index after probing
      u32 _i = (hash + i) % ht->cap;

      // If not present, return
      if (!ht->elems[_i].present)
        {
          return HTAR_DOESNT_EXIST;
        }

      // Short cut - DIB invariant is broken
      if (ht->elems[_i].dib < dibn)
        {
          return HTAR_DOESNT_EXIST;
        }

      // Check for value match
      if (ht->elems[_i].hash == hash && CMP_FUNC (ht->elems[_i].value, value))
        {
          if (dest)
            {
              *dest = ht->elems[_i].value;
            }
          break;
        }
    }

  // Shift all full elements to the left
  for (; i < ht->cap; ++i)
    {
      // Mapped index after probing
      u32 hole = (hash + i) % ht->cap;
      u32 next = (hash + i + 1) % ht->cap;

      // Right value is empty, set this to empty and return
      if (!ht->elems[next].present || ht->elems[next].dib == 0)
        {
          ht->elems[hole].present = false;
          return HTAR_SUCCESS;
        }

      // Shift left
      ht->elems[hole].value = ht->elems[next].value;
      ht->elems[hole].hash = ht->elems[next].hash;
      ASSERT (ht->elems[next].dib > 0);
      ht->elems[hole].dib = ht->elems[next].dib - 1;
      ASSERT (ht->elems[hole].present);
    }

  return HTAR_DOESNT_EXIST;
}

HEADER_FUNC void
HT_DELETE_EXPECT (HASH_TABLE_T *ht, VTYPE *dest, VTYPE value)
{
  hta_res ret = HT_DELETE (ht, dest, value);
  ASSERT (ret == HTAR_SUCCESS);
}

#undef HENWRAP_T
#undef HASH_TABLE_T
#undef HT_INIT
#undef HT_INSERT
#undef HT_INSERT_EXPECT
#undef HT_GET
#undef HT_GET_EXPECT
#undef HT_DELETE
#undef HT_DELETE_EXPECT
#undef VTYPE
#undef SUFFIX
#undef HASH_FUNC
#undef CMP_FUNC
