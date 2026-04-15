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

// #pragma once - ignore this

#include "core/assert.h"
#include "core/ht_models.h"
#include "intf/os.h"
#include "numstore/types.h"

#include <string.h>

#ifndef KTYPE
#define KTYPE int
#endif
#ifndef VTYPE
#define VTYPE int
#endif
#ifndef SUFFIX
#define SUFFIX int
#endif

#define HDATA_T RH__XCAT (hdata_, SUFFIX)
#define HENWRAP_T RH__XCAT (hentry_, SUFFIX)
#define HTIR_T RH__XCAT (htir_res_, SUFFIX)
#define HT_T RH__XCAT (ht_, SUFFIX)
#define HASH_TABLE_T RH__XCAT (hash_table_, SUFFIX)

// Functions
#define HT_INIT RH__XCAT (ht_init_, SUFFIX)
#define HT_INSERT RH__XCAT (ht_insert_, SUFFIX)
#define HT_INSERT_EXPECT RH__XCAT (ht_insert_expect_, SUFFIX)
#define HT_GET RH__XCAT (ht_get_, SUFFIX)
#define HT_GET_EXPECT RH__XCAT (ht_get_expect_, SUFFIX)
#define HT_DELETE RH__XCAT (ht_delete_, SUFFIX)
#define HT_COUNT RH__XCAT (ht_count_, SUFFIX)
#define HT_DELETE_EXPECT RH__XCAT (ht_delete_expect_, SUFFIX)

/// @cond INTERNAL
typedef struct
{
  KTYPE key; // Hash key
  VTYPE value;
} HDATA_T;

/// @cond INTERNAL
typedef struct
{
  HDATA_T data; // The data we store
  KTYPE dib;    // Distance from initial bucket
  bool present; // Exists or not
} HENWRAP_T;

/// @cond INTERNAL
typedef struct
{
  u32 cap;
  HENWRAP_T *elems;
} HASH_TABLE_T;

HEADER_FUNC void
HT_INIT (HASH_TABLE_T *dest, HENWRAP_T *arr, const u32 nelem)
{
  ASSERT (dest);
  ASSERT (arr);

  memset (arr, 0, nelem * sizeof *arr);
  dest->elems = arr;
  dest->cap = nelem;
}

HEADER_FUNC hti_res
HT_INSERT (HASH_TABLE_T *ht, HDATA_T data)
{
  ASSERT (ht);
  ASSERT (ht->cap > 0);

  KTYPE dibn = 0; // Current distance from initial bucket
  hti_res ret = HTIR_FULL;

  for (KTYPE i = 0; i < (KTYPE)ht->cap; ++i, ++dibn)
    {
      // Mapped index after probing
      KTYPE _i = (data.key + dibn) % (KTYPE)ht->cap;

      // If not present, insert
      if (!ht->elems[_i].present)
        {
          ht->elems[_i].data = data;
          ht->elems[_i].dib = dibn;
          ht->elems[_i].present = true;
          ret = HTIR_SUCCESS;
          goto theend;
        }

      // Swap (lt means dib != dibn, therefore key != key)
      if (ht->elems[_i].dib < dibn)
        {
          HDATA_T temp_data = ht->elems[_i].data;
          KTYPE temp_dib = ht->elems[_i].dib;

          ht->elems[_i].data = data;
          ht->elems[_i].dib = dibn;

          dibn = temp_dib;
          data = temp_data;
        }

      // Compare keys for duplicates
      if (ht->elems[_i].data.key == data.key)
        {
          ret = HTIR_EXISTS;
          goto theend;
        }
    }

theend:
  return ret;
}

HEADER_FUNC void
HT_INSERT_EXPECT (HASH_TABLE_T *ht, HDATA_T data)
{
  const hti_res ret = HT_INSERT (ht, data);
  ASSERT (ret == HTIR_SUCCESS);
}

HEADER_FUNC hta_res
HT_GET (HASH_TABLE_T *ht, HDATA_T *dest, KTYPE key)
{
  ASSERT (ht);
  ASSERT (ht->cap > 0);

  KTYPE dibn = 0;

  hta_res ret = HTAR_DOESNT_EXIST;

  for (KTYPE i = 0; i < (KTYPE)ht->cap; ++i, ++dibn)
    {
      // Mapped index after probing
      KTYPE _i = (key + i) % (KTYPE)ht->cap;

      // If not present, return
      if (!ht->elems[_i].present)
        {
          ret = HTAR_DOESNT_EXIST;
          goto theend;
        }

      // Short cut - DIB invariant is broken
      if (ht->elems[_i].dib < dibn)
        {
          ret = HTAR_DOESNT_EXIST;
          goto theend;
        }

      // Check for key
      if (ht->elems[_i].data.key == key)
        {
          if (dest)
            {
              *dest = (HDATA_T){
                .value = ht->elems[_i].data.value,
                .key = ht->elems[_i].data.key,
              };
            }
          ret = HTAR_SUCCESS;
          goto theend;
        }
    }

theend:
  return ret;
}

HEADER_FUNC void
HT_GET_EXPECT (HASH_TABLE_T *ht, HDATA_T *dest, KTYPE key)
{
  const hta_res ret = HT_GET (ht, dest, key);
  ASSERT (ret == HTAR_SUCCESS);
}

HEADER_FUNC hta_res
HT_DELETE (HASH_TABLE_T *ht, HDATA_T *dest, KTYPE key)
{
  ASSERT (ht);
  ASSERT (ht->cap > 0);

  KTYPE dibn = 0;
  KTYPE i = 0;

  hta_res ret = HTAR_DOESNT_EXIST;

  for (i = 0; i < (KTYPE)ht->cap; ++i, ++dibn)
    {
      // Mapped index after probing
      KTYPE _i = (key + i) % (KTYPE)ht->cap;

      // If not present, return
      if (!ht->elems[_i].present)
        {
          ret = HTAR_DOESNT_EXIST;
          goto theend;
        }

      // Short cut - DIB invariant is broken
      if (ht->elems[_i].dib < dibn)
        {
          ret = HTAR_DOESNT_EXIST;
          goto theend;
        }

      // Check for key
      if (ht->elems[_i].data.key == key)
        {
          if (dest)
            {
              *dest = ht->elems[_i].data;
            }
          break;
        }
    }

  // Shift all full elements to the left
  for (; i < (KTYPE)ht->cap; ++i)
    {
      // Mapped index after probing
      KTYPE hole = (key + i) % (KTYPE)ht->cap;
      KTYPE next = (key + i + 1) % (KTYPE)ht->cap;

      // Right value is empty, set this to empty and return
      if (!ht->elems[next].present || ht->elems[next].dib == 0)
        {
          ht->elems[hole].present = false;
          ret = HTAR_SUCCESS;
          goto theend;
        }

      // Shift left
      ht->elems[hole].data = ht->elems[next].data;
      ASSERT (ht->elems[next].dib > 0);
      ht->elems[hole].dib = ht->elems[next].dib - 1;
      ASSERT (ht->elems[hole].present);
    }

theend:
  return ret;
}

HEADER_FUNC u32
HT_COUNT (HASH_TABLE_T *ht)
{
  ASSERT (ht);
  ASSERT (ht->cap > 0);

  u32 ret = 0;

  for (u32 i = 0; i < ht->cap; ++i)
    {
      if (ht->elems[i].present)
        {
          ret++;
        }
    }

  return ret;
}

HEADER_FUNC void
HT_DELETE_EXPECT (HASH_TABLE_T *ht, HDATA_T *dest, KTYPE key)
{
  const hta_res ret = HT_DELETE (ht, dest, key);
  ASSERT (ret == HTAR_SUCCESS);
}

#undef HDATA_T
#undef HENWRAP_T
#undef HTIR_T
#undef HT_T
#undef HASH_TABLE_T
#undef HT_INIT
#undef HT_INSERT
#undef HT_INSERT_EXPECT
#undef HT_GET
#undef HT_GET_EXPECT
#undef HT_DELETE
#undef HT_COUNT
#undef HT_DELETE_EXPECT
