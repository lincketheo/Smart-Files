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

#include "numstore/stdtypes.h"

struct stride
{
  b_size start;
  b_size stride;
  b_size nelems;
};

struct user_stride
{
  sb_size start;
  sb_size step;
  sb_size stop;
  int present;
};

struct string
{
  u32 len;
  const char *data;
};

struct type
{
  enum type_t
  {
    T_PRIM = 0,
    T_STRUCT = 1,
    T_UNION = 2,
    T_ENUM = 3,
    T_SARRAY = 4,
  } type;

  union
  {
    enum prim_t
    {
      U8 = 0,
      U16 = 1,
      U32 = 2,
      U64 = 3,
      I8 = 4,
      I16 = 5,
      I32 = 6,
      I64 = 7,
      F16 = 8,
      F32 = 9,
      F64 = 10,
      F128 = 11,
      CF32 = 12,
      CF64 = 13,
      CF128 = 14,
      CF256 = 15,
      CI16 = 16,
      CI32 = 17,
      CI64 = 18,
      CI128 = 19,
      CU16 = 20,
      CU32 = 21,
      CU64 = 22,
      CU128 = 23,
    } p;

    struct struct_t
    {
      u16 len;
      struct string *keys;
      struct type **types;
    } st;

    struct union_t
    {
      u16 len;
      struct string *keys;
      struct type **types;
    } un;

    struct enum_t
    {
      u16 len;
      struct string *keys;
    } en;

    struct sarray_t
    {
      u16 rank;
      u32 *dims;
      struct type *t;
    } sa;
  };
};

void type_free (struct type *t);

struct variable
{
  struct string vname;
  struct type *dtype;
  pgno var_root;
  pgno rpt_root;
  b_size nbytes;
};

void variable_free (struct variable *v);

enum ta_type
{
  TA_TAKE,
  TA_SELECT,
  TA_RANGE,
};

struct type_accessor
{
  enum ta_type type;

  union
  {
    struct select_ta
    {
      struct string key;
      struct type_accessor *sub_ta;
    } select;

    struct range_ta
    {
      struct user_stride *dim_accessors;
      u32 dlen;
      struct type_accessor *sub_ta;
    } range;
  };
};

enum type_ref_t
{
  TR_TAKE,
  TR_STRUCT,
};

struct type_ref
{
  enum type_ref_t type;

  union
  {
    struct take_tr
    {
      struct string vname;
      struct type_accessor ta;
    } tk;

    struct struct_tr
    {
      u16 len;
      struct string *keys;
      struct type_ref *types;
    } st;
  };
};
