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

#include "tlclib/intf/os/compiler.h"

#include <stddef.h>

#define container_of(ptr, type, member) \
  ((type *)((char *)(ptr) - offsetof (type, member)))

#define is_alpha(c) \
  (((c) >= 'a' && (c) <= 'z') || ((c) >= 'A' && (c) <= 'Z') || (c) == '_')

#define is_num(c) ((c) >= '0' && (c) <= '9')

#define is_alpha_num(c) (is_alpha (c) || is_num (c))

#define is_friendly_punc(c) (c == '.' || c == '/' || c == '-')

#define is_alpha_num_generous(c) \
  (is_alpha (c) || is_num (c) || is_friendly_punc (c))

#define arrlen(a) (sizeof (a) / sizeof (*a))

#define MIN(a, b) ((a) < (b) ? (a) : (b))

#define MAX(a, b) ((a) > (b) ? (a) : (b))

#define STRINGIFY(x) #x

#define TOSTRING(x) STRINGIFY (x)

#define case_ENUM_RETURN_STRING(en) \
  case en:                          \
    return #en

#define CJOIN2(val) val, val
#define CJOIN3(val) val, val, val
#define CJOIN4(val) val, val, val, val
#define CJOIN5(val) val, val, val, val, val

#ifndef RH__CAT
#define RH__CAT(a, b) a##b
#define RH__XCAT(a, b) RH__CAT (a, b)
#endif

// Address: [ file type ] [ file number ] [ file offset ]
#define FILE_TYPE_BITS 4
#define FILE_NUM_BITS 36
#define FILE_OFST_BITS 24
