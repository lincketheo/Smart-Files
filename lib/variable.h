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
#include "smfile.h"

struct variable
{
  struct string vname; // Name of this variable
  pgno var_root;       // The root of this page
  pgno rpt_root;       // The root of the rptree
  b_size nbytes;       // Size of this variable
};

HEADER_FUNC b_size
var_resolve_index (struct variable *v, sb_size bofst)
{
  // Translate negative
  if (bofst < 0)
    {
      bofst = v->nbytes + bofst;
    }

  // was so negative it's still negative after conversion
  if (bofst < 0)
    {
      bofst = 0;
    }

  // Translate indexes past nybtes
  if ((b_size)bofst > v->nbytes) // also: > not >=, so nbytes itself is valid (append)
    {
      bofst = v->nbytes;
    }

  return bofst;
}

HEADER_FUNC b_size
var_resolve_nelem (struct variable *v, b_size bofst, b_size nelem, t_size size)
{
  b_size remainder = (v->nbytes - bofst) / size;
  if (nelem > remainder)
    {
      nelem = remainder;
    }
  return nelem;
}
