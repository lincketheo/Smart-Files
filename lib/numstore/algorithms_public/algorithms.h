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

#include "numstore/algorithms_internal/rope/algorithms.h"

#include "tlclib/memory/chunk_alloc.h"
#include "tlclib/dev/error.h"
#include "tlclib/memory/malloc_plan.h"
#include "tlclib/ds/stream.h"
#include "tlclib/ds/string.h"
#include "tlclib/intf/os/memory.h"
#include "numstore.h"
#include "numstore/errors.h"
#include "numstore/types.h"
#include "paging/pager.h"
#include "paging/pager/page_h.h"
