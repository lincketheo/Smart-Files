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

#include "algorithms/nsdb/var/algorithms.h"
#include "algorithms/smfile/_smfile.h"
#include "c_specx/dev/error.h"
#include "c_specx/memory/chunk_alloc.h"
#include "errors.h"
#include "smfile.h"

smfile_t *
smfile_new_context (smfile_t *ns)
{
  ns->e.cause_code = SUCCESS;
  ns->e.cmlen = 0;
  return _smfile_root_load (ns->root, &ns->e);
}
