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

#include "algorithms/smfile/smfile.h"
#include "c_specx.h"
#include "pager.h"
#include "smfile.h"

static err_t
_smfile_close (struct smfile *n, error *e)
{
  pgr_close (n->root->db.p, e);
  i_free (n);
  return error_trace (e);
}

int
smfile_close (smfile_t *ns)
{
  return _smfile_close (ns, &ns->e);
}
