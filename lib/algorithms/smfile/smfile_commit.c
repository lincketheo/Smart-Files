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
#include "c_specx/dev/error.h"
#include "nsfile.h"
#include "pager.h"

static err_t
_smfile_commit (smfile_t *smf, error *e)
{
  if (!smf->atx)
    {
      return error_causef (e, ERR_INVALID_ARGUMENT,
                           "Can't commit transaction, not a part of an existing transaction");
    }

  WRAP (pgr_commit (smf->db.p, &smf->tx, &smf->e));
  smf->atx = &smf->tx;

  return SUCCESS;
}

int
smfile_commit (smfile_t *smf)
{
  smf->e.cause_code = SUCCESS;
  smf->e.cmlen = 0;

  return _smfile_commit (smf, &smf->e);
}
