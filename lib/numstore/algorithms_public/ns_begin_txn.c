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

#include "numstore/algorithms_public/algorithms.h"

struct txn *
ns_begin_txn (const struct nsdb *n, error *e)
{
  struct txn *ret = i_malloc (1, sizeof *ret, e);
  if (ret == NULL)
    {
      goto failed;
    }

  if (pgr_begin_txn (ret, n->p, e))
    {
      goto fail_ret;
    }

  return ret;

fail_ret:
  i_free (ret);
failed:
  return NULL;
}
