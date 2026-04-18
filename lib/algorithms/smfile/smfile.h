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

#include "algorithms/nsdb.h"
#include "c_specx/dev/assert.h"
#include "c_specx/dev/error.h"
#include "c_specx/dev/signatures.h"
#include "pager.h"
#include "variable.h"

struct smfile
{
  struct nsdb db;         // The database resources
  int is_auto_txn;        // If atx is an auto transaction
  struct txn *atx;        // Active transaction
  struct txn tx;          // Transaction storage
  struct string path;     // Path to the database
  struct variable loaded; // The currently loaded variable (always loaded)

  struct chunk_alloc *alloc;   // Allocator that holds stuff for [loaded]
  struct chunk_alloc *staging; // Temporary allocator for error handling

  struct chunk_alloc alloc_1;
  struct chunk_alloc alloc_2;

  error e;
};

#define DEFAULT_VARIABLE "."

HEADER_FUNC err_t
_smfile_auto_begin_txn (struct smfile *sm, error *e)
{
  if (sm->atx == NULL)
    {
      WRAP (pgr_begin_txn (&sm->tx, sm->db.p, e));
      sm->is_auto_txn = 1;
      sm->atx = &sm->tx;
    }

  return SUCCESS;
}

HEADER_FUNC err_t
_smfile_auto_commit (struct smfile *sm, error *e)
{
  if (sm->is_auto_txn)
    {
      ASSERT (sm->atx);
      WRAP (pgr_commit (sm->db.p, sm->atx, e));
      sm->atx = NULL;
    }
  return SUCCESS;
}

HEADER_FUNC void
_smfile_auto_rollback (struct smfile *sm)
{
  if (pgr_rollback (sm->db.p, sm->atx, 0, &sm->e))
    {
      panic ("Failed to rollback");
    }
  sm->atx = NULL;
}

err_t _smfile_load (struct smfile *sm, const char *vname, error *e);
