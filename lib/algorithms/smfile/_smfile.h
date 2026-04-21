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
#include "c_specx.h"
#include "pager.h"
#include "variable.h"

struct smfile_root
{
  struct nsdb db;     // The database resources
  struct string path; // Path to the database
  int count;          // When this reaches 0 - close the root
  error e;
};

struct smfile
{
  // Shared root file
  struct smfile_root *root;

  int is_auto_txn; // If atx is an auto transaction
  struct txn *atx; // Active transaction
  struct txn tx;   // Transaction storage

  error e;
};

#define DEFAULT_VARIABLE "."

// Auto Transactions
err_t _smfile_auto_begin_txn (struct smfile *sm, error *e);
err_t _smfile_auto_commit (struct smfile *sm, error *e);
void _smfile_auto_rollback (struct smfile *sm);

err_t _smfile_root_close (struct smfile_root *root, error *e);
struct smfile *_smfile_root_load (struct smfile_root *root, error *e);
void _smfile_root_release (struct smfile_root *root, struct smfile *sm);

HEADER_FUNC struct string
vname_or_default (const char *name)
{
  if (name != NULL)
    {
      return strfcstr (name);
    }
  else
    {
      return strfcstr (DEFAULT_VARIABLE);
    }
}
