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

#include "algorithms.h"
#include "algorithms/nsdb/rope/algorithms.h"

#include "c_specx.h"
#include "pager.h"
#include "pager/page_h.h"
#include "variable.h"

////////////////////////////////////////////////////////////
// Validation

err_t _ns_valid (struct nsdb *db, error *e);
err_t _ns_variable_valid (struct nsdb *db, pgno var_root, error *e);

struct var_retrieval
{
  enum
  {
    VR_PG,
    VR_NAME,
  } type;

  union
  {
    struct string vname;
    pgno root;
  };
};

////////////////////////////////////////////////////////////
// Fetching Variables

struct _ns_var_get_params
{
  struct nsdb *db;
  struct txn *tx;

  struct string vname;
  struct chunk_alloc *alloc;

  struct variable dest;
};

err_t _ns_var_get (struct _ns_var_get_params *params, error *e);

struct _ns_read_var_page_params
{
  struct nsdb *db;
  struct txn *tx;

  page_h *vp;                // The currently loaded variable page
  struct chunk_alloc *alloc; // Where to allocate stuff
  struct variable *dest;     // Output variable

  bool matches;
  const struct string *check;
};

err_t _ns_read_var_page (struct _ns_read_var_page_params *params, error *e);

////////////////////////////////////////////////////////////
// Creating Variables

struct _ns_var_create_params
{
  struct nsdb *db;
  struct txn *tx;

  struct string vname;
};

spgno _ns_var_create (struct _ns_var_create_params params, error *e);

////////////////////////////////////////////////////////////
// Updating Variables

struct _ns_var_update_params
{
  struct nsdb *db;
  struct txn *tx;

  struct var_retrieval retr;

  // New values
  pgno newpg;
  b_size nbytes;
};

err_t _ns_var_update (struct _ns_var_update_params params, error *e);

////////////////////////////////////////////////////////////
// Updating Variables

struct _ns_var_delete_params
{
  struct nsdb *db;
  struct txn *tx;

  struct string vname;
};

err_t _ns_var_delete (struct _ns_var_delete_params params, error *e);

struct _ns_write_var_page_params
{
  struct nsdb *db;
  struct txn *tx;
  page_h *vp;                 // The currently loaded variable page
  const struct variable *var; // The variable to write
};

err_t _ns_write_var_page (struct _ns_write_var_page_params *params, error *e);

/**
 * Searches for page by variable name [vname]
 * and outputs result to variable [dvar]
 * If mode is FP_CREATE, expects vname to not
 * exist, and creates a new variable
 * if mode is FP_FIND, expects vname to exist
 * Also if prev is set, holds prev in memory at the end
 * and same with cur (which is the variable page)
 * hpos is the hash position that was computed
 */
struct _ns_find_var_page_params
{
  struct nsdb *db;
  struct txn *tx;
  struct chunk_alloc *alloc;

  struct string vname;
  struct variable *dvar;
  enum
  {
    FP_CREATE,
    FP_FIND,
  } mode;

  pgno hpos;
  page_h *prev;
  page_h *cur;
};

err_t _ns_find_var_page (struct _ns_find_var_page_params *params, error *e);
