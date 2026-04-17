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

#include "numstore.h"
#include "c_specx.h"

struct lt_lock
{
  enum lt_lock_type
  {
    LOCK_DB, // The whole database

    LOCK_ROOT, // The root page

    LOCK_VHP,     // The variable hash page
    LOCK_VHP_POS, // An individual spot in the variable hash page

    // Individual Variables
    LOCK_VAR,        // A variable
    LOCK_VAR_NEXT,   // The next pointer of a variable
    LOCK_VAR_ROOT,   // The variable root of a variable
    LOCK_VAR_NBYTES, // The number of bytes in a variable

    // Individual rptrees
    LOCK_RPTREE, // An entire rptree
  } type;

  union lt_lock_data
  {
    pgno vhp_pos;     // LOCK_VHP_POS,
    pgno var_root;    // LOCK_VAR, LOCK_VAR_NEXT, LOCK_VAR_ROOT,
                      // LOCK_VAR_NBYTES,
    pgno rptree_root; // LOCK_RPTREE,
  } data;
};

u32 lt_lock_key (struct lt_lock lock);
bool lt_lock_equal (const struct lt_lock left, const struct lt_lock right);
void i_print_lt_lock (int log_level, struct lt_lock l);
bool get_parent (struct lt_lock *parent, struct lt_lock lock);

HEADER_FUNC struct lt_lock
lock_create (const enum lt_lock_type type, const union lt_lock_data data)
{
  return (struct lt_lock){ .type = type, .data = data };
}

HEADER_FUNC struct lt_lock
lock_db (void)
{
  return lock_create (LOCK_DB, (union lt_lock_data){ 0 });
}

HEADER_FUNC struct lt_lock
lock_root (void)
{
  return lock_create (LOCK_ROOT, (union lt_lock_data){ 0 });
}

HEADER_FUNC struct lt_lock
lock_vhp (void)
{
  return lock_create (LOCK_VHP, (union lt_lock_data){ 0 });
}

HEADER_FUNC struct lt_lock
lock_vhp_pos (const pgno vhp_pos)
{
  return lock_create (LOCK_VHP_POS,
                      (union lt_lock_data){ .vhp_pos = vhp_pos });
}

HEADER_FUNC struct lt_lock
lock_var (const pgno var_root)
{
  return lock_create (LOCK_VAR, (union lt_lock_data){ .var_root = var_root });
}

HEADER_FUNC struct lt_lock
lock_var_next (const pgno var_root)
{
  return lock_create (LOCK_VAR_NEXT,
                      (union lt_lock_data){ .var_root = var_root });
}

HEADER_FUNC struct lt_lock
lock_var_root (const pgno var_root)
{
  return lock_create (LOCK_VAR_ROOT,
                      (union lt_lock_data){ .var_root = var_root });
}

HEADER_FUNC struct lt_lock
lock_var_nbytes (const pgno var_root)
{
  return lock_create (LOCK_VAR_NBYTES,
                      (union lt_lock_data){ .var_root = var_root });
}

HEADER_FUNC struct lt_lock
lock_rptree (const pgno rptree_root)
{
  return lock_create (LOCK_RPTREE,
                      (union lt_lock_data){ .rptree_root = rptree_root });
}

#define LT_LOCK_FOR_EACH(X) \
  X (LOCK_DB)               \
  X (LOCK_ROOT)             \
  X (LOCK_VHP)              \
  X (LOCK_VHP_POS)          \
  X (LOCK_VAR)              \
  X (LOCK_VAR_NEXT)         \
  X (LOCK_VAR_ROOT)         \
  X (LOCK_VAR_NBYTES)       \
  X (LOCK_RPTREE)

#define LT_LOCK_FOR_EACH_RANDOM(X)                           \
  X (LOCK_DB, lock_db ())                                    \
  X (LOCK_ROOT, lock_root ())                                \
  X (LOCK_VHP, lock_vhp ())                                  \
  X (LOCK_VHP_POS, lock_vhp_pos (randu64r (0, 10000)))       \
  X (LOCK_VAR, lock_var (randu64r (0, 10000)))               \
  X (LOCK_VAR_NEXT, lock_var_next (randu64r (0, 10000)))     \
  X (LOCK_VAR_ROOT, lock_var_root (randu64r (0, 10000)))     \
  X (LOCK_VAR_NBYTES, lock_var_nbytes (randu64r (0, 10000))) \
  X (LOCK_RPTREE, lock_rptree (randu64r (0, 10000)))

struct lt_lock random_lt_lock (void);
