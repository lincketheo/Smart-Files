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

#include "lockt/lt_lock.h"

#include "c_specx.h"

#include <string.h>

u32
lt_lock_key (const struct lt_lock lock)
{
  char hcode[sizeof (union lt_lock_data) + sizeof (u8)];
  u32 hcodelen = 0;
  const u8 _type = lock.type;

  memcpy (&hcode[hcodelen], &_type, sizeof (_type));
  hcodelen += sizeof (_type);

  switch (lock.type)
    {
    case LOCK_DB:
    case LOCK_ROOT:
    case LOCK_VHP:
      {
        break;
      }
    case LOCK_VHP_POS:
      {
        memcpy (&hcode[hcodelen], &lock.data.vhp_pos,
                sizeof (lock.data.vhp_pos));
        hcodelen += sizeof (lock.data.vhp_pos);
        break;
      }
    case LOCK_VAR:
    case LOCK_VAR_NEXT:
    case LOCK_VAR_ROOT:
    case LOCK_VAR_NBYTES:
      {
        memcpy (&hcode[hcodelen], &lock.data.var_root,
                sizeof (lock.data.var_root));
        hcodelen += sizeof (lock.data.var_root);
        break;
      }
    case LOCK_RPTREE:
      {
        memcpy (&hcode[hcodelen], &lock.data.rptree_root,
                sizeof (lock.data.rptree_root));
        hcodelen += sizeof (lock.data.rptree_root);
        break;
      }
    }

  const struct string lock_type_hcode = {
    .data = hcode,
    .len = hcodelen,
  };

  return fnv1a_hash (lock_type_hcode);
}

bool
lt_lock_equal (const struct lt_lock left, const struct lt_lock right)
{
  if (left.type != right.type)
    {
      return false;
    }

  switch (left.type)
    {
    case LOCK_DB:
      {
        return true;
      }
    case LOCK_ROOT:
      {
        return true;
      }
    case LOCK_VHP:
      {
        return true;
      }
    case LOCK_VHP_POS:
      {
        return left.data.vhp_pos == right.data.vhp_pos;
      }
    case LOCK_VAR:
    case LOCK_VAR_NEXT:
    case LOCK_VAR_ROOT:
    case LOCK_VAR_NBYTES:
      {
        return left.data.var_root == right.data.var_root;
      }
    case LOCK_RPTREE:
      {
        return left.data.rptree_root == right.data.rptree_root;
      }
    }
  UNREACHABLE ();
}

void
i_print_lt_lock (const int log_level, const struct lt_lock l)
{
  switch (l.type)
    {
    case LOCK_DB:
      {
        i_printf (log_level, "LOCK_DB\n");
        return;
      }
    case LOCK_ROOT:
      {
        i_printf (log_level, "LOCK_ROOT\n");
        return;
      }
    case LOCK_VHP:
      {
        i_printf (log_level, "LOCK_VHP\n");
        return;
      }
    case LOCK_VHP_POS:
      {
        i_printf (log_level, "LOCK_VHP_POS(%" PRpgno ")\n", l.data.vhp_pos);
        return;
      }
    case LOCK_VAR:
      {
        i_printf (log_level, "LOCK_VAR(%" PRpgno ")\n", l.data.var_root);
        return;
      }
    case LOCK_VAR_NEXT:
      {
        i_printf (log_level, "LOCK_VAR_NEXT(%" PRpgno ")\n", l.data.var_root);
        return;
      }
    case LOCK_VAR_ROOT:
      {
        i_printf (log_level, "LOCK_VAR_ROOT(%" PRpgno ")\n", l.data.var_root);
        return;
      }
    case LOCK_VAR_NBYTES:
      {
        i_printf (log_level, "LOCK_VAR_NBYTES(%" PRpgno ")\n",
                  l.data.var_root);
        return;
      }
    case LOCK_RPTREE:
      {
        i_printf (log_level, "LOCK_RPTREE(%" PRpgno ")\n", l.data.rptree_root);
        return;
      }
    }
  UNREACHABLE ();
}

bool
get_parent (struct lt_lock *parent, const struct lt_lock lock)
{
  switch (lock.type)
    {
    case LOCK_DB:
      {
        return false;
      }
    case LOCK_ROOT:
      {
        parent->type = LOCK_DB;
        parent->data = (union lt_lock_data){ 0 };
        return true;
      }
    case LOCK_VHP:
      {
        parent->type = LOCK_DB;
        parent->data = (union lt_lock_data){ 0 };
        return true;
      }
    case LOCK_VHP_POS:
      {
        parent->type = LOCK_VHP;
        parent->data = (union lt_lock_data){ 0 };
        return true;
      }
    case LOCK_VAR:
      {
        parent->type = LOCK_DB;
        parent->data = (union lt_lock_data){ 0 };
        return true;
      }
    case LOCK_VAR_NEXT:
      {
        parent->type = LOCK_VAR;
        parent->data = (union lt_lock_data){ .var_root = lock.data.var_root };
        return true;
      }
    case LOCK_VAR_ROOT:
      {
        parent->type = LOCK_VAR;
        parent->data = (union lt_lock_data){ .var_root = lock.data.var_root };
        return true;
      }
    case LOCK_VAR_NBYTES:
      {
        parent->type = LOCK_VAR;
        parent->data = (union lt_lock_data){ .var_root = lock.data.var_root };
        return true;
      }
    case LOCK_RPTREE:
      {
        parent->type = LOCK_DB;
        parent->data = (union lt_lock_data){ 0 };
        return true;
      }
    }

  UNREACHABLE ();
}

struct lt_lock
random_lt_lock (void)
{
#define func(type, r) \
  case type:          \
    {                 \
      return r;       \
    }

  switch ((enum lt_lock_type)randu32r (0, 10))
    {
      LT_LOCK_FOR_EACH_RANDOM (func)
    }

#undef func

  UNREACHABLE ();
}
