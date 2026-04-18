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

#include "algorithms/nsdb.h"
#include "algorithms/nsdb/var/algorithms.h"
#include "algorithms/smfile/smfile.h"
#include "c_specx.h"
#include "c_specx/dev/error.h"
#include "c_specx/intf/os/memory.h"
#include "c_specx/memory/chunk_alloc.h"
#include "nsfile.h"
#include "pager.h"
#include "pager/page_h.h"

static struct smfile *
_smfile_open (const char *path, error *e)
{
  struct smfile_root *ret = i_malloc (1, sizeof *ret, e);
  page_h hp = page_h_create ();

  if (ret == NULL)
    {
      return NULL;
    }

  // Initialize inner values
  {
    ret->e = error_create ();

    // path
    ret->path.len = strlen (path);
    ret->path.data = i_malloc (ret->path.len, 1, e);
    if (ret->path.data == NULL)
      {
        goto failed;
      }

    // db
    ret->db.p = pgr_open_single_file (path, e);
    if (ret->db.p == NULL)
      {
        goto failed;
      }
  }

  // BEGIN TXN
  struct txn tx;
  if (pgr_begin_txn (&tx, ret->db.p, e))
    {
      goto failed;
    }

  // Upfront initialization
  if (pgr_isnew (ret->db.p))
    {
      // Create a new variable hash page
      if (pgr_new (&hp, ret->db.p, &tx, PG_VAR_HASH_PAGE, e))
        {
          goto failed;
        }

      // Next page should be valid - this is a weak contract - but assumes the structure of the pager,
      // it's good enough but might need to change
      ASSERT (page_h_pgno (&hp) == VHASH_PGNO);

      if (pgr_release (ret->db.p, &hp, PG_VAR_HASH_PAGE, e))
        {
          goto failed;
        }

      // Create the default variable
      struct _ns_var_create_params params = {
        .db = &ret->db,
        .tx = &tx,
        .vname = strfcstr (DEFAULT_VARIABLE),
      };
      if (_ns_var_create (params, e))
        {
          goto failed;
        }
    }

  // COMMIT
  if (pgr_commit (ret->db.p, &tx, e))
    {
      goto failed;
    }

  // Load the default context
  struct smfile *sret = _smfile_root_load (ret, e);

  return sret;

failed:
  // TODO just delete the file
  i_free (ret);
  return NULL;
}

smfile_t *
smfile_open (const char *path)
{
  error e = error_create ();
  struct smfile *ret = _smfile_open (path, &e);
  if (ret == NULL)
    {
      return NULL;
    }
  return ret;
}
