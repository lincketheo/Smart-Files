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

#include "algorithms_public/algorithms.h"

static err_t
varh_init_hash_page (struct pager *p, error *e)
{
  page_h hp = page_h_create ();

  // BEGIN TXN
  struct txn tx;
  if (pgr_begin_txn (&tx, p, e))
    {
      goto failed;
    }

  if (pgr_new (&hp, p, &tx, PG_VAR_HASH_PAGE, e))
    {
      goto failed;
    }

  ASSERT (page_h_pgno (&hp) == VHASH_PGNO);

  if (pgr_release (p, &hp, PG_VAR_HASH_PAGE, e))
    {
      goto failed;
    }

  if (pgr_commit (p, &tx, e))
    {
      goto failed;
    }

  return SUCCESS;

failed:
  pgr_cancel_if_exists (p, &hp);
  return error_trace (e);
}

struct nsdb *
ns_open (const char *dbname, error *e)
{
  struct nsdb *ret = i_malloc (1, sizeof *ret, e);

  if (ret == NULL)
    {
      goto failed;
    }

  ret->dbname = dbname;

  // Pager
  ret->p = pgr_open_single_file (dbname, e);
  if (ret->p == NULL)
    {
      goto fail_ret;
    }

  // Maybe initialize variable hash page
  if (pgr_isnew (ret->p))
    {
      // Variable hash page
      if (varh_init_hash_page (ret->p, e))
        {
          goto fail_pgr;
        }
    }

  return ret;

fail_pgr:
  pgr_close (ret->p, e);
fail_ret:
  i_free (ret);

failed:
  return NULL;
}
