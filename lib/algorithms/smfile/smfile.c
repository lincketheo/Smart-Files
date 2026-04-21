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
#include "smfile.h"

#include "c_specx/dev/error.h"
#include "pager.h"

// smfile

int
smfile_perror (smfile_t *ns, const char *prefix)
{
  const char *err = smfile_strerror (ns);
  if (err)
    {
      return fprintf (stderr, "%s: %s\n", prefix, smfile_strerror (ns));
    }
  else
    {
      return fprintf (stderr, "%s: success\n", prefix);
    }
}

const char *
smfile_strerror (smfile_t *ns)
{
  if (ns->e.cause_code < 0)
    {
      return ns->e.cause_msg;
    }
  else
    {
      return NULL;
    }
}

int
smfile_cleanup (const char *path)
{
  error e = error_create ();
  pgr_delete_single_file (path, &e);
  return error_trace (&e);
}

// smfile_root

err_t
_smfile_root_close (struct smfile_root *root, error *e)
{
  ASSERT (root->count == 0);
  err_t err = pgr_close (root->db.p, e);
  i_free ((void *)root->path.data);
  i_free (root);
  return err;
}

struct smfile *
_smfile_root_load (struct smfile_root *ns, error *e)
{
  struct smfile *ret = i_malloc (1, sizeof *ret, e);
  if (ret == NULL)
    {
      return NULL;
    }

  ret->root = ns;
  ret->is_auto_txn = 0;
  ret->atx = NULL;
  ret->e = error_create ();
  ns->count++;

  return ret;
}

void
_smfile_root_release (struct smfile_root *root, struct smfile *sm)
{
  ASSERT (root->count > 0);
  i_free (sm);
  root->count -= 1;
}

// Core Operations
sb_size
smfile_size (smfile_t *smf)
{
  return smfile_psize (smf, NULL);
}

sb_size
smfile_insert (smfile_t *smf, const void *src, sb_size bofst, b_size slen)
{
  return smfile_pinsert (smf, NULL, src, bofst, slen);
}

sb_size
smfile_write (smfile_t *smf, const void *src, b_size bofst, b_size nelem)
{
  return smfile_pwrite (smf, NULL, src, 1, bofst, 1, nelem);
}

sb_size
smfile_read (smfile_t *smf, void *dest, sb_size bofst, b_size nelem)
{
  return smfile_pread (smf, NULL, dest, 1, bofst, 1, nelem);
}

sb_size
smfile_remove (smfile_t *smf, void *dest, sb_size bofst, b_size nelem)
{
  return smfile_premove (smf, NULL, dest, 1, bofst, 1, nelem);
}

// Transactional Support
err_t
_smfile_auto_begin_txn (struct smfile *sm, error *e)
{
  if (sm->atx == NULL)
    {
      WRAP (pgr_begin_txn (&sm->tx, sm->root->db.p, e));
      sm->is_auto_txn = 1;
      sm->atx = &sm->tx;
    }

  return SUCCESS;
}

err_t
_smfile_auto_commit (struct smfile *sm, error *e)
{
  if (sm->is_auto_txn)
    {
      ASSERT (sm->atx);
      WRAP (pgr_commit (sm->root->db.p, sm->atx, e));
      sm->atx = NULL;
    }
  return SUCCESS;
}

void
_smfile_auto_rollback (struct smfile *sm)
{
  if (pgr_rollback (sm->root->db.p, sm->atx, 0, &sm->e))
    {
      panic ("Failed to rollback");
    }
  sm->atx = NULL;
}
