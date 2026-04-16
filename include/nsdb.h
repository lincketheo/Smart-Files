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

#include <stdio.h>
#include <stdint.h>
#include "nstypes.h"

typedef struct nsdb nsdb_t;
typedef struct nsvar nsvar_t;

// nsfile
nsdb_t *nsdb_open (const char *path);
int nsdb_close (nsdb_t *ns);

// nsvar
nsvar_t *nsvar_load (nsdb_t *ns, const char *vname);
int nsvar_delete (nsdb_t *ns, const char *vname);
nsvar_t *nsvar_open (const char *path, const char *vname);
int nsvar_close (nsvar_t *nsp);

// transactions (without them - it just runs auto transactions)
int nsvar_begin    (nsvar_t *nsp);
int nsvar_commit   (nsvar_t *nsp);
int nsvar_rollback (nsvar_t *nsp);

// Basic operations on bytes
sb_size nsvar_insert (nsvar_t *nsp, const void *src, t_size size, b_size bofst, sb_size stride, b_size nelem);
sb_size nsvar_write  (nsvar_t *nsp, const void *src, t_size size, b_size bofst, sb_size stride, b_size nelem);
sb_size nsvar_read   (nsvar_t *nsp,       void *dest, t_size size, b_size bofst, sb_size stride, b_size nelem);
sb_size nsvar_remove (nsvar_t *nsp,       void *dest, t_size size, b_size bofst, sb_size stride, b_size nelem);

// Operations on FILE's (if nelem = 0 - consumes all)
sb_size nsvar_finsert (nsvar_t *nsp, FILE *f, t_size size, b_size bofst, sb_size stride, b_size nelem);
sb_size nsvar_fwrite  (nsvar_t *nsp, FILE *f, t_size size, b_size bofst, sb_size stride, b_size nelem);
sb_size nsvar_fread   (nsvar_t *nsp, FILE *f, t_size size, b_size bofst, sb_size stride, b_size nelem);
sb_size nsvar_fremove (nsvar_t *nsp, FILE *f, t_size size, b_size bofst, sb_size stride, b_size nelem);
