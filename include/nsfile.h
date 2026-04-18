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

#include "nstypes.h"

#include <stdint.h>
#include <stdio.h>

typedef struct smfile smfile_t;

// open / close
smfile_t *smfile_open (const char *path);
smfile_t *smfile_new_context (smfile_t *ns);
int smfile_close (smfile_t *ns);

// Error handling
const char *smfile_strerror (smfile_t *ns);
int smfile_perror (smfile_t *ns);

// Delete a variable
int smfile_delete (smfile_t *ns, const char *vname);

// Transaction Support
int smfile_begin (smfile_t *smf);
int smfile_commit (smfile_t *smf);
int smfile_rollback (smfile_t *smf);

// Core Operations
sb_size smfile_insert (smfile_t *smf, const void *src, b_size bofst, b_size slen);
sb_size smfile_write (smfile_t *smf, const void *src, b_size bofst, b_size nelem);
sb_size smfile_read (smfile_t *smf, void *dest, b_size bofst, b_size nelem);
sb_size smfile_remove (smfile_t *smf, void *dest, b_size bofst, b_size nelem);

// "Power" Operations
sb_size smfile_pinsert (
    smfile_t *smf,
    const char *name,
    const void *src,
    b_size bofst,
    b_size slen);

sb_size smfile_pwrite (
    smfile_t *smf,
    const char *name,
    const void *src,
    t_size size,
    b_size bofst,
    sb_size stride,
    b_size nelem);

sb_size smfile_pread (
    smfile_t *smf,
    const char *name,
    void *dest,
    t_size size,
    b_size bofst,
    sb_size stride,
    b_size nelem);

sb_size smfile_premove (
    smfile_t *smf,
    const char *name,
    void *dest,
    t_size size,
    b_size bofst,
    sb_size stride,
    b_size nelem);
