#pragma once

#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include "nstypes.h"

typedef struct nsfile nsfile_t;

// lifecycle
nsfile_t *nsf_open  (const char *path);
int nsf_close (nsfile_t *ns);

// transactions (without them - it just runs auto transactions)
int nsf_begin    (nsfile_t *ns);
int nsf_commit   (nsfile_t *ns);
int nsf_rollback (nsfile_t *ns);

// Basic operations on bytes
sb_size nsf_insert (nsfile_t *ns, const void *src, t_size size, b_size bofst, sb_size stride, b_size nelem);
sb_size nsf_write  (nsfile_t *ns, const void *src, t_size size, b_size bofst, sb_size stride, b_size nelem);
sb_size nsf_read   (nsfile_t *ns, void *dest, t_size size, b_size bofst, sb_size stride, b_size nelem);
sb_size nsf_remove (nsfile_t *ns, void *dest, t_size size, b_size bofst, sb_size stride, b_size nelem);

// Operations on FILE's (if nelem = 0 - consumes all)
sb_size nsf_finsert (nsfile_t *ns, FILE *f, t_size size, b_size bofst, sb_size stride, b_size nelem);
sb_size nsf_fwrite (nsfile_t *ns, FILE *f, t_size size, b_size bofst, sb_size stride, b_size nelem);
sb_size nsf_fread (nsfile_t *ns, FILE *f, t_size size, b_size bofst, sb_size stride, b_size nelem);
sb_size nsf_fremove (nsfile_t *ns, FILE *f, t_size size, b_size bofst, sb_size stride, b_size nelem);
