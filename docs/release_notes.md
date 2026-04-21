# Smart Files

For the people in a hurry:

SmartFiles is a new kind of file. It looks and acts like an ISO-C `FILE`, but under the hood it's a transactional database with a rope-backed storage engine. That means two things:

1. **Insert and remove in the middle of a file are first class** — `O(log n)` instead of the `O(n)` shuffle you'd write by hand.
2. **Every operation is atomic** — it either fully completes or it doesn't happen at all. No partial writes, no half-corrupted files, no recovery code on your end.

As a bonus, you also get multiple named "variables" per file and strided reads/writes/removes.

---

## The shortcomings of ISO-C files

ISO C has the common read / write interface:

- `fread(destination, size, length, file)` → number read
- `fwrite(source, size, length, file)` → number written
- `fseek(file, offset)` → location

This works fine and has worked fine for years, but it misses two main niceties:

1. Inner mutations are up to the application developer to figure out
2. Reads and writes are not atomic

### 1. Inner mutations aren't first class

There's no:

```c
finsert(source, size, length, offset, file)
```

To do that, you'd need to write code kind of like this:

```c
tailsize = file.size - offset;
tail = malloc(tailsize);
fseek(file, offset);
fread(tail, 1, tailsize, file);
fwrite(newdata, 1, newlength, file);
fwrite(tail, 1, tailsize, file);
```

Or `fremove(source, size, length, offset, file)`, which needs the same shape of boilerplate in reverse. And that's the *happy path* — handle a crash halfway through that sequence and you've got a corrupt file.

Every one of these operations is `O(n)` in the size of the tail. For a 1GB file with an insert near the front, you're rewriting ~1GB of data to add a handful of bytes.

### 2. Reads and writes aren't atomic

Notoriously this is by design — ISO C files are a thin wrapper over `read(2)` and `write(2)`, and the kernel makes no promises. A write can be partially applied. A crash mid-write leaves you with a file in a state that never legally existed. `fsync` helps durability but doesn't help atomicity. If you want all-or-nothing semantics on top of a `FILE*`, you're writing a WAL yourself.

---

## Introducing SmartFiles

SmartFiles solves points 1 and 2, and adds two more nice features on top of files:

### 1. Insert and remove are first class

SmartFiles treats `insert` (put data in the middle of a file) and `remove` (take data out of the middle of a file) as first-class operations. It uses a novel rope algorithm — optimized for disk writing — to take these from `O(n)` to `O(log n)`.

You write:

```c
smfile_insert(smf, newdata, offset, length);
```

...and that's it. No tail buffer, no malloc, no fseek dance. And because the rope is tree-structured on disk, the cost scales with tree depth, not file size.

### 2. Fully transactional

SmartFiles is a fully featured database with transactional support, a write-ahead log, and two-phase locking (all that is under the hood) — meaning modifications either complete or do not complete. There's no in-between state like ISO files.

- Yes — read, write, and remove do return sizes, but this is because of data boundaries. There is never a case where read / write / remove / insert fails with a partial mutation to the database. The only "partial" thing that can happen is that we write 4 elements to a 5-element-long file when we requested 10. But that's arguably not a partial write — it's just hitting the end.

Pair that with `smfile_begin` / `smfile_commit` / `smfile_rollback` and you can batch a whole sequence of inserts, removes, and writes into one atomic unit.

## Bonus features

**3. Multiple variables per file.** SmartFiles lets you store as many "variables" as you want in one file. This isn't crucial — the default behavior looks just like a regular file — but if you're a power user, you can keep multiple keys inside a single file and switch between them with `smfile_load`.

**4. Strided operations.** SmartFiles adds the notion of *stride* on top of reads, writes, and removes (not insert). A strided operation means we touch every nth element (with respect to the size parameter). Useful when you're storing structs on disk and only want to read out one field across all records — no more "read everything, throw most of it away."

---

## API Overview

```c
#pragma once
#include "smfile.h"
#include <stdint.h>
#include <stdio.h>

typedef struct smfile smfile_t;
```

### Open / close

```c
smfile_t *smfile_open (const char *path);
smfile_t *smfile_load (smfile_t *ns, const char *vname);
int       smfile_close(smfile_t *ns);
```

`smfile_open` gives you a handle to the file on disk. `smfile_load` switches the active variable inside that file — if you're using it like a plain file, you can ignore this. `smfile_close` flushes and releases.

### Error handling

```c
const char *smfile_strerror(smfile_t *ns);
int         smfile_perror  (smfile_t *ns);
```

Same shape as `strerror` / `perror`. The last error lives on the handle, so there's no global `errno` contention.

### Context switching

```c
int smfile_delete(smfile_t *ns, const char *vname);
```

Drops a variable from the file entirely.

### Transactions

```c
int smfile_begin   (smfile_t *smf);
int smfile_commit  (smfile_t *smf);
int smfile_rollback(smfile_t *smf);
```

Individual operations are already atomic. Wrap a sequence in `begin`/`commit` when you need *multiple* operations to land together or not at all.

### Core operations

```c
int     smfile_insert(smfile_t *smf, const void *src,
                      b_size bofst, b_size slen);

sb_size smfile_write (smfile_t *smf, const void *src,
                      t_size size, b_size bofst,
                      sb_size stride, b_size nelem);

sb_size smfile_read  (smfile_t *smf, void *dest,
                      t_size size, b_size bofst,
                      sb_size stride, b_size nelem);

sb_size smfile_remove(smfile_t *smf, void *dest,
                      t_size size, b_size bofst,
                      sb_size stride, b_size nelem);
```

A quick tour of the parameters:

- `size` — width of a single element, in bytes
- `bofst` — byte offset where the operation starts
- `stride` — element stride; `1` means contiguous, `n` means every nth element
- `nelem` — how many elements to touch

`insert` is the odd one out: no `stride`, no element count — it takes a raw byte blob and splices it in. `read`, `write`, and `remove` all speak the element language and all support strided access.

---

That's the release. Go build something on top of a file that actually behaves like a data structure.
