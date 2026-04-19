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

// =============================================================================
// sample3_multithreaded — all four power-API methods across concurrent threads.
//
// A single smfile_t handle is attached to one transaction at a time, so it
// cannot be shared across threads directly. smfile_new_context clones the
// handle into a new, independent transaction space: same underlying file, its
// own lock state, its own WAL cursor. Each thread gets one context.
//
// This sample also introduces the power API (smfile_p*), which adds two knobs
// over the simple API:
//   name   — identifies a named variable within the file (multiple independent
//             sequences coexist in one file, each with its own rope)
//   stride — operate on every nth element rather than contiguous bytes
//
// Threads:
//   A  smfile_pinsert  — splice data into "stream_a"
//   B  smfile_pwrite   — overwrite every other byte in "stream_b" (stride=2)
//   C  smfile_pread    — read from "stream_c" (pre-seeded by main)
//   D  smfile_premove  — excise every other byte from "stream_d" (stride=2)
// =============================================================================

#include "smfile.h"

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

typedef struct
{
  smfile_t *ctx;
  int       id;
} thread_arg;

// Thread A: pinsert — demonstrate named-variable insertion on a fresh context.
static void *
thread_pinsert (void *raw)
{
  smfile_t *ctx = ((thread_arg *)raw)->ctx;

  uint8_t data[32];
  for (int i = 0; i < 32; ++i)
    data[i] = (uint8_t)i;

  // pinsert to "stream_a": splice 32 bytes at offset 0 (the variable is empty,
  // so this is effectively an initial population).
  sb_size r = smfile_pinsert (ctx, "stream_a", data, 0, sizeof (data));
  if (r < 0)
    smfile_perror (ctx, "[A] pinsert");

  return NULL;
}

// Thread B: pwrite — overwrite every other byte (stride=2) in "stream_b".
// The file holds stream_b with 32 bytes pre-seeded by main. This thread
// writes 0xBB into positions [0, 2, 4, ...] without touching the odd bytes.
static void *
thread_pwrite (void *raw)
{
  smfile_t *ctx = ((thread_arg *)raw)->ctx;

  uint8_t val[16];
  memset (val, 0xBB, sizeof (val));

  // smfile_pwrite(smf, name, src, size, bofst, stride, nelem)
  //   size   = 1 (one byte per element)
  //   bofst  = 0 (start at the first byte)
  //   stride = 2 (write every other element — even indices only)
  //   nelem  = 16
  sb_size r = smfile_pwrite (ctx, "stream_b", val, 1, 0, 2, 16);
  if (r < 0)
    smfile_perror (ctx, "[B] pwrite");

  return NULL;
}

// Thread C: pread — read a contiguous slice from "stream_c" (stride=1).
static void *
thread_pread (void *raw)
{
  smfile_t *ctx = ((thread_arg *)raw)->ctx;

  uint8_t buf[8];
  // Contiguous read (stride=1): reads bytes [0..7] from stream_c.
  sb_size n = smfile_pread (ctx, "stream_c", buf, 1, 0, 1, sizeof (buf));
  if (n < 0)
    {
      smfile_perror (ctx, "[C] pread");
      return NULL;
    }

  printf ("[C] pread %lld bytes from stream_c:", (long long)n);
  for (sb_size i = 0; i < n; ++i)
    printf (" %02x", buf[i]);
  printf ("\n");

  return NULL;
}

// Thread D: premove — excise every other byte (stride=2) from "stream_d".
// Removes positions [0, 2, 4, 6] and closes each gap: the four remaining
// odd-indexed bytes compact into a contiguous 4-byte sequence.
static void *
thread_premove (void *raw)
{
  smfile_t *ctx = ((thread_arg *)raw)->ctx;

  uint8_t evicted[4];
  // smfile_premove(smf, name, dest, size, bofst, stride, nelem)
  //   size   = 1
  //   bofst  = 0
  //   stride = 2 (remove every other element)
  //   nelem  = 4 (remove 4 elements total)
  sb_size n = smfile_premove (ctx, "stream_d", evicted, 1, 0, 2, 4);
  if (n < 0)
    {
      smfile_perror (ctx, "[D] premove");
      return NULL;
    }

  printf ("[D] premove removed %lld bytes:", (long long)n);
  for (sb_size i = 0; i < n; ++i)
    printf (" %02x", evicted[i]);
  printf ("\n");

  return NULL;
}

int
main (void)
{
  smfile_t *smf = smfile_open ("sample3_mt");
  if (!smf)
    {
      fprintf (stderr, "smfile_open failed\n");
      return 1;
    }

  // Pre-seed the variables that threads B, C, and D will operate on.
  // Thread A creates its own variable (stream_a) from scratch.
  uint8_t seed[32];
  for (int i = 0; i < 32; ++i)
    seed[i] = (uint8_t)(i * 2); // 00 02 04 06 ... 3e

  smfile_pinsert (smf, "stream_b", seed, 0, 32);
  smfile_pinsert (smf, "stream_c", seed, 0, 32);
  smfile_pinsert (smf, "stream_d", seed, 0, 32);

  // smfile_new_context clones the file handle into a new transaction space.
  // The four contexts share the same underlying file but hold independent
  // transaction state, so the threads can run truly concurrently.
  smfile_t *ctx_a = smfile_new_context (smf);
  smfile_t *ctx_b = smfile_new_context (smf);
  smfile_t *ctx_c = smfile_new_context (smf);
  smfile_t *ctx_d = smfile_new_context (smf);

  if (!ctx_a || !ctx_b || !ctx_c || !ctx_d)
    {
      fprintf (stderr, "smfile_new_context failed\n");
      return 1;
    }

  thread_arg args[4] = {
    { ctx_a, 0 },
    { ctx_b, 1 },
    { ctx_c, 2 },
    { ctx_d, 3 },
  };

  pthread_t threads[4];
  pthread_create (&threads[0], NULL, thread_pinsert, &args[0]);
  pthread_create (&threads[1], NULL, thread_pwrite,  &args[1]);
  pthread_create (&threads[2], NULL, thread_pread,   &args[2]);
  pthread_create (&threads[3], NULL, thread_premove, &args[3]);

  for (int i = 0; i < 4; ++i)
    pthread_join (threads[i], NULL);

  // Each context must be closed independently — they each hold their own
  // transaction state and WAL references.
  smfile_close (ctx_a);
  smfile_close (ctx_b);
  smfile_close (ctx_c);
  smfile_close (ctx_d);

  return smfile_close (smf);
}
