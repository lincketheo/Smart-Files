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
// sample4_stride — operating on a single struct field across an array,
// without loading the full struct array into memory.
//
// The intuition: imagine an array of 10 000 {x, y, z, w} float vectors stored
// as raw bytes. You want to update every .z field — maybe re-normalizing a
// depth buffer. The naive approach loads all 160 000 bytes, patches every 12th
// byte, and writes 160 000 bytes back. The stride API lets you do this with a
// single smfile_pwrite call that touches only the .z bytes.
//
// Stride mechanics:
//   stride = N means "access every Nth element". For a packed struct of K
//   same-type fields, stride=K lands on the same field in each successive
//   struct instance. The byte distance between accesses is stride * size.
//
// Struct layout (vec4, 16 bytes):
//   offset  0: float x  (4 bytes)
//   offset  4: float y  (4 bytes)
//   offset  8: float z  (4 bytes)
//   offset 12: float w  (4 bytes)
//
// To reach .x across 64 structs:  size=4, bofst=0,  stride=4, nelem=64
// To reach .z across 64 structs:  size=4, bofst=8,  stride=4, nelem=64
// To read whole structs (stride=1):size=16,bofst=0,  stride=1, nelem=64
// =============================================================================

#include "smfile.h"

#include <stdint.h>
#include <stdio.h>

// vec4 must be exactly 16 bytes — stride arithmetic assumes no padding.
// On all major ABI targets (x86-64 SysV, ARM64 AAPCS) four consecutive floats
// are naturally 4-byte aligned with no inter-field padding, so this is safe.
typedef struct
{
  float x;
  float y;
  float z;
  float w;
} vec4;

#define N_VECS     64
#define FLOATS_PER_VEC4 4

int
main (void)
{
  smfile_t *smf = smfile_open ("sample4_stride");
  if (!smf)
    {
      fprintf (stderr, "smfile_open failed\n");
      return 1;
    }

  // === Seed: insert 64 vec4s as raw bytes into named variable "vectors" ===
  vec4 vecs[N_VECS];
  for (int i = 0; i < N_VECS; ++i)
    {
      vecs[i].x = (float)i;
      vecs[i].y = (float)(i * 2);
      vecs[i].z = (float)(i * 3);
      vecs[i].w = (float)(i * 4);
    }

  if (smfile_pinsert (smf, "vectors", vecs, 0, sizeof (vecs)) < 0)
    {
      smfile_perror (smf, "pinsert vectors");
      return 1;
    }
  // On-disk layout: [x0 y0 z0 w0 | x1 y1 z1 w1 | ... | x63 y63 z63 w63]
  // Total: 64 * 16 = 1024 bytes.

  // === Stride read: extract only the .x column ===
  //
  // smfile_pread(smf, name, dest, size, bofst, stride, nelem)
  //   size   = sizeof(float) = 4 bytes per element
  //   bofst  = 0  (.x is at offset 0 within the first struct)
  //   stride = FLOATS_PER_VEC4 = 4  (each step skips one full vec4 = 4 floats)
  //   nelem  = N_VECS
  //
  // Access pattern: bytes 0, 16, 32, 48, ... — every vec4's .x field.
  float xs[N_VECS];
  sb_size n = smfile_pread (smf, "vectors", xs, sizeof (float), 0, FLOATS_PER_VEC4, N_VECS);
  if (n < 0)
    {
      smfile_perror (smf, "pread .x");
      return 1;
    }

  printf ("first 8 .x values (expect 0.0 1.0 2.0 ... 7.0):\n");
  for (int i = 0; i < 8 && i < (int)n; ++i)
    printf ("  vecs[%d].x = %.1f\n", i, xs[i]);

  // === Stride write: replace the .z column with negated values ===
  //
  //   bofst  = 8  (.z is at byte offset 8 within each struct: skip x=4B + y=4B)
  //   stride = FLOATS_PER_VEC4  (same jump as before)
  //
  // Access pattern: bytes 8, 24, 40, ... — every vec4's .z field.
  // The .x, .y, .w fields are not touched.
  float new_zs[N_VECS];
  for (int i = 0; i < N_VECS; ++i)
    new_zs[i] = -1.0f * (float)i;

  n = smfile_pwrite (smf, "vectors", new_zs, sizeof (float), 8, FLOATS_PER_VEC4, N_VECS);
  if (n < 0)
    {
      smfile_perror (smf, "pwrite .z");
      return 1;
    }

  // === Verify: read back full structs — only .z should have changed ===
  //
  // stride=1 with size=sizeof(vec4) reads contiguous structs, no skipping.
  vec4 readback[4];
  n = smfile_pread (smf, "vectors", readback, sizeof (vec4), 0, 1, 4);
  if (n < 0)
    {
      smfile_perror (smf, "pread verify");
      return 1;
    }

  printf ("first 4 structs after .z update:\n");
  for (int i = 0; i < (int)n; ++i)
    printf ("  vecs[%d]: x=%.1f y=%.1f z=%.1f w=%.1f  (z should be -i)\n",
            i, readback[i].x, readback[i].y, readback[i].z, readback[i].w);
  // Expected: x=i, y=2i, z=-i, w=4i

  return smfile_close (smf);
}
