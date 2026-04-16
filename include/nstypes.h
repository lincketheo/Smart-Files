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

#include <stdint.h>

typedef uint32_t t_size;  // Represents the size of a single type in bytes
typedef int32_t  st_size; // Represents the size of a single type in bytes
typedef uint32_t p_size;  // To index inside a page
typedef int32_t  sp_size; // Signed p size
typedef uint64_t b_size;  // Bytes size to index into a contiguous rope bytes
typedef int64_t  sb_size; // Signed b size
typedef uint64_t pgno;    // Page number
typedef int64_t  spgno;   // Signed Page number
typedef uint64_t txid;    // Transaction id
typedef int64_t  stxid;   // Signed Transaction id
typedef int64_t  slsn;    // Wall Index (often called LSN)
typedef uint64_t lsn;     // Wall Index (often called LSN)
typedef uint8_t  pgh;     // Page header
typedef uint8_t  wlh;     // Wal Header

#define SLSN_MAX I64_MAX

#define PGNO_NULL U64_MAX
#define WLH_NULL U8_MAX

#define PRt_size PRIu32
#define PRsp_size PRId32
#define PRp_size PRIu32
#define PRb_size PRIu64
#define PRsb_size PRId64
#define PRspgno PRId64
#define PRpgno PRIu64
#define PRpgh PRIu8
#define PRtxid PRIu64
#define PRstxid PRId64
#define PRlsn PRIu64
#define PRslsn PRId64
