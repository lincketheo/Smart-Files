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

#include "numstore.h"
#include "tlclib.h"

struct wal_istream;

struct wal_istream *walis_open (const char *fname, error *e);
err_t walis_close (struct wal_istream *w, error *e);
err_t walis_crash (struct wal_istream *w, error *e);

/**
 * Seeks to a certain position on disk
 *
 * If the position is out of bounds, it
 * returns an error
 */
err_t walis_seek (struct wal_istream *w, lsn pos, error *e);

/**
 * This is the main read method
 */
err_t walis_read_all (
    struct wal_istream *w,
    bool *iseof,   // At the end, this is set to true or false if we encountered
                   // the eof
    lsn *rlsn,     // NULLABLE The lsn that we just read
    u32 *checksum, // NULLABLE If passed, aggregates the checksum on [data]
    void *dest,    // The data to read into
    u32 len,       // Length of the data to read
    error *e);

/**
 * Because the reader can read sections of a log, and not
 * a whole log, these methods are provided to keep track of the
 * current lsn (for populating [rlsn] in walis_read_all
 *
 * When reading a _full_ log, you'd typically do:
 *
 * walis_mark_start_log()
 *
 * // read part 1
 * // read part 2
 * // read part 3
 * ...
 * // done reading this log
 *
 * walis_mark_end_log()
 */
void walis_mark_start_log (struct wal_istream *w);
void walis_mark_end_log (struct wal_istream *w);
