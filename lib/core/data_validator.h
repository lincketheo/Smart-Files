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

#include "core/data_writer.h"
#include "core/signatures.h"

/**
 * A data validator is used in testing to verify that a data writer
 * matches it's reference data writer. There are two data writers:
 *    ref - The reference writer. This is assumed correct
 *    sut - System under test. This is what we're testing.
 */
struct dvalidtr
{
  struct data_writer ref;
  struct data_writer sut;
  isvalid_func isvalid;
};

/**
 * @brief Conducts a data validator random test. Loops through
 * [niters] times and calls a random method from insert read remove write
 * with random ranges. It does it for both ref and sut and compares the results
 * to ensure they match
 *
 * @param d The data validator to test on
 * @param size The size to use in the data validator test
 * @param niters The number of iterations to run
 * @param max_insert Maximum insertion length for one insert
 * @param e An error object to handle errors
 * @return Error result
 */
err_t dvalidtr_random_test (
    struct dvalidtr *d,
    u32 size,
    u32 niters,
    u64 max_insert,
    error *e);
