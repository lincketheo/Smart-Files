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

struct dvalidtr
{
  struct data_writer ref;
  struct data_writer sut;
  isvalid_func isvalid;
};

err_t dvalidtr_random_test (struct dvalidtr *d, u32 size, u32 niters,
                            u64 max_insert, error *e);
