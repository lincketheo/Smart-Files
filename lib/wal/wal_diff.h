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

#include "smfile.h"

struct wal_page_diff
{
  p_size bit_start;
  void *undo;
  void *redo;
  p_size len;
};

struct wal_page_diff wpd_from_pages (const void *before, const void *after);
void wpd_apply_undo (void *page, struct wal_page_diff *diff);
void wpd_apply_redo (void *page, struct wal_page_diff *diff);
