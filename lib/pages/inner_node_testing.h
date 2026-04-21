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

#ifndef NTEST
// smartfiles
#include "pages/page.h"

void in_print (const page *in);

void in_print_as_arrays (const pgno *pgs, const b_size *keys, p_size len);

void inner_node_init_for_testing (page *in,           // Destination page
                                  const pgno *pgs,    // The pages to have
                                  const b_size *keys, // The keys to have
                                  p_size len          // len of [pgs] and [keys]
);

void test_assert_inner_node_equal (const page *actual,   // Actual page numbers
                                   const pgno *e_pgs,    // Expected pages
                                   const b_size *e_keys, // Expected keys
                                   p_size len            // Len of [e_pgs] and [e_keys]
);

#endif
