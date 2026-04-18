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

#include "pager.h"

/**
 * nsdb is a holder of generic resources in smart files
 *
 * I seperated it from smfile so I could seperate the internals from
 * the user facing data structure.
 *
 * Anything inside nsdb is "core" - e.g. it's the good algorithms that user
 * facing wrappers like smfile can use
 *
 * For now it's just a pager - but if there are more db related resources, it
 * could hold more
 */
struct nsdb
{
  struct pager *p; ///< The pager backing this database
};
