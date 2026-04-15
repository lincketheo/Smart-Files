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

#define ERR_PAGER_FULL -5                 ///< The buffer pool has no free frames and cannot evict; try committing or reducing working set.
#define ERR_PG_OUT_OF_RANGE -6            ///< Attempted to access a page number beyond the current database extent.
#define ERR_SYNTAX -7                     ///< Statement string failed to parse (compile-time syntax error).
#define ERR_INTERP -8                     ///< Statement parsed successfully but is semantically invalid (compile-time semantic error).
#define ERR_RPTREE_PAGE_STACK_OVERFLOW -9 ///< Internal R+Tree traversal stack overflowed; tree depth exceeds the compiled limit (rare).
#define ERR_DUPLICATE_VARIABLE -10        ///< ns_create() was called with a name that already exists in the variable hash table.
#define ERR_VARIABLE_NE -11               ///< The referenced variable does not exist in the hash table.
#define ERR_DUPLICATE_COMMIT -13          ///< ns_commit() was called on a transaction that has already been committed.
