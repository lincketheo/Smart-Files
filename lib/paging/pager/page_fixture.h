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

#include "core/chunk_alloc.h"
#include "paging/pager/page_h.h"
#include "paging/pages/data_list.h"
#include "paging/pages/inner_node.h"

// numstore
////////////////////////////////////////////////////////////
/// PAGE FIXTURE

struct pgr_fixture
{
  error e;
  struct pager *p;
  struct chunk_alloc alloc;
};

err_t pgr_fixture_create (struct pgr_fixture *dest);
err_t pgr_fixture_teardown (struct pgr_fixture *f);

////////////////////////////////////////////////////////////
/// FAKE DB BUILDING

struct in_page_builder
{
  struct pager *pager;
  struct txn *txn;

  // NULL = no linking, else link to this page
  page_h *prev;
  page_h *next;

  struct in_data children;

  u32 dclen; // desired len of [children] (fills the rest with random)
};

struct dl_page_builder
{
  struct pager *pager;
  struct txn *txn;

  // NULL = no linking, else link to this page
  page_h *prev;
  page_h *next;

  struct dl_data data; // NULL = all random, else use these first

  u32 dclen; // desired len of [children] (fills the rest with random)
};

err_t build_fake_inner_node (page_h *dest, struct in_page_builder b, error *e);
err_t build_fake_data_list (page_h *dest, struct dl_page_builder b, error *e);

////////////////////////////////////////////////////////////
/// DECLARATIVE API

struct page_desc
{
  enum page_type type;
  page_h out;
  b_size size;

  union
  {
    struct
    {
      struct page_desc *children;
      u32 clen;
      u32 dclen;
    } inner;

    struct dl_data data_list;
  };
};

struct page_tree_builder
{
  struct pager *pager;
  struct page_desc root;
  struct txn *txn;
};

////////////////////////////////////////////////////////////
/// COMMON BUILDERS

err_t build_page_tree (struct page_tree_builder *builder, error *e);
err_t page_tree_builder_release_all (struct page_tree_builder *b, error *e);

////////////////////////////////////////////////////////////
/// COMMON TREES

// IN -> DL, DL, ...
#define in1dl(_pager, txid, inl, dl0)                                         \
  {                                                                           \
    .root = {                                                                 \
        .type = PG_INNER_NODE,                                                \
        .out = page_h_create (),                                              \
        .inner = {                                                            \
            .dclen = inl,                                                     \
            .clen = 2,                                                        \
            .children = (struct page_desc[]){                                 \
                {                                                             \
                    .type = PG_DATA_LIST,                                     \
                    .size = dl0,                                              \
                    .out = page_h_create (),                                  \
                    .data_list = (struct dl_data){ .data = NULL, .blen = 0 }, \
                },                                                            \
                                                                              \
            },                                                                \
        },                                                                    \
    },                                                                        \
    .pager = _pager,                                                          \
    .txn = txid,                                                              \
  }

#define in2dl(_pager, txid, inl, dl0, dl1)                                    \
  {                                                                           \
    .root = {                                                                 \
        .type = PG_INNER_NODE,                                                \
        .out = page_h_create (),                                              \
        .inner = {                                                            \
            .dclen = inl,                                                     \
            .clen = 2,                                                        \
            .children = (struct page_desc[]){                                 \
                {                                                             \
                    .type = PG_DATA_LIST,                                     \
                    .size = dl0,                                              \
                    .out = page_h_create (),                                  \
                    .data_list = (struct dl_data){ .data = NULL, .blen = 0 }, \
                },                                                            \
                                                                              \
                {                                                             \
                    .type = PG_DATA_LIST,                                     \
                    .size = dl1,                                              \
                    .out = page_h_create (),                                  \
                    .data_list = (struct dl_data){ .data = NULL, .blen = 0 }, \
                },                                                            \
                                                                              \
            },                                                                \
        },                                                                    \
    },                                                                        \
    .pager = _pager,                                                          \
    .txn = txid,                                                              \
  }

#define in3dl(_pager, txid, inl, dl0, dl1, dl2)                               \
  {                                                                           \
    .root = {                                                                 \
        .type = PG_INNER_NODE,                                                \
        .out = page_h_create (),                                              \
        .inner = {                                                            \
            .dclen = inl,                                                     \
            .clen = 3,                                                        \
            .children = (struct page_desc[]){                                 \
                {                                                             \
                    .type = PG_DATA_LIST,                                     \
                    .size = dl0,                                              \
                    .out = page_h_create (),                                  \
                    .data_list = (struct dl_data){ .data = NULL, .blen = 0 }, \
                },                                                            \
                                                                              \
                {                                                             \
                    .type = PG_DATA_LIST,                                     \
                    .size = dl1,                                              \
                    .out = page_h_create (),                                  \
                    .data_list = (struct dl_data){ .data = NULL, .blen = 0 }, \
                },                                                            \
                                                                              \
                {                                                             \
                    .type = PG_DATA_LIST,                                     \
                    .size = dl2,                                              \
                    .out = page_h_create (),                                  \
                    .data_list = (struct dl_data){ .data = NULL, .blen = 0 }, \
                },                                                            \
                                                                              \
            },                                                                \
        },                                                                    \
    },                                                                        \
    .pager = _pager,                                                          \
    .txn = txid,                                                              \
  }

#define in4dl(_pager, txid, inl, dl0, dl1, dl2, dl3)                          \
  {                                                                           \
    .root = {                                                                 \
        .type = PG_INNER_NODE,                                                \
        .out = page_h_create (),                                              \
        .inner = {                                                            \
            .dclen = inl,                                                     \
            .clen = 4,                                                        \
            .children = (struct page_desc[]){                                 \
                {                                                             \
                    .type = PG_DATA_LIST,                                     \
                    .size = dl0,                                              \
                    .out = page_h_create (),                                  \
                    .data_list = (struct dl_data){ .data = NULL, .blen = 0 }, \
                },                                                            \
                                                                              \
                {                                                             \
                    .type = PG_DATA_LIST,                                     \
                    .size = dl1,                                              \
                    .out = page_h_create (),                                  \
                    .data_list = (struct dl_data){ .data = NULL, .blen = 0 }, \
                },                                                            \
                                                                              \
                {                                                             \
                    .type = PG_DATA_LIST,                                     \
                    .size = dl2,                                              \
                    .out = page_h_create (),                                  \
                    .data_list = (struct dl_data){ .data = NULL, .blen = 0 }, \
                },                                                            \
                                                                              \
                {                                                             \
                    .type = PG_DATA_LIST,                                     \
                    .size = dl3,                                              \
                    .out = page_h_create (),                                  \
                    .data_list = (struct dl_data){ .data = NULL, .blen = 0 }, \
                },                                                            \
                                                                              \
            },                                                                \
        },                                                                    \
    },                                                                        \
    .pager = _pager,                                                          \
    .txn = txid,                                                              \
  }

#define in5dl(_pager, txid, inl, dl0, dl1, dl2, dl3, dl4)                     \
  {                                                                           \
    .root = {                                                                 \
        .type = PG_INNER_NODE,                                                \
        .out = page_h_create (),                                              \
        .inner = {                                                            \
            .dclen = inl,                                                     \
            .clen = 5,                                                        \
            .children = (struct page_desc[]){                                 \
                {                                                             \
                    .type = PG_DATA_LIST,                                     \
                    .size = dl0,                                              \
                    .out = page_h_create (),                                  \
                    .data_list = (struct dl_data){ .data = NULL, .blen = 0 }, \
                },                                                            \
                                                                              \
                {                                                             \
                    .type = PG_DATA_LIST,                                     \
                    .size = dl1,                                              \
                    .out = page_h_create (),                                  \
                    .data_list = (struct dl_data){ .data = NULL, .blen = 0 }, \
                },                                                            \
                                                                              \
                {                                                             \
                    .type = PG_DATA_LIST,                                     \
                    .size = dl2,                                              \
                    .out = page_h_create (),                                  \
                    .data_list = (struct dl_data){ .data = NULL, .blen = 0 }, \
                },                                                            \
                                                                              \
                {                                                             \
                    .type = PG_DATA_LIST,                                     \
                    .size = dl3,                                              \
                    .out = page_h_create (),                                  \
                    .data_list = (struct dl_data){ .data = NULL, .blen = 0 }, \
                },                                                            \
                                                                              \
                {                                                             \
                    .type = PG_DATA_LIST,                                     \
                    .size = dl4,                                              \
                    .out = page_h_create (),                                  \
                    .data_list = (struct dl_data){ .data = NULL, .blen = 0 }, \
                },                                                            \
            },                                                                \
        },                                                                    \
    },                                                                        \
    .pager = _pager,                                                          \
    .txn = txid,                                                              \
  }

// IN -> IN, IN,
#define in1in(_pager, txid, inl, in0)                     \
  {                                                       \
    .root = {                                             \
        .type = PG_INNER_NODE,                            \
        .out = page_h_create (),                          \
        .inner = {                                        \
            .dclen = inl,                                 \
            .clen = 2,                                    \
            .children = (struct page_desc[]){             \
                {                                         \
                    .type = PG_INNER_NODE,                \
                    .out = page_h_create (),              \
                    .inner = { .dclen = in0, .clen = 0 }, \
                },                                        \
                                                          \
            },                                            \
        },                                                \
    },                                                    \
    .pager = _pager,                                      \
    .txn = txid,                                          \
  }

#define in2in(_pager, txid, inl, in0, in1)                \
  {                                                       \
    .root = {                                             \
        .type = PG_INNER_NODE,                            \
        .out = page_h_create (),                          \
        .inner = {                                        \
            .dclen = inl,                                 \
            .clen = 2,                                    \
            .children = (struct page_desc[]){             \
                {                                         \
                    .type = PG_INNER_NODE,                \
                    .out = page_h_create (),              \
                    .inner = { .dclen = in0, .clen = 0 }, \
                },                                        \
                                                          \
                {                                         \
                    .type = PG_INNER_NODE,                \
                    .out = page_h_create (),              \
                    .inner = { .dclen = in1, .clen = 0 }, \
                },                                        \
                                                          \
            },                                            \
        },                                                \
    },                                                    \
    .pager = _pager,                                      \
    .txn = txid,                                          \
  }

#define in3in(_pager, txid, inl, in0, in1, in2)           \
  {                                                       \
    .root = {                                             \
        .type = PG_INNER_NODE,                            \
        .out = page_h_create (),                          \
        .inner = {                                        \
            .dclen = inl,                                 \
            .clen = 3,                                    \
            .children = (struct page_desc[]){             \
                {                                         \
                    .type = PG_INNER_NODE,                \
                    .out = page_h_create (),              \
                    .inner = { .dclen = in0, .clen = 0 }, \
                },                                        \
                                                          \
                {                                         \
                    .type = PG_INNER_NODE,                \
                    .out = page_h_create (),              \
                    .inner = { .dclen = in1, .clen = 0 }, \
                },                                        \
                                                          \
                {                                         \
                    .type = PG_INNER_NODE,                \
                    .out = page_h_create (),              \
                    .inner = { .dclen = in2, .clen = 0 }, \
                },                                        \
                                                          \
            },                                            \
        },                                                \
    },                                                    \
    .pager = _pager,                                      \
    .txn = txid,                                          \
  }

#define in4in(_pager, txid, inl, in0, in1, in2, in3)      \
  {                                                       \
    .root = {                                             \
        .type = PG_INNER_NODE,                            \
        .out = page_h_create (),                          \
        .inner = {                                        \
            .dclen = inl,                                 \
            .clen = 4,                                    \
            .children = (struct page_desc[]){             \
                {                                         \
                    .type = PG_INNER_NODE,                \
                    .out = page_h_create (),              \
                    .inner = { .dclen = in0, .clen = 0 }, \
                },                                        \
                                                          \
                {                                         \
                    .type = PG_INNER_NODE,                \
                    .out = page_h_create (),              \
                    .inner = { .dclen = in1, .clen = 0 }, \
                },                                        \
                                                          \
                {                                         \
                    .type = PG_INNER_NODE,                \
                    .out = page_h_create (),              \
                    .inner = { .dclen = in2, .clen = 0 }, \
                },                                        \
                                                          \
                {                                         \
                    .type = PG_INNER_NODE,                \
                    .out = page_h_create (),              \
                    .inner = { .dclen = in3, .clen = 0 }, \
                },                                        \
                                                          \
            },                                            \
        },                                                \
    },                                                    \
    .pager = _pager,                                      \
    .txn = txid,                                          \
  }

#define in5in(_pager, txid, inl, in0, in1, in2, in3, in4) \
  {                                                       \
    .root = {                                             \
        .type = PG_INNER_NODE,                            \
        .out = page_h_create (),                          \
        .inner = {                                        \
            .dclen = inl,                                 \
            .clen = 5,                                    \
            .children = (struct page_desc[]){             \
                {                                         \
                    .type = PG_INNER_NODE,                \
                    .out = page_h_create (),              \
                    .inner = { .dclen = in0, .clen = 0 }, \
                },                                        \
                                                          \
                {                                         \
                    .type = PG_INNER_NODE,                \
                    .out = page_h_create (),              \
                    .inner = { .dclen = in1, .clen = 0 }, \
                },                                        \
                                                          \
                {                                         \
                    .type = PG_INNER_NODE,                \
                    .out = page_h_create (),              \
                    .inner = { .dclen = in2, .clen = 0 }, \
                },                                        \
                                                          \
                {                                         \
                    .type = PG_INNER_NODE,                \
                    .out = page_h_create (),              \
                    .inner = { .dclen = in3, .clen = 0 }, \
                },                                        \
                                                          \
                {                                         \
                    .type = PG_INNER_NODE,                \
                    .out = page_h_create (),              \
                    .inner = { .dclen = in4, .clen = 0 }, \
                },                                        \
            },                                            \
        },                                                \
    },                                                    \
    .pager = _pager,                                      \
    .txn = txid,                                          \
  }
