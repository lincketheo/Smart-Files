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

#include "core/llist.h"

#include "core/assert.h"
#include "test/testing.h"

struct int_node
{
  int value;
  struct llnode node;
};

TEST (TT_UNIT, llist)
{
  struct int_node nodes[10];
  nodes[0].value = 0;
  llnode_init (&nodes[0].node);

  struct llnode *head = &nodes[0].node;

  for (u32 i = 1; i < 10; ++i)
    {
      nodes[i].value = i;
      list_push (&head, &nodes[i].node);
    }

  for (u32 i = 10; i > 0; --i)
    {
      struct llnode *ret = list_pop (&head);
      const struct int_node *node = container_of (ret, struct int_node, node);
      test_assert_int_equal (node->value, i - 1);
    }

  const struct llnode *ret = list_pop (&head);
  test_assert_equal (ret, NULL);
  test_assert_equal (head, NULL);

  head = &nodes[0].node;
}
