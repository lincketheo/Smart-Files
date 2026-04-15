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

#include "tlclib/dev/signatures.h"

struct llnode
{
  struct llnode *next;
};

HEADER_FUNC void
llnode_init (struct llnode *n)
{
  n->next = NULL;
}

HEADER_FUNC u32
list_length (const struct llnode *head)
{
  u32 len = 0;
  for (const struct llnode *cur = head; cur; cur = cur->next)
    {
      len++;
    }
  return len;
}

HEADER_FUNC void
list_push (struct llnode **head, struct llnode *n)
{
  n->next = *head;
  *head = n;
}

HEADER_FUNC void
list_append (struct llnode **head, struct llnode *n)
{
  n->next = NULL;
  if (!*head)
    {
      *head = n;
    }
  else
    {
      struct llnode *cur = *head;
      while (cur->next)
        {
          cur = cur->next;
        }
      cur->next = n;
    }
}

HEADER_FUNC struct llnode *
list_pop (struct llnode **head)
{
  if (!*head)
    {
      return NULL;
    }

  struct llnode *n = *head;
  *head = n->next;
  n->next = NULL;

  return n;
}

HEADER_FUNC struct llnode *
list_find (u32 *didx, struct llnode *head, const struct llnode *node,
           bool (*eq) (const struct llnode *left, const struct llnode *right))
{
  *didx = 0;
  for (struct llnode *iter = (head); iter;
       iter = iter->next, *didx = *didx + 1)
    {
      if (eq (iter, node))
        {
          return iter;
        }
    }
  return NULL;
}

HEADER_FUNC void
list_remove (struct llnode **head, struct llnode *n)
{
  struct llnode **cur = head;
  while (*cur && *cur != n)
    {
      cur = &(*cur)->next;
    }
  if (*cur)
    {
      *cur = n->next;
      n->next = NULL;
    }
}

HEADER_FUNC struct llnode *
llnode_get_n (struct llnode *head, const u32 index)
{
  struct llnode *cur = head;
  for (u32 i = 0; cur && i < index; ++i)
    {
      cur = cur->next;
    }
  return cur;
}

// Iterate over list
#define LLIST_FOR_EACH(head, iter) \
  for (llnode *iter = (head); iter; iter = iter->next)
