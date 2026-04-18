#pragma once

#include "txns/txn.h"

struct nsvar {
  struct nsdb* parent;
  bool iownnsdb;
  struct variable* v;
  struct chunk_alloc alloc;
  struct txn tx;
};
