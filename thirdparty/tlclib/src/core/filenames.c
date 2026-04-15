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

#include "tlclib/core/filenames.h"

#include "tlclib/dev/testing.h"

#include <string.h>

const char *
file_basename (const char *path)
{
  const char *p1 = strrchr (path, '/');
  const char *p2 = strrchr (path, '\\');
  const char *p = p1 > p2 ? p1 : p2;
  return p ? p + 1 : path;
}

#ifndef NTEST
TEST (file_basename)
{
  test_assert (strcmp (file_basename ("foo/bar"), "bar") == 0);
  test_assert (strcmp (file_basename ("/foo/bar/baz.c"), "baz.c") == 0);
  test_assert (strcmp (file_basename ("/foo/bar/"), "") == 0);
  test_assert (strcmp (file_basename ("/"), "") == 0);

  test_assert (strcmp (file_basename ("foo\\bar"), "bar") == 0);
  test_assert (strcmp (file_basename ("C:\\path\\file.txt"), "file.txt") == 0);
  test_assert (strcmp (file_basename ("C:\\"), "") == 0);

  test_assert (strcmp (file_basename ("/mixed\\sep/file"), "file") == 0);
  test_assert (strcmp (file_basename ("mixed/sep\\file.ext"), "file.ext") == 0);

  test_assert (strcmp (file_basename ("filename"), "filename") == 0);
  test_assert (strcmp (file_basename (""), "") == 0);
}
#endif
