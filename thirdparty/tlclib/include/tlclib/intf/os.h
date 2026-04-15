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

#include "tlclib/intf/os/file_system.h"
#include "tlclib/intf/os/memory.h"
#include "tlclib/intf/os/socket.h"
#include "tlclib/intf/os/threading.h"
#include "tlclib/intf/os/time.h"

////////////////////////////////////////////////////////////
// Runtime
#if defined(_MSC_VER)

#define spin_pause() YieldProcessor ()

#elif defined(__x86_64__) || defined(__i386__)

#define spin_pause() __asm__ volatile ("pause" :: \
                                           : "memory")

#elif defined(__aarch64__) || defined(__arm__)

#define spin_pause() __asm__ volatile ("yield" :: \
                                           : "memory")

#else

#define spin_pause() atomic_signal_fence (memory_order_seq_cst)

#endif
