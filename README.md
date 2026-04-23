<p align="center">
  <img src="docs/logo.png" alt="NumStore Logo" width="200"/>
</p>

# Smart Files

The concept of a file has had the same definition for the past 50 years. Today I'm announcing the launch of Smart Files, a new API that gets past the hurdles of old school linear, non-transactional system files.

Lots of programming has been built on top of the standard file. All programming languages use files. Files are arrays of bytes. They grow, they shrink, and they seek. But the standard file has two fundamental problems:

**THE SHORT COMINGS OF STANDARD FILES**

1. **Not transactional.** A call to `fwrite` does not guarantee that many bytes actually landed on disk. A crash mid-write leaves your file in an unknown state with no way to recover.
2. **No first-class inner mutations.** There is no standard way to insert or remove a chunk of bytes in the middle of a file without rewriting everything after it.

**SMART FILES FIXES BOTH, AND ADDS TWO MORE THINGS:**

1. **Transactions.** Smart files log modifications in a write ahead log so that every mutation commits fully or rolls back - a crash mid-write leaves nothing corrupt.
2. **Inner mutations.** Insert or remove bytes anywhere in the stream in O(log N) time.
3. **Stride access.** Read, write, and remove at regular "strided" intervals without manual offset arithmetic.
4. **Multiple named arrays.** Store more than one named byte stream per file - no separate file handles, no embedded database.

Written in C with no dependencies. Developed in POSIX - Windows builds compile but are not integrated into CI/CD yet (good first contribution).

## Quick Start

```bash
git clone https://github.com/lincketheo/smartfiles.git 
cd smartfiles
git submodule update --init 
cmake --preset release
cd build/release
make
./samples/sample1_simple
sudo make install
```

```c
#include "smfile.h"

// your code here
```

## Contributing

File a ticket on GitHub for bugs, feature requests, or questions. Pull requests welcome - see [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache 2.0. See [LICENSE](LICENSE).
