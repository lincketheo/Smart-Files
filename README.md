<p align="center">
  <img src="docs/logo.png" alt="NumStore Logo" width="200"/>
</p>

# NumStore

A transactional database for contiguous byte streams. Supports O(log N) insert, delete, write, and seek anywhere in the stream - compared to O(N) for a flat file.

Under the hood it's a Rope+Tree (a B+Tree variant where inner nodes track subtree element counts instead of key boundaries), wrapped in a Steal No Force transactional engine with a WAL, buffer pool, and CLOCK eviction. All mutations are atomic and attached to transactions.

Written in C with no dependencies. Developed in POSIX - Windows builds compile but are not integrated into CI/CD yet (good first contribution).

## Quick Start

```bash
git clone https://gitlab.com/lincketheo/numstore.git
cmake --preset release
cd build/release
make
./samples/sample1_simple
sudo make install
```

```c
#include "numstore.h"

// your code here
```

## Contributing

File a ticket on GitHub for bugs, feature requests, or questions. Pull requests welcome - see [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache 2.0. See [LICENSE](LICENSE).
