<p align="center">
  <img src="docs/logo.png" alt="NumStore Logo" width="200"/>
</p>

# Smart Files

The concept of a file has had the same definition for the past 50 years. Today I'm announcing the launch of Smart Files, a new API that gets past the hurdles of old school linear, non-transactional system files.

Lots of programming has been built on top of the standard file. All programming languages use files. Files are arrays of bytes. They grow, they shrink, and they seek. But the standard file has two fundamental problems:

**The short comings of standard files:**

1. **Not transactional.** A call to `fwrite` does not guarantee that many bytes actually landed on disk. A crash mid-write leaves your file in an unknown state with no way to recover.
2. **No first-class inner mutations.** There is no standard way to insert or remove a chunk of bytes in the middle of a file without rewriting everything after it.

**Smart files fixes both, and adds two more features:**

1. **Transactions.** Smart files log modifications in a write ahead log so that every mutation commits fully or rolls back - a crash mid-write leaves nothing corrupt.
2. **Inner mutations.** Insert or remove bytes anywhere in the stream in O(log N) time.
3. **Stride access.** Read, write, and remove at regular "strided" intervals without manual offset arithmetic.
4. **Multiple named arrays.** Store more than one named byte stream per file - no separate file handles, no embedded database.

Written in C with no dependencies. Developed in POSIX - Windows builds compile but are not integrated into CI/CD yet (good first contribution).

## Quick Start

### Build the release mode:
```bash
$ git clone https://github.com/lincketheo/smartfiles.git 
$ cd smartfiles
$ git submodule update --init 
$ cmake --preset release
$ cmake --build --preset release
$ ./build/release/samples/sample1_simple # Run a sample 
$ sudo cmake --install ./build/release
```

### Write a quick sample app to test that it works
```c
#include "smfile.h"
#include <stddef.h>
#include <stdint.h>

void print_array(const char* prefix, uint32_t* arr, int len)
{
  printf("%s\n", prefix);
  printf("     ");
  for (int i = 0; i < len; ++i) {
    printf("%d ", arr[i]);
  }
  printf("\n");
}

int main()
{
  // The NULL second parameter is the stream name, feel free to name it any
  // const char* name - NULL is the default
  smfile_t* f = smfile_open("example");

  // We'll write a really big array
  uint32_t data[200000];
  for (int i = 0; i < 200000; ++i) {
    data[i] = i;
  }

  // Write data to the file (length is in bytes)
  smfile_pinsert(f, NULL, data, 0, sizeof(data));

  // Push data into offset 10 (offset is in bytes)
  smfile_pinsert(f, NULL, data, 10 * sizeof(uint32_t), 200000);

  // We'll read a bunch of data in - strided by skipping every 2nd element
  uint32_t read_data[200];
  smfile_pread(f, NULL, read_data, sizeof(uint32_t), 0, 2, 200);
  print_array("Expect: [0, 2, 4, 6, ...], ", read_data, 20);

  // Next we'll remove every 3rd element (it copies what it removed to read_data)
  smfile_premove(f, NULL, read_data, sizeof(uint32_t), 0, 3, 200);
  print_array("Expect: [0, 3, 6, 9, ...], ", read_data, 20);

  // We'll do the same read as before, it should look different now
  smfile_pread(f, NULL, read_data, sizeof(uint32_t), 0, 2, 200);
  print_array("Expect: [1, 4, 7, 0, ...], ", read_data, 20); // Notice the 0 because we wrote an array at index 10

  // Next we'll overwrite every 2nd element
  smfile_pwrite(f, NULL, data, sizeof(uint32_t), 0, 2, 200);

  // One last read, it should be identical to data (0...20)
  smfile_pread(f, NULL, read_data, sizeof(uint32_t), 0, 2, 200);
  print_array("Expect: [0, 1, 2, 3, ...]", read_data, 20);

  smfile_close(f);

  return 0;
}
```

### Compile it:
```
$ gcc main.c -o main -lsmartfiles -L/usr/local/lib
$ ./main
```

You should see:
```
Expect: [0, 2, 4, 6, ...], 
     0 2 4 6 8 0 2 4 6 8 10 12 14 16 18 20 22 24 26 28 
Expect: [0, 3, 6, 9, ...], 
     0 3 6 9 2 5 8 11 14 17 20 23 26 29 32 35 38 41 44 47 
Expect: [1, 4, 7, 0, ...], 
     1 4 7 0 3 6 9 12 15 18 21 24 27 30 33 36 39 42 45 48 
Expect: [0, 1, 2, 3, ...]
     0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 
```

### Explanation

In this example we work with a stream of 200,000 `uint32_t` values (integers 0–199,999) and demonstrate the four core stride operations.

**Step 1 — insert at position 0**

We write the full 200,000-element array at byte offset 0. The file now contains `[0, 1, 2, 3, ...]`.

**Step 2 — insert in the middle**

We insert another 200,000-byte chunk at byte offset 40 (element index 10). This pushes everything from index 10 onwards to the right — the file now has `[0, 1, ..., 9, 0, 1, 2, ..., 9, 10, 11, ...]` at the join.

**Step 3 — strided read (stride=2)**

`smfile_pread` reads every 2nd `uint32_t` starting at offset 0. The result is `[0, 2, 4, 6, 8, 0, 2, ...]` — notice the `0` appearing at index 5 where the inserted block begins.

**Step 4 — strided remove (stride=3)**

`smfile_premove` pulls out every 3rd `uint32_t` and copies what it removed into `read_data`. The removed values are `[0, 3, 6, 9, ...]` — the gaps left behind close up, shifting the remaining elements down.

**Step 5 — strided read again (stride=2)**

The same read as step 3, but the stream has changed shape after the removal. Elements have shifted so the sequence now starts `[1, 4, 7, 0, ...]` — the `0` is still visible from the mid-stream insert.

**Step 6 — strided write (stride=2)**

`smfile_pwrite` overwrites every 2nd element with values from `data` (0, 1, 2, 3, ...). This fills the even-indexed slots with a clean sequence.

**Step 7 — final read**

The last strided read returns `[0, 1, 2, 3, ...]` — the overwrite in step 6 restored a clean sequence at every even position.

## Project Structure:

All the public headers are in `include` all the source code is in `lib`. I pulled off c_specx as core software that I could reuse for other c applications inside thirdparty/c_specx, but that's all my own code - no dependencies.

`lib` is roughly ordered by function
- `algorithms` contains the rope algorithms and database traversal algorithms
- `aries` contains all the logic for rollback, and crash recovery 
- `dpgt` is the dirty page table logic 
- `lockt` is the lock table 
- `os_pager` is a single file pager which simply reads pages from a file
- `pager` is a really important module - it contains all the pager logic for initializing a buffer pool, and reading pages and writing WAL entries 
- `pages` contains all the different  types of pages that Smart Files uses 
- `testing` has some test specific code 
- `txns` has the transaction table, and anything that revolves around transactions 
- `wal` contains write ahead log code

## Contributing

File a ticket on GitHub for bugs, feature requests, or questions. Pull requests welcome - see [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache 2.0. See [LICENSE](LICENSE).
