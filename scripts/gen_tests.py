#!/usr/bin/env python3
import re
import sys
from pathlib import Path
from collections import defaultdict

def find_tests(root):
    pattern = re.compile(r'TEST\s*\(\s*\w+\s*,\s*(\w+)\s*\)')
    root = Path(root)

    for suite_dir in sorted(root.iterdir()):
        if not suite_dir.is_dir():
            continue

        suite = suite_dir.name
        by_file = defaultdict(list)

        for path in sorted(suite_dir.rglob("*.c")):
            matches = pattern.findall(path.read_text(errors="replace"))
            if matches:
                by_file[path.name].extend(matches)

        if not by_file:
            continue

        print(f"  // --------------------------------------------------------")
        print(f"  // {suite}")
        print(f"  // --------------------------------------------------------")
        print()
        for fname, tests in by_file.items():
            print(f"  // {fname}")
            for t in tests:
                print(f"  REGISTER ({suite}, {t});")
        print()

if __name__ == "__main__":
    find_tests(sys.argv[1] if len(sys.argv) > 1 else ".")
