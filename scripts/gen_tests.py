#!/usr/bin/env python3
import re
import sys
from pathlib import Path
from collections import defaultdict

def find_tests(root):
    pattern = re.compile(r'TEST\s*\(\s*(\w+)\s*\)')
    root = Path(root)
    for path in sorted(root.rglob("*.c")):
        matches = pattern.findall(path.read_text(errors="replace"))
        if not matches:
            continue
        print(f"  // {path.name}")
        for t in matches:
            print(f"  REGISTER (numstore, {t});")
        print()

if __name__ == "__main__":
    find_tests(sys.argv[1] if len(sys.argv) > 1 else ".")
