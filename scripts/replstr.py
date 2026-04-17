#!/usr/bin/env python3

import sys
from pathlib import Path

directory = sys.argv[1] if len(sys.argv) > 1 else "."
script = Path(__file__).resolve()

for path in Path(directory).rglob("*"):
    if ".git" in path.parts or not path.is_file():
        continue
    if path.resolve() == script:
        continue
    try:
        text = path.read_text()
        if "tlclib" in text:
            path.write_text(text.replace("tlclib", "c_specx"))
            print(f"Updated: {path}")
    except (UnicodeDecodeError, PermissionError):
        pass
