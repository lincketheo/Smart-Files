#!/usr/bin/env python3
"""
lint_headers.py - Apply source file rules to C/H files.
Usage: python lint_headers.py [directory]   (default: .)
"""

import os
import re
import argparse
from abc import ABC, abstractmethod
from pathlib import Path

FILE_EXTENSIONS = [".c", ".h"]

IGNORE_BASENAMES = {"testing.h", "assert.h"}

COPYRIGHT_HEADER = """\
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
/// limitations under the License."""

STANDARD_HEADERS = {
    "complex.h", "ctype.h", "errno.h", "fenv.h", "float.h",
    "inttypes.h", "iso646.h", "limits.h", "locale.h", "math.h", "setjmp.h",
    "signal.h", "stdalign.h", "stdarg.h", "stdatomic.h", "stdbool.h",
    "stddef.h", "stdint.h", "stdio.h", "stdlib.h", "stdnoreturn.h",
    "string.h", "tgmath.h", "threads.h", "time.h", "uchar.h", "wchar.h",
    "wctype.h",
    "unistd.h", "fcntl.h", "sys/types.h", "sys/stat.h", "sys/mman.h",
    "sys/socket.h", "sys/wait.h", "sys/time.h", "sys/resource.h",
    "sys/ioctl.h", "sys/uio.h", "sys/un.h", "sys/select.h",
    "pthread.h", "semaphore.h", "dirent.h", "glob.h", "fnmatch.h",
    "poll.h", "netdb.h", "netinet/in.h", "arpa/inet.h",
    "termios.h", "syslog.h", "pwd.h", "grp.h",
}

class Rule(ABC):
    @property
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def apply(self, path: Path, lines: list[str]) -> tuple[list[str], list[str]]:
        """Returns (new_lines, warnings, errors)."""
        ...

class CopyrightRule(Rule):
    name = "copyright"
    _LINE_COMMENT_RE = re.compile(r"^\s*//")
    _COPYRIGHT_RE    = re.compile(r"copyright|license|licenced|spdx", re.IGNORECASE)

    def apply(self, path, lines):
        start = 0
        while start < len(lines) and lines[start].strip() == "":
            start += 1

        end = start
        if end < len(lines):
            if lines[end].strip().startswith("/*"):
                while end < len(lines) and "*/" not in lines[end]:
                    end += 1
                end += 1
            elif self._LINE_COMMENT_RE.match(lines[end]):
                while end < len(lines) and self._LINE_COMMENT_RE.match(lines[end]):
                    end += 1

        existing = "".join(lines[start:end])
        rest = lines[end:] if (existing.strip() and self._COPYRIGHT_RE.search(existing)) else lines[start:]
        return (COPYRIGHT_HEADER + "\n\n").splitlines(keepends=True) + rest, [], []

class AngleBracketIncludeRule(Rule):
    name = "angle_bracket_includes"
    _RE = re.compile(r'^(\s*#\s*include\s*)[<"]([^>"]+)[>"]')

    def apply(self, path, lines):
        new_lines = []
        for line in lines:
            m = self._RE.match(line)
            if m:
                prefix, p = m.group(1), m.group(2)
                if p in STANDARD_HEADERS:
                    line = f"{prefix}<{p}>\n"
                else:
                    line = f'{prefix}"{p}"\n'
            new_lines.append(line)
        return new_lines, [], []

class IncludeOrderRule(Rule):
    name = "include_order"
    _INCLUDE_RE = re.compile(r"^\s*#\s*include\s*([<\"])([^>\"]+)[>\"]")

    def _classify(self, include: str, stem: str) -> int:
        if include in STANDARD_HEADERS:
            return 2  # system: <header>
        base = include.split("/")[-1]
        if (base[:-2] if base.endswith(".h") else base) == stem:
            return 0  # own header: "foo.h" in foo.c
        return 1      # project header: "other.h"

    def _fmt(self, include: str, bucket: int) -> str:
        return f"#include <{include}>\n" if bucket == 2 else f'#include "tlclib/{include}"\n'

    def apply(self, path, lines):
        warnings = []
        stem = path.stem

        start = end = None
        for i, line in enumerate(lines):
            if self._INCLUDE_RE.match(line):
                if start is None: start = i
                end = i + 1
            elif start is not None and line.strip() and not line.strip().startswith("//"):
                break

        if start is None:
            return lines, [], []

        buckets: dict[int, list[str]] = {0: [], 1: [], 2: []}
        for line in lines[start:end]:
            m = self._INCLUDE_RE.match(line)
            if m:
                p = m.group(2)
                b = self._classify(p, stem)
                buckets[b].append(self._fmt(p, b))

        ordered: list[str] = []
        for b in [0, 1, 2]:
            if buckets[b]:
                ordered.extend(sorted(buckets[b]))
                ordered.append("\n")

        return lines[:start] + ordered + lines[end:], warnings, []

class BannerCommentRule(Rule):
    name = "banner_comments"
    _BANNER_RE = re.compile(r'^(\s*)/{8,}(.*)$')
    DIVIDER_LEN = 60

    def apply(self, path, lines):
        new_lines = []
        i = 0
        while i < len(lines):
            m = self._BANNER_RE.match(lines[i])
            if not m:
                new_lines.append(lines[i])
                i += 1
                continue

            indent = m.group(1)
            group = []
            while i < len(lines):
                bm = self._BANNER_RE.match(lines[i])
                if bm and bm.group(1) == indent:
                    group.append(bm.group(2).lstrip("/").strip())
                    i += 1
                else:
                    break

            texts = [t for t in group if t]
            new_lines.append(indent + "/" * self.DIVIDER_LEN + "\n")
            for t in texts:
                new_lines.append(f"{indent}// {t}\n")

        return new_lines, [], []

class HeaderSectionSpacingRule(Rule):
    name = "header_section_spacing"
    _COPYRIGHT_RE = re.compile(r"^\s*///")
    _PRAGMA_RE = re.compile(r"^\s*(?://\s*)?#\s*pragma\s+once")

    def apply(self, path, lines):
        # Find end of copyright block
        copyright_end = 0
        for i, line in enumerate(lines):
            if self._COPYRIGHT_RE.match(line):
                copyright_end = i + 1
            elif copyright_end and line.strip():
                break

        # Find pragma once near the top
        pragma_idx = None
        for i, line in enumerate(lines):
            if self._PRAGMA_RE.match(line):
                pragma_idx = i
                break

        # Headers must have pragma once
        if pragma_idx is None:
            if path.suffix == ".h":
                return lines, [], [f"  missing #pragma once"]
            return lines, [], []

        # Find first non-blank line after pragma
        rest_start = pragma_idx + 1
        while rest_start < len(lines) and lines[rest_start].strip() == "":
            rest_start += 1

        new_lines = lines[:copyright_end]
        new_lines += ["\n"]                  # 1 blank line after license
        new_lines += [lines[pragma_idx]]
        new_lines += ["\n"]                  # 1 blank line after pragma
        new_lines += lines[rest_start:]
        return new_lines, [], []

class CollapseBlankLinesRule(Rule):
    name = "collapse_blank_lines"

    def apply(self, path, lines):
        new_lines = []
        prev_blank = False
        for line in lines:
            is_blank = line.strip() == ""
            if is_blank and prev_blank:
                continue
            new_lines.append(line)
            prev_blank = is_blank
        return new_lines, [], []

RULES: list[Rule] = [
    CopyrightRule(),
    HeaderSectionSpacingRule(),
    CollapseBlankLinesRule(),
]

def process_file(path: Path) -> bool:
    """Returns True if the file had errors (not just warnings or modifications)."""
    if path.name in IGNORE_BASENAMES:
        return False
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        lines = f.readlines()

    original = lines[:]
    had_errors = False
    for rule in RULES:
        lines, warnings, errors = rule.apply(path, lines)
        for w in warnings:
            print(f"WARN  [{rule.name}] {path}: {w}")
        for e in errors:
            print(f"ERROR [{rule.name}] {path}: {e}")
            had_errors = True

    if lines != original:
        with open(path, "w", encoding="utf-8") as f:
            f.writelines(lines)
        print(f"MODIFIED  {path}")

    return had_errors
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", nargs="?", default=".")
    args = parser.parse_args()

    had_errors = False
    for root, _, files in os.walk(args.directory):
        parts = Path(root).parts
        if any(p.startswith(".") or p in {"build", "node_modules"} for p in parts):
            continue
        for fname in sorted(files):
            if any(fname.endswith(ext) for ext in FILE_EXTENSIONS):
                if process_file(Path(root) / fname):
                    had_errors = True

    if had_errors:
        raise SystemExit(1)

if __name__ == "__main__":
    main()
