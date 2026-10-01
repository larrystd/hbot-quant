#!/usr/bin/env python3
"""Reject direct absl error constructors in production C++ sources."""

from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[1]
PATTERN = re.compile(r"absl::[A-Za-z]+Error\s*\(")
EXEMPT = ROOT / "hquant/src/base/error.cc"


def main() -> int:
    violations = []
    for source_root in (ROOT / "hquant/src", ROOT / "apps"):
        for path in sorted(source_root.rglob("*")):
            if path.suffix not in {".cc", ".h"} or path == EXEMPT:
                continue
            for line_number, line in enumerate(path.read_text().splitlines(), 1):
                if PATTERN.search(line):
                    violations.append(f"{path.relative_to(ROOT)}:{line_number}: {line.strip()}")
    if violations:
        print("Use hquant::Error(ErrorCode, message) in production code:", file=sys.stderr)
        print("\n".join(violations), file=sys.stderr)
        return 1
    print("No direct absl error constructors in production C++ sources.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
