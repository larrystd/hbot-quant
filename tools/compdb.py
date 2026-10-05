#!/usr/bin/env python3
"""Generate compile_commands.json from Bazel C++ compile actions."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
QUERY = 'mnemonic("CppCompile", //...)'
SOURCE_ROOTS = ("hquant/", "dev/")


def bazel_output(bazel: str, *arguments: str) -> str:
    result = subprocess.run(
        [bazel, *arguments], cwd=ROOT, text=True, capture_output=True
    )
    if result.returncode:
        raise RuntimeError(result.stderr or result.stdout)
    return result.stdout


def xcode_substitutions(execution_root: Path) -> list[tuple[str, str]]:
    """apple_support emits placeholders that only its wrapper resolves; clangd
    needs the real compiler and SDK paths instead."""
    if sys.platform != "darwin":
        return []
    def xcrun(*arguments: str) -> str:
        return subprocess.run(["xcrun", *arguments], text=True,
                              capture_output=True, check=True).stdout.strip()
    developer_dir = subprocess.run(["xcode-select", "-p"], text=True,
                                   capture_output=True, check=True).stdout.strip()
    return [
        ("__BAZEL_XCODE_SDKROOT__", xcrun("--sdk", "macosx", "--show-sdk-path")),
        ("__BAZEL_XCODE_DEVELOPER_DIR__", developer_dir),
        ("__BAZEL_EXECUTION_ROOT__", str(execution_root)),
    ]


def main() -> None:
    bazel = os.environ.get("BAZEL_BIN") or shutil.which("bazelisk") or shutil.which("bazel")
    if not bazel:
        raise RuntimeError("Bazelisk or Bazel was not found; set BAZEL_BIN")
    extra = sys.argv[1:]  # e.g. --config=asan, so flags match that build
    execution_root = Path(bazel_output(bazel, "info", *extra, "execution_root").strip())
    actions = json.loads(
        bazel_output(bazel, "aquery", *extra, QUERY, "--output=jsonproto")
    ).get("actions", [])

    substitutions = xcode_substitutions(execution_root)
    compiler = shutil.which("clang++") if substitutions else None

    entries = []
    for action in actions:
        arguments = action.get("arguments", [])
        if "-c" not in arguments:
            continue
        source = arguments[arguments.index("-c") + 1]
        if not source.startswith(SOURCE_ROOTS):
            continue
        path = ROOT / source
        if not path.is_file():
            continue
        if compiler and "wrapped_clang" in arguments[0]:
            arguments = [compiler, *arguments[1:]]
        for placeholder, value in substitutions:
            arguments = [argument.replace(placeholder, value) for argument in arguments]
        entries.append({
            "directory": str(execution_root),
            "file": str(path),
            "arguments": arguments,
        })

    if not entries:
        raise RuntimeError("Bazel returned no local C++ compile actions")
    entries.sort(key=lambda entry: entry["file"])
    destination = ROOT / "compile_commands.json"
    temporary = destination.with_suffix(".json.tmp")
    try:
        temporary.write_text(json.dumps(entries, indent=2) + "\n", encoding="utf-8")
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)
    print(f"Wrote {len(entries)} Bazel compile commands to {destination}")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, ValueError) as error:
        print(error, file=sys.stderr)
        sys.exit(1)
