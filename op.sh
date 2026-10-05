#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BAZEL_BIN="${BAZEL_BIN:-$(command -v bazelisk || command -v bazel || true)}"
PYTHON_BIN="${PYTHON_BIN:-$(command -v python3 || true)}"
LLVM_MAJOR=20
SOURCE_ROOTS=(hquant dev)

cd "${PROJECT_DIR}"

usage() {
    cat <<'HELP'
Usage: ./op.sh <command> [arguments]

Build and test:
  build [targets...]      Build targets (default //...)
  release [targets...]    Build with --config=release
  test [targets...]       Run tests (default //...)
  ci                      Build and test every target
  asan | tsan             Run the sanitizer smoke test
  clean                   Remove Bazel build outputs

C++ quality:
  fmt-check [files...]    Check named files, or all handwritten C++ files
  fmt <files...>          Format only named files
  fmt-all                 Format all handwritten C++ files
  compdb [bazel flags]    Generate compile_commands.json (e.g. compdb --config=asan)
  doctor                  Print the selected tools

Overrides: BAZEL_BIN, PYTHON_BIN, HQUANT_LLVM_BIN
HELP
}

require_bazel() {
    if [[ -z "${BAZEL_BIN}" || ! -x "${BAZEL_BIN}" ]]; then
        echo "Bazelisk or Bazel was not found; set BAZEL_BIN." >&2
        exit 1
    fi
}

require_python() {
    if [[ -z "${PYTHON_BIN}" || ! -x "${PYTHON_BIN}" ]]; then
        echo "Python 3 was not found; set PYTHON_BIN." >&2
        exit 1
    fi
}

# Prefer LLVM 20 so formatting is stable across machines; Homebrew keeps it keg-only.
find_clang_format() {
    local candidates=()
    if [[ -n "${HQUANT_LLVM_BIN:-}" ]]; then
        candidates+=("${HQUANT_LLVM_BIN}/clang-format")
    else
        candidates+=("$(command -v "clang-format-${LLVM_MAJOR}" || true)")
        candidates+=("/opt/homebrew/opt/llvm@${LLVM_MAJOR}/bin/clang-format")
        candidates+=("/usr/local/opt/llvm@${LLVM_MAJOR}/bin/clang-format")
        candidates+=("$(command -v clang-format || true)")
    fi
    local tool
    for tool in "${candidates[@]}"; do
        [[ -n "${tool}" && -x "${tool}" ]] || continue
        if "${tool}" --version | grep -Eq "version ${LLVM_MAJOR}(\.|$| )"; then
            echo "${tool}"
            return
        fi
    done
    echo "clang-format ${LLVM_MAJOR} not found. On macOS: brew install llvm@${LLVM_MAJOR};" \
         "or set HQUANT_LLVM_BIN to its bin directory." >&2
    exit 1
}

all_source_files() {
    find "${SOURCE_ROOTS[@]}" -type f \
        \( -name '*.h' -o -name '*.hpp' -o -name '*.cc' -o -name '*.cpp' \) | sort
}

check_source_file() {
    local path="$1"
    if [[ ! -f "${path}" ]]; then
        echo "not a file: ${path}" >&2; exit 2
    fi
    case "${path}" in
        *.h|*.hpp|*.cc|*.cpp) ;;
        *) echo "not a supported C++ file: ${path}" >&2; exit 2 ;;
    esac
}

run_format() {
    local mode="$1"; shift
    local tool
    tool="$(find_clang_format)"
    local options=(--dry-run --Werror)
    [[ "${mode}" == write ]] && options=(-i)
    local failed=0 path
    for path in "$@"; do
        check_source_file "${path}"
        if ! "${tool}" --style=file "${options[@]}" "${path}" 2>/dev/null; then
            echo "  needs formatting: ${path}" >&2
            failed=$((failed + 1))
        fi
    done
    if [[ "${failed}" -ne 0 ]]; then
        echo "${failed} of $# files failed formatting ${mode}." >&2
        echo "Run ./op.sh fmt <files...> or ./op.sh fmt-all to fix." >&2
        return 1
    fi
    echo "$([[ "${mode}" == write ]] && echo Formatted || echo Checked) $# C++ files."
}

case "${1:-help}" in
    build)
        require_bazel
        shift
        "${BAZEL_BIN}" build "${@:-//...}"
        ;;
    release)
        require_bazel
        shift
        "${BAZEL_BIN}" build --config=release "${@:-//...}"
        ;;
    test)
        require_bazel
        shift
        "${BAZEL_BIN}" test "${@:-//...}"
        ;;
    ci)
        require_bazel
        "${BAZEL_BIN}" build //...
        "${BAZEL_BIN}" test //...
        ;;
    asan|tsan)
        require_bazel
        "${BAZEL_BIN}" test --config="$1" //dev:sanitizer_smoke
        ;;
    clean)
        require_bazel
        "${BAZEL_BIN}" clean
        ;;
    fmt-check)
        shift
        if [[ $# -eq 0 ]]; then
            mapfile_files=()
            while IFS= read -r f; do mapfile_files+=("${f}"); done < <(all_source_files)
            run_format check "${mapfile_files[@]}"
        else
            run_format check "$@"
        fi
        ;;
    fmt)
        shift
        if [[ $# -eq 0 ]]; then echo "fmt requires explicit files." >&2; exit 2; fi
        run_format write "$@"
        ;;
    fmt-all)
        shift
        if [[ $# -ne 0 ]]; then echo "fmt-all takes no files." >&2; exit 2; fi
        mapfile_files=()
        while IFS= read -r f; do mapfile_files+=("${f}"); done < <(all_source_files)
        run_format write "${mapfile_files[@]}"
        ;;
    compdb)
        require_bazel
        require_python
        shift
        BAZEL_BIN="${BAZEL_BIN}" "${PYTHON_BIN}" tools/compdb.py "$@"
        ;;
    doctor)
        echo "Project: ${PROJECT_DIR}"
        echo "Bazel: ${BAZEL_BIN:-not found}"
        if [[ -n "${BAZEL_BIN}" && -x "${BAZEL_BIN}" ]]; then "${BAZEL_BIN}" --version; fi
        echo "Python: ${PYTHON_BIN:-not found}"
        if [[ -n "${PYTHON_BIN}" && -x "${PYTHON_BIN}" ]]; then "${PYTHON_BIN}" --version; fi
        if tool="$(find_clang_format 2>/dev/null)"; then
            echo "clang-format: ${tool} ($("${tool}" --version))"
        else
            echo "clang-format: ${LLVM_MAJOR} not found"
        fi
        echo "Native outputs: ${PROJECT_DIR}/bazel-bin"
        ;;
    help|-h|--help)
        usage
        ;;
    *)
        echo "Unknown command: $1" >&2
        usage >&2
        exit 2
        ;;
esac
