# Agent Development Guide

A file for [guiding coding agents](https://agents.md/).

## Commands

- **Build:** `zig build`
- **Test (Zig):** `zig build test`
- **Test filter (Zig)**: `zig build test -Dtest-filter=<test name>`
- **Formatting (Zig)**: `zig fmt .`
- **Fetching New Dependencies** `zig fetch --save git+https://...`

### Python Package (using pixi)

- **Build native extension:** `pixi run build-native`
- **Build wheel:** `pixi run build-wheel`
- **Dev install:** `pixi run dev-install`
- **Run tests:** `pixi run python python/tests/test_dataloader.py`

## Directory Structure

- Shared Zig code: `*.zig`
- Python ABI3 bindings: `python/`
- Webapp, zig version: `ultar_httpd/`

## Zig documentation and std are authoritative

- **Primary sources**:
  - Language reference: [Zig Language Reference](https://ziglang.org/documentation/master/)
  - Standard library: [Zig std reference](https://ziglang.org/documentation/master/std/)
- **Rule**: Prefer these docs over blogs or search results. Do not use external search engines for topics covered by the official docs.
- **Canon**: When choosing patterns or idioms, follow approaches demonstrated in `std` documentation and examples.

## Usage guidance

- **Look up before coding**: Read the relevant language/stdlib section end-to-end before implementing.
- **API usage**: Cross-check function signatures, error semantics, allocation patterns, and iterators in the std reference.
- **Ambiguity**: If docs appear ambiguous, re-read the surrounding sections and prefer idioms showcased in `std` docs/examples.
- **Versioning**: Assume code targets the docs linked above unless the repository pins a different Zig version.

> Zig evolves quickly; keeping to official docs and `std` patterns ensures correctness and idiomatic style.
