# Agent Development Guide

A file for [guiding coding agents](https://agents.md/).

## Commands

- **Build:** `zig build`
- **Test (Zig):** `zig build test`
- **Test filter (Zig)**: `zig build test -Dtest-filter=<test name>`
- **Formatting (Zig)**: `zig fmt .`
- **Fetching New Dependencies** `zig fetch --save git+https://...`

## Directory Structure

- Shared Zig code: `*.zig`
- A reference Python Flask webapp to view tar files based on index: `demo/`
- Example usage of the lua driven dataloader: `examples/`
- Webapp, zig version: `ultar_httpd`
