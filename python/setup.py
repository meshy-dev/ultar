"""
Setup script for ultar-dataloader.

The native extension should be pre-built by `zig build python-bindings` which places
the library directly in src/ultar_dataloader/. The pyproject.toml package-data config
includes *.so and *.dylib files automatically.

Usage:
    # Build the native extension first
    zig build python-bindings -Doptimize=ReleaseSafe

    # Then build the wheel
    python -m build --wheel --no-isolation
"""

from setuptools import setup

# All configuration is in pyproject.toml
# This file exists for compatibility with older tools
setup()
