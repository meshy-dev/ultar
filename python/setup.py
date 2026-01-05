"""
Setup script for ultar-dataloader.

The native extension should be pre-built by `zig build python-bindings` which places
the library directly in src/ultar_dataloader/.

Usage:
    # Build the native extension first
    zig build python-bindings -Doptimize=ReleaseSafe

    # Then build the wheel
    python -m build --wheel --no-isolation
"""

import shutil
from pathlib import Path

from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext


class CopyPrebuiltExtension(build_ext):
    """Copy pre-built extension instead of compiling."""

    def build_extension(self, ext):
        # Destination path for the extension
        ext_path = Path(self.get_ext_fullpath(ext.name))
        ext_path.parent.mkdir(parents=True, exist_ok=True)

        # Source: pre-built library in src/ultar_dataloader/
        src_dir = Path(__file__).parent / "src" / "ultar_dataloader"
        
        # Find .so or .dylib
        for pattern in ["_native.abi3.so", "_native.abi3.dylib"]:
            src = src_dir / pattern
            if src.exists():
                print(f"Copying {src} -> {ext_path}")
                shutil.copy2(src, ext_path)
                return

        raise RuntimeError(
            f"Pre-built extension not found in {src_dir}.\n"
            "Build it first with: zig build python-bindings -Doptimize=ReleaseSafe"
        )


# Declare extension with py_limited_api for ABI3 wheel tag (Python 3.11+)
_native = Extension(
    "ultar_dataloader._native",
    sources=[],  # No sources - we copy pre-built
    py_limited_api=True,
)

setup(
    ext_modules=[_native],
    cmdclass={"build_ext": CopyPrebuiltExtension},
    options={
        "bdist_wheel": {
            "py_limited_api": "cp311",
        },
    },
)
