"""
Setup script for ultar-dataloader.

This script expects the native extension to be pre-built by `zig build python-bindings`.
It simply copies the built .so file into the wheel during packaging.

Usage:
    # Build the native extension first
    zig build python-bindings -Doptimize=ReleaseFast

    # Then build the wheel
    python -m build --wheel --no-isolation

    # Or use pixi:
    pixi run build-wheel
"""

import shutil
from pathlib import Path

from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext


class CopyPrebuiltExtension(build_ext):
    """Custom build_ext that copies pre-built extension from zig-out."""

    def build_extension(self, ext):
        libpath = Path(self.get_ext_fullpath(ext.name)).resolve().absolute()
        dirname = libpath.parent
        dirname.mkdir(parents=True, exist_ok=True)

        # Look for pre-built library in zig-out
        root_dir = Path(__file__).parent.parent.resolve()
        zig_out = root_dir / "zig-out" / "lib"

        if not zig_out.exists():
            raise RuntimeError(
                f"zig-out/lib not found at {zig_out}.\n"
                "Build the native extension first with:\n"
                "  zig build python-bindings -Doptimize=ReleaseFast\n"
                "Or use pixi:\n"
                "  pixi run build-native"
            )

        # Find the native library (.so on Linux, .dylib on macOS)
        so_files = list(zig_out.glob("*_native.abi3.so"))
        dylib_files = list(zig_out.glob("*_native.abi3.dylib"))
        lib_files = so_files + dylib_files

        if not lib_files:
            raise RuntimeError(
                f"Could not find *_native.abi3.so or *_native.abi3.dylib in {zig_out}.\n"
                "Build the native extension first with:\n"
                "  zig build python-bindings -Doptimize=ReleaseFast"
            )

        so_file = lib_files[0]
        print(f"Copying {so_file} to {libpath}")
        shutil.copy2(so_file, libpath)


_native = Extension(
    "ultar_dataloader._native",
    sources=[],  # No sources - we copy pre-built
    py_limited_api=True,
)


setup(
    ext_modules=[_native],
    cmdclass={"build_ext": CopyPrebuiltExtension},
)
