"""
Setup script for ultar-dataloader.

This script expects the native extension to be pre-built by `zig build python-bindings`.
The build script places the library directly in src/ultar_dataloader/.

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
    """Custom build_ext that copies pre-built extension from src/ultar_dataloader/."""

    def build_extension(self, ext):
        libpath = Path(self.get_ext_fullpath(ext.name)).resolve().absolute()
        dirname = libpath.parent
        dirname.mkdir(parents=True, exist_ok=True)

        # Look for pre-built library in src/ultar_dataloader/ (placed there by zig build)
        setup_py_dir = Path(__file__).parent.resolve()
        pkg_dir = setup_py_dir / "src" / "ultar_dataloader"

        # Find the native library (.so on Linux, .dylib on macOS)
        so_files = list(pkg_dir.glob("_native.abi3.so"))
        dylib_files = list(pkg_dir.glob("_native.abi3.dylib"))
        lib_files = so_files + dylib_files

        if not lib_files:
            raise RuntimeError(
                f"Could not find _native.abi3.so or _native.abi3.dylib in {pkg_dir}.\n"
                "Build the native extension first with:\n"
                "  zig build python-bindings -Doptimize=ReleaseSafe"
            )

        native_lib = lib_files[0]
        print(f"Copying {native_lib} to {libpath}")
        shutil.copy2(native_lib, libpath)


_native = Extension(
    "ultar_dataloader._native",
    sources=[],  # No sources - we copy pre-built
    py_limited_api=True,
)


setup(
    ext_modules=[_native],
    cmdclass={"build_ext": CopyPrebuiltExtension},
)
