"""CLI tools for ultar-dataloader."""

import argparse
import json
import logging
import sys
from pathlib import Path

logger = logging.getLogger("ultar_dataloader")


def get_lua_types_path() -> Path:
    """Get the path to the shipped lua-types directory."""
    # lua-types is shipped alongside this module
    return Path(__file__).parent / "lua-types"


def init_lsp(target_dir: Path | None = None) -> int:
    """
    Initialize Lua LSP configuration for ultar scripts.
    
    Creates a .luarc.json file pointing to the shipped type stubs.
    
    Args:
        target_dir: Directory to create config in (defaults to CWD)
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    if target_dir is None:
        target_dir = Path.cwd()
    
    config_path = target_dir / ".luarc.json"
    lua_types = get_lua_types_path()
    
    if not lua_types.exists():
        logger.error("lua-types not found at %s", lua_types)
        logger.error("This may indicate a broken installation.")
        return 1
    
    if config_path.exists():
        logger.warning("%s already exists, not overwriting.", config_path)
        logger.warning("Remove the existing file first if you want to regenerate it.")
        return 1
    
    config = {
        "$schema": "https://raw.githubusercontent.com/LuaLS/vscode-lua/master/setting/schema.json",
        "runtime.version": "LuaJIT",
        "workspace.library": [str(lua_types.resolve())],
        "workspace.checkThirdParty": False,
    }
    
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)
        f.write("\n")
    
    logger.info("Created %s", config_path)
    logger.info("Lua LSP will now recognize ultar modules from: %s", lua_types.resolve())
    return 0


def main() -> int:
    """Main CLI entry point."""
    # Configure logging to stderr
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
        stream=sys.stderr,
    )
    
    parser = argparse.ArgumentParser(
        prog="ultar-dataloader",
        description="ultar-dataloader CLI tools",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # init-lsp command
    init_parser = subparsers.add_parser(
        "init-lsp",
        help="Initialize Lua LSP configuration for ultar scripts",
        description=(
            "Creates a .luarc.json file in the current directory that configures "
            "LuaLS to recognize ultar modules (ultar.loader, ultar.utix, etc.) "
            "with full type information and autocompletion."
        ),
    )
    init_parser.add_argument(
        "--dir", "-d",
        type=Path,
        default=None,
        help="Target directory (defaults to current working directory)",
    )
    
    # types-path command (for debugging/scripting)
    subparsers.add_parser(
        "types-path",
        help="Print the path to the shipped Lua type stubs",
    )
    
    args = parser.parse_args()
    
    if args.command == "init-lsp":
        return init_lsp(args.dir)
    elif args.command == "types-path":
        # This one goes to stdout for scripting use
        print(get_lua_types_path().resolve())
        return 0
    else:
        parser.print_help()
        return 0


if __name__ == "__main__":
    sys.exit(main())

