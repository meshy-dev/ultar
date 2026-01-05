const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const engine = b.option([]const u8, "lua", "Use specified lua engine (default to luajit). Available: luajit, luau, lua54") orelse "luajit";

    // Python bindings options
    const build_python = b.option(bool, "python-bindings", "Build Python ABI3 extension module") orelse false;
    const python_exe = b.option([]const u8, "python", "Python interpreter path for bindings") orelse
        std.process.getEnvVarOwned(b.allocator, "PYTHON") catch null;

    const clap = b.dependency("clap", .{ .target = target, .optimize = optimize });
    const xev = b.dependency("libxev", .{ .target = target, .optimize = optimize });

    var zlua: *std.Build.Dependency = undefined;
    if (std.mem.eql(u8, engine, "luajit")) {
        zlua = b.dependency("zlua", .{
            .target = target,
            .optimize = optimize,
            .lang = .luajit,
            .shared = false,
        });
    } else if (std.mem.eql(u8, engine, "lua54")) {
        zlua = b.dependency("zlua", .{
            .target = target,
            .optimize = optimize,
            .lang = .lua54,
            .shared = false,
        });
    } else if (std.mem.eql(u8, engine, "luau")) {
        zlua = b.dependency("zlua", .{
            .target = target,
            .optimize = optimize,
            .lang = .luau,
            .shared = false,
        });
    } else {
        std.debug.panic("Unknown lua engine: {s}", .{engine});
    }

    const indexer = b.addExecutable(.{
        .name = "indexer",
        .root_module = b.createModule(.{
            .root_source_file = b.path("indexer.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    indexer.root_module.addImport("clap", clap.module("clap"));
    indexer.root_module.addImport("xev", xev.module("xev"));
    b.installArtifact(indexer);

    const lib_dataloader = b.addLibrary(.{
        .name = "dataloader",
        .linkage = .dynamic,
        .root_module = b.createModule(.{
            .link_libc = true,
            .single_threaded = false,
            .pic = true,
            .target = target,
            .optimize = optimize,
            .root_source_file = b.path("lua_dataloader.zig"),
        }),
    });
    lib_dataloader.root_module.addImport("xev", xev.module("xev"));
    lib_dataloader.root_module.addImport("zlua", zlua.module("zlua"));
    b.installArtifact(lib_dataloader);

    // Create lua_dataloader module for sharing with Python bindings
    const lua_dataloader_mod = b.createModule(.{
        .root_source_file = b.path("lua_dataloader.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .pic = true,
    });
    lua_dataloader_mod.addImport("xev", xev.module("xev"));
    lua_dataloader_mod.addImport("zlua", zlua.module("zlua"));

    // Python bindings (optional, enabled with -Dpython-bindings=true)
    if (build_python) {
        buildPythonBindings(b, target, optimize, lua_dataloader_mod, python_exe);
    }

    // Named step for building Python bindings
    const python_step = b.step("python-bindings", "Build Python ABI3 extension module");
    const python_lib = buildPythonBindingsStep(b, target, optimize, lua_dataloader_mod, python_exe);

    // Install directly into python/src/ultar_dataloader/ (used by both dev and wheel builds)
    // Use shell to copy with correct extension based on what was built
    const copy_native = b.addSystemCommand(&.{
        "sh", "-c",
        \\for f in zig-out/lib/*_native.abi3.*; do
        \\  ext="${f##*.}"
        \\  cp -f "$f" "python/src/ultar_dataloader/_native.abi3.$ext"
        \\done
        ,
    });
    copy_native.step.dependOn(&b.addInstallArtifact(python_lib, .{}).step);
    python_step.dependOn(&copy_native.step);

    // Copy lua-types to python package
    const copy_lua_types = b.addSystemCommand(&.{
        "cp", "-rf", "lua-types/ultar", "python/src/ultar_dataloader/lua-types/",
    });
    python_step.dependOn(&copy_lua_types.step);

    // Generate _version.py from build.zig.zon version + git info
    // Version format: X.Y.Z or X.Y.Z+gSHORTSHA (if not on exact tag)
    const base_version = @import("build.zig.zon").version;
    const gen_version = b.addSystemCommand(&.{
        "sh", "-c",
        \\if git describe --exact-match --tags HEAD >/dev/null 2>&1; then
        \\  echo '__version__ = "
        ++ base_version ++
            \\"' > python/src/ultar_dataloader/_version.py
            \\else
            \\  sha=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
            \\  echo "__version__ = \"
        ++ base_version ++
            \\+g${sha}\"" > python/src/ultar_dataloader/_version.py
            \\fi
        ,
    });
    python_step.dependOn(&gen_version.step);

    // Add Zap dependency
    const zap = b.dependency("zap", .{
        .target = target,
        .optimize = optimize,
        .openssl = false, // Set to true to enable TLS support
    });

    // Add htmx dependency (non-Zig project; used to embed dist/htmx.min.js)
    const htmx = b.dependency("htmx", .{ .target = target, .optimize = optimize });
    // Prepare generated files for embedding htmx
    const write_files = b.addWriteFiles();
    const htmx_abs = htmx.path("dist/htmx.min.js").getPath(b);
    const htmx_bytes = std.fs.cwd().readFileAlloc(b.allocator, htmx_abs, 16 * 1024 * 1024) catch @panic("failed to read htmx.min.js");
    _ = write_files.add("assets/htmx.min.js", htmx_bytes);
    const embed_code = "pub const htmx_js: []const u8 = @embedFile(\"assets/htmx.min.js\");\n";
    const embed_file = write_files.add("htmx_embed.zig", embed_code);
    const htmx_module = b.createModule(.{ .root_source_file = embed_file });

    // Create the web application executable
    const webapp = b.addExecutable(.{
        .name = "ultar_httpd",
        .root_module = b.createModule(.{
            .root_source_file = b.path("ultar_httpd/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    webapp.root_module.addImport("zap", zap.module("zap"));
    webapp.root_module.addImport("clap", clap.module("clap"));
    webapp.root_module.addImport("htmx_embed", htmx_module);
    // Expose root-level msgpack implementation to nested main.zig
    const msgpack_module = b.createModule(.{ .root_source_file = b.path("msgpack.zig") });
    webapp.root_module.addImport("msgpack", msgpack_module);
    webapp.linkLibC();
    b.installArtifact(webapp);

    // Named step for building CLI tools only (no Lua/dataloader)
    const cli_step = b.step("cli", "Build CLI tools (indexer, ultar_httpd)");
    cli_step.dependOn(&b.addInstallArtifact(indexer, .{}).step);
    cli_step.dependOn(&b.addInstallArtifact(webapp, .{}).step);

    // Add a step to run the webapp
    const run_webapp = b.addRunArtifact(webapp);
    const run_step = b.step("run", "Run the web application");
    run_step.dependOn(&run_webapp.step);

    const test_step = b.step("test", "Run unit tests");

    const unit_tests = b.addTest(.{ .root_module = b.createModule(.{
        .root_source_file = b.path("tests.zig"),
        .target = target,
        .link_libc = true,
    }) });
    unit_tests.root_module.addImport("xev", xev.module("xev"));

    const run_unit_tests = b.addRunArtifact(unit_tests);
    test_step.dependOn(&run_unit_tests.step);
}

/// Build Python ABI3 extension and install it
fn buildPythonBindings(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    lua_dataloader_mod: *std.Build.Module,
    python_exe: ?[]const u8,
) void {
    const lib = buildPythonBindingsStep(b, target, optimize, lua_dataloader_mod, python_exe);
    b.installArtifact(lib);
}

/// Create the Python bindings library artifact
fn buildPythonBindingsStep(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    lua_dataloader_mod: *std.Build.Module,
    python_exe: ?[]const u8,
) *std.Build.Step.Compile {
    // Get Python include path
    const python_include = getPythonIncludePath(b.allocator, python_exe) orelse {
        std.log.err(
            \\Failed to get Python include path.
            \\
            \\Make sure Python is installed and try one of:
            \\  - zig build -Dpython=/path/to/python3 -Dpython-bindings=true
            \\  - PYTHON=/path/to/python3 zig build -Dpython-bindings=true
            \\  - Add python3 to your PATH
        , .{});
        @panic("Python include path not found");
    };

    const python_mod = b.createModule(.{
        .root_source_file = b.path("python/python.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .pic = true,
    });

    // Add Python include path
    python_mod.addSystemIncludePath(.{ .cwd_relative = python_include });

    // Import lua_dataloader module
    python_mod.addImport("lua_dataloader", lua_dataloader_mod);

    const lib = b.addLibrary(.{
        .linkage = .dynamic,
        .name = "_native.abi3",
        .root_module = python_mod,
    });

    // Allow undefined symbols - Python symbols are resolved at runtime
    lib.root_module.addRPathSpecial("$ORIGIN");
    lib.linker_allow_shlib_undefined = true;

    return lib;
}

/// Get Python include path by running the interpreter
fn getPythonIncludePath(allocator: std.mem.Allocator, python_exe: ?[]const u8) ?[]const u8 {
    const get_include_cmd = "import sysconfig; print(sysconfig.get_path('include'), end='')";

    if (python_exe) |py| {
        return runPythonCommand(allocator, py, get_include_cmd);
    }

    // Fallback: try common Python interpreters
    const interpreters = [_][]const u8{ "python3", "python" };
    for (interpreters) |python| {
        if (runPythonCommand(allocator, python, get_include_cmd)) |result| {
            return result;
        }
    }
    return null;
}

/// Run a Python command and return stdout if successful
fn runPythonCommand(allocator: std.mem.Allocator, python: []const u8, cmd: []const u8) ?[]const u8 {
    const result = std.process.Child.run(.{
        .allocator = allocator,
        .argv = &.{ python, "-c", cmd },
    }) catch return null;

    defer allocator.free(result.stderr);

    // Check if process exited successfully
    switch (result.term) {
        .Exited => |code| {
            if (code == 0 and result.stdout.len > 0) {
                return result.stdout;
            }
        },
        else => {},
    }

    allocator.free(result.stdout);
    return null;
}
