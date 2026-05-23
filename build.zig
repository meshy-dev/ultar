const std = @import("std");
const builtin = @import("builtin");
const httpd_build = @import("ultar_httpd/build.zig");

pub fn build(b: *std.Build) void {
    // On glibc-based Linux hosts, pin glibc to 2.34 to force Zig's bundled crt.
    // macOS and non-glibc Linux (musl, etc.) use the host's native defaults.
    const default_target: std.Target.Query = if (builtin.os.tag == .linux and builtin.abi.isGnu())
        .{ .abi = .gnu, .os_tag = .linux, .glibc_version = .{ .major = 2, .minor = 34, .patch = 0 } }
    else
        .{};
    const target = b.standardTargetOptions(.{ .default_target = default_target });
    const optimize = b.standardOptimizeOption(.{});

    const engine = b.option([]const u8, "lua", "Use specified lua engine (default to luajit). Available: luajit, luau, lua54") orelse "luajit";

    const build_python = b.option(bool, "python-bindings", "Build Python ABI3 extension module") orelse false;
    const python_exe = b.option([]const u8, "python", "Python interpreter path for bindings") orelse
        b.graph.environ_map.get("PYTHON");

    const clap = b.dependency("clap", .{ .target = target, .optimize = optimize });
    const xev = b.dependency("libxev", .{ .target = target, .optimize = optimize });
    const httpz = b.dependency("httpz", .{ .target = target, .optimize = optimize });
    const mustach_dep = b.dependency("mustach", .{});

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

    const msgpack_module = b.createModule(.{ .root_source_file = b.path("msgpack.zig") });

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
    indexer.root_module.addImport("msgpack", msgpack_module);
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

    // Reused by the Python bindings library below.
    const lua_dataloader_mod = b.createModule(.{
        .root_source_file = b.path("lua_dataloader.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .pic = true,
    });
    lua_dataloader_mod.addImport("xev", xev.module("xev"));
    lua_dataloader_mod.addImport("zlua", zlua.module("zlua"));

    if (build_python) {
        buildPythonBindings(b, target, optimize, lua_dataloader_mod, python_exe);
    }

    const python_step = b.step("python-bindings", "Build Python ABI3 extension module");
    const python_lib = buildPythonBindingsStep(b, target, optimize, lua_dataloader_mod, python_exe);

    // Stage the built artifact into the Python source tree. Python's import
    // system on macOS recognizes .so (not .dylib), so rewrite .dylib to .so.
    const copy_native = b.addSystemCommand(&.{
        "sh", "-c",
        \\for f in zig-out/lib/*_native.abi3.*; do
        \\  ext="${f##*.}"
        \\  case "$ext" in dylib) ext=so ;; esac
        \\  cp -f "$f" "python/src/ultar_dataloader/_native.abi3.$ext"
        \\done
        ,
    });
    copy_native.step.dependOn(&b.addInstallArtifact(python_lib, .{}).step);
    python_step.dependOn(&copy_native.step);

    const copy_lua_types = b.addSystemCommand(&.{
        "cp", "-rf", "lua-types/ultar", "python/src/ultar_dataloader/lua-types/",
    });
    python_step.dependOn(&copy_lua_types.step);

    // ULTAR_VERSION wins (CI); otherwise emit X.Y.Z on a tagged HEAD or
    // X.Y.Z+gSHORTSHA off-tag.
    const base_version = @import("build.zig.zon").version;
    const gen_version = b.addSystemCommand(&.{
        "sh", "-c",
        \\if [ -n "$ULTAR_VERSION" ]; then
        \\  echo "__version__ = \"$ULTAR_VERSION\"" > python/src/ultar_dataloader/_version.py
        \\elif git describe --exact-match --tags HEAD >/dev/null 2>&1; then
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

    const webapp = httpd_build.addWebapp(b, .{
        .target = target,
        .optimize = optimize,
        .clap = clap.module("clap"),
        .xev = xev.module("xev"),
        .msgpack = msgpack_module,
        .httpz = httpz.module("httpz"),
        .mustach = mustach_dep,
    });
    b.installArtifact(webapp);

    const cli_step = b.step("cli", "Build CLI tools (indexer, ultar_httpd)");
    cli_step.dependOn(&b.addInstallArtifact(indexer, .{}).step);
    cli_step.dependOn(&b.addInstallArtifact(webapp, .{}).step);

    const run_step = b.step("run", "Run the web application");
    const run = b.addRunArtifact(webapp);
    run.step.dependOn(b.getInstallStep());
    if (b.args) |args| run.addArgs(args);
    run_step.dependOn(&run.step);

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

fn buildPythonBindingsStep(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    lua_dataloader_mod: *std.Build.Module,
    python_exe: ?[]const u8,
) *std.Build.Step.Compile {
    const python_include = getPythonIncludePath(b.allocator, b.graph.io, python_exe) orelse {
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

    python_mod.addSystemIncludePath(.{ .cwd_relative = python_include });
    python_mod.addImport("lua_dataloader", lua_dataloader_mod);

    const lib = b.addLibrary(.{
        .linkage = .dynamic,
        .name = "_native.abi3",
        .root_module = python_mod,
    });

    lib.root_module.addRPathSpecial("$ORIGIN");
    // Python C-API symbols are resolved by the host interpreter at load time.
    lib.linker_allow_shlib_undefined = true;

    return lib;
}

fn getPythonIncludePath(allocator: std.mem.Allocator, io: std.Io, python_exe: ?[]const u8) ?[]const u8 {
    const get_include_cmd = "import sysconfig; print(sysconfig.get_path('include'), end='')";

    if (python_exe) |py| {
        return runPythonCommand(allocator, io, py, get_include_cmd);
    }

    const interpreters = [_][]const u8{ "python3", "python" };
    for (interpreters) |python| {
        if (runPythonCommand(allocator, io, python, get_include_cmd)) |result| {
            return result;
        }
    }
    return null;
}

/// Runs `python -c cmd`; returns stdout (allocator-owned) on exit 0, else null.
fn runPythonCommand(allocator: std.mem.Allocator, io: std.Io, python: []const u8, cmd: []const u8) ?[]const u8 {
    const result = std.process.run(allocator, io, .{
        .argv = &.{ python, "-c", cmd },
    }) catch return null;

    defer allocator.free(result.stderr);

    switch (result.term) {
        .exited => |code| {
            if (code == 0 and result.stdout.len > 0) {
                return result.stdout;
            }
        },
        else => {},
    }

    allocator.free(result.stdout);
    return null;
}
