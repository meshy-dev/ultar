const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const engine = b.option([]const u8, "lua", "Use specified lua engine (default to luajit). Available: luajit, luau, lua54") orelse "luajit";

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
