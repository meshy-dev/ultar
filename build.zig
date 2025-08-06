const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const clap = b.dependency("clap", .{ .target = target, .optimize = optimize });
    const xev = b.dependency("libxev", .{ .target = target, .optimize = optimize });
    const zlua = b.dependency("zlua", .{
        .target = target,
        .optimize = optimize,
        .lang = .lua54,
        .shared = false,
    });

    const indexer = b.addExecutable(.{
        .name = "indexer",
        .root_source_file = b.path("indexer.zig"),
        .target = target,
        .optimize = optimize,
        .strip = false,
    });
    indexer.root_module.addImport("clap", clap.module("clap"));
    indexer.root_module.addImport("xev", xev.module("xev"));
    b.installArtifact(indexer);

    const lib_dataloader = b.addSharedLibrary(.{
        .name = "dataloader",
        .link_libc = true,
        .single_threaded = false,
        .pic = true,
        .target = target,
        .optimize = optimize,
        .root_source_file = b.path("lua_dataloader.zig"),
        .strip = false,
    });
    lib_dataloader.root_module.addImport("xev", xev.module("xev"));
    lib_dataloader.root_module.addImport("zlua", zlua.module("zlua"));
    b.installArtifact(lib_dataloader);

    const test_step = b.step("test", "Run unit tests");

    const unit_tests = b.addTest(.{
        .root_source_file = b.path("tests.zig"),
        .target = target,
        .link_libc = true,
    });
    unit_tests.root_module.addImport("xev", xev.module("xev"));

    const run_unit_tests = b.addRunArtifact(unit_tests);
    test_step.dependOn(&run_unit_tests.step);
}
