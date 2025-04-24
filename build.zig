const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const indexer = b.addExecutable(.{
        .name = "indexer",
        .root_source_file = b.path("indexer.zig"),
        .target = target,
        .optimize = optimize,
        .strip = if (optimize == .ReleaseFast) true else null,
    });

    const clap = b.dependency("clap", .{ .target = target, .optimize = optimize });
    const xev = b.dependency("libxev", .{ .target = target, .optimize = optimize });
    indexer.root_module.addImport("clap", clap.module("clap"));
    indexer.root_module.addImport("xev", xev.module("xev"));

    b.installArtifact(indexer);

    const test_step = b.step("test", "Run unit tests");

    const unit_tests = b.addTest(.{
        .root_source_file = b.path("msgpack.zig"),
        .target = target,
    });

    const run_unit_tests = b.addRunArtifact(unit_tests);
    test_step.dependOn(&run_unit_tests.step);
}
