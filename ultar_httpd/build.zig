const std = @import("std");

pub const WebappOptions = struct {
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,

    clap: *std.Build.Module,
    xev: *std.Build.Module,
    msgpack: *std.Build.Module,
    httpz: *std.Build.Module,

    mustach: *std.Build.Dependency,
};

pub fn addWebapp(b: *std.Build, opts: WebappOptions) *std.Build.Step.Compile {
    const mustach_module = b.createModule(.{
        .target = opts.target,
        .optimize = opts.optimize,
        .link_libc = true,
    });
    mustach_module.addCSourceFile(.{
        .file = opts.mustach.path("mustach.c"),
        .flags = &.{ "-std=c11", "-Wall" },
    });
    const mustach_lib = b.addLibrary(.{
        .name = "mustach",
        .root_module = mustach_module,
        .linkage = .static,
    });

    const webapp = b.addExecutable(.{
        .name = "ultar_httpd",
        .root_module = b.createModule(.{
            .root_source_file = b.path("ultar_httpd/main.zig"),
            .target = opts.target,
            .optimize = opts.optimize,
        }),
    });
    webapp.root_module.addImport("clap", opts.clap);
    webapp.root_module.addImport("xev", opts.xev);
    webapp.root_module.addImport("msgpack", opts.msgpack);
    webapp.root_module.addImport("httpz", opts.httpz);

    const indexer_mod = b.createModule(.{
        .root_source_file = b.path("indexer.zig"),
        .target = opts.target,
        .optimize = opts.optimize,
    });
    indexer_mod.addImport("clap", opts.clap);
    indexer_mod.addImport("xev", opts.xev);
    indexer_mod.addImport("msgpack", opts.msgpack);
    webapp.root_module.addImport("indexer", indexer_mod);

    webapp.root_module.linkLibrary(mustach_lib);
    webapp.root_module.addIncludePath(opts.mustach.path("."));
    return webapp;
}
