const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // ── Library module (public API for consumers) ──────────────────────
    const lib_mod = b.addModule("zenith", .{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });

    // ── Executable ─────────────────────────────────────────────────────
    const exe = b.addExecutable(.{
        .name = "zenith",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zenith", .module = lib_mod },
            },
        }),
    });
    b.installArtifact(exe);

    // ── Static library ─────────────────────────────────────────────────
    const lib = b.addLibrary(.{
        .name = "zenith",
        .root_module = lib_mod,
    });

    const lib_step = b.step("lib", "Build static library");
    lib_step.dependOn(&lib.step);

    // ── Run step ───────────────────────────────────────────────────────
    const run_step = b.step("run", "Run the Zenith interpreter");
    const run_cmd = b.addRunArtifact(exe);
    run_step.dependOn(&run_cmd.step);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    // ── Tests ──────────────────────────────────────────────────────────
    // Test each source module individually so that inline test blocks
    // in every file are discovered and executed.
    const test_step = b.step("test", "Run all unit tests");

    const test_sources = [_][]const u8{
        "src/compiler/token.zig",
        "src/runtime/memory.zig",
        "src/runtime/value.zig",
        "src/runtime/obj.zig",
        "src/runtime/chunk.zig",
        "src/common/error.zig",
        "src/common/debug.zig",
        "src/lib.zig",
    };

    for (test_sources) |src| {
        const t = b.addTest(.{
            .root_module = b.createModule(.{
                .root_source_file = b.path(src),
                .target = target,
                .optimize = optimize,
            }),
        });
        const run_t = b.addRunArtifact(t);
        test_step.dependOn(&run_t.step);
    }
}
