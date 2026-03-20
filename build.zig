const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // ── Source modules (defined once, shared everywhere) ───────────────
    const token_mod = b.createModule(.{
        .root_source_file = b.path("src/compiler/token.zig"),
        .target = target,
        .optimize = optimize,
    });

    const memory_mod = b.createModule(.{
        .root_source_file = b.path("src/runtime/memory.zig"),
        .target = target,
        .optimize = optimize,
    });

    const obj_mod = b.createModule(.{
        .root_source_file = b.path("src/runtime/obj.zig"),
        .target = target,
        .optimize = optimize,
    });

    const value_mod = b.createModule(.{
        .root_source_file = b.path("src/runtime/value.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "obj", .module = obj_mod },
        },
    });

    const chunk_mod = b.createModule(.{
        .root_source_file = b.path("src/runtime/chunk.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "value", .module = value_mod },
        },
    });

    const error_mod = b.createModule(.{
        .root_source_file = b.path("src/common/error.zig"),
        .target = target,
        .optimize = optimize,
    });

    const debug_mod = b.createModule(.{
        .root_source_file = b.path("src/common/debug.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "chunk", .module = chunk_mod },
            .{ .name = "value", .module = value_mod },
        },
    });

    const lexer_mod = b.createModule(.{
        .root_source_file = b.path("src/compiler/lexer.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "token", .module = token_mod },
            .{ .name = "error", .module = error_mod },
        },
    });

    const ast_mod = b.createModule(.{
        .root_source_file = b.path("src/compiler/ast.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "token", .module = token_mod },
            .{ .name = "error", .module = error_mod },
        },
    });

    const parser_mod = b.createModule(.{
        .root_source_file = b.path("src/compiler/parser.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "token", .module = token_mod },
            .{ .name = "error", .module = error_mod },
            .{ .name = "ast", .module = ast_mod },
            .{ .name = "lexer", .module = lexer_mod },
        },
    });

    const builtins_mod = b.createModule(.{
        .root_source_file = b.path("src/stdlib/builtins.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "value", .module = value_mod },
            .{ .name = "obj", .module = obj_mod },
        },
    });

    const vm_mod = b.createModule(.{
        .root_source_file = b.path("src/runtime/vm.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "chunk", .module = chunk_mod },
            .{ .name = "value", .module = value_mod },
            .{ .name = "obj", .module = obj_mod },
            .{ .name = "error", .module = error_mod },
            .{ .name = "builtins", .module = builtins_mod },
        },
    });

    const compiler_mod = b.createModule(.{
        .root_source_file = b.path("src/compiler/compiler.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "token", .module = token_mod },
            .{ .name = "error", .module = error_mod },
            .{ .name = "ast", .module = ast_mod },
            .{ .name = "chunk", .module = chunk_mod },
            .{ .name = "value", .module = value_mod },
            .{ .name = "obj", .module = obj_mod },
            .{ .name = "lexer", .module = lexer_mod },
            .{ .name = "parser", .module = parser_mod },
        },
    });

    // ── Library module (public API) ────────────────────────────────────
    const lib_mod = b.addModule("zenith", .{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "token", .module = token_mod },
            .{ .name = "memory", .module = memory_mod },
            .{ .name = "obj", .module = obj_mod },
            .{ .name = "value", .module = value_mod },
            .{ .name = "chunk", .module = chunk_mod },
            .{ .name = "error", .module = error_mod },
            .{ .name = "debug", .module = debug_mod },
            .{ .name = "lexer", .module = lexer_mod },
            .{ .name = "ast", .module = ast_mod },
            .{ .name = "parser", .module = parser_mod },
            .{ .name = "compiler", .module = compiler_mod },
            .{ .name = "builtins", .module = builtins_mod },
            .{ .name = "vm", .module = vm_mod },
        },
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
    const test_step = b.step("test", "Run all unit tests");

    // Test each module individually. Since modules are shared,
    // each test runs exactly the inline tests from that module file.
    const test_modules = [_]*std.Build.Module{
        token_mod,
        memory_mod,
        obj_mod,
        value_mod,
        chunk_mod,
        error_mod,
        debug_mod,
        lexer_mod,
        ast_mod,
        parser_mod,
        compiler_mod,
        builtins_mod,
        vm_mod,
    };

    for (test_modules) |mod| {
        const t = b.addTest(.{ .root_module = mod });
        const run_t = b.addRunArtifact(t);
        test_step.dependOn(&run_t.step);
    }

    // Also test lib.zig to exercise the full transitive import graph.
    {
        const t = b.addTest(.{ .root_module = lib_mod });
        const run_t = b.addRunArtifact(t);
        test_step.dependOn(&run_t.step);
    }
}
