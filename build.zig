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

    const intern_mod = b.createModule(.{
        .root_source_file = b.path("src/runtime/intern.zig"),
        .target = target,
        .optimize = optimize,
    });

    const obj_mod = b.createModule(.{
        .root_source_file = b.path("src/runtime/obj.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "intern", .module = intern_mod },
        },
    });

    // Wire circular import: intern_mod needs obj for ObjString.
    intern_mod.addImport("obj", obj_mod);

    // NOTE: obj_mod needs value and chunk imports for ObjFunction/ObjClosure.
    // These are added below via addImport after value_mod and chunk_mod are created,
    // to break the circular build-script dependency.

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
            .{ .name = "obj", .module = obj_mod },
        },
    });

    // Wire circular imports for obj_mod (ObjFunction needs Value and Chunk).
    obj_mod.addImport("value", value_mod);
    obj_mod.addImport("chunk", chunk_mod);

    const arena_mod = b.createModule(.{
        .root_source_file = b.path("src/runtime/arena.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "value", .module = value_mod },
            .{ .name = "obj", .module = obj_mod },
        },
    });

    const gc_mod = b.createModule(.{
        .root_source_file = b.path("src/runtime/gc.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "obj", .module = obj_mod },
            .{ .name = "intern", .module = intern_mod },
            .{ .name = "arena", .module = arena_mod },
        },
    });

    const gc_nursery_mod = b.createModule(.{
        .root_source_file = b.path("src/runtime/gc_nursery.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "obj", .module = obj_mod },
            .{ .name = "value", .module = value_mod },
            .{ .name = "gc", .module = gc_mod },
        },
    });

    // gc_nursery needs chunk for ObjFunction.chunk.constants (via obj -> chunk).
    gc_nursery_mod.addImport("chunk", chunk_mod);

    const gc_oldgen_mod = b.createModule(.{
        .root_source_file = b.path("src/runtime/gc_oldgen.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "obj", .module = obj_mod },
            .{ .name = "value", .module = value_mod },
            .{ .name = "gc", .module = gc_mod },
            .{ .name = "gc_nursery", .module = gc_nursery_mod },
        },
    });

    // gc_oldgen needs chunk for ObjFunction.chunk.constants (via obj -> chunk).
    gc_oldgen_mod.addImport("chunk", chunk_mod);

    // gc_mod needs gc_nursery, gc_oldgen, and gc_roots (added after vm_mod is created below).
    gc_mod.addImport("gc_nursery", gc_nursery_mod);
    gc_mod.addImport("gc_oldgen", gc_oldgen_mod);
    gc_mod.addImport("value", value_mod);

    // Wire memory_mod to import gc for createGC function.
    memory_mod.addImport("gc", gc_mod);

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

    const stream_mod_build = b.createModule(.{
        .root_source_file = b.path("src/stdlib/stream.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "value", .module = value_mod },
            .{ .name = "obj", .module = obj_mod },
        },
    });

    // Wire obj_mod to import stream for ObjStream/StreamState.
    obj_mod.addImport("stream", stream_mod_build);

    // Wire gc_nursery and gc_oldgen to import stream for GC traversal.
    gc_nursery_mod.addImport("stream", stream_mod_build);
    gc_oldgen_mod.addImport("stream", stream_mod_build);

    const builtins_mod = b.createModule(.{
        .root_source_file = b.path("src/stdlib/builtins.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "value", .module = value_mod },
            .{ .name = "obj", .module = obj_mod },
            .{ .name = "stream", .module = stream_mod_build },
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
            .{ .name = "gc", .module = gc_mod },
        },
    });

    const gc_roots_mod = b.createModule(.{
        .root_source_file = b.path("src/runtime/gc_roots.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "obj", .module = obj_mod },
            .{ .name = "value", .module = value_mod },
            .{ .name = "gc", .module = gc_mod },
            .{ .name = "gc_nursery", .module = gc_nursery_mod },
            .{ .name = "gc_oldgen", .module = gc_oldgen_mod },
            .{ .name = "arena", .module = arena_mod },
            .{ .name = "vm", .module = vm_mod },
        },
    });

    // Wire gc_mod to import gc_roots (for collectNursery orchestration).
    gc_mod.addImport("gc_roots", gc_roots_mod);
    // Wire gc_mod to import vm for VM type access.
    gc_mod.addImport("vm", vm_mod);

    // Wire gc_oldgen_mod to import gc_roots (for scanRootsForOldGen in mark phase).
    gc_oldgen_mod.addImport("gc_roots", gc_roots_mod);

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
            .{ .name = "gc", .module = gc_mod },
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
        intern_mod,
        arena_mod,
        gc_mod,
        gc_nursery_mod,
        gc_oldgen_mod,
        gc_roots_mod,
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
        stream_mod_build,
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

    // End-to-end test runner.
    {
        const e2e_mod = b.createModule(.{
            .root_source_file = b.path("tests/run_e2e.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zenith", .module = lib_mod },
            },
        });
        const t = b.addTest(.{ .root_module = e2e_mod });
        const run_t = b.addRunArtifact(t);
        test_step.dependOn(&run_t.step);
    }
}
