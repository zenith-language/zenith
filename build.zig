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

    // ── Fiber scheduler primitives (Phase 7) ─────────────────────────────
    const deque_mod = b.createModule(.{
        .root_source_file = b.path("src/runtime/deque.zig"),
        .target = target,
        .optimize = optimize,
    });

    const fiber_mod = b.createModule(.{
        .root_source_file = b.path("src/runtime/fiber.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "obj", .module = obj_mod },
            .{ .name = "value", .module = value_mod },
            .{ .name = "chunk", .module = chunk_mod },
        },
    });

    // Wire obj_mod to import fiber for ObjFiber destroy.
    obj_mod.addImport("fiber", fiber_mod);

    // ── Channel module (Phase 7, Plan 03) ───────────────────────────────
    const channel_mod = b.createModule(.{
        .root_source_file = b.path("src/runtime/channel.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "obj", .module = obj_mod },
            .{ .name = "value", .module = value_mod },
            .{ .name = "fiber", .module = fiber_mod },
        },
    });

    // Wire obj_mod to import channel for ObjChannel destroy.
    obj_mod.addImport("channel", channel_mod);

    const context_switch_mod = b.createModule(.{
        .root_source_file = b.path("src/runtime/context_switch.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Add platform-specific assembly file to context_switch module.
    if (target.result.cpu.arch == .x86_64) {
        context_switch_mod.addAssemblyFile(b.path("src/runtime/arch/x86_64.s"));
    } else if (target.result.cpu.arch == .aarch64) {
        context_switch_mod.addAssemblyFile(b.path("src/runtime/arch/aarch64.s"));
    }

    // Wire context_switch into fiber_mod for future use.
    fiber_mod.addImport("context_switch", context_switch_mod);

    // ── Scheduler module (Phase 7, Plan 02) ──────────────────────────────
    const scheduler_mod = b.createModule(.{
        .root_source_file = b.path("src/runtime/scheduler.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "fiber", .module = fiber_mod },
            .{ .name = "deque", .module = deque_mod },
            .{ .name = "obj", .module = obj_mod },
            .{ .name = "value", .module = value_mod },
            .{ .name = "chunk", .module = chunk_mod },
        },
    });

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
    // gc_nursery needs fiber for ObjFiber GC scanning.
    gc_nursery_mod.addImport("fiber", fiber_mod);

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
    // gc_oldgen needs fiber for ObjFiber GC scanning.
    gc_oldgen_mod.addImport("fiber", fiber_mod);
    // gc_nursery and gc_oldgen need channel for ObjChannel GC scanning.
    gc_nursery_mod.addImport("channel", channel_mod);
    gc_oldgen_mod.addImport("channel", channel_mod);

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
            .{ .name = "obj", .module = obj_mod },
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

    const json_mod = b.createModule(.{
        .root_source_file = b.path("src/stdlib/json.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "value", .module = value_mod },
            .{ .name = "obj", .module = obj_mod },
        },
    });

    // Wire stream_mod_build to import json for JSONL parsing.
    stream_mod_build.addImport("json", json_mod);

    // Wire stream_mod_build to import fiber and scheduler for par_map dispatch.
    stream_mod_build.addImport("fiber", fiber_mod);
    stream_mod_build.addImport("scheduler", scheduler_mod);

    const uri_mod = b.createModule(.{
        .root_source_file = b.path("src/stdlib/uri.zig"),
        .target = target,
        .optimize = optimize,
    });

    const aws_sig_mod = b.createModule(.{
        .root_source_file = b.path("src/stdlib/aws_sig.zig"),
        .target = target,
        .optimize = optimize,
    });

    const azure_sig_mod = b.createModule(.{
        .root_source_file = b.path("src/stdlib/azure_sig.zig"),
        .target = target,
        .optimize = optimize,
    });

    const auth_mod = b.createModule(.{
        .root_source_file = b.path("src/stdlib/auth.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "value", .module = value_mod },
            .{ .name = "obj", .module = obj_mod },
        },
    });

    const builtins_mod = b.createModule(.{
        .root_source_file = b.path("src/stdlib/builtins.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "value", .module = value_mod },
            .{ .name = "obj", .module = obj_mod },
            .{ .name = "stream", .module = stream_mod_build },
            .{ .name = "json", .module = json_mod },
            .{ .name = "uri", .module = uri_mod },
            .{ .name = "aws_sig", .module = aws_sig_mod },
            .{ .name = "azure_sig", .module = azure_sig_mod },
            .{ .name = "auth", .module = auth_mod },
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
            .{ .name = "fiber", .module = fiber_mod },
            .{ .name = "channel", .module = channel_mod },
            .{ .name = "scheduler", .module = scheduler_mod },
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

    // Wire gc_roots_mod to import fiber and scheduler for multi-fiber scanning.
    gc_roots_mod.addImport("fiber", fiber_mod);
    gc_roots_mod.addImport("scheduler", scheduler_mod);

    // Wire gc_mod to import gc_roots (for collectNursery orchestration).
    gc_mod.addImport("gc_roots", gc_roots_mod);
    // Wire gc_mod to import vm for VM type access.
    gc_mod.addImport("vm", vm_mod);
    // Wire gc_mod to import scheduler for safepoint protocol.
    gc_mod.addImport("scheduler", scheduler_mod);

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

    // ── REPL modules ────────────────────────────────────────────────────
    const colors_mod = b.createModule(.{
        .root_source_file = b.path("src/repl/colors.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "value", .module = value_mod },
            .{ .name = "obj", .module = obj_mod },
        },
    });

    const line_editor_mod = b.createModule(.{
        .root_source_file = b.path("src/repl/line_editor.zig"),
        .target = target,
        .optimize = optimize,
    });

    const repl_mod = b.createModule(.{
        .root_source_file = b.path("src/repl/repl.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "value", .module = value_mod },
            .{ .name = "obj", .module = obj_mod },
            .{ .name = "chunk", .module = chunk_mod },
            .{ .name = "error", .module = error_mod },
            .{ .name = "token", .module = token_mod },
            .{ .name = "lexer", .module = lexer_mod },
            .{ .name = "parser", .module = parser_mod },
            .{ .name = "compiler", .module = compiler_mod },
            .{ .name = "builtins", .module = builtins_mod },
            .{ .name = "vm", .module = vm_mod },
            .{ .name = "gc", .module = gc_mod },
            .{ .name = "line_editor", .module = line_editor_mod },
            .{ .name = "colors", .module = colors_mod },
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
            .{ .name = "repl", .module = repl_mod },
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
        json_mod,
        vm_mod,
        colors_mod,
        line_editor_mod,
        repl_mod,
        deque_mod,
        fiber_mod,
        context_switch_mod,
        scheduler_mod,
        channel_mod,
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
