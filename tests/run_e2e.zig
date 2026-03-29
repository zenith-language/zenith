/// End-to-end test runner for Zenith.
///
/// Scans tests/zen/ for .zen files, runs the full pipeline (lex -> parse ->
/// compile -> VM), captures stdout output, and compares to .expected files.
///
/// For error tests (tests/zen/errors/), the runner checks that the error
/// output CONTAINS the expected fragments (not exact match).
const std = @import("std");
const zenith = @import("zenith");
const Lexer = zenith.lexer.Lexer;
const Parser = zenith.parser.Parser;
const Compiler = zenith.compiler.Compiler;
const CompileResult = zenith.compiler.CompileResult;
const VM = zenith.vm.VM;
const Chunk = zenith.chunk.Chunk;
const Value = zenith.value.Value;
const builtins = zenith.builtins;
const GC = zenith.gc.GC;
const GCAllocator = zenith.gc.GCAllocator;

// ── Test cases ─────────────────────────────────────────────────────────────

const TestCase = struct {
    name: []const u8,
    zen_path: []const u8,
    expected_path: []const u8,
    is_error_test: bool,
};

const normal_tests = [_]TestCase{
    // Phase 1 tests
    .{ .name = "arithmetic", .zen_path = "tests/zen/arithmetic.zen", .expected_path = "tests/zen/arithmetic.expected", .is_error_test = false },
    .{ .name = "let_bindings", .zen_path = "tests/zen/let_bindings.zen", .expected_path = "tests/zen/let_bindings.expected", .is_error_test = false },
    .{ .name = "if_else", .zen_path = "tests/zen/if_else.zen", .expected_path = "tests/zen/if_else.expected", .is_error_test = false },
    .{ .name = "loops", .zen_path = "tests/zen/loops.zen", .expected_path = "tests/zen/loops.expected", .is_error_test = false },
    .{ .name = "atoms", .zen_path = "tests/zen/atoms.zen", .expected_path = "tests/zen/atoms.expected", .is_error_test = false },
    .{ .name = "blocks", .zen_path = "tests/zen/blocks.zen", .expected_path = "tests/zen/blocks.expected", .is_error_test = false },
    .{ .name = "strings", .zen_path = "tests/zen/strings.zen", .expected_path = "tests/zen/strings.expected", .is_error_test = false },
    .{ .name = "builtins", .zen_path = "tests/zen/builtins.zen", .expected_path = "tests/zen/builtins.expected", .is_error_test = false },
    // Phase 2 tests
    .{ .name = "functions", .zen_path = "tests/zen/functions.zen", .expected_path = "tests/zen/functions.expected", .is_error_test = false },
    .{ .name = "closures", .zen_path = "tests/zen/closures.zen", .expected_path = "tests/zen/closures.expected", .is_error_test = false },
    .{ .name = "pipes", .zen_path = "tests/zen/pipes.zen", .expected_path = "tests/zen/pipes.expected", .is_error_test = false },
    .{ .name = "lambdas", .zen_path = "tests/zen/lambdas.zen", .expected_path = "tests/zen/lambdas.expected", .is_error_test = false },
    .{ .name = "named_args", .zen_path = "tests/zen/named_args.zen", .expected_path = "tests/zen/named_args.expected", .is_error_test = false },
    .{ .name = "tail_calls", .zen_path = "tests/zen/tail_calls.zen", .expected_path = "tests/zen/tail_calls.expected", .is_error_test = false },
    // Phase 3 tests
    .{ .name = "lists", .zen_path = "tests/zen/lists.zen", .expected_path = "tests/zen/lists.expected", .is_error_test = false },
    .{ .name = "maps", .zen_path = "tests/zen/maps.zen", .expected_path = "tests/zen/maps.expected", .is_error_test = false },
    .{ .name = "tuples", .zen_path = "tests/zen/tuples.zen", .expected_path = "tests/zen/tuples.expected", .is_error_test = false },
    .{ .name = "records", .zen_path = "tests/zen/records.zen", .expected_path = "tests/zen/records.expected", .is_error_test = false },
    .{ .name = "adts", .zen_path = "tests/zen/adts.zen", .expected_path = "tests/zen/adts.expected", .is_error_test = false },
    .{ .name = "pattern_matching", .zen_path = "tests/zen/pattern_matching.zen", .expected_path = "tests/zen/pattern_matching.expected", .is_error_test = false },
    .{ .name = "result_option", .zen_path = "tests/zen/result_option.zen", .expected_path = "tests/zen/result_option.expected", .is_error_test = false },
    .{ .name = "string_ops", .zen_path = "tests/zen/string_ops.zen", .expected_path = "tests/zen/string_ops.expected", .is_error_test = false },
    // Phase 4 tests
    .{ .name = "gc", .zen_path = "tests/zen/gc.zen", .expected_path = "tests/zen/gc.expected", .is_error_test = false },
    .{ .name = "gc_stress", .zen_path = "tests/zen/gc_stress.zen", .expected_path = "tests/zen/gc_stress.expected", .is_error_test = false },
    // Phase 5 tests
    .{ .name = "streams_basic", .zen_path = "tests/zen/streams_basic.zen", .expected_path = "tests/zen/streams_basic.expected", .is_error_test = false },
    .{ .name = "streams_ops", .zen_path = "tests/zen/streams_ops.zen", .expected_path = "tests/zen/streams_ops.expected", .is_error_test = false },
    .{ .name = "streams_partition", .zen_path = "tests/zen/streams_partition.zen", .expected_path = "tests/zen/streams_partition.expected", .is_error_test = false },
    .{ .name = "json", .zen_path = "tests/zen/json.zen", .expected_path = "tests/zen/json.expected", .is_error_test = false },
    .{ .name = "file_io", .zen_path = "tests/zen/file_io.zen", .expected_path = "tests/zen/file_io.expected", .is_error_test = false },
    // Phase 5 e2e tests
    .{ .name = "streams", .zen_path = "tests/zen/streams.zen", .expected_path = "tests/zen/streams.expected", .is_error_test = false },
    .{ .name = "streams_lazy", .zen_path = "tests/zen/streams_lazy.zen", .expected_path = "tests/zen/streams_lazy.expected", .is_error_test = false },
    .{ .name = "jsonl", .zen_path = "tests/zen/jsonl.zen", .expected_path = "tests/zen/jsonl.expected", .is_error_test = false },
    .{ .name = "json_source", .zen_path = "tests/zen/json_source.zen", .expected_path = "tests/zen/json_source.expected", .is_error_test = false },
    .{ .name = "sugar", .zen_path = "tests/zen/sugar.zen", .expected_path = "tests/zen/sugar.expected", .is_error_test = false },
    // Phase 7 tests
    .{ .name = "fibers", .zen_path = "tests/zen/fibers.zen", .expected_path = "tests/zen/fibers.expected", .is_error_test = false },
    .{ .name = "fibers_error", .zen_path = "tests/zen/fibers_error.zen", .expected_path = "tests/zen/fibers_error.expected", .is_error_test = false },
    .{ .name = "channels", .zen_path = "tests/zen/channels.zen", .expected_path = "tests/zen/channels.expected", .is_error_test = false },
    .{ .name = "fiber_closures", .zen_path = "tests/zen/fiber_closures.zen", .expected_path = "tests/zen/fiber_closures.expected", .is_error_test = false },
    .{ .name = "select", .zen_path = "tests/zen/select.zen", .expected_path = "tests/zen/select.expected", .is_error_test = false },
    .{ .name = "par_map", .zen_path = "tests/zen/par_map.zen", .expected_path = "tests/zen/par_map.expected", .is_error_test = false },
    // Stream operators: buffer and throttle
    .{ .name = "buffer", .zen_path = "tests/zen/buffer.zen", .expected_path = "tests/zen/buffer.expected", .is_error_test = false },
    .{ .name = "throttle", .zen_path = "tests/zen/throttle.zen", .expected_path = "tests/zen/throttle.expected", .is_error_test = false },
};

const error_tests = [_]TestCase{
    // Phase 1 error tests
    .{ .name = "type_mismatch", .zen_path = "tests/zen/errors/type_mismatch.zen", .expected_path = "tests/zen/errors/type_mismatch.expected", .is_error_test = true },
    .{ .name = "overflow", .zen_path = "tests/zen/errors/overflow.zen", .expected_path = "tests/zen/errors/overflow.expected", .is_error_test = true },
    .{ .name = "div_zero", .zen_path = "tests/zen/errors/div_zero.zen", .expected_path = "tests/zen/errors/div_zero.expected", .is_error_test = true },
    .{ .name = "undefined_var", .zen_path = "tests/zen/errors/undefined_var.zen", .expected_path = "tests/zen/errors/undefined_var.expected", .is_error_test = true },
    // Phase 2 error tests
    .{ .name = "not_callable", .zen_path = "tests/zen/errors/not_callable.zen", .expected_path = "tests/zen/errors/not_callable.expected", .is_error_test = true },
    .{ .name = "arity_mismatch", .zen_path = "tests/zen/errors/arity_mismatch.zen", .expected_path = "tests/zen/errors/arity_mismatch.expected", .is_error_test = true },
    // Phase 3 error tests
    .{ .name = "match_fail", .zen_path = "tests/zen/errors/match_fail.zen", .expected_path = "tests/zen/errors/match_fail.expected", .is_error_test = true },
};

/// Warning tests: pipeline succeeds but error output contains expected warning fragments.
const warning_tests = [_]TestCase{
    .{ .name = "non_exhaustive", .zen_path = "tests/zen/errors/non_exhaustive.zen", .expected_path = "tests/zen/errors/non_exhaustive.expected", .is_error_test = false },
};

// ── Pipeline runner ────────────────────────────────────────────────────────

const PipelineResult = struct {
    output: []const u8,
    error_output: []const u8,
    succeeded: bool,
};

fn runPipeline(source: []const u8, file_name: []const u8, allocator: std.mem.Allocator) !PipelineResult {
    var output_buf = std.ArrayListUnmanaged(u8){};
    errdefer output_buf.deinit(allocator);
    var error_buf = std.ArrayListUnmanaged(u8){};
    errdefer error_buf.deinit(allocator);

    // 1. Lex
    var lex = Lexer.init(source);
    try lex.tokenize(allocator);
    defer lex.tokens.deinit(allocator);
    defer lex.errors.deinit(allocator);

    if (lex.errors.hasErrors()) {
        for (lex.errors.items.items) |diag| {
            diag.render(source, file_name, error_buf.writer(allocator), false) catch {};
        }
        return .{
            .output = try output_buf.toOwnedSlice(allocator),
            .error_output = try error_buf.toOwnedSlice(allocator),
            .succeeded = false,
        };
    }

    // 2. Parse
    var ast = Parser.parse(lex.tokens.items, source, allocator) catch {
        return .{
            .output = try output_buf.toOwnedSlice(allocator),
            .error_output = try error_buf.toOwnedSlice(allocator),
            .succeeded = false,
        };
    };
    defer ast.deinit(allocator);

    if (ast.errors.hasErrors()) {
        for (ast.errors.items.items) |diag| {
            diag.render(source, file_name, error_buf.writer(allocator), false) catch {};
        }
        return .{
            .output = try output_buf.toOwnedSlice(allocator),
            .error_output = try error_buf.toOwnedSlice(allocator),
            .succeeded = false,
        };
    }

    // 3. Compile
    var compile_result = try Compiler.compile(&ast, allocator);
    defer compile_result.deinit(allocator);

    if (compile_result.hasErrors()) {
        for (compile_result.errors.items) |diag| {
            diag.render(source, file_name, error_buf.writer(allocator), false) catch {};
        }
        return .{
            .output = try output_buf.toOwnedSlice(allocator),
            .error_output = try error_buf.toOwnedSlice(allocator),
            .succeeded = false,
        };
    }

    // Render any compiler warnings to error_buf (for warning tests).
    for (compile_result.errors.items) |diag| {
        if (diag.severity == .warning) {
            diag.render(source, file_name, error_buf.writer(allocator), false) catch {};
        }
    }

    // Build atom name list.
    const atom_names = try buildAtomNames(&compile_result, allocator);
    defer allocator.free(atom_names);

    // Build ADT type info for pretty-printing.
    const adt_info = try buildAdtTypeInfo(&compile_result, allocator);
    defer allocator.free(adt_info);

    // 4. Execute -- use closure-based VM with GC.
    var gc_state = try GC.init(allocator);
    defer gc_state.deinit();
    var gc_alloc = GCAllocator{ .gc = &gc_state };
    const gc_allocator = gc_alloc.allocator();

    var vm = VM.initWithClosure(compile_result.closure, gc_allocator, &gc_state, allocator);
    vm.trackCompilerObjects(compile_result.closure);
    compile_result.vm_owns_constants = true;
    try vm.setAtomNames(atom_names, allocator);
    if (adt_info.len > 0) {
        vm.setAdtTypes(adt_info);
    }
    vm.output_buf = &output_buf;

    _ = vm.run() catch {
        for (vm.errors.items) |diag| {
            diag.render(source, file_name, error_buf.writer(allocator), false) catch {};
        }
        vm.deinit();
        return .{
            .output = try output_buf.toOwnedSlice(allocator),
            .error_output = try error_buf.toOwnedSlice(allocator),
            .succeeded = false,
        };
    };

    vm.deinit();
    return .{
        .output = try output_buf.toOwnedSlice(allocator),
        .error_output = try error_buf.toOwnedSlice(allocator),
        .succeeded = true,
    };
}

fn buildAtomNames(compile_result: *CompileResult, allocator: std.mem.Allocator) ![]const []const u8 {
    const atom_count = compile_result.atom_count;
    if (atom_count == 0) return allocator.alloc([]const u8, 0);

    const names = try allocator.alloc([]const u8, atom_count);
    for (names) |*n| {
        n.* = "?";
    }

    var iter = compile_result.atom_table.iterator();
    while (iter.next()) |entry| {
        const idx = entry.value_ptr.*;
        if (idx < atom_count) {
            names[idx] = entry.key_ptr.*;
        }
    }

    return names;
}

fn buildAdtTypeInfo(compile_result: *CompileResult, allocator: std.mem.Allocator) ![]const builtins.AdtTypeInfo {
    const adt_types = compile_result.adt_types.items;
    if (adt_types.len == 0) return allocator.alloc(builtins.AdtTypeInfo, 0);

    const info = try allocator.alloc(builtins.AdtTypeInfo, adt_types.len);
    for (adt_types, 0..) |meta, i| {
        info[i] = .{
            .name = meta.name,
            .variant_names = meta.variant_names,
        };
    }
    return info;
}

/// Trim trailing whitespace/newlines from a string.
fn trimTrailing(s: []const u8) []const u8 {
    var end = s.len;
    while (end > 0 and (s[end - 1] == '\n' or s[end - 1] == '\r' or s[end - 1] == ' ')) {
        end -= 1;
    }
    return s[0..end];
}

// ── Normal tests ───────────────────────────────────────────────────────────

fn runNormalTest(tc: TestCase) !void {
    const allocator = std.testing.allocator;

    const source = try std.fs.cwd().readFileAlloc(allocator, tc.zen_path, 10 * 1024 * 1024);
    defer allocator.free(source);

    const expected = try std.fs.cwd().readFileAlloc(allocator, tc.expected_path, 10 * 1024 * 1024);
    defer allocator.free(expected);

    const result = try runPipeline(source, tc.zen_path, allocator);
    defer allocator.free(result.output);
    defer allocator.free(result.error_output);

    if (!result.succeeded) {
        std.debug.print("\n[FAIL] {s}: pipeline failed\n", .{tc.name});
        if (result.error_output.len > 0) {
            std.debug.print("Error output:\n{s}\n", .{result.error_output});
        }
        return error.TestFailed;
    }

    const actual_trimmed = trimTrailing(result.output);
    const expected_trimmed = trimTrailing(expected);

    if (!std.mem.eql(u8, actual_trimmed, expected_trimmed)) {
        std.debug.print("\n[FAIL] {s}: output mismatch\n", .{tc.name});
        std.debug.print("Expected:\n---\n{s}\n---\nActual:\n---\n{s}\n---\n", .{ expected_trimmed, actual_trimmed });
        return error.TestFailed;
    }
}

const TestFailed = error{TestFailed};

test "e2e: arithmetic" {
    try runNormalTest(normal_tests[0]);
}

test "e2e: let_bindings" {
    try runNormalTest(normal_tests[1]);
}

test "e2e: if_else" {
    try runNormalTest(normal_tests[2]);
}

test "e2e: loops" {
    try runNormalTest(normal_tests[3]);
}

test "e2e: atoms" {
    try runNormalTest(normal_tests[4]);
}

test "e2e: blocks" {
    try runNormalTest(normal_tests[5]);
}

test "e2e: strings" {
    try runNormalTest(normal_tests[6]);
}

test "e2e: builtins" {
    try runNormalTest(normal_tests[7]);
}

// ── Phase 2 normal tests ──────────────────────────────────────────────────

test "e2e: functions" {
    try runNormalTest(normal_tests[8]);
}

test "e2e: closures" {
    try runNormalTest(normal_tests[9]);
}

test "e2e: pipes" {
    try runNormalTest(normal_tests[10]);
}

test "e2e: lambdas" {
    try runNormalTest(normal_tests[11]);
}

test "e2e: named_args" {
    try runNormalTest(normal_tests[12]);
}

test "e2e: tail_calls" {
    try runNormalTest(normal_tests[13]);
}

// ── Error tests ────────────────────────────────────────────────────────────

fn runErrorTest(tc: TestCase) !void {
    const allocator = std.testing.allocator;

    const source = try std.fs.cwd().readFileAlloc(allocator, tc.zen_path, 10 * 1024 * 1024);
    defer allocator.free(source);

    const expected = try std.fs.cwd().readFileAlloc(allocator, tc.expected_path, 10 * 1024 * 1024);
    defer allocator.free(expected);

    const result = try runPipeline(source, tc.zen_path, allocator);
    defer allocator.free(result.output);
    defer allocator.free(result.error_output);

    // For error tests, the pipeline should have failed.
    if (result.succeeded) {
        std.debug.print("\n[FAIL] {s}: expected error but pipeline succeeded\n", .{tc.name});
        return error.TestFailed;
    }

    // Check that error output contains each line from the expected file.
    var lines = std.mem.splitSequence(u8, expected, "\n");
    while (lines.next()) |line| {
        const trimmed = trimTrailing(line);
        if (trimmed.len == 0) continue;
        if (std.mem.indexOf(u8, result.error_output, trimmed) == null) {
            std.debug.print("\n[FAIL] {s}: error output missing fragment: '{s}'\n", .{ tc.name, trimmed });
            std.debug.print("Full error output:\n{s}\n", .{result.error_output});
            return error.TestFailed;
        }
    }
}

test "e2e error: type_mismatch" {
    try runErrorTest(error_tests[0]);
}

test "e2e error: overflow" {
    try runErrorTest(error_tests[1]);
}

test "e2e error: div_zero" {
    try runErrorTest(error_tests[2]);
}

test "e2e error: undefined_var" {
    try runErrorTest(error_tests[3]);
}

// ── Phase 2 error tests ──────────────────────────────────────────────────

test "e2e error: not_callable" {
    try runErrorTest(error_tests[4]);
}

test "e2e error: arity_mismatch" {
    try runErrorTest(error_tests[5]);
}

// ── Phase 3 normal tests ──────────────────────────────────────────────────

test "e2e: lists" {
    try runNormalTest(normal_tests[14]);
}

test "e2e: maps" {
    try runNormalTest(normal_tests[15]);
}

test "e2e: tuples" {
    try runNormalTest(normal_tests[16]);
}

test "e2e: records" {
    try runNormalTest(normal_tests[17]);
}

test "e2e: adts" {
    try runNormalTest(normal_tests[18]);
}

test "e2e: pattern_matching" {
    try runNormalTest(normal_tests[19]);
}

test "e2e: result_option" {
    try runNormalTest(normal_tests[20]);
}

test "e2e: string_ops" {
    try runNormalTest(normal_tests[21]);
}

// ── Phase 4 normal tests ──────────────────────────────────────────────────

test "e2e: gc" {
    try runNormalTest(normal_tests[22]);
}

test "e2e: gc_stress" {
    try runNormalTest(normal_tests[23]);
}

// ── Phase 5 normal tests ──────────────────────────────────────────────────

test "e2e: streams_basic" {
    try runNormalTest(normal_tests[24]);
}

test "e2e: streams_ops" {
    try runNormalTest(normal_tests[25]);
}

test "e2e: streams_partition" {
    try runNormalTest(normal_tests[26]);
}

test "e2e: json" {
    try runNormalTest(normal_tests[27]);
}

test "e2e: file_io" {
    try runNormalTest(normal_tests[28]);
}

test "e2e: streams" {
    try runNormalTest(normal_tests[29]);
}

test "e2e: streams_lazy" {
    try runNormalTest(normal_tests[30]);
}

test "e2e: jsonl" {
    try runNormalTest(normal_tests[31]);
}

test "e2e: sugar" {
    try runNormalTest(normal_tests[33]);
}

// ── Phase 7 normal tests ──────────────────────────────────────────────────

test "e2e: fibers" {
    try runNormalTest(normal_tests[34]);
}

test "e2e: fibers_error" {
    try runNormalTest(normal_tests[35]);
}

test "e2e: channels" {
    try runNormalTest(normal_tests[36]);
}

test "e2e: fiber_closures" {
    try runNormalTest(normal_tests[37]);
}

test "e2e: select" {
    try runNormalTest(normal_tests[38]);
}

test "e2e: par_map" {
    try runNormalTest(normal_tests[39]);
}

// ── Phase 3 error tests ──────────────────────────────────────────────────

test "e2e error: match_fail" {
    try runErrorTest(error_tests[6]);
}

// ── Warning tests ─────────────────────────────────────────────────────────

fn runWarningTest(tc: TestCase) !void {
    const allocator = std.testing.allocator;

    const source = try std.fs.cwd().readFileAlloc(allocator, tc.zen_path, 10 * 1024 * 1024);
    defer allocator.free(source);

    const expected = try std.fs.cwd().readFileAlloc(allocator, tc.expected_path, 10 * 1024 * 1024);
    defer allocator.free(expected);

    const result = try runPipeline(source, tc.zen_path, allocator);
    defer allocator.free(result.output);
    defer allocator.free(result.error_output);

    // Warning tests: pipeline should succeed but error output should contain warning fragments.
    if (!result.succeeded) {
        std.debug.print("\n[FAIL] {s}: expected success but pipeline failed\n", .{tc.name});
        if (result.error_output.len > 0) {
            std.debug.print("Error output:\n{s}\n", .{result.error_output});
        }
        return error.TestFailed;
    }

    // Check that error output contains each expected fragment.
    var lines = std.mem.splitSequence(u8, expected, "\n");
    while (lines.next()) |line_raw| {
        const trimmed = trimTrailing(line_raw);
        if (trimmed.len == 0) continue;
        if (std.mem.indexOf(u8, result.error_output, trimmed) == null) {
            std.debug.print("\n[FAIL] {s}: warning output missing fragment: '{s}'\n", .{ tc.name, trimmed });
            std.debug.print("Full error output:\n{s}\n", .{result.error_output});
            return error.TestFailed;
        }
    }
}

test "e2e warning: non_exhaustive" {
    try runWarningTest(warning_tests[0]);
}

// ── Bytecode roundtrip test ────────────────────────────────────────────────

test "e2e: bytecode roundtrip for arithmetic" {
    const allocator = std.testing.allocator;

    const source = try std.fs.cwd().readFileAlloc(allocator, "tests/zen/arithmetic.zen", 10 * 1024 * 1024);
    defer allocator.free(source);

    const expected = try std.fs.cwd().readFileAlloc(allocator, "tests/zen/arithmetic.expected", 10 * 1024 * 1024);
    defer allocator.free(expected);

    // Run from source first.
    const result1 = try runPipeline(source, "tests/zen/arithmetic.zen", allocator);
    defer allocator.free(result1.output);
    defer allocator.free(result1.error_output);

    if (!result1.succeeded) return error.TestFailed;

    // Now compile to bytecode, deserialize, and run.
    // Lex
    var lex = Lexer.init(source);
    try lex.tokenize(allocator);
    defer lex.tokens.deinit(allocator);
    defer lex.errors.deinit(allocator);

    // Parse
    var ast = try Parser.parse(lex.tokens.items, source, allocator);
    defer ast.deinit(allocator);

    // Compile
    var compile_result = try Compiler.compile(&ast, allocator);
    defer compile_result.deinit(allocator);

    // Build atom names
    const atom_names = try buildAtomNames(&compile_result, allocator);
    defer allocator.free(atom_names);

    const top_chunk2 = &compile_result.closure.function.chunk;
    for (atom_names) |name| {
        try top_chunk2.atom_names.append(allocator, name);
    }
    top_chunk2.name = "test.zen";

    // Serialize
    var ser_buf = std.ArrayListUnmanaged(u8){};
    defer ser_buf.deinit(allocator);
    try top_chunk2.serialize(ser_buf.writer(allocator));

    // Deserialize
    var stream = std.io.fixedBufferStream(ser_buf.items);
    var chunk = try Chunk.deserialize(stream.reader(), allocator);
    defer {
        for (chunk.constants.items) |val| {
            if (val.isObj()) val.asObj().destroy(allocator);
        }
        chunk.deinit(allocator);
    }

    // Run from deserialized chunk
    var output_buf = std.ArrayListUnmanaged(u8){};
    defer output_buf.deinit(allocator);

    var vm = VM.init(&chunk, allocator);
    if (chunk.atom_names.items.len > 0) {
        try vm.setAtomNames(chunk.atom_names.items, allocator);
    }
    vm.output_buf = &output_buf;

    _ = try vm.run();
    vm.deinit();

    // Compare output from bytecode run to expected.
    const actual_trimmed = trimTrailing(output_buf.items);
    const expected_trimmed = trimTrailing(expected);

    try std.testing.expectEqualStrings(expected_trimmed, actual_trimmed);
}

// ── Phase 6: CLI e2e tests (process spawning) ─────────────────────────────
//
// These tests spawn the zenith binary and verify stdout/stderr output.
// They test CLI-only features: REPL, disassembler, stdin mode, -e mode.
// The binary must be built first (zig build) before these tests run.

const cli_binary_path = "zig-out/bin/zenith";

const CliTestResult = struct {
    stdout: []u8,
    stderr: []u8,
    allocator: std.mem.Allocator,

    fn deinit(self: *CliTestResult) void {
        self.allocator.free(self.stdout);
        self.allocator.free(self.stderr);
    }
};

/// Spawn the zenith binary with given args and optional stdin, collect output.
fn runCliProcess(args: []const []const u8, stdin_input: ?[]const u8) !CliTestResult {
    const allocator = std.testing.allocator;

    // Check binary exists.
    std.fs.cwd().access(cli_binary_path, .{}) catch {
        return error.SkipZigTest;
    };

    var argv = std.ArrayListUnmanaged([]const u8){};
    defer argv.deinit(allocator);
    try argv.append(allocator, cli_binary_path);
    for (args) |arg| {
        try argv.append(allocator, arg);
    }

    var child = std.process.Child.init(argv.items, allocator);
    child.stdin_behavior = if (stdin_input != null) .Pipe else .Inherit;
    child.stdout_behavior = .Pipe;
    child.stderr_behavior = .Pipe;

    try child.spawn();

    // Write stdin if provided, then close.
    if (stdin_input) |input| {
        if (child.stdin) |stdin_fd| {
            stdin_fd.writeAll(input) catch {};
            stdin_fd.close();
            child.stdin = null;
        }
    }

    // Collect stdout and stderr BEFORE wait (wait closes pipes).
    var stdout_list: std.ArrayList(u8) = .empty;
    errdefer stdout_list.deinit(allocator);
    var stderr_list: std.ArrayList(u8) = .empty;
    errdefer stderr_list.deinit(allocator);

    try child.collectOutput(allocator, &stdout_list, &stderr_list, 1024 * 1024);
    _ = try child.wait();

    return .{
        .stdout = try stdout_list.toOwnedSlice(allocator),
        .stderr = try stderr_list.toOwnedSlice(allocator),
        .allocator = allocator,
    };
}

/// Spawn the zenith binary with given args, optional stdin, and verify output.
fn runCliTest(args: []const []const u8, stdin_input: ?[]const u8, expected_fragments: []const []const u8, check_stderr: bool) !void {
    var result = runCliProcess(args, stdin_input) catch |err| {
        if (err == error.SkipZigTest) {
            std.debug.print("\n[SKIP] CLI test: binary not found at {s}\n", .{cli_binary_path});
            return;
        }
        return err;
    };
    defer result.deinit();

    const search_buf = if (check_stderr) result.stderr else result.stdout;

    // Check each expected fragment exists in the output.
    for (expected_fragments) |fragment| {
        if (std.mem.indexOf(u8, search_buf, fragment) == null) {
            std.debug.print("\n[FAIL] CLI test: output missing fragment: '{s}'\n", .{fragment});
            std.debug.print("Args: ", .{});
            for (args) |a| std.debug.print("{s} ", .{a});
            std.debug.print("\nStdout ({d} bytes):\n{s}\nStderr ({d} bytes):\n{s}\n", .{ result.stdout.len, result.stdout, result.stderr.len, result.stderr });
            return error.TestFailed;
        }
    }
}

// ── REPL tests (TOOL-01) ──────────────────────────────────────────────────

test "Phase 6 CLI: REPL basic expression evaluation" {
    try runCliTest(
        &.{"repl"},
        "let x = 42\nx + 1\n",
        &.{ "42", "43" },
        false,
    );
}

test "Phase 6 CLI: REPL error recovery" {
    // Use an undefined variable error (not 'let +' which triggers continuation).
    // Verify error message appears on stderr.
    try runCliTest(
        &.{"repl"},
        "xyz\n1 + 2\n",
        &.{"error[E"},
        true,
    );
    // Also verify recovery produces correct result on stdout.
    try runCliTest(
        &.{"repl"},
        "xyz\n1 + 2\n",
        &.{"3"},
        false,
    );
}

test "Phase 6 CLI: REPL continuation" {
    try runCliTest(
        &.{"repl"},
        "{\n  1 + 2\n}\n",
        &.{"3"},
        false,
    );
}

test "Phase 6 CLI: REPL nil suppression" {
    // print(42) outputs 42 to stdout but REPL should not print nil separately.
    try runCliTest(
        &.{"repl"},
        "print(42)\n",
        &.{"42"},
        false,
    );
}

test "Phase 6 CLI: REPL variable persistence" {
    try runCliTest(
        &.{"repl"},
        "let name = \"zenith\"\nname\n",
        &.{"zenith"},
        false,
    );
}

// ── Disassembler tests (TOOL-05) ──────────────────────────────────────────

test "Phase 6 CLI: disassembler basic" {
    try runCliTest(
        &.{ "dis", "tests/zen/arithmetic.zen" },
        null,
        &.{ "== <script> ==", "OP_CONSTANT", "OP_ADD" },
        false,
    );
}

test "Phase 6 CLI: disassembler verbose" {
    try runCliTest(
        &.{ "dis", "-v", "tests/zen/arithmetic.zen" },
        null,
        &.{ "-- Constant Pool --", "-- Atom Table --", "-- Debug Info --" },
        false,
    );
}

test "Phase 6 CLI: disassembler recursive" {
    try runCliTest(
        &.{ "dis", "tests/zen/functions.zen" },
        null,
        &.{ "== <script> ==", "== add ==" },
        false,
    );
}

// ── CLI mode tests (TOOL-04) ──────────────────────────────────────────────

test "Phase 6 CLI: stdin mode" {
    try runCliTest(
        &.{ "run", "-" },
        "print(42)",
        &.{"42"},
        false,
    );
}

test "Phase 6 CLI: eval mode" {
    try runCliTest(
        &.{ "run", "-e", "print(1 + 2)" },
        null,
        &.{"3"},
        false,
    );
}

test "Phase 6 CLI: help text" {
    try runCliTest(
        &.{"--help"},
        null,
        &.{ "repl", "dis", "run -", "run -e" },
        false,
    );
}

// ── Single binary test (TOOL-03) ──────────────────────────────────────────

test "Phase 6 CLI: single binary no external deps" {
    const allocator = std.testing.allocator;

    // Check binary exists.
    std.fs.cwd().access(cli_binary_path, .{}) catch {
        std.debug.print("\n[SKIP] Single binary test: binary not found\n", .{});
        return;
    };

    // Use otool -L on macOS to check dynamic dependencies.
    const otool_result = std.process.Child.run(.{
        .allocator = allocator,
        .argv = &.{ "otool", "-L", cli_binary_path },
    }) catch {
        // otool not available (non-macOS), skip gracefully.
        std.debug.print("\n[SKIP] Single binary test: otool not available\n", .{});
        return;
    };
    defer allocator.free(otool_result.stdout);
    defer allocator.free(otool_result.stderr);

    const output = otool_result.stdout;

    // Verify only libSystem dependency (macOS).
    if (std.mem.indexOf(u8, output, "/usr/lib/libSystem") == null) {
        std.debug.print("\n[WARN] Single binary test: expected libSystem in otool output\n", .{});
        std.debug.print("Output:\n{s}\n", .{output});
    }

    // Count number of dylib lines (excluding the header line).
    var lib_count: usize = 0;
    var lines = std.mem.splitSequence(u8, output, "\n");
    while (lines.next()) |line| {
        if (std.mem.indexOf(u8, line, ".dylib") != null) {
            lib_count += 1;
        }
    }

    // Should have exactly 1 dylib (libSystem).
    if (lib_count > 1) {
        std.debug.print("\n[FAIL] Single binary test: expected 1 dylib but found {d}\n", .{lib_count});
        std.debug.print("Output:\n{s}\n", .{output});
        return error.TestFailed;
    }
}
