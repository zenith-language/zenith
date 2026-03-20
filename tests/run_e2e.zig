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

// ── Test cases ─────────────────────────────────────────────────────────────

const TestCase = struct {
    name: []const u8,
    zen_path: []const u8,
    expected_path: []const u8,
    is_error_test: bool,
};

const normal_tests = [_]TestCase{
    .{ .name = "arithmetic", .zen_path = "tests/zen/arithmetic.zen", .expected_path = "tests/zen/arithmetic.expected", .is_error_test = false },
    .{ .name = "let_bindings", .zen_path = "tests/zen/let_bindings.zen", .expected_path = "tests/zen/let_bindings.expected", .is_error_test = false },
    .{ .name = "if_else", .zen_path = "tests/zen/if_else.zen", .expected_path = "tests/zen/if_else.expected", .is_error_test = false },
    .{ .name = "loops", .zen_path = "tests/zen/loops.zen", .expected_path = "tests/zen/loops.expected", .is_error_test = false },
    .{ .name = "atoms", .zen_path = "tests/zen/atoms.zen", .expected_path = "tests/zen/atoms.expected", .is_error_test = false },
    .{ .name = "blocks", .zen_path = "tests/zen/blocks.zen", .expected_path = "tests/zen/blocks.expected", .is_error_test = false },
    .{ .name = "strings", .zen_path = "tests/zen/strings.zen", .expected_path = "tests/zen/strings.expected", .is_error_test = false },
    .{ .name = "builtins", .zen_path = "tests/zen/builtins.zen", .expected_path = "tests/zen/builtins.expected", .is_error_test = false },
};

const error_tests = [_]TestCase{
    .{ .name = "type_mismatch", .zen_path = "tests/zen/errors/type_mismatch.zen", .expected_path = "tests/zen/errors/type_mismatch.expected", .is_error_test = true },
    .{ .name = "overflow", .zen_path = "tests/zen/errors/overflow.zen", .expected_path = "tests/zen/errors/overflow.expected", .is_error_test = true },
    .{ .name = "div_zero", .zen_path = "tests/zen/errors/div_zero.zen", .expected_path = "tests/zen/errors/div_zero.expected", .is_error_test = true },
    .{ .name = "undefined_var", .zen_path = "tests/zen/errors/undefined_var.zen", .expected_path = "tests/zen/errors/undefined_var.expected", .is_error_test = true },
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

    // Build atom name list.
    const atom_names = try buildAtomNames(&compile_result, allocator);
    defer allocator.free(atom_names);

    // 4. Execute -- use closure-based VM.
    var vm = VM.initWithClosure(compile_result.closure, allocator);
    try vm.setAtomNames(atom_names, allocator);
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
