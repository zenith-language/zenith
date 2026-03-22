/// Interactive REPL for Zenith with accumulated-source compilation,
/// error recovery, and colorized output.
const std = @import("std");
const posix = std.posix;
const Allocator = std.mem.Allocator;

const value_mod = @import("value");
const Value = value_mod.Value;
const obj_mod = @import("obj");
const error_mod = @import("error");
const Diagnostic = error_mod.Diagnostic;
const token_mod = @import("token");
const Tag = token_mod.Tag;
const Token = token_mod.Token;
const lexer_mod = @import("lexer");
const Lexer = lexer_mod.Lexer;
const parser_mod = @import("parser");
const Parser = parser_mod.Parser;
const compiler_mod = @import("compiler");
const Compiler = compiler_mod.Compiler;
const CompileResult = compiler_mod.CompileResult;
const builtins = @import("builtins");
const vm_mod = @import("vm");
const VM = vm_mod.VM;
const gc_mod = @import("gc");
const GC = gc_mod.GC;
const GCAllocator = gc_mod.GCAllocator;
const LineEditor = @import("line_editor").LineEditor;
const colors = @import("colors");

const version_string = "Zenith v0.1.0";
const prompt_normal = "zenith> ";
const prompt_continuation = "  ...> ";

/// Launch the interactive REPL.
pub fn runRepl(allocator: Allocator) !void {
    const is_tty = posix.isatty(posix.STDIN_FILENO);
    const use_color = is_tty and posix.isatty(posix.STDOUT_FILENO);

    // Accumulated source across REPL lines.
    var accumulated: std.ArrayListUnmanaged(u8) = .empty;
    defer accumulated.deinit(allocator);

    const stdout = std.fs.File.stdout();

    // Interactive (TTY) mode: use line editor.
    // Non-interactive (piped) mode: read from stdin directly.
    var editor: ?LineEditor = null;
    defer if (editor) |*e| e.deinit();

    if (is_tty) {
        editor = try LineEditor.init(allocator);
        // Print welcome banner (using raw mode, need \r\n).
        stdout.writeAll(version_string ++ "\r\nType expressions to evaluate. Ctrl+D to exit.\r\n\r\n") catch {};
    }

    var in_continuation = false;

    // For non-TTY mode, read line by reading byte-by-byte.
    var pipe_line_buf: std.ArrayListUnmanaged(u8) = .empty;
    defer pipe_line_buf.deinit(allocator);

    while (true) {
        const prompt = if (in_continuation) prompt_continuation else prompt_normal;

        // Read a line.
        const maybe_line: ?[]const u8 = if (editor) |*e|
            try e.readLine(prompt)
        else blk: {
            // Non-TTY: read line from stdin byte-by-byte until newline or EOF.
            pipe_line_buf.clearRetainingCapacity();
            while (true) {
                var buf: [1]u8 = undefined;
                const n = posix.read(posix.STDIN_FILENO, &buf) catch break :blk null;
                if (n == 0) {
                    // EOF
                    if (pipe_line_buf.items.len > 0) break; // return what we have
                    break :blk null;
                }
                if (buf[0] == '\n') break;
                pipe_line_buf.append(allocator, buf[0]) catch break :blk null;
            }
            break :blk pipe_line_buf.items;
        };

        const line = maybe_line orelse {
            // EOF
            if (is_tty) {
                stdout.writeAll("\r\n") catch {};
            }
            break;
        };

        // Empty line (e.g., from Ctrl+C).
        if (line.len == 0 and !in_continuation) continue;

        // Track accumulated source length before appending (for error recovery).
        const prev_len = accumulated.items.len;

        // Append line + newline to accumulated source.
        try accumulated.appendSlice(allocator, line);
        try accumulated.append(allocator, '\n');

        // Free the line if we're in editor mode (readLine allocates).
        if (editor != null) {
            allocator.free(line);
        }

        // Check for continuation (incomplete input).
        if (needsContinuation(accumulated.items, allocator)) {
            in_continuation = true;
            continue;
        }
        in_continuation = false;

        // Attempt to compile and execute.
        const source = accumulated.items;

        // 1. Lex
        var lex = Lexer.init(source);
        lex.tokenize(allocator) catch {
            accumulated.shrinkRetainingCapacity(prev_len);
            continue;
        };
        defer lex.tokens.deinit(allocator);
        defer lex.errors.deinit(allocator);

        if (lex.errors.hasErrors()) {
            renderErrors(lex.errors.items.items, source, use_color, allocator);
            accumulated.shrinkRetainingCapacity(prev_len);
            continue;
        }

        // 2. Parse
        var ast = Parser.parse(lex.tokens.items, source, allocator) catch {
            accumulated.shrinkRetainingCapacity(prev_len);
            continue;
        };
        defer ast.deinit(allocator);

        if (ast.errors.hasErrors()) {
            renderErrors(ast.errors.items.items, source, use_color, allocator);
            accumulated.shrinkRetainingCapacity(prev_len);
            continue;
        }

        // 3. Compile (REPL mode: keep last value on stack).
        var compile_result = Compiler.compileRepl(&ast, allocator) catch {
            accumulated.shrinkRetainingCapacity(prev_len);
            continue;
        };
        defer compile_result.deinit(allocator);

        if (compile_result.hasErrors()) {
            renderErrors(compile_result.errors.items, source, use_color, allocator);
            accumulated.shrinkRetainingCapacity(prev_len);
            continue;
        }

        // Build atom names from compiler's atom_table.
        const atom_names = buildAtomNames(&compile_result, allocator) catch {
            accumulated.shrinkRetainingCapacity(prev_len);
            continue;
        };
        defer allocator.free(atom_names);

        // Build ADT type info.
        const adt_info = buildAdtTypeInfo(&compile_result, allocator) catch {
            accumulated.shrinkRetainingCapacity(prev_len);
            continue;
        };
        defer allocator.free(adt_info);

        // 4. Execute with GC.
        var gc_state = GC.init(allocator) catch {
            accumulated.shrinkRetainingCapacity(prev_len);
            continue;
        };
        defer gc_state.deinit();
        var gc_alloc = GCAllocator{ .gc = &gc_state };
        const gc_allocator = gc_alloc.allocator();

        var vm = VM.initWithClosure(compile_result.closure, gc_allocator, &gc_state, allocator);
        vm.trackCompilerObjects(compile_result.closure);
        compile_result.vm_owns_constants = true;
        vm.setAtomNames(atom_names, allocator) catch {};
        if (adt_info.len > 0) {
            vm.setAdtTypes(adt_info);
        }

        const run_result = vm.run() catch {
            renderErrors(vm.errors.items, source, use_color, allocator);
            vm.deinit();
            // Runtime error: discard failed input.
            accumulated.shrinkRetainingCapacity(prev_len);
            continue;
        };

        vm.deinit();

        // 5. Print result (suppress nil).
        if (!run_result.isNil()) {
            printValue(run_result, allocator, use_color, atom_names, is_tty);
        }

        // Success -- keep accumulated source for next iteration.
    }
}

/// Check if the accumulated source needs continuation (incomplete input).
fn needsContinuation(source: []const u8, allocator: Allocator) bool {
    // Quick check: try lexing to see token balance.
    var lex = Lexer.init(source);
    lex.tokenize(allocator) catch return false;
    defer lex.tokens.deinit(allocator);
    defer lex.errors.deinit(allocator);

    // If lexer has errors (e.g., unterminated string), assume continuation.
    if (lex.errors.hasErrors()) return true;

    const tokens = lex.tokens.items;
    if (tokens.len == 0) return false;

    // Check delimiter balance.
    var open_parens: i32 = 0;
    var open_braces: i32 = 0;
    var open_brackets: i32 = 0;

    for (tokens) |tok| {
        switch (tok.tag) {
            .left_paren => open_parens += 1,
            .right_paren => open_parens -= 1,
            .left_brace => open_braces += 1,
            .right_brace => open_braces -= 1,
            .left_bracket => open_brackets += 1,
            .right_bracket => open_brackets -= 1,
            else => {},
        }
    }

    if (open_parens > 0 or open_braces > 0 or open_brackets > 0) return true;

    // Check last non-eof token for continuation operators.
    var last_tag: ?Tag = null;
    var i = tokens.len;
    while (i > 0) {
        i -= 1;
        if (tokens[i].tag != .eof and tokens[i].tag != .line_comment) {
            last_tag = tokens[i].tag;
            break;
        }
    }

    if (last_tag) |tag| {
        return switch (tag) {
            .pipe_greater,
            .plus,
            .minus,
            .star,
            .slash,
            .percent,
            .plus_plus,
            .equal,
            .kw_and,
            .kw_or,
            .comma,
            .arrow,
            .pipe,
            => true,
            else => false,
        };
    }

    return false;
}

/// Print a value with optional colorization.
fn printValue(val: Value, allocator: Allocator, use_color: bool, atom_names: []const []const u8, is_tty: bool) void {
    const formatted = builtins.formatValue(val, allocator, atom_names) catch return;
    defer allocator.free(formatted);

    const stdout = std.fs.File.stdout();
    const line_end = if (is_tty) "\r\n" else "\n";

    if (use_color) {
        var buf: std.ArrayListUnmanaged(u8) = .empty;
        defer buf.deinit(allocator);
        colors.formatColorValue(val, formatted, buf.writer(allocator), use_color) catch return;
        stdout.writeAll(buf.items) catch {};
        stdout.writeAll(line_end) catch {};
    } else {
        stdout.writeAll(formatted) catch {};
        stdout.writeAll(line_end) catch {};
    }
}

/// Render diagnostics to stderr.
fn renderErrors(diagnostics: []const Diagnostic, source: []const u8, use_color: bool, allocator: Allocator) void {
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(allocator);

    for (diagnostics) |diag| {
        diag.render(source, "<repl>", buf.writer(allocator), use_color) catch continue;
    }

    if (buf.items.len > 0) {
        const stderr = std.fs.File.stderr();
        stderr.writeAll(buf.items) catch {};
    }
}

/// Build an atom name array from compile result's atom_table.
fn buildAtomNames(compile_result: *CompileResult, allocator: Allocator) ![]const []const u8 {
    const atom_count = compile_result.atom_count;
    if (atom_count == 0) return allocator.alloc([]const u8, 0);

    const names = try allocator.alloc([]const u8, atom_count);
    for (names) |*n| n.* = "?";

    var iter = compile_result.atom_table.iterator();
    while (iter.next()) |entry| {
        const idx = entry.value_ptr.*;
        if (idx < atom_count) {
            names[idx] = entry.key_ptr.*;
        }
    }
    return names;
}

/// Build ADT type info array from compiler's adt_types.
fn buildAdtTypeInfo(compile_result: *CompileResult, allocator: Allocator) ![]const builtins.AdtTypeInfo {
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
