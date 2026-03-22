const std = @import("std");
const zenith = @import("zenith");
const Lexer = zenith.lexer.Lexer;
const Parser = zenith.parser.Parser;
const Compiler = zenith.compiler.Compiler;
const CompileResult = zenith.compiler.CompileResult;
const VM = zenith.vm.VM;
const Chunk = zenith.chunk.Chunk;
const Diagnostic = zenith.err.Diagnostic;
const ErrorCode = zenith.err.ErrorCode;
const Value = zenith.value.Value;
const builtins = zenith.builtins;
const GC = zenith.gc.GC;
const GCAllocator = zenith.gc.GCAllocator;

const version_string = "Zenith v0.1.0";

pub fn main() !void {
    var gpa_state = std.heap.DebugAllocator(.{}).init;
    const gpa = gpa_state.allocator();
    defer {
        if (gpa_state.deinit() == .leak) {
            @panic("Memory leak detected");
        }
    }

    const args = try std.process.argsAlloc(gpa);
    defer std.process.argsFree(gpa, args);

    if (args.len < 2) {
        printUsage();
        return;
    }

    const command = args[1];

    if (std.mem.eql(u8, command, "--help") or std.mem.eql(u8, command, "-h")) {
        printUsage();
        return;
    }

    if (std.mem.eql(u8, command, "--version") or std.mem.eql(u8, command, "-V")) {
        const stdout = std.fs.File.stdout();
        stdout.writeAll(version_string ++ "\n") catch {};
        return;
    }

    if (std.mem.eql(u8, command, "run")) {
        if (args.len < 3) {
            writeStderr("error: 'run' requires a file argument\n");
            printUsage();
            std.process.exit(1);
        }
        const arg2 = args[2];

        // zenith run - (stdin mode)
        if (std.mem.eql(u8, arg2, "-")) {
            const source = readStdin(gpa);
            defer gpa.free(source);
            try runSourceFromString(source, "<stdin>", gpa);
            return;
        }

        // zenith run -e '<expr>' (inline expression mode)
        if (std.mem.eql(u8, arg2, "-e")) {
            if (args.len < 4) {
                writeStderr("error: '-e' requires an expression argument\n");
                printUsage();
                std.process.exit(1);
            }
            try runSourceFromString(args[3], "<eval>", gpa);
            return;
        }

        if (std.mem.endsWith(u8, arg2, ".znth")) {
            try runBytecode(arg2, gpa);
        } else {
            try runSource(arg2, gpa);
        }
        return;
    }

    if (std.mem.eql(u8, command, "compile")) {
        if (args.len < 3) {
            writeStderr("error: 'compile' requires a file argument\n");
            printUsage();
            std.process.exit(1);
        }
        const file_path = args[2];
        try compileToFile(file_path, gpa);
        return;
    }

    if (std.mem.eql(u8, command, "dis")) {
        if (args.len < 3) {
            writeStderr("error: 'dis' requires a file argument\n");
            printUsage();
            std.process.exit(1);
        }

        // Parse -v/--verbose flag and file path.
        var verbose = false;
        var dis_file_path: []const u8 = undefined;
        if (std.mem.eql(u8, args[2], "-v") or std.mem.eql(u8, args[2], "--verbose")) {
            if (args.len < 4) {
                writeStderr("error: 'dis -v' requires a file argument\n");
                printUsage();
                std.process.exit(1);
            }
            verbose = true;
            dis_file_path = args[3];
        } else {
            dis_file_path = args[2];
        }

        try disassembleFile(dis_file_path, verbose, gpa);
        return;
    }

    if (std.mem.eql(u8, command, "explain")) {
        if (args.len < 3) {
            writeStderr("error: 'explain' requires an error code (e.g., E001)\n");
            std.process.exit(1);
        }
        explainError(args[2]);
        return;
    }

    writeStderr("error: unknown command '");
    writeStderr(command);
    writeStderr("'\n");
    printUsage();
    std.process.exit(1);
}

// ── Pipeline: run .zen source ────────────────────────────────────────────

fn runSource(file_path: []const u8, allocator: std.mem.Allocator) !void {
    // Read source file.
    const source = std.fs.cwd().readFileAlloc(allocator, file_path, 10 * 1024 * 1024) catch |err| {
        writeStderr("error: cannot read file '");
        writeStderr(file_path);
        writeStderr("': ");
        writeStderr(@errorName(err));
        writeStderr("\n");
        std.process.exit(1);
    };
    defer allocator.free(source);

    try runSourceFromString(source, file_path, allocator);
}

/// Run Zenith source code from a string with a given display name.
/// Shared pipeline for file, stdin, and -e modes.
fn runSourceFromString(source: []const u8, file_name: []const u8, allocator: std.mem.Allocator) !void {
    const use_color = detectColor();

    // 1. Lex
    var lex = Lexer.init(source);
    try lex.tokenize(allocator);
    defer lex.tokens.deinit(allocator);
    defer lex.errors.deinit(allocator);

    if (lex.errors.hasErrors()) {
        renderDiagnostics(lex.errors.items.items, source, file_name, use_color, allocator);
        std.process.exit(1);
    }

    // 2. Parse
    var ast = Parser.parse(lex.tokens.items, source, allocator) catch {
        std.process.exit(1);
    };
    defer ast.deinit(allocator);

    if (ast.errors.hasErrors()) {
        renderDiagnostics(ast.errors.items.items, source, file_name, use_color, allocator);
        std.process.exit(1);
    }

    // 3. Compile
    var compile_result = try Compiler.compile(&ast, allocator);
    defer compile_result.deinit(allocator);

    if (compile_result.hasErrors()) {
        renderDiagnostics(compile_result.errors.items, source, file_name, use_color, allocator);
        std.process.exit(1);
    }

    // Build atom name list from compiler's atom_table.
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

    _ = vm.run() catch {
        renderDiagnostics(vm.errors.items, source, file_name, use_color, allocator);
        vm.deinit();
        std.process.exit(1);
    };

    vm.deinit();
}

// ── Pipeline: disassemble .zen or .znth ──────────────────────────────────

fn disassembleFile(file_path: []const u8, verbose: bool, allocator: std.mem.Allocator) !void {
    const debug = zenith.debug;

    if (std.mem.endsWith(u8, file_path, ".znth")) {
        // Deserialize bytecode and disassemble.
        const file_data = std.fs.cwd().readFileAlloc(allocator, file_path, 10 * 1024 * 1024) catch |err| {
            writeStderr("error: cannot open file '");
            writeStderr(file_path);
            writeStderr("': ");
            writeStderr(@errorName(err));
            writeStderr("\n");
            std.process.exit(1);
        };
        defer allocator.free(file_data);

        var stream = std.io.fixedBufferStream(file_data);
        var chunk = Chunk.deserialize(stream.reader(), allocator) catch |err| {
            writeStderr("error: cannot read bytecode file '");
            writeStderr(file_path);
            writeStderr("': ");
            writeStderr(@errorName(err));
            writeStderr("\n");
            std.process.exit(1);
        };
        defer chunk.deinit(allocator);

        const display_name = if (!std.mem.eql(u8, chunk.name, "<script>")) chunk.name else "<bytecode>";

        var out_buf = std.ArrayListUnmanaged(u8){};
        defer out_buf.deinit(allocator);
        debug.disassembleRecursive(&chunk, display_name, out_buf.writer(allocator), verbose) catch |err| {
            writeStderr("error: disassembly failed: ");
            writeStderr(@errorName(err));
            writeStderr("\n");
            std.process.exit(1);
        };
        const stdout = std.fs.File.stdout();
        stdout.writeAll(out_buf.items) catch {};
    } else {
        // Compile .zen source and disassemble the result.
        const source = std.fs.cwd().readFileAlloc(allocator, file_path, 10 * 1024 * 1024) catch |err| {
            writeStderr("error: cannot read file '");
            writeStderr(file_path);
            writeStderr("': ");
            writeStderr(@errorName(err));
            writeStderr("\n");
            std.process.exit(1);
        };
        defer allocator.free(source);

        const use_color = detectColor();

        // 1. Lex
        var lex = Lexer.init(source);
        try lex.tokenize(allocator);
        defer lex.tokens.deinit(allocator);
        defer lex.errors.deinit(allocator);

        if (lex.errors.hasErrors()) {
            renderDiagnostics(lex.errors.items.items, source, file_path, use_color, allocator);
            std.process.exit(1);
        }

        // 2. Parse
        var ast = Parser.parse(lex.tokens.items, source, allocator) catch {
            std.process.exit(1);
        };
        defer ast.deinit(allocator);

        if (ast.errors.hasErrors()) {
            renderDiagnostics(ast.errors.items.items, source, file_path, use_color, allocator);
            std.process.exit(1);
        }

        // 3. Compile
        var compile_result = try Compiler.compile(&ast, allocator);
        defer compile_result.deinit(allocator);

        if (compile_result.hasErrors()) {
            renderDiagnostics(compile_result.errors.items, source, file_path, use_color, allocator);
            std.process.exit(1);
        }

        // Build atom names and store in chunk for verbose display.
        const atom_names = try buildAtomNames(&compile_result, allocator);
        defer allocator.free(atom_names);

        const top_chunk = &compile_result.closure.function.chunk;
        for (atom_names) |name| {
            try top_chunk.atom_names.append(allocator, name);
        }

        var dis_buf = std.ArrayListUnmanaged(u8){};
        defer dis_buf.deinit(allocator);
        debug.disassembleRecursive(top_chunk, "<script>", dis_buf.writer(allocator), verbose) catch |err| {
            writeStderr("error: disassembly failed: ");
            writeStderr(@errorName(err));
            writeStderr("\n");
            std.process.exit(1);
        };
        const stdout = std.fs.File.stdout();
        stdout.writeAll(dis_buf.items) catch {};
    }
}

/// Build ADT type info array from compiler's adt_types for pretty-printing.
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

/// Build an atom name array ordered by atom ID from the compiler's atom_table.
fn buildAtomNames(compile_result: *CompileResult, allocator: std.mem.Allocator) ![]const []const u8 {
    const atom_count = compile_result.atom_count;
    if (atom_count == 0) return allocator.alloc([]const u8, 0);

    const names = try allocator.alloc([]const u8, atom_count);
    // Fill with placeholders.
    for (names) |*n| {
        n.* = "?";
    }

    // Map from atom_table.
    var iter = compile_result.atom_table.iterator();
    while (iter.next()) |entry| {
        const idx = entry.value_ptr.*;
        if (idx < atom_count) {
            names[idx] = entry.key_ptr.*;
        }
    }

    return names;
}

// ── Pipeline: run .znth bytecode ─────────────────────────────────────────

fn runBytecode(file_path: []const u8, allocator: std.mem.Allocator) !void {
    const use_color = detectColor();

    // Read the entire file into memory.
    const file_data = std.fs.cwd().readFileAlloc(allocator, file_path, 10 * 1024 * 1024) catch |err| {
        writeStderr("error: cannot open file '");
        writeStderr(file_path);
        writeStderr("': ");
        writeStderr(@errorName(err));
        writeStderr("\n");
        std.process.exit(1);
    };
    defer allocator.free(file_data);

    // Deserialize from in-memory buffer.
    var stream = std.io.fixedBufferStream(file_data);
    var chunk = Chunk.deserialize(stream.reader(), allocator) catch |err| {
        writeStderr("error: cannot read bytecode file '");
        writeStderr(file_path);
        writeStderr("': ");
        writeStderr(@errorName(err));
        writeStderr("\n");
        std.process.exit(1);
    };
    defer chunk.deinit(allocator);

    var vm = VM.init(&chunk, allocator);

    // Load atom names from the chunk if available.
    if (chunk.atom_names.items.len > 0) {
        try vm.setAtomNames(chunk.atom_names.items, allocator);
    }

    _ = vm.run() catch {
        const empty_source: []const u8 = "";
        renderDiagnostics(vm.errors.items, empty_source, file_path, use_color, allocator);
        vm.deinit();
        std.process.exit(1);
    };

    vm.deinit();
}

// ── Pipeline: compile .zen to .znth ──────────────────────────────────────

fn compileToFile(file_path: []const u8, allocator: std.mem.Allocator) !void {
    const use_color = detectColor();

    // Read source file.
    const source = std.fs.cwd().readFileAlloc(allocator, file_path, 10 * 1024 * 1024) catch |err| {
        writeStderr("error: cannot read file '");
        writeStderr(file_path);
        writeStderr("': ");
        writeStderr(@errorName(err));
        writeStderr("\n");
        std.process.exit(1);
    };
    defer allocator.free(source);

    // 1. Lex
    var lex = Lexer.init(source);
    try lex.tokenize(allocator);
    defer lex.tokens.deinit(allocator);
    defer lex.errors.deinit(allocator);

    if (lex.errors.hasErrors()) {
        renderDiagnostics(lex.errors.items.items, source, file_path, use_color, allocator);
        std.process.exit(1);
    }

    // 2. Parse
    var ast = Parser.parse(lex.tokens.items, source, allocator) catch {
        std.process.exit(1);
    };
    defer ast.deinit(allocator);

    if (ast.errors.hasErrors()) {
        renderDiagnostics(ast.errors.items.items, source, file_path, use_color, allocator);
        std.process.exit(1);
    }

    // 3. Compile
    var compile_result = try Compiler.compile(&ast, allocator);
    defer compile_result.deinit(allocator);

    if (compile_result.hasErrors()) {
        renderDiagnostics(compile_result.errors.items, source, file_path, use_color, allocator);
        std.process.exit(1);
    }

    // Build atom names and store them in the chunk for serialization.
    const atom_names = try buildAtomNames(&compile_result, allocator);
    defer allocator.free(atom_names);

    // Store atom names in chunk for serialization.
    const top_chunk = &compile_result.closure.function.chunk;
    for (atom_names) |name| {
        try top_chunk.atom_names.append(allocator, name);
    }

    // Set the source file name on the chunk.
    top_chunk.name = file_path;

    // 4. Serialize to in-memory buffer, then write to file.
    var out_buf = std.ArrayListUnmanaged(u8){};
    defer out_buf.deinit(allocator);
    top_chunk.serialize(out_buf.writer(allocator)) catch |err| {
        writeStderr("error: cannot serialize bytecode: ");
        writeStderr(@errorName(err));
        writeStderr("\n");
        std.process.exit(1);
    };

    const output_path = try makeOutputPath(file_path, allocator);
    defer allocator.free(output_path);

    const out_file = std.fs.cwd().createFile(output_path, .{}) catch |err| {
        writeStderr("error: cannot create output file '");
        writeStderr(output_path);
        writeStderr("': ");
        writeStderr(@errorName(err));
        writeStderr("\n");
        std.process.exit(1);
    };
    defer out_file.close();

    out_file.writeAll(out_buf.items) catch |err| {
        writeStderr("error: cannot write bytecode: ");
        writeStderr(@errorName(err));
        writeStderr("\n");
        std.process.exit(1);
    };

    const stdout = std.fs.File.stdout();
    stdout.writeAll("Compiled to ") catch {};
    stdout.writeAll(output_path) catch {};
    stdout.writeAll("\n") catch {};
}

/// Given "path/to/file.zen", return "path/to/file.znth".
fn makeOutputPath(input_path: []const u8, allocator: std.mem.Allocator) ![]const u8 {
    // Strip .zen extension if present, then add .znth.
    const base = if (std.mem.endsWith(u8, input_path, ".zen"))
        input_path[0 .. input_path.len - 4]
    else
        input_path;

    const result = try allocator.alloc(u8, base.len + 5);
    @memcpy(result[0..base.len], base);
    @memcpy(result[base.len..], ".znth");
    return result;
}

// ── Explain error ────────────────────────────────────────────────────────

fn explainError(code_str: []const u8) void {
    const stdout = std.fs.File.stdout();

    // Parse error code like "E001"
    if (code_str.len == 4 and code_str[0] == 'E') {
        const num = std.fmt.parseInt(u16, code_str[1..], 10) catch {
            writeStderr("error: invalid error code format. Expected EXXX (e.g., E001)\n");
            std.process.exit(1);
        };

        // Look up the error code.
        const error_code: ErrorCode = @enumFromInt(num);
        const name = error_code.name();

        stdout.writeAll(code_str) catch {};
        stdout.writeAll(": ") catch {};
        stdout.writeAll(name) catch {};
        stdout.writeAll("\n\n") catch {};

        // Print detailed explanation based on error code.
        const explanation = getExplanation(error_code);
        stdout.writeAll(explanation) catch {};
        stdout.writeAll("\n") catch {};
        return;
    }

    writeStderr("error: invalid error code format. Expected EXXX (e.g., E001)\n");
    std.process.exit(1);
}

fn getExplanation(code: ErrorCode) []const u8 {
    return switch (code) {
        .E001 => "Type mismatch occurs when an operation receives values of incompatible types.\n\nExample:\n  let x = 1 + \"hello\"  -- error: cannot add Int and String\n\nFix: Ensure operand types match, or use str() / int() to convert.",
        .E002 => "Undefined variable occurs when referencing a name that hasn't been declared.\n\nExample:\n  print(x)  -- error: 'x' is not defined\n\nFix: Declare the variable with 'let' before using it.",
        .E003 => "Integer overflow occurs when an arithmetic operation produces a result\nthat exceeds the i32 range (-2147483648 to 2147483647).\n\nExample:\n  let x = 2147483647\n  let y = x + 1  -- error: integer overflow\n\nFix: Use smaller values or restructure the computation.",
        .E004 => "Division by zero occurs when the right operand of / or % is zero.\n\nExample:\n  print(10 / 0)  -- error: division by zero\n\nFix: Check the divisor before dividing.",
        .E005 => "Unexpected token occurs when the parser encounters a token that doesn't\nfit the expected grammar.\n\nExample:\n  let = 42  -- error: expected identifier after 'let'\n\nFix: Check syntax and ensure statements are properly formed.",
        .E006 => "Unterminated string occurs when a string literal is missing its closing quote.\n\nExample:\n  let s = \"hello  -- error: unterminated string\n\nFix: Add the closing double-quote.",
        .E007 => "Invalid number literal occurs when a numeric literal cannot be parsed.\n\nExample:\n  let x = 12.34.56  -- error: invalid number\n\nFix: Ensure number literals have at most one decimal point.",
        .E008 => "Too many constants occurs when a single chunk exceeds the constant pool limit.\n\nFix: Simplify the expression or split into smaller functions.",
        .E009 => "Too many local variables in a single scope (maximum 256).\n\nFix: Reduce the number of variables or use nested scopes.",
        .E010 => "Break outside loop occurs when 'break' is used outside a while or for loop.\n\nFix: Only use 'break' inside loop bodies.",
        .E011 => "Undefined atom -- internal error for atom resolution failure.\n\nThis usually indicates a compiler bug.",
        .E012 => "Arity mismatch occurs when a function is called with the wrong number of arguments.\n\nExample:\n  print()  -- error: expected 1 argument, got 0\n\nFix: Check the function signature and provide the correct number of arguments.",
    };
}

// ── Diagnostic rendering ─────────────────────────────────────────────────

fn renderDiagnostics(diagnostics: []const Diagnostic, source: []const u8, file_name: []const u8, use_color: bool, allocator: std.mem.Allocator) void {
    var buf = std.ArrayListUnmanaged(u8){};
    defer buf.deinit(allocator);

    for (diagnostics) |diag| {
        diag.render(source, file_name, buf.writer(allocator), use_color) catch continue;
    }

    if (buf.items.len > 0) {
        const stderr = std.fs.File.stderr();
        stderr.writeAll(buf.items) catch {};
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────

fn detectColor() bool {
    return std.posix.isatty(std.posix.STDERR_FILENO);
}

fn printUsage() void {
    const stdout = std.fs.File.stdout();
    stdout.writeAll(
        \\Usage: zenith <command> [arguments]
        \\
        \\Commands:
        \\  run <file>          Run a .zen source file or .znth bytecode file
        \\  run -               Read and run Zenith source from stdin
        \\  run -e <expr>       Evaluate an inline expression
        \\  compile <file>      Compile a .zen file to .znth bytecode
        \\  dis <file>          Disassemble a .zen or .znth file
        \\  dis -v <file>       Disassemble with verbose output (constants, atoms, debug info)
        \\  repl                Launch interactive REPL
        \\  explain <code>      Explain an error code (e.g., zenith explain E001)
        \\
        \\Options:
        \\  --help, -h          Show this help message
        \\  --version, -V       Show version information
        \\
    ) catch {};
}

/// Read all of stdin into a heap-allocated buffer.
fn readStdin(allocator: std.mem.Allocator) []const u8 {
    const stdin_file = std.fs.File.stdin();
    var read_buf: [256 * 1024]u8 = undefined;
    var rdr = stdin_file.reader(&read_buf);
    const source = rdr.interface.allocRemaining(allocator, .limited(10 * 1024 * 1024)) catch {
        writeStderr("error: cannot read stdin\n");
        std.process.exit(1);
    };
    return source;
}

fn writeStderr(msg: []const u8) void {
    const stderr = std.fs.File.stderr();
    stderr.writeAll(msg) catch {};
}
