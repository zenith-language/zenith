const std = @import("std");
const chunk_mod = @import("chunk");
const value_mod = @import("value");
const obj_mod = @import("obj");

const Chunk = chunk_mod.Chunk;
const OpCode = chunk_mod.OpCode;
const Value = value_mod.Value;
const Obj = obj_mod.Obj;
const ObjFunction = obj_mod.ObjFunction;
const ObjString = obj_mod.ObjString;

/// Disassemble an entire chunk, printing all instructions.
pub fn disassembleChunk(chunk: *const Chunk, name: []const u8, writer: anytype) !void {
    try writer.print("== {s} ==\n", .{name});

    var offset: usize = 0;
    while (offset < chunk.code.items.len) {
        offset = try disassembleInstruction(chunk, offset, writer);
    }
}

/// Disassemble a single instruction at `offset`.
/// Returns the offset of the *next* instruction.
pub fn disassembleInstruction(chunk: *const Chunk, offset: usize, writer: anytype) !usize {
    // Offset column.
    try writer.print("{d:0>4} ", .{offset});

    // Line number column (show line if different from previous).
    if (offset > 0 and chunk.getLine(offset) == chunk.getLine(offset - 1)) {
        try writer.writeAll("   | ");
    } else {
        try writer.print("{d:>4} ", .{chunk.getLine(offset)});
    }

    const instruction: OpCode = @enumFromInt(chunk.code.items[offset]);
    return switch (instruction) {
        .op_constant => try constantInstruction("OP_CONSTANT", chunk, offset, writer),
        .op_constant_long => try constantLongInstruction("OP_CONSTANT_LONG", chunk, offset, writer),
        .op_nil => try simpleInstruction("OP_NIL", offset, writer),
        .op_true => try simpleInstruction("OP_TRUE", offset, writer),
        .op_false => try simpleInstruction("OP_FALSE", offset, writer),
        .op_add => try simpleInstruction("OP_ADD", offset, writer),
        .op_subtract => try simpleInstruction("OP_SUBTRACT", offset, writer),
        .op_multiply => try simpleInstruction("OP_MULTIPLY", offset, writer),
        .op_divide => try simpleInstruction("OP_DIVIDE", offset, writer),
        .op_modulo => try simpleInstruction("OP_MODULO", offset, writer),
        .op_negate => try simpleInstruction("OP_NEGATE", offset, writer),
        .op_equal => try simpleInstruction("OP_EQUAL", offset, writer),
        .op_not_equal => try simpleInstruction("OP_NOT_EQUAL", offset, writer),
        .op_less => try simpleInstruction("OP_LESS", offset, writer),
        .op_greater => try simpleInstruction("OP_GREATER", offset, writer),
        .op_less_equal => try simpleInstruction("OP_LESS_EQUAL", offset, writer),
        .op_greater_equal => try simpleInstruction("OP_GREATER_EQUAL", offset, writer),
        .op_not => try simpleInstruction("OP_NOT", offset, writer),
        .op_concat => try simpleInstruction("OP_CONCAT", offset, writer),
        .op_get_local => try byteInstruction("OP_GET_LOCAL", chunk, offset, writer),
        .op_set_local => try byteInstruction("OP_SET_LOCAL", chunk, offset, writer),
        .op_get_global => try constantInstruction("OP_GET_GLOBAL", chunk, offset, writer),
        .op_set_global => try constantInstruction("OP_SET_GLOBAL", chunk, offset, writer),
        .op_define_global => try constantInstruction("OP_DEFINE_GLOBAL", chunk, offset, writer),
        .op_jump => try jumpInstruction("OP_JUMP", 1, chunk, offset, writer),
        .op_jump_if_false => try jumpInstruction("OP_JUMP_IF_FALSE", 1, chunk, offset, writer),
        .op_loop => try jumpInstruction("OP_LOOP", -1, chunk, offset, writer),
        .op_pop => try simpleInstruction("OP_POP", offset, writer),
        .op_print => try simpleInstruction("OP_PRINT", offset, writer),
        .op_closure => try byteInstruction("OP_CLOSURE", chunk, offset, writer),
        .op_get_upvalue => try byteInstruction("OP_GET_UPVALUE", chunk, offset, writer),
        .op_set_upvalue => try byteInstruction("OP_SET_UPVALUE", chunk, offset, writer),
        .op_close_upvalue => try simpleInstruction("OP_CLOSE_UPVALUE", offset, writer),
        .op_close_upvalue_at => try byteInstruction("OP_CLOSE_UPVALUE_AT", chunk, offset, writer),
        .op_call => try byteInstruction("OP_CALL", chunk, offset, writer),
        .op_tail_call => try byteInstruction("OP_TAIL_CALL", chunk, offset, writer),
        .op_return => try simpleInstruction("OP_RETURN", offset, writer),
        .op_atom => try constantInstruction("OP_ATOM", chunk, offset, writer),
        .op_get_builtin => try byteInstruction("OP_GET_BUILTIN", chunk, offset, writer),
        .op_for_iter => try jumpInstruction("OP_FOR_ITER", 1, chunk, offset, writer),

        // Phase 3: Collections
        .op_list => try u16Instruction("OP_LIST", chunk, offset, writer),
        .op_map => try u16Instruction("OP_MAP", chunk, offset, writer),
        .op_tuple => try u16Instruction("OP_TUPLE", chunk, offset, writer),
        .op_record => try recordInstruction("OP_RECORD", chunk, offset, writer),
        .op_record_spread => try byteInstruction("OP_RECORD_SPREAD", chunk, offset, writer),

        // Phase 3: ADTs
        .op_adt_construct => try adtConstructInstruction("OP_ADT_CONSTRUCT", chunk, offset, writer),
        .op_adt_get_field => try byteInstruction("OP_ADT_GET_FIELD", chunk, offset, writer),

        // Phase 3: Pattern matching support
        .op_get_field => try u16Instruction("OP_GET_FIELD", chunk, offset, writer),
        .op_get_index => try u16Instruction("OP_GET_INDEX", chunk, offset, writer),
        .op_check_tag => try checkTagInstruction("OP_CHECK_TAG", chunk, offset, writer),
        .op_list_len => try simpleInstruction("OP_LIST_LEN", offset, writer),
        .op_list_slice => try u16Instruction("OP_LIST_SLICE", chunk, offset, writer),
        .op_dup => try simpleInstruction("OP_DUP", offset, writer),

        // Phase 7: Concurrency
        .op_spawn => try byteInstruction("OP_SPAWN", chunk, offset, writer),
        .op_channel => try byteInstruction("OP_CHANNEL", chunk, offset, writer),
        .op_send => try simpleInstruction("OP_SEND", offset, writer),
        .op_recv => try simpleInstruction("OP_RECV", offset, writer),
        .op_close_channel => try simpleInstruction("OP_CLOSE_CHANNEL", offset, writer),
        .op_join => try simpleInstruction("OP_JOIN", offset, writer),
        .op_try_join => try simpleInstruction("OP_TRY_JOIN", offset, writer),
    };
}

// ── Instruction format helpers ─────────────────────────────────────────

fn simpleInstruction(name: []const u8, offset: usize, writer: anytype) !usize {
    try writer.print("{s}\n", .{name});
    return offset + 1;
}

fn constantInstruction(name: []const u8, chunk: *const Chunk, offset: usize, writer: anytype) !usize {
    const idx = chunk.code.items[offset + 1];
    try writer.print("{s:<20} {d:>4} '", .{ name, idx });
    if (idx < chunk.constants.items.len) {
        try chunk.constants.items[idx].format("", .{}, writer);
    }
    try writer.writeAll("'\n");
    return offset + 2;
}

fn constantLongInstruction(name: []const u8, chunk: *const Chunk, offset: usize, writer: anytype) !usize {
    const hi: u16 = chunk.code.items[offset + 1];
    const lo: u16 = chunk.code.items[offset + 2];
    const idx = (hi << 8) | lo;
    try writer.print("{s:<20} {d:>4} '", .{ name, idx });
    if (idx < chunk.constants.items.len) {
        try chunk.constants.items[idx].format("", .{}, writer);
    }
    try writer.writeAll("'\n");
    return offset + 3;
}

fn byteInstruction(name: []const u8, chunk: *const Chunk, offset: usize, writer: anytype) !usize {
    const slot = chunk.code.items[offset + 1];
    try writer.print("{s:<20} {d:>4}\n", .{ name, slot });
    return offset + 2;
}

fn u16Instruction(name: []const u8, chunk: *const Chunk, offset: usize, writer: anytype) !usize {
    const hi: u16 = chunk.code.items[offset + 1];
    const lo: u16 = chunk.code.items[offset + 2];
    const val = (hi << 8) | lo;
    try writer.print("{s:<20} {d:>4}\n", .{ name, val });
    return offset + 3;
}

fn recordInstruction(name: []const u8, chunk: *const Chunk, offset: usize, writer: anytype) !usize {
    const hi: u16 = chunk.code.items[offset + 1];
    const lo: u16 = chunk.code.items[offset + 2];
    const count = (hi << 8) | lo;
    try writer.print("{s:<20} {d:>4} fields\n", .{ name, count });
    // Skip count u16 field-name constant indices.
    return offset + 3 + @as(usize, count) * 2;
}

fn adtConstructInstruction(name: []const u8, chunk: *const Chunk, offset: usize, writer: anytype) !usize {
    const type_hi: u16 = chunk.code.items[offset + 1];
    const type_lo: u16 = chunk.code.items[offset + 2];
    const type_id = (type_hi << 8) | type_lo;
    const var_hi: u16 = chunk.code.items[offset + 3];
    const var_lo: u16 = chunk.code.items[offset + 4];
    const variant_idx = (var_hi << 8) | var_lo;
    const arity = chunk.code.items[offset + 5];
    try writer.print("{s:<20} type={d} variant={d} arity={d}\n", .{ name, type_id, variant_idx, arity });
    return offset + 6;
}

fn checkTagInstruction(name: []const u8, chunk: *const Chunk, offset: usize, writer: anytype) !usize {
    const type_hi: u16 = chunk.code.items[offset + 1];
    const type_lo: u16 = chunk.code.items[offset + 2];
    const type_id = (type_hi << 8) | type_lo;
    const var_hi: u16 = chunk.code.items[offset + 3];
    const var_lo: u16 = chunk.code.items[offset + 4];
    const variant_idx = (var_hi << 8) | var_lo;
    try writer.print("{s:<20} type={d} variant={d}\n", .{ name, type_id, variant_idx });
    return offset + 5;
}

fn jumpInstruction(name: []const u8, sign: i32, chunk: *const Chunk, offset: usize, writer: anytype) !usize {
    const hi: u16 = chunk.code.items[offset + 1];
    const lo: u16 = chunk.code.items[offset + 2];
    const jump = (hi << 8) | lo;
    const target: i64 = @as(i64, @intCast(offset)) + 3 + @as(i64, sign) * @as(i64, jump);
    try writer.print("{s:<20} {d:>4} -> {d}\n", .{ name, offset, target });
    return offset + 3;
}

// ── Recursive disassembly with verbose mode ────────────────────────────

/// Disassemble a chunk and all nested function bodies found in the constant pool.
/// If `verbose` is true, also prints the constant pool, atom table, and debug info.
pub fn disassembleRecursive(chunk: *const Chunk, name: []const u8, writer: anytype, verbose: bool) !void {
    // Disassemble the top-level chunk.
    try disassembleChunk(chunk, name, writer);

    // Verbose: show constant pool, atom table, and debug info.
    if (verbose) {
        try printConstantPool(chunk, writer);
        try printAtomTable(chunk, writer);
        try printDebugInfo(chunk, writer);
    }

    // Recurse into nested function bodies found in constants.
    for (chunk.constants.items) |val| {
        if (val.isObj()) {
            const obj_ptr = val.asObj();
            if (obj_ptr.obj_type == .function) {
                const func = ObjFunction.fromObj(obj_ptr);
                const func_name = func.name orelse "<anonymous>";
                try writer.writeAll("\n");
                try disassembleRecursive(&func.chunk, func_name, writer, verbose);
            }
        }
    }
}

/// Print the constant pool for a chunk.
fn printConstantPool(chunk: *const Chunk, writer: anytype) !void {
    if (chunk.constants.items.len == 0) return;

    try writer.writeAll("\n-- Constant Pool --\n");
    for (chunk.constants.items, 0..) |val, i| {
        try writer.print("  [{d}] ", .{i});

        if (val.isFloat()) {
            try writer.print("Float: {d}\n", .{val.asFloat()});
        } else if (val.isNil()) {
            try writer.writeAll("Nil: nil\n");
        } else if (val.isBool()) {
            try writer.print("Bool: {s}\n", .{if (val.asBool()) "true" else "false"});
        } else if (val.isInt()) {
            try writer.print("Int: {d}\n", .{val.asInt()});
        } else if (val.isAtom()) {
            try writer.print("Atom: :{d}\n", .{val.asAtom()});
        } else if (val.isObj()) {
            const obj_ptr = val.asObj();
            switch (obj_ptr.obj_type) {
                .string => {
                    const str = ObjString.fromObj(obj_ptr);
                    try writer.print("String: \"{s}\"\n", .{str.bytes});
                },
                .function => {
                    const func = ObjFunction.fromObj(obj_ptr);
                    if (func.name) |fname| {
                        try writer.print("Function: <fn {s}>\n", .{fname});
                    } else {
                        try writer.writeAll("Function: <fn>\n");
                    }
                },
                else => {
                    try writer.writeAll("Object: ");
                    try val.format("", .{}, writer);
                    try writer.writeByte('\n');
                },
            }
        } else {
            try writer.writeAll("Unknown\n");
        }
    }
}

/// Print the atom table from a chunk's atom_names.
fn printAtomTable(chunk: *const Chunk, writer: anytype) !void {
    if (chunk.atom_names.items.len == 0) return;

    try writer.writeAll("\n-- Atom Table --\n");
    for (chunk.atom_names.items, 0..) |name, i| {
        try writer.print("  [{d}] {s}\n", .{ i, name });
    }
}

/// Print debug info metadata from a chunk.
fn printDebugInfo(chunk: *const Chunk, writer: anytype) !void {
    try writer.writeAll("\n-- Debug Info --\n");
    try writer.print("  Source: {s}\n", .{chunk.name});
    try writer.print("  Code size: {d} bytes\n", .{chunk.code.items.len});
    try writer.print("  Constants: {d}\n", .{chunk.constants.items.len});
}

// ── Tests ──────────────────────────────────────────────────────────────

test "disassembleChunk prints header and instructions" {
    const allocator = std.testing.allocator;
    var chunk: Chunk = .{};
    defer chunk.deinit(allocator);

    const idx = try chunk.addConstant(Value.fromFloat(1.5), allocator);
    try chunk.write(@intFromEnum(OpCode.op_constant), 1, allocator);
    try chunk.write(@intCast(idx), 1, allocator);
    try chunk.write(@intFromEnum(OpCode.op_return), 2, allocator);

    var buf: [1024]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buf);
    const writer = stream.writer();

    try disassembleChunk(&chunk, "test_chunk", writer);

    const output = stream.getWritten();
    try std.testing.expect(std.mem.indexOf(u8, output, "== test_chunk ==") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "OP_CONSTANT") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "OP_RETURN") != null);
}

test "disassembleInstruction shows line numbers" {
    const allocator = std.testing.allocator;
    var chunk: Chunk = .{};
    defer chunk.deinit(allocator);

    try chunk.write(@intFromEnum(OpCode.op_nil), 10, allocator);
    try chunk.write(@intFromEnum(OpCode.op_pop), 10, allocator);
    try chunk.write(@intFromEnum(OpCode.op_true), 11, allocator);

    var buf: [512]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buf);
    const writer = stream.writer();

    _ = try disassembleInstruction(&chunk, 0, writer);
    _ = try disassembleInstruction(&chunk, 1, writer);
    _ = try disassembleInstruction(&chunk, 2, writer);

    const output = stream.getWritten();
    // First instruction shows line 10, second shows "|", third shows 11.
    try std.testing.expect(std.mem.indexOf(u8, output, "  10 ") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "   | ") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "  11 ") != null);
}
