const std = @import("std");
const chunk_mod = @import("chunk");
const value_mod = @import("value");

const Chunk = chunk_mod.Chunk;
const OpCode = chunk_mod.OpCode;
const Value = value_mod.Value;

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

fn jumpInstruction(name: []const u8, sign: i32, chunk: *const Chunk, offset: usize, writer: anytype) !usize {
    const hi: u16 = chunk.code.items[offset + 1];
    const lo: u16 = chunk.code.items[offset + 2];
    const jump = (hi << 8) | lo;
    const target: i64 = @as(i64, @intCast(offset)) + 3 + @as(i64, sign) * @as(i64, jump);
    try writer.print("{s:<20} {d:>4} -> {d}\n", .{ name, offset, target });
    return offset + 3;
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
