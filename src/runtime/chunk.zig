const std = @import("std");
const Allocator = std.mem.Allocator;
const value_mod = @import("value");
const Value = value_mod.Value;

/// All Phase 1 bytecode opcodes.
pub const OpCode = enum(u8) {
    // ── Constants ──────────────────────────────────────────────────────
    op_constant, // 1-byte constant index
    op_constant_long, // 2-byte (16-bit) constant index

    // ── Literals ───────────────────────────────────────────────────────
    op_nil,
    op_true,
    op_false,

    // ── Arithmetic ─────────────────────────────────────────────────────
    op_add,
    op_subtract,
    op_multiply,
    op_divide,
    op_modulo,
    op_negate,

    // ── Comparison ─────────────────────────────────────────────────────
    op_equal,
    op_not_equal,
    op_less,
    op_greater,
    op_less_equal,
    op_greater_equal,

    // ── Logical ────────────────────────────────────────────────────────
    op_not,

    // ── String/List ────────────────────────────────────────────────────
    op_concat,

    // ── Variables ──────────────────────────────────────────────────────
    op_get_local,
    op_set_local,
    op_get_global,
    op_set_global,
    op_define_global,

    // ── Control flow ───────────────────────────────────────────────────
    op_jump,
    op_jump_if_false,
    op_loop,

    // ── Stack ──────────────────────────────────────────────────────────
    op_pop,
    op_print,

    // ── Calls ──────────────────────────────────────────────────────────
    op_call, // reserved for builtins
    op_return,

    // ── Atoms ──────────────────────────────────────────────────────────
    op_atom,

    // ── Built-ins ──────────────────────────────────────────────────────
    op_get_builtin,

    // ── Iteration ──────────────────────────────────────────────────────
    op_for_iter,
};

/// Bytecode container with constant pool and line information.
pub const Chunk = struct {
    /// The bytecode stream.
    code: std.ArrayListUnmanaged(u8) = .empty,
    /// Constant pool -- literals, strings, function prototypes.
    constants: std.ArrayListUnmanaged(Value) = .empty,
    /// Line number for each byte in `code`, 1:1 mapping.
    lines: std.ArrayListUnmanaged(u32) = .empty,
    /// Source file name (for error messages).
    name: []const u8 = "<script>",

    /// Append a single byte (opcode or operand) with its source line.
    pub fn write(self: *Chunk, byte: u8, line: u32, allocator: Allocator) !void {
        try self.code.append(allocator, byte);
        try self.lines.append(allocator, line);
    }

    /// Add a value to the constant pool and return its index.
    pub fn addConstant(self: *Chunk, val: Value, allocator: Allocator) !u32 {
        const idx: u32 = @intCast(self.constants.items.len);
        try self.constants.append(allocator, val);
        return idx;
    }

    /// Return the source line number for the instruction at `offset`.
    pub fn getLine(self: *const Chunk, offset: usize) u32 {
        if (offset < self.lines.items.len) {
            return self.lines.items[offset];
        }
        return 0;
    }

    /// Free all owned memory.
    pub fn deinit(self: *Chunk, allocator: Allocator) void {
        self.code.deinit(allocator);
        self.constants.deinit(allocator);
        self.lines.deinit(allocator);
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

test "Chunk.write appends opcode bytes" {
    const allocator = std.testing.allocator;
    var chunk: Chunk = .{};
    defer chunk.deinit(allocator);

    try chunk.write(@intFromEnum(OpCode.op_constant), 1, allocator);
    try chunk.write(0, 1, allocator); // constant index
    try chunk.write(@intFromEnum(OpCode.op_return), 1, allocator);

    try std.testing.expectEqual(@as(usize, 3), chunk.code.items.len);
    try std.testing.expectEqual(@intFromEnum(OpCode.op_constant), chunk.code.items[0]);
    try std.testing.expectEqual(@intFromEnum(OpCode.op_return), chunk.code.items[2]);
}

test "Chunk.addConstant returns constant index" {
    const allocator = std.testing.allocator;
    var chunk: Chunk = .{};
    defer chunk.deinit(allocator);

    const idx0 = try chunk.addConstant(Value.fromFloat(1.5), allocator);
    const idx1 = try chunk.addConstant(Value.fromInt(42), allocator);

    try std.testing.expectEqual(@as(u32, 0), idx0);
    try std.testing.expectEqual(@as(u32, 1), idx1);
    try std.testing.expectEqual(@as(usize, 2), chunk.constants.items.len);
}

test "Chunk stores line information correlated with bytecodes" {
    const allocator = std.testing.allocator;
    var chunk: Chunk = .{};
    defer chunk.deinit(allocator);

    try chunk.write(@intFromEnum(OpCode.op_constant), 10, allocator);
    try chunk.write(0, 10, allocator);
    try chunk.write(@intFromEnum(OpCode.op_add), 11, allocator);

    try std.testing.expectEqual(@as(u32, 10), chunk.getLine(0));
    try std.testing.expectEqual(@as(u32, 10), chunk.getLine(1));
    try std.testing.expectEqual(@as(u32, 11), chunk.getLine(2));
    // Out of bounds returns 0.
    try std.testing.expectEqual(@as(u32, 0), chunk.getLine(999));
}

test "OpCode enum includes all Phase 1 opcodes" {
    // Verify key opcodes exist (compilation would fail if missing).
    const opcodes = [_]OpCode{
        .op_constant,
        .op_constant_long,
        .op_add,
        .op_subtract,
        .op_multiply,
        .op_divide,
        .op_modulo,
        .op_negate,
        .op_not,
        .op_equal,
        .op_not_equal,
        .op_less,
        .op_greater,
        .op_less_equal,
        .op_greater_equal,
        .op_true,
        .op_false,
        .op_nil,
        .op_pop,
        .op_get_local,
        .op_set_local,
        .op_get_global,
        .op_set_global,
        .op_define_global,
        .op_jump,
        .op_jump_if_false,
        .op_loop,
        .op_concat,
        .op_print,
        .op_return,
        .op_call,
        .op_atom,
        .op_get_builtin,
        .op_for_iter,
    };
    try std.testing.expect(opcodes.len >= 30);
}

test "Chunk deinit does not leak" {
    const allocator = std.testing.allocator;
    var chunk: Chunk = .{};

    try chunk.write(@intFromEnum(OpCode.op_nil), 1, allocator);
    _ = try chunk.addConstant(Value.fromFloat(3.14), allocator);
    _ = try chunk.addConstant(Value.fromInt(7), allocator);

    chunk.deinit(allocator);
    // If this test passes without leak reports, memory is properly freed.
}
