const std = @import("std");
const Allocator = std.mem.Allocator;
const value_mod = @import("value");
const Value = value_mod.Value;
const obj_mod = @import("obj");
const ObjString = obj_mod.ObjString;

/// All Phase 1 bytecode opcodes.
pub const OpCode = enum(u8) {
    // -- Constants ----------------------------------------------------------
    op_constant, // 1-byte constant index
    op_constant_long, // 2-byte (16-bit) constant index

    // -- Literals -----------------------------------------------------------
    op_nil,
    op_true,
    op_false,

    // -- Arithmetic ---------------------------------------------------------
    op_add,
    op_subtract,
    op_multiply,
    op_divide,
    op_modulo,
    op_negate,

    // -- Comparison ---------------------------------------------------------
    op_equal,
    op_not_equal,
    op_less,
    op_greater,
    op_less_equal,
    op_greater_equal,

    // -- Logical ------------------------------------------------------------
    op_not,

    // -- String/List --------------------------------------------------------
    op_concat,

    // -- Variables ----------------------------------------------------------
    op_get_local,
    op_set_local,
    op_get_global,
    op_set_global,
    op_define_global,

    // -- Control flow -------------------------------------------------------
    op_jump,
    op_jump_if_false,
    op_loop,

    // -- Stack --------------------------------------------------------------
    op_pop,
    op_print,

    // -- Closures -----------------------------------------------------------
    op_closure, // [const_idx as u32] followed by upvalue_count pairs of (is_local: u8, index: u8)
    op_get_upvalue, // [slot: u8]
    op_set_upvalue, // [slot: u8]
    op_close_upvalue, // no operand, closes upvalue at stack_top - 1
    op_close_upvalue_at, // [slot: u8] closes upvalues at frame.base_slot + slot without popping

    // -- Calls --------------------------------------------------------------
    op_call, // reserved for builtins
    op_tail_call, // [arg_count: u8]
    op_return,

    // -- Atoms --------------------------------------------------------------
    op_atom,

    // -- Built-ins ----------------------------------------------------------
    op_get_builtin,

    // -- Iteration ----------------------------------------------------------
    op_for_iter,

    // -- Collections (Phase 3) -----------------------------------------------
    op_list, // [count: u16] pops count values, creates ObjList
    op_map, // [count: u16] pops 2*count values (key-value pairs), creates ObjMap
    op_tuple, // [count: u16] pops count values, creates ObjTuple
    op_record, // [count: u16] followed by count u16 field-name constant indices, pops count values

    // -- Record spread -------------------------------------------------------
    op_record_spread, // [override_count: u8] pops base record + override values, creates new record

    // -- ADTs (Phase 3) ------------------------------------------------------
    op_adt_construct, // [type_id: u16, variant_idx: u16, arity: u8] pops arity values, creates ObjAdt
    op_adt_get_field, // [field_idx: u8] pops ADT, pushes payload[field_idx]

    // -- Pattern matching support (Phase 3) ----------------------------------
    op_get_field, // [const_idx: u16] pops obj (record/map), pushes field/key value or nil
    op_get_index, // [index: u16] pops list/tuple, pushes element at index
    op_check_tag, // [type_id: u16, variant_idx: u16] peeks top, pushes bool if ADT matches
    op_list_len, // pops list, pushes its length as int
    op_list_slice, // [start: u16] pops list, pushes new list from start to end (for ..rest)
    op_dup, // duplicates top of stack (needed for pattern matching to keep scrutinee)

    // -- Concurrency (Phase 7) ---------------------------------------------------
    op_spawn, // [arg_count: u8] pops closure (+ optional name string), pushes ObjFiber handle
    op_channel, // [has_capacity: u8] if 1, pops capacity int; if 0, unbuffered. Pushes ObjChannel
    op_send, // pops channel and value, sends value to channel
    op_recv, // pops channel, pushes Option (Some(val) or None)
    op_close_channel, // pops channel, closes it
    op_join, // pops fiber handle, pushes Result (blocks if fiber not done)
    op_try_join, // pops fiber handle, pushes Option(Result) (non-blocking)
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
    /// Atom names list (for serialization and runtime display).
    atom_names: std.ArrayListUnmanaged([]const u8) = .empty,
    /// Whether atom_names strings are owned (allocated during deserialization).
    owns_atom_names: bool = false,
    /// Whether name is owned (allocated during deserialization).
    owns_name: bool = false,
    /// Owned strings from deserialized constants.
    owned_strings: std.ArrayListUnmanaged([]const u8) = .empty,

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
        // Free owned strings from deserialization.
        for (self.owned_strings.items) |s| {
            allocator.free(s);
        }
        self.owned_strings.deinit(allocator);
        if (self.owns_atom_names) {
            for (self.atom_names.items) |n| {
                allocator.free(n);
            }
        }
        self.atom_names.deinit(allocator);
        if (self.owns_name) {
            allocator.free(self.name);
        }
    }

    // -- .znth Bytecode Serialization ----------------------------------------
    //
    // File format (all multi-byte integers are little-endian):
    //
    //   Header (16 bytes):
    //     Magic: "ZNTH" (4 bytes)
    //     Major version: u16 LE = 0
    //     Minor version: u16 LE = 1
    //     Flags: u32 LE = 0 (reserved)
    //     Padding: 4 bytes (zeros)
    //
    //   String Table Section:
    //     Count: u32 (number of strings)
    //     For each string: length (u32) + UTF-8 bytes
    //
    //   Atom Name Section:
    //     Count: u32 (number of atom names)
    //     For each name: length (u32) + UTF-8 bytes
    //
    //   Constant Pool Section:
    //     Count: u32
    //     For each constant:
    //       Type tag: u8 (0=nil, 1=bool, 2=int, 3=float, 4=string_ref, 5=atom)
    //       Data varies by type
    //
    //   Code Section:
    //     Length: u32 (number of bytes)
    //     Bytes: raw bytecodes
    //
    //   Debug Section:
    //     Source file name: length (u32) + UTF-8 bytes
    //     Line table count: u32
    //     Lines: u32[] LE (one per code byte)

    const MAGIC = [4]u8{ 'Z', 'N', 'T', 'H' };
    const MAJOR_VERSION: u16 = 0;
    const MINOR_VERSION: u16 = 1;

    // Constant type tags
    const TAG_NIL: u8 = 0;
    const TAG_BOOL: u8 = 1;
    const TAG_INT: u8 = 2;
    const TAG_FLOAT: u8 = 3;
    const TAG_STRING_REF: u8 = 4;
    const TAG_ATOM: u8 = 5;

    /// Serialize this chunk to a writer in .znth format.
    pub fn serialize(self: *const Chunk, writer: anytype) !void {
        // -- Header --
        try writer.writeAll(&MAGIC);
        try writeU16LE(writer, MAJOR_VERSION);
        try writeU16LE(writer, MINOR_VERSION);
        try writeU32LE(writer, 0); // flags
        try writeU32LE(writer, 0); // padding

        // -- Collect strings from constant pool --
        // Build string table: collect all string constants.
        var string_count: u32 = 0;
        for (self.constants.items) |val| {
            if (val.isString()) {
                string_count += 1;
            }
        }

        // -- String Table Section --
        try writeU32LE(writer, string_count);
        // Write each string and assign indices.
        for (self.constants.items) |val| {
            if (val.isString()) {
                const str = ObjString.fromObj(val.asObj());
                try writeU32LE(writer, @intCast(str.bytes.len));
                try writer.writeAll(str.bytes);
            }
        }

        // -- Atom Name Section --
        try writeU32LE(writer, @intCast(self.atom_names.items.len));
        for (self.atom_names.items) |name| {
            try writeU32LE(writer, @intCast(name.len));
            try writer.writeAll(name);
        }

        // -- Constant Pool Section --
        try writeU32LE(writer, @intCast(self.constants.items.len));
        var str_idx: u32 = 0;
        for (self.constants.items) |val| {
            if (val.isNil()) {
                try writer.writeByte(TAG_NIL);
            } else if (val.isBool()) {
                try writer.writeByte(TAG_BOOL);
                try writer.writeByte(if (val.asBool()) 1 else 0);
            } else if (val.isInt()) {
                try writer.writeByte(TAG_INT);
                try writeI64LE(writer, @as(i64, val.asInt()));
            } else if (val.isFloat()) {
                try writer.writeByte(TAG_FLOAT);
                try writeF64LE(writer, val.asFloat());
            } else if (val.isString()) {
                try writer.writeByte(TAG_STRING_REF);
                try writeU32LE(writer, str_idx);
                str_idx += 1;
            } else if (val.isAtom()) {
                try writer.writeByte(TAG_ATOM);
                try writeU32LE(writer, val.asAtom());
            } else {
                // Unknown value type -- write as nil.
                try writer.writeByte(TAG_NIL);
            }
        }

        // -- Code Section --
        try writeU32LE(writer, @intCast(self.code.items.len));
        try writer.writeAll(self.code.items);

        // -- Debug Section --
        try writeU32LE(writer, @intCast(self.name.len));
        try writer.writeAll(self.name);
        try writeU32LE(writer, @intCast(self.lines.items.len));
        for (self.lines.items) |line| {
            try writeU32LE(writer, line);
        }
    }

    /// Deserialize a chunk from a reader in .znth format.
    pub fn deserialize(reader: anytype, allocator: Allocator) !Chunk {
        var chunk = Chunk{};
        errdefer chunk.deinit(allocator);

        // -- Header --
        var magic: [4]u8 = undefined;
        const magic_read = try reader.readAll(&magic);
        if (magic_read != 4 or !std.mem.eql(u8, &magic, &MAGIC)) {
            return error.InvalidFormat;
        }

        const major = try readU16LE(reader);
        if (major != MAJOR_VERSION) {
            return error.IncompatibleVersion;
        }
        _ = try readU16LE(reader); // minor version (forward-compatible)
        _ = try readU32LE(reader); // flags
        _ = try readU32LE(reader); // padding

        // -- String Table Section --
        const string_count = try readU32LE(reader);
        var string_table = try allocator.alloc([]const u8, string_count);
        defer allocator.free(string_table);
        for (0..string_count) |i| {
            const len = try readU32LE(reader);
            const bytes = try allocator.alloc(u8, len);
            const bytes_read = try reader.readAll(bytes);
            if (bytes_read != len) {
                allocator.free(bytes);
                return error.UnexpectedEof;
            }
            string_table[i] = bytes;
            try chunk.owned_strings.append(allocator, bytes);
        }

        // -- Atom Name Section --
        const atom_name_count = try readU32LE(reader);
        chunk.owns_atom_names = true;
        for (0..atom_name_count) |_| {
            const len = try readU32LE(reader);
            const bytes = try allocator.alloc(u8, len);
            const bytes_read = try reader.readAll(bytes);
            if (bytes_read != len) {
                allocator.free(bytes);
                return error.UnexpectedEof;
            }
            try chunk.atom_names.append(allocator, bytes);
        }

        // -- Constant Pool Section --
        const const_count = try readU32LE(reader);
        for (0..const_count) |_| {
            var tag_buf: [1]u8 = undefined;
            const tag_read = try reader.readAll(&tag_buf);
            if (tag_read != 1) return error.UnexpectedEof;
            const tag = tag_buf[0];

            const val: Value = switch (tag) {
                TAG_NIL => Value.nil,
                TAG_BOOL => blk: {
                    var b_buf: [1]u8 = undefined;
                    const b_read = try reader.readAll(&b_buf);
                    if (b_read != 1) return error.UnexpectedEof;
                    break :blk Value.fromBool(b_buf[0] != 0);
                },
                TAG_INT => blk: {
                    const i = try readI64LE(reader);
                    // For deserialization, values are always within i32 range in Phase 1.
                    if (i >= std.math.minInt(i32) and i <= std.math.maxInt(i32)) {
                        break :blk Value.fromInt(@intCast(i));
                    }
                    // Large integers: try to create an ObjInt.
                    const big = try Value.fromI64(i, allocator);
                    break :blk big;
                },
                TAG_FLOAT => blk: {
                    const f = try readF64LE(reader);
                    break :blk Value.fromFloat(f);
                },
                TAG_STRING_REF => blk: {
                    const idx = try readU32LE(reader);
                    if (idx >= string_count) return error.InvalidFormat;
                    // Create an ObjString from the string table data.
                    const str_obj = try ObjString.create(allocator, string_table[idx], null);
                    break :blk Value.fromObj(&str_obj.obj);
                },
                TAG_ATOM => blk: {
                    const id = try readU32LE(reader);
                    break :blk Value.fromAtom(id);
                },
                else => return error.InvalidFormat,
            };

            try chunk.constants.append(allocator, val);
        }

        // -- Code Section --
        const code_len = try readU32LE(reader);
        const code_bytes = try allocator.alloc(u8, code_len);
        errdefer allocator.free(code_bytes);
        const code_read = try reader.readAll(code_bytes);
        if (code_read != code_len) return error.UnexpectedEof;
        // Transfer to chunk's code array.
        chunk.code = .{};
        try chunk.code.appendSlice(allocator, code_bytes);
        allocator.free(code_bytes);

        // -- Debug Section --
        const name_len = try readU32LE(reader);
        const name_bytes = try allocator.alloc(u8, name_len);
        const name_read = try reader.readAll(name_bytes);
        if (name_read != name_len) {
            allocator.free(name_bytes);
            return error.UnexpectedEof;
        }
        chunk.name = name_bytes;
        chunk.owns_name = true;

        const line_count = try readU32LE(reader);
        for (0..line_count) |_| {
            const line = try readU32LE(reader);
            try chunk.lines.append(allocator, line);
        }

        return chunk;
    }

    // -- Binary I/O helpers (little-endian) --------------------------------

    fn writeU16LE(writer: anytype, val: u16) !void {
        var buf: [2]u8 = undefined;
        std.mem.writeInt(u16, &buf, val, .little);
        try writer.writeAll(&buf);
    }

    fn writeU32LE(writer: anytype, val: u32) !void {
        var buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &buf, val, .little);
        try writer.writeAll(&buf);
    }

    fn writeI64LE(writer: anytype, val: i64) !void {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(i64, &buf, val, .little);
        try writer.writeAll(&buf);
    }

    fn writeF64LE(writer: anytype, val: f64) !void {
        var buf: [8]u8 = undefined;
        const bits: u64 = @bitCast(val);
        std.mem.writeInt(u64, &buf, bits, .little);
        try writer.writeAll(&buf);
    }

    fn readU16LE(reader: anytype) !u16 {
        var buf: [2]u8 = undefined;
        const n = try reader.readAll(&buf);
        if (n != 2) return error.UnexpectedEof;
        return std.mem.readInt(u16, &buf, .little);
    }

    fn readU32LE(reader: anytype) !u32 {
        var buf: [4]u8 = undefined;
        const n = try reader.readAll(&buf);
        if (n != 4) return error.UnexpectedEof;
        return std.mem.readInt(u32, &buf, .little);
    }

    fn readI64LE(reader: anytype) !i64 {
        var buf: [8]u8 = undefined;
        const n = try reader.readAll(&buf);
        if (n != 8) return error.UnexpectedEof;
        return std.mem.readInt(i64, &buf, .little);
    }

    fn readF64LE(reader: anytype) !f64 {
        var buf: [8]u8 = undefined;
        const n = try reader.readAll(&buf);
        if (n != 8) return error.UnexpectedEof;
        const bits = std.mem.readInt(u64, &buf, .little);
        return @bitCast(bits);
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

test "OpCode enum includes Phase 2 closure opcodes" {
    const closure_opcodes = [_]OpCode{
        .op_closure,
        .op_get_upvalue,
        .op_set_upvalue,
        .op_close_upvalue,
        .op_tail_call,
    };
    // Verify all 5 new opcodes exist and are distinct.
    for (closure_opcodes, 0..) |op, i| {
        for (closure_opcodes[i + 1 ..]) |other| {
            try std.testing.expect(@intFromEnum(op) != @intFromEnum(other));
        }
    }
    try std.testing.expectEqual(@as(usize, 5), closure_opcodes.len);
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

test "Chunk serialize and deserialize round-trip" {
    const allocator = std.testing.allocator;

    // Build a chunk with various constant types.
    var original: Chunk = .{};

    // Add constants.
    _ = try original.addConstant(Value.fromInt(42), allocator);
    _ = try original.addConstant(Value.fromFloat(3.14), allocator);
    _ = try original.addConstant(Value.nil, allocator);
    _ = try original.addConstant(Value.true_val, allocator);
    _ = try original.addConstant(Value.fromAtom(0), allocator);

    // Add a string constant.
    const str_obj = try ObjString.create(allocator, "hello", null);
    _ = try original.addConstant(Value.fromObj(&str_obj.obj), allocator);

    // Add bytecodes.
    try original.write(@intFromEnum(OpCode.op_constant), 1, allocator);
    try original.write(0, 1, allocator);
    try original.write(@intFromEnum(OpCode.op_print), 1, allocator);
    try original.write(@intFromEnum(OpCode.op_return), 2, allocator);

    // Add atom names.
    try original.atom_names.append(allocator, "ok");

    original.name = "test.zen";

    // Remember sizes before freeing.
    const orig_code_len = original.code.items.len;
    const orig_line_count = original.lines.items.len;

    // Serialize to a buffer.
    var buf = std.ArrayListUnmanaged(u8){};
    defer buf.deinit(allocator);
    try original.serialize(buf.writer(allocator));

    // Clean up original (including the ObjString).
    str_obj.obj.destroy(allocator);
    original.code.deinit(allocator);
    original.constants.deinit(allocator);
    original.lines.deinit(allocator);
    original.atom_names.deinit(allocator);

    // Deserialize from the buffer.
    var stream = std.io.fixedBufferStream(buf.items);
    var deserialized = try Chunk.deserialize(stream.reader(), allocator);

    // Clean up deserialized ObjString constants before deinit.
    defer {
        for (deserialized.constants.items) |val| {
            if (val.isObj()) {
                val.asObj().destroy(allocator);
            }
        }
        deserialized.deinit(allocator);
    }

    // Verify code matches.
    try std.testing.expectEqual(orig_code_len, deserialized.code.items.len);

    // Verify constant count matches.
    try std.testing.expectEqual(@as(usize, 6), deserialized.constants.items.len);

    // Verify specific constants.
    try std.testing.expect(deserialized.constants.items[0].isInt());
    try std.testing.expectEqual(@as(i32, 42), deserialized.constants.items[0].asInt());

    try std.testing.expect(deserialized.constants.items[1].isFloat());

    try std.testing.expect(deserialized.constants.items[2].isNil());
    try std.testing.expect(deserialized.constants.items[3].isBool());
    try std.testing.expect(deserialized.constants.items[3].asBool());

    try std.testing.expect(deserialized.constants.items[4].isAtom());
    try std.testing.expectEqual(@as(u32, 0), deserialized.constants.items[4].asAtom());

    // Verify string constant.
    try std.testing.expect(deserialized.constants.items[5].isString());

    // Verify atom names.
    try std.testing.expectEqual(@as(usize, 1), deserialized.atom_names.items.len);
    try std.testing.expectEqualStrings("ok", deserialized.atom_names.items[0]);

    // Verify source name.
    try std.testing.expectEqualStrings("test.zen", deserialized.name);

    // Verify line info.
    try std.testing.expectEqual(orig_line_count, deserialized.lines.items.len);
}

test "Chunk deserialize rejects invalid magic" {
    const allocator = std.testing.allocator;
    const bad_data = [_]u8{ 'B', 'A', 'D', '!' } ++ [_]u8{0} ** 12;
    var stream = std.io.fixedBufferStream(&bad_data);
    const result = Chunk.deserialize(stream.reader(), allocator);
    try std.testing.expectError(error.InvalidFormat, result);
}

test "Chunk deserialize rejects incompatible major version" {
    const allocator = std.testing.allocator;
    var buf: [16]u8 = undefined;
    @memcpy(buf[0..4], "ZNTH");
    std.mem.writeInt(u16, buf[4..6], 1, .little); // major = 1 (incompatible)
    std.mem.writeInt(u16, buf[6..8], 0, .little); // minor
    std.mem.writeInt(u32, buf[8..12], 0, .little); // flags
    std.mem.writeInt(u32, buf[12..16], 0, .little); // padding
    var stream = std.io.fixedBufferStream(&buf);
    const result = Chunk.deserialize(stream.reader(), allocator);
    try std.testing.expectError(error.IncompatibleVersion, result);
}

test "Chunk serialize round-trips closure opcodes" {
    const allocator = std.testing.allocator;

    // Build a chunk with Phase 2 closure opcodes.
    var original: Chunk = .{};

    // op_closure with a constant index of 0, followed by upvalue descriptor (is_local=1, index=1).
    try original.write(@intFromEnum(OpCode.op_closure), 1, allocator);
    try original.write(0, 1, allocator); // const_idx
    try original.write(1, 1, allocator); // is_local
    try original.write(1, 1, allocator); // index

    // op_get_upvalue with slot 0.
    try original.write(@intFromEnum(OpCode.op_get_upvalue), 2, allocator);
    try original.write(0, 2, allocator);

    // op_set_upvalue with slot 0.
    try original.write(@intFromEnum(OpCode.op_set_upvalue), 3, allocator);
    try original.write(0, 3, allocator);

    // op_close_upvalue.
    try original.write(@intFromEnum(OpCode.op_close_upvalue), 4, allocator);

    // op_tail_call with arg_count 2.
    try original.write(@intFromEnum(OpCode.op_tail_call), 5, allocator);
    try original.write(2, 5, allocator);

    // Add a constant (needed for op_closure reference).
    _ = try original.addConstant(Value.fromInt(99), allocator);

    original.name = "closure_test.zen";

    // Serialize.
    var buf = std.ArrayListUnmanaged(u8){};
    defer buf.deinit(allocator);
    try original.serialize(buf.writer(allocator));

    // Clean up original.
    original.code.deinit(allocator);
    original.constants.deinit(allocator);
    original.lines.deinit(allocator);
    original.atom_names.deinit(allocator);

    // Deserialize.
    var stream = std.io.fixedBufferStream(buf.items);
    var deserialized = try Chunk.deserialize(stream.reader(), allocator);
    defer {
        for (deserialized.constants.items) |val| {
            if (val.isObj()) val.asObj().destroy(allocator);
        }
        deserialized.deinit(allocator);
    }

    // Verify code length matches.
    try std.testing.expectEqual(@as(usize, 11), deserialized.code.items.len);

    // Verify opcodes round-tripped correctly.
    try std.testing.expectEqual(@intFromEnum(OpCode.op_closure), deserialized.code.items[0]);
    try std.testing.expectEqual(@intFromEnum(OpCode.op_get_upvalue), deserialized.code.items[4]);
    try std.testing.expectEqual(@intFromEnum(OpCode.op_set_upvalue), deserialized.code.items[6]);
    try std.testing.expectEqual(@intFromEnum(OpCode.op_close_upvalue), deserialized.code.items[8]);
    try std.testing.expectEqual(@intFromEnum(OpCode.op_tail_call), deserialized.code.items[9]);

    // Verify operands.
    try std.testing.expectEqual(@as(u8, 0), deserialized.code.items[1]); // const_idx for closure
    try std.testing.expectEqual(@as(u8, 1), deserialized.code.items[2]); // is_local
    try std.testing.expectEqual(@as(u8, 1), deserialized.code.items[3]); // index
    try std.testing.expectEqual(@as(u8, 2), deserialized.code.items[10]); // tail_call arg_count
}
