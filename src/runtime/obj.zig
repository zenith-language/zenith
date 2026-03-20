const std = @import("std");
const Allocator = std.mem.Allocator;
const value_mod = @import("value");
const Value = value_mod.Value;
const chunk_mod = @import("chunk");
const Chunk = chunk_mod.Chunk;

/// Object type tag for heap-allocated values.
pub const ObjType = enum(u8) {
    string,
    bytes,
    int_big,
    range,
    function,
    closure,
    upvalue,
};

/// Common header for all heap-allocated objects.
/// Every Obj-derived type embeds this as its first field so
/// that we can always recover the type tag from any `*Obj` pointer.
pub const Obj = struct {
    obj_type: ObjType,
    /// Linked-list pointer for GC traversal (future phases).
    next: ?*Obj = null,

    /// Free the memory for this object and its payload.
    pub fn destroy(self: *Obj, allocator: Allocator) void {
        switch (self.obj_type) {
            .string => {
                const str = ObjString.fromObj(self);
                allocator.free(str.bytes);
                allocator.destroy(str);
            },
            .bytes => {
                const b = ObjBytes.fromObj(self);
                allocator.free(b.data);
                allocator.destroy(b);
            },
            .int_big => {
                const big = ObjInt.fromObj(self);
                allocator.destroy(big);
            },
            .range => {
                const r = ObjRange.fromObj(self);
                allocator.destroy(r);
            },
            .function => {
                const func = ObjFunction.fromObj(self);
                func.chunk.deinit(allocator);
                if (func.param_names) |names| {
                    allocator.free(names);
                }
                if (func.param_defaults) |defaults| {
                    allocator.free(defaults);
                }
                allocator.destroy(func);
            },
            .closure => {
                const clos = ObjClosure.fromObj(self);
                allocator.free(clos.upvalues);
                allocator.destroy(clos);
            },
            .upvalue => {
                const uv = ObjUpvalue.fromObj(self);
                allocator.destroy(uv);
            },
        }
    }
};

/// Heap-allocated immutable string (UTF-8 bytes).
pub const ObjString = struct {
    obj: Obj,
    bytes: []const u8,
    hash: u32,

    /// Create a new ObjString by copying `source` bytes onto the heap.
    pub fn create(allocator: Allocator, source: []const u8) !*ObjString {
        const copy = try allocator.dupe(u8, source);
        errdefer allocator.free(copy);

        const str = try allocator.create(ObjString);
        str.* = .{
            .obj = .{ .obj_type = .string },
            .bytes = copy,
            .hash = hashBytes(source),
        };
        return str;
    }

    /// Recover the containing `ObjString` from an `*Obj` pointer.
    pub fn fromObj(obj: *Obj) *ObjString {
        return @fieldParentPtr("obj", obj);
    }

    fn hashBytes(data: []const u8) u32 {
        // FNV-1a hash.
        var h: u32 = 2166136261;
        for (data) |byte| {
            h ^= @as(u32, byte);
            h *%= 16777619;
        }
        return h;
    }
};

/// Heap-allocated immutable byte sequence (not necessarily valid UTF-8).
pub const ObjBytes = struct {
    obj: Obj,
    data: []const u8,
    hash: u32,

    pub fn create(allocator: Allocator, source: []const u8) !*ObjBytes {
        const copy = try allocator.dupe(u8, source);
        errdefer allocator.free(copy);

        const b = try allocator.create(ObjBytes);
        b.* = .{
            .obj = .{ .obj_type = .bytes },
            .data = copy,
            .hash = ObjString.hashBytes(source),
        };
        return b;
    }

    pub fn fromObj(obj: *Obj) *ObjBytes {
        return @fieldParentPtr("obj", obj);
    }
};

/// Heap-allocated 64-bit integer for values outside inline i32 range.
pub const ObjInt = struct {
    obj: Obj,
    value: i64,

    pub fn create(allocator: Allocator, val: i64) !*ObjInt {
        const big = try allocator.create(ObjInt);
        big.* = .{
            .obj = .{ .obj_type = .int_big },
            .value = val,
        };
        return big;
    }

    pub fn fromObj(obj: *Obj) *ObjInt {
        return @fieldParentPtr("obj", obj);
    }
};

/// Heap-allocated range descriptor for for-in iteration.
pub const ObjRange = struct {
    obj: Obj,
    start: i32,
    end: i32,
    step: i32,

    pub fn create(allocator: Allocator, start: i32, end_val: i32, step: i32) !*ObjRange {
        const r = try allocator.create(ObjRange);
        r.* = .{
            .obj = .{ .obj_type = .range },
            .start = start,
            .end = end_val,
            .step = step,
        };
        return r;
    }

    pub fn fromObj(o: *Obj) *ObjRange {
        return @fieldParentPtr("obj", o);
    }
};

/// Compiled function prototype (compile-time container).
/// ObjFunction is the compile-time representation of a function.
/// At runtime, functions are ALWAYS wrapped in ObjClosure (even if they
/// capture no upvalues). ObjFunction is purely compile-time.
pub const ObjFunction = struct {
    obj: Obj,
    /// Number of required positional parameters.
    arity: u8,
    /// Total parameters including optional named ones.
    arity_max: u8,
    /// Number of upvalues captured by this function.
    upvalue_count: u8,
    /// The function's bytecode.
    chunk: Chunk,
    /// Function name (null for anonymous lambdas). Not owned -- points into source.
    name: ?[]const u8,
    /// Parameter names (token slices, not owned). Null if no params.
    param_names: ?[]const []const u8,
    /// Default values for named parameters. Null if no defaults.
    param_defaults: ?[]const Value,

    pub fn create(allocator: Allocator) !*ObjFunction {
        const func = try allocator.create(ObjFunction);
        func.* = .{
            .obj = .{ .obj_type = .function },
            .arity = 0,
            .arity_max = 0,
            .upvalue_count = 0,
            .chunk = .{},
            .name = null,
            .param_names = null,
            .param_defaults = null,
        };
        return func;
    }

    pub fn fromObj(obj: *Obj) *ObjFunction {
        return @fieldParentPtr("obj", obj);
    }
};

/// Runtime closure wrapping a function prototype + captured upvalues.
/// The VM only ever calls ObjClosure, never ObjFunction directly.
pub const ObjClosure = struct {
    obj: Obj,
    /// The underlying function prototype.
    function: *ObjFunction,
    /// Captured upvalue pointers. Length == function.upvalue_count.
    upvalues: []?*ObjUpvalue,

    pub fn create(allocator: Allocator, function: *ObjFunction) !*ObjClosure {
        const upvalue_count = function.upvalue_count;
        const upvalues = try allocator.alloc(?*ObjUpvalue, upvalue_count);
        @memset(upvalues, null);

        const clos = try allocator.create(ObjClosure);
        clos.* = .{
            .obj = .{ .obj_type = .closure },
            .function = function,
            .upvalues = upvalues,
        };
        return clos;
    }

    pub fn fromObj(obj: *Obj) *ObjClosure {
        return @fieldParentPtr("obj", obj);
    }
};

/// Captured variable indirection for closures.
/// An upvalue starts "open" (pointing to a stack slot). When the enclosing
/// scope exits, it becomes "closed" (value copied into the `closed` field).
pub const ObjUpvalue = struct {
    obj: Obj,
    /// Points to the stack slot when open.
    location: *Value,
    /// Storage for the value after the upvalue is closed.
    closed: Value,
    /// Linked list of open upvalues (for the VM's open upvalue tracking).
    next: ?*ObjUpvalue,

    pub fn create(allocator: Allocator, slot: *Value) !*ObjUpvalue {
        const uv = try allocator.create(ObjUpvalue);
        uv.* = .{
            .obj = .{ .obj_type = .upvalue },
            .location = slot,
            .closed = Value.nil,
            .next = null,
        };
        return uv;
    }

    pub fn fromObj(obj: *Obj) *ObjUpvalue {
        return @fieldParentPtr("obj", obj);
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

test "ObjFunction create and destroy lifecycle" {
    const allocator = std.testing.allocator;
    const func = try ObjFunction.create(allocator);
    defer func.obj.destroy(allocator);

    try std.testing.expectEqual(ObjType.function, func.obj.obj_type);
    try std.testing.expectEqual(@as(u8, 0), func.arity);
    try std.testing.expectEqual(@as(u8, 0), func.arity_max);
    try std.testing.expectEqual(@as(u8, 0), func.upvalue_count);
    try std.testing.expect(func.name == null);
    try std.testing.expect(func.param_names == null);
    try std.testing.expect(func.param_defaults == null);
}

test "ObjFunction with chunk data" {
    const allocator = std.testing.allocator;
    const func = try ObjFunction.create(allocator);
    defer func.obj.destroy(allocator);

    // Add some bytecode to the function's chunk.
    try func.chunk.write(@intFromEnum(chunk_mod.OpCode.op_return), 1, allocator);
    _ = try func.chunk.addConstant(Value.fromInt(42), allocator);

    try std.testing.expectEqual(@as(usize, 1), func.chunk.code.items.len);
    try std.testing.expectEqual(@as(usize, 1), func.chunk.constants.items.len);
}

test "ObjClosure create with 0 upvalues" {
    const allocator = std.testing.allocator;
    const func = try ObjFunction.create(allocator);
    defer func.obj.destroy(allocator);

    const clos = try ObjClosure.create(allocator, func);
    defer clos.obj.destroy(allocator);

    try std.testing.expectEqual(ObjType.closure, clos.obj.obj_type);
    try std.testing.expectEqual(func, clos.function);
    try std.testing.expectEqual(@as(usize, 0), clos.upvalues.len);
}

test "ObjClosure create with 3 upvalues" {
    const allocator = std.testing.allocator;
    const func = try ObjFunction.create(allocator);
    func.upvalue_count = 3;
    defer func.obj.destroy(allocator);

    const clos = try ObjClosure.create(allocator, func);
    defer clos.obj.destroy(allocator);

    try std.testing.expectEqual(@as(usize, 3), clos.upvalues.len);
    // All upvalues should be initialized to null.
    for (clos.upvalues) |uv| {
        try std.testing.expect(uv == null);
    }
}

test "ObjUpvalue create with stack slot" {
    const allocator = std.testing.allocator;
    var slot = Value.fromInt(99);
    const uv = try ObjUpvalue.create(allocator, &slot);
    defer uv.obj.destroy(allocator);

    try std.testing.expectEqual(ObjType.upvalue, uv.obj.obj_type);
    try std.testing.expectEqual(@as(i32, 99), uv.location.asInt());
    try std.testing.expect(uv.closed.isNil());
    try std.testing.expect(uv.next == null);
}

test "ObjFunction fromObj recovers original" {
    const allocator = std.testing.allocator;
    const func = try ObjFunction.create(allocator);
    defer func.obj.destroy(allocator);
    func.arity = 2;
    func.name = "add";

    const obj_ptr: *Obj = &func.obj;
    const recovered = ObjFunction.fromObj(obj_ptr);
    try std.testing.expectEqual(@as(u8, 2), recovered.arity);
    try std.testing.expectEqualStrings("add", recovered.name.?);
}

test "ObjClosure fromObj recovers original" {
    const allocator = std.testing.allocator;
    const func = try ObjFunction.create(allocator);
    defer func.obj.destroy(allocator);
    const clos = try ObjClosure.create(allocator, func);
    defer clos.obj.destroy(allocator);

    const obj_ptr: *Obj = &clos.obj;
    const recovered = ObjClosure.fromObj(obj_ptr);
    try std.testing.expectEqual(func, recovered.function);
}

test "ObjRange create and destroy round-trip" {
    const allocator = std.testing.allocator;
    const r = try ObjRange.create(allocator, 0, 5, 1);
    defer r.obj.destroy(allocator);

    try std.testing.expectEqual(@as(i32, 0), r.start);
    try std.testing.expectEqual(@as(i32, 5), r.end);
    try std.testing.expectEqual(@as(i32, 1), r.step);
    try std.testing.expectEqual(ObjType.range, r.obj.obj_type);
}

test "ObjRange fromObj recovers original fields" {
    const allocator = std.testing.allocator;
    const r = try ObjRange.create(allocator, 2, 10, 3);
    defer r.obj.destroy(allocator);

    const obj_ptr: *Obj = &r.obj;
    const recovered = ObjRange.fromObj(obj_ptr);
    try std.testing.expectEqual(@as(i32, 2), recovered.start);
    try std.testing.expectEqual(@as(i32, 10), recovered.end);
    try std.testing.expectEqual(@as(i32, 3), recovered.step);
}

test "ObjRange destroy frees memory (testing allocator)" {
    const allocator = std.testing.allocator;
    const r = try ObjRange.create(allocator, 0, 100, 1);
    // Destroy via Obj.destroy -- testing allocator will catch leaks.
    r.obj.destroy(allocator);
}

test "ObjString create and destroy round-trip" {
    const allocator = std.testing.allocator;
    const str = try ObjString.create(allocator, "hello");
    defer str.obj.destroy(allocator);

    try std.testing.expectEqualStrings("hello", str.bytes);
    try std.testing.expectEqual(ObjType.string, str.obj.obj_type);
    try std.testing.expect(str.hash != 0);
}

test "ObjString empty string" {
    const allocator = std.testing.allocator;
    const str = try ObjString.create(allocator, "");
    defer str.obj.destroy(allocator);

    try std.testing.expectEqualStrings("", str.bytes);
}

test "ObjBytes create and destroy" {
    const allocator = std.testing.allocator;
    const data = [_]u8{ 0xFF, 0x00, 0xAB };
    const b = try ObjBytes.create(allocator, &data);
    defer b.obj.destroy(allocator);

    try std.testing.expectEqualSlices(u8, &data, b.data);
    try std.testing.expectEqual(ObjType.bytes, b.obj.obj_type);
}

test "ObjInt create and destroy" {
    const allocator = std.testing.allocator;
    const big = try ObjInt.create(allocator, 9_999_999_999);
    defer big.obj.destroy(allocator);

    try std.testing.expectEqual(@as(i64, 9_999_999_999), big.value);
    try std.testing.expectEqual(ObjType.int_big, big.obj.obj_type);
}

test "ObjInt negative big value" {
    const allocator = std.testing.allocator;
    const big = try ObjInt.create(allocator, -9_999_999_999);
    defer big.obj.destroy(allocator);

    try std.testing.expectEqual(@as(i64, -9_999_999_999), big.value);
}

test "ObjString fromObj recovers original" {
    const allocator = std.testing.allocator;
    const str = try ObjString.create(allocator, "test");
    defer str.obj.destroy(allocator);

    const obj_ptr: *Obj = &str.obj;
    const recovered = ObjString.fromObj(obj_ptr);
    try std.testing.expectEqualStrings("test", recovered.bytes);
}
