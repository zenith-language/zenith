const std = @import("std");
const Allocator = std.mem.Allocator;

/// Object type tag for heap-allocated values.
pub const ObjType = enum(u8) {
    string,
    bytes,
    int_big,
    range,
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

// ── Tests ──────────────────────────────────────────────────────────────

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
