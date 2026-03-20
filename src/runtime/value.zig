/// NaN-boxed value representation.
///
/// All runtime values are stored as a `u64`. IEEE 754 doubles use their natural
/// bit pattern. Non-float values encode a type tag in the quiet NaN payload.
///
/// Bit layout (bits 50-62 form the quiet NaN base, leaving bits 0-49 as payload):
///
///   Float:     any valid IEEE 754 double where `(bits & QNAN) != QNAN`
///   Nil:       QNAN | 1
///   False:     QNAN | 2
///   True:      QNAN | 3
///   Int(i32):  QNAN | TAG_INT  | (i32 as u32 in lower 32 bits)
///   Atom(u32): QNAN | TAG_ATOM | (atom_id in lower 32 bits)
///   Object:    SIGN_BIT | QNAN | (pointer in lower 48 bits)
///
/// TAG_INT  = bit 48 set  = 0x0001_0000_0000_0000
/// TAG_ATOM = bit 49 set  = 0x0002_0000_0000_0000
///
/// NEVER use `@setFloatMode(.optimized)` in this file.

const std = @import("std");
const Allocator = std.mem.Allocator;
const obj_mod = @import("obj");
const Obj = obj_mod.Obj;
const ObjString = obj_mod.ObjString;
const ObjInt = obj_mod.ObjInt;

pub const Value = struct {
    bits: u64,

    // ── Bit-level constants ────────────────────────────────────────────

    /// Quiet NaN base: exponent all 1s + quiet bit + safety bit (bits 50-62).
    const QNAN: u64 = 0x7FFC_0000_0000_0000;
    /// Sign bit, used to distinguish object pointers.
    const SIGN_BIT: u64 = 0x8000_0000_0000_0000;

    /// Tag for inline i32 integers -- bit 48 in the NaN payload.
    const TAG_INT: u64 = 0x0001_0000_0000_0000;
    /// Tag for atom IDs -- bit 49 in the NaN payload.
    const TAG_ATOM: u64 = 0x0002_0000_0000_0000;

    /// Mask for tag bits 48-49 (distinguishes singletons, ints, atoms).
    const TAG_BITS: u64 = 0x0003_0000_0000_0000;

    // Singleton values: QNAN + small payload (bits 48-49 both zero).
    const SINGLETON_NIL: u64 = 1;
    const SINGLETON_FALSE: u64 = 2;
    const SINGLETON_TRUE: u64 = 3;

    // Compile-time guard: NaN-boxing requires 64-bit pointers.
    comptime {
        if (@sizeOf(usize) != 8) @compileError("NaN boxing requires 64-bit architecture");
    }

    // ── Singletons ─────────────────────────────────────────────────────

    pub const nil: Value = .{ .bits = QNAN | SINGLETON_NIL };
    pub const true_val: Value = .{ .bits = QNAN | SINGLETON_TRUE };
    pub const false_val: Value = .{ .bits = QNAN | SINGLETON_FALSE };

    // ── Constructors ───────────────────────────────────────────────────

    pub fn fromFloat(f: f64) Value {
        return .{ .bits = @bitCast(f) };
    }

    /// Encode an inline i32 integer.
    pub fn fromInt(i: i32) Value {
        return .{ .bits = QNAN | TAG_INT | @as(u64, @as(u32, @bitCast(i))) };
    }

    /// Encode an i64.  If the value fits in i32, stores it inline;
    /// otherwise heap-allocates an ObjInt.
    pub fn fromI64(i: i64, allocator: Allocator) !Value {
        if (i >= std.math.minInt(i32) and i <= std.math.maxInt(i32)) {
            return fromInt(@intCast(i));
        }
        const big = try ObjInt.create(allocator, i);
        return fromObj(&big.obj);
    }

    pub fn fromBool(b: bool) Value {
        return if (b) true_val else false_val;
    }

    pub fn fromObj(ptr: *Obj) Value {
        const addr: u64 = @intFromPtr(ptr);
        std.debug.assert(addr <= 0x0000_FFFF_FFFF_FFFF); // 48-bit pointer check
        return .{ .bits = SIGN_BIT | QNAN | addr };
    }

    pub fn fromAtom(id: u32) Value {
        return .{ .bits = QNAN | TAG_ATOM | @as(u64, id) };
    }

    // ── Type checks ────────────────────────────────────────────────────

    pub fn isFloat(self: Value) bool {
        return (self.bits & QNAN) != QNAN;
    }

    pub fn isNil(self: Value) bool {
        return self.bits == nil.bits;
    }

    pub fn isBool(self: Value) bool {
        return self.bits == true_val.bits or self.bits == false_val.bits;
    }

    pub fn isInt(self: Value) bool {
        // Must be a QNAN, not an object (no SIGN_BIT), and have TAG_INT set.
        return (self.bits & (QNAN | SIGN_BIT | TAG_BITS)) == (QNAN | TAG_INT);
    }

    pub fn isAtom(self: Value) bool {
        return (self.bits & (QNAN | SIGN_BIT | TAG_BITS)) == (QNAN | TAG_ATOM);
    }

    pub fn isObj(self: Value) bool {
        return (self.bits & (QNAN | SIGN_BIT)) == (QNAN | SIGN_BIT);
    }

    pub fn isObjType(self: Value, t: obj_mod.ObjType) bool {
        return self.isObj() and self.asObj().obj_type == t;
    }

    pub fn isString(self: Value) bool {
        return self.isObjType(.string);
    }

    /// Returns true if this value represents an integer (either inline i32 or heap ObjInt).
    pub fn isInteger(self: Value) bool {
        return self.isInt() or self.isObjType(.int_big);
    }

    // ── Decoders ───────────────────────────────────────────────────────

    pub fn asFloat(self: Value) f64 {
        return @bitCast(self.bits);
    }

    /// Decode an inline i32 value.
    pub fn asInt(self: Value) i32 {
        return @bitCast(@as(u32, @truncate(self.bits)));
    }

    /// Decode an integer value.  For inline i32 values, returns the i32
    /// widened to i64. For heap-allocated big ints, returns the full i64.
    pub fn asI64(self: Value) i64 {
        if (self.isInt()) {
            return @as(i64, self.asInt());
        }
        // Must be an ObjInt.
        const big = ObjInt.fromObj(self.asObj());
        return big.value;
    }

    pub fn asBool(self: Value) bool {
        return self.bits == true_val.bits;
    }

    pub fn asObj(self: Value) *Obj {
        return @ptrFromInt(self.bits & ~(SIGN_BIT | QNAN));
    }

    pub fn asAtom(self: Value) u32 {
        return @truncate(self.bits);
    }

    // ── Equality ───────────────────────────────────────────────────────

    /// Value equality.  NaN != NaN per IEEE 754.
    pub fn eql(a: Value, b: Value) bool {
        // Fast path: identical bit patterns.
        if (a.bits == b.bits) {
            // But NaN != NaN.
            if (a.isFloat()) {
                const fa = a.asFloat();
                return fa == fa; // false if NaN
            }
            return true;
        }

        // Both are strings -- compare contents.
        if (a.isString() and b.isString()) {
            const sa = ObjString.fromObj(a.asObj());
            const sb = ObjString.fromObj(b.asObj());
            return std.mem.eql(u8, sa.bytes, sb.bytes);
        }

        // Both are big ints -- compare values.
        if (a.isObjType(.int_big) and b.isObjType(.int_big)) {
            return ObjInt.fromObj(a.asObj()).value == ObjInt.fromObj(b.asObj()).value;
        }

        // Inline int vs big int.
        if (a.isInt() and b.isObjType(.int_big)) {
            return @as(i64, a.asInt()) == ObjInt.fromObj(b.asObj()).value;
        }
        if (b.isInt() and a.isObjType(.int_big)) {
            return ObjInt.fromObj(a.asObj()).value == @as(i64, b.asInt());
        }

        return false;
    }

    // ── Formatting ─────────────────────────────────────────────────────

    /// std.fmt compatible formatting for print/debug.
    pub fn format(self: Value, comptime _: []const u8, _: std.fmt.FormatOptions, writer: anytype) !void {
        if (self.isFloat()) {
            try writer.print("{d}", .{self.asFloat()});
        } else if (self.isNil()) {
            try writer.writeAll("nil");
        } else if (self.isBool()) {
            try writer.writeAll(if (self.asBool()) "true" else "false");
        } else if (self.isInt()) {
            try writer.print("{d}", .{self.asInt()});
        } else if (self.isAtom()) {
            try writer.print(":{d}", .{self.asAtom()});
        } else if (self.isObj()) {
            const obj_ptr = self.asObj();
            switch (obj_ptr.obj_type) {
                .string => {
                    const str = ObjString.fromObj(obj_ptr);
                    try writer.print("\"{s}\"", .{str.bytes});
                },
                .bytes => {
                    try writer.writeAll("<bytes>");
                },
                .int_big => {
                    const big = ObjInt.fromObj(obj_ptr);
                    try writer.print("{d}", .{big.value});
                },
            }
        } else {
            try writer.writeAll("<unknown>");
        }
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

test "Value.fromFloat round-trip" {
    const v = Value.fromFloat(3.14);
    try std.testing.expect(v.isFloat());
    try std.testing.expectApproxEqAbs(@as(f64, 3.14), v.asFloat(), 1e-15);
}

test "Value.fromFloat zero" {
    const v = Value.fromFloat(0.0);
    try std.testing.expect(v.isFloat());
    try std.testing.expectEqual(@as(f64, 0.0), v.asFloat());
}

test "Value.fromFloat negative" {
    const v = Value.fromFloat(-42.5);
    try std.testing.expect(v.isFloat());
    try std.testing.expectEqual(@as(f64, -42.5), v.asFloat());
}

test "Value.fromInt positive round-trip" {
    const v = Value.fromInt(42);
    try std.testing.expect(v.isInt());
    try std.testing.expect(!v.isFloat());
    try std.testing.expectEqual(@as(i32, 42), v.asInt());
}

test "Value.fromInt negative round-trip" {
    const v = Value.fromInt(-1);
    try std.testing.expect(v.isInt());
    try std.testing.expectEqual(@as(i32, -1), v.asInt());
}

test "Value.fromInt zero" {
    const v = Value.fromInt(0);
    try std.testing.expect(v.isInt());
    try std.testing.expectEqual(@as(i32, 0), v.asInt());
}

test "Value.fromInt min/max i32" {
    const min_v = Value.fromInt(std.math.minInt(i32));
    try std.testing.expectEqual(std.math.minInt(i32), min_v.asInt());

    const max_v = Value.fromInt(std.math.maxInt(i32));
    try std.testing.expectEqual(std.math.maxInt(i32), max_v.asInt());
}

test "Value.nil" {
    try std.testing.expect(Value.nil.isNil());
    try std.testing.expect(!Value.fromFloat(0.0).isNil());
    try std.testing.expect(!Value.fromInt(0).isNil());
    try std.testing.expect(!Value.false_val.isNil());
}

test "Value.bool round-trip" {
    try std.testing.expect(Value.true_val.isBool());
    try std.testing.expect(Value.true_val.asBool() == true);

    try std.testing.expect(Value.false_val.isBool());
    try std.testing.expect(Value.false_val.asBool() == false);

    try std.testing.expect(!Value.nil.isBool());
    try std.testing.expect(!Value.fromInt(1).isBool());
}

test "Value.fromBool" {
    try std.testing.expectEqual(Value.true_val.bits, Value.fromBool(true).bits);
    try std.testing.expectEqual(Value.false_val.bits, Value.fromBool(false).bits);
}

test "Value.fromAtom round-trip" {
    const v = Value.fromAtom(5);
    try std.testing.expect(v.isAtom());
    try std.testing.expectEqual(@as(u32, 5), v.asAtom());
    try std.testing.expect(!v.isInt());
    try std.testing.expect(!v.isFloat());
}

test "Value.fromAtom zero" {
    const v = Value.fromAtom(0);
    try std.testing.expect(v.isAtom());
    try std.testing.expectEqual(@as(u32, 0), v.asAtom());
}

test "Value.isFloat distinguishes floats from tagged values" {
    try std.testing.expect(Value.fromFloat(1.0).isFloat());
    try std.testing.expect(Value.fromFloat(-0.0).isFloat());
    try std.testing.expect(!Value.nil.isFloat());
    try std.testing.expect(!Value.true_val.isFloat());
    try std.testing.expect(!Value.fromInt(42).isFloat());
    try std.testing.expect(!Value.fromAtom(0).isFloat());
}

test "type checks are mutually exclusive" {
    // Int is not obj, float, nil, bool, atom
    const int_v = Value.fromInt(42);
    try std.testing.expect(int_v.isInt());
    try std.testing.expect(!int_v.isFloat());
    try std.testing.expect(!int_v.isNil());
    try std.testing.expect(!int_v.isBool());
    try std.testing.expect(!int_v.isAtom());
    try std.testing.expect(!int_v.isObj());

    // Atom is not int
    const atom_v = Value.fromAtom(5);
    try std.testing.expect(atom_v.isAtom());
    try std.testing.expect(!atom_v.isInt());
    try std.testing.expect(!atom_v.isObj());
}

test "ObjString value round-trip via fromObj" {
    const allocator = std.testing.allocator;
    const str = try ObjString.create(allocator, "hello");
    defer str.obj.destroy(allocator);

    const v = Value.fromObj(&str.obj);
    try std.testing.expect(v.isObj());
    try std.testing.expect(v.isString());
    try std.testing.expect(!v.isFloat());
    try std.testing.expect(!v.isInt());

    const recovered = ObjString.fromObj(v.asObj());
    try std.testing.expectEqualStrings("hello", recovered.bytes);
}

test "Value.eql NaN != NaN" {
    const nan = Value.fromFloat(std.math.nan(f64));
    try std.testing.expect(!nan.eql(nan));
}

test "Value.eql same float" {
    const a = Value.fromFloat(3.14);
    const b = Value.fromFloat(3.14);
    try std.testing.expect(a.eql(b));
}

test "Value.eql different types" {
    try std.testing.expect(!Value.fromInt(0).eql(Value.fromFloat(0.0)));
    try std.testing.expect(!Value.nil.eql(Value.false_val));
    try std.testing.expect(!Value.fromInt(1).eql(Value.true_val));
}

test "Value.eql strings by content" {
    const allocator = std.testing.allocator;
    const a = try ObjString.create(allocator, "hello");
    defer a.obj.destroy(allocator);
    const b = try ObjString.create(allocator, "hello");
    defer b.obj.destroy(allocator);
    const c = try ObjString.create(allocator, "world");
    defer c.obj.destroy(allocator);

    const va = Value.fromObj(&a.obj);
    const vb = Value.fromObj(&b.obj);
    const vc = Value.fromObj(&c.obj);

    try std.testing.expect(va.eql(vb));
    try std.testing.expect(!va.eql(vc));
}

test "i32 overflow promotion to heap ObjInt" {
    const allocator = std.testing.allocator;

    // Value larger than i32 max.
    const big_val: i64 = @as(i64, std.math.maxInt(i32)) + 1;
    const v = try Value.fromI64(big_val, allocator);

    // Must be an object (not inline int).
    try std.testing.expect(v.isObj());
    try std.testing.expect(v.isObjType(.int_big));
    try std.testing.expectEqual(big_val, v.asI64());

    // Cleanup.
    v.asObj().destroy(allocator);
}

test "i64 within i32 range stays inline" {
    const allocator = std.testing.allocator;
    const v = try Value.fromI64(42, allocator);
    try std.testing.expect(v.isInt());
    try std.testing.expectEqual(@as(i64, 42), v.asI64());
    // No cleanup needed -- inline value.
}

test "negative i64 overflow to heap" {
    const allocator = std.testing.allocator;
    const big_neg: i64 = @as(i64, std.math.minInt(i32)) - 1;
    const v = try Value.fromI64(big_neg, allocator);
    try std.testing.expect(v.isObj());
    try std.testing.expectEqual(big_neg, v.asI64());
    v.asObj().destroy(allocator);
}

test "Value.format prints correctly" {
    var buf: [128]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buf);
    const writer = stream.writer();

    try Value.fromInt(42).format("", .{}, writer);
    try std.testing.expectEqualStrings("42", stream.getWritten());

    stream.reset();
    try Value.nil.format("", .{}, writer);
    try std.testing.expectEqualStrings("nil", stream.getWritten());

    stream.reset();
    try Value.true_val.format("", .{}, writer);
    try std.testing.expectEqualStrings("true", stream.getWritten());

    stream.reset();
    try Value.false_val.format("", .{}, writer);
    try std.testing.expectEqualStrings("false", stream.getWritten());
}
