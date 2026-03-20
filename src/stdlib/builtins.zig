const std = @import("std");
const Allocator = std.mem.Allocator;
const value_mod = @import("value");
const Value = value_mod.Value;
const obj_mod = @import("obj");
const ObjString = obj_mod.ObjString;

/// Error type for native function execution.
pub const NativeError = error{
    RuntimeError,
} || Allocator.Error;

/// Native function signature.
/// Each built-in receives its arguments and an allocator, and returns a Value
/// or a NativeError. The `err_msg` out-parameter is set on RuntimeError.
pub const NativeFn = *const fn (args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value;

/// Built-in function descriptor.
pub const BuiltinDesc = struct {
    name: []const u8,
    func: NativeFn,
    arity_min: u8,
    arity_max: u8,
};

/// All built-in functions available in Phase 1.
pub const builtins = [_]BuiltinDesc{
    .{ .name = "print", .func = &builtinPrint, .arity_min = 1, .arity_max = 1 },
    .{ .name = "str", .func = &builtinStr, .arity_min = 1, .arity_max = 1 },
    .{ .name = "len", .func = &builtinLen, .arity_min = 1, .arity_max = 1 },
    .{ .name = "type_of", .func = &builtinTypeOf, .arity_min = 1, .arity_max = 1 },
    .{ .name = "assert", .func = &builtinAssert, .arity_min = 1, .arity_max = 1 },
    .{ .name = "panic", .func = &builtinPanic, .arity_min = 1, .arity_max = 1 },
    .{ .name = "range", .func = &builtinRange, .arity_min = 1, .arity_max = 3 },
    .{ .name = "show", .func = &builtinShow, .arity_min = 1, .arity_max = 1 },
};

/// Format a value as a string (shared helper for print, str, show).
pub fn formatValue(val: Value, allocator: Allocator, atom_names: ?[]const []const u8) ![]u8 {
    var buf = std.ArrayListUnmanaged(u8){};
    const writer = buf.writer(allocator);

    if (val.isFloat()) {
        // Format floats so they always show at least one decimal place,
        // distinguishing them from integers visually (e.g., 4.0 not 4).
        const f = val.asFloat();
        // Check if the float is an integer value (no fractional part).
        if (f == @trunc(f) and !std.math.isNan(f) and !std.math.isInf(f)) {
            // Format with exactly one decimal place.
            try writer.print("{d:.1}", .{f});
        } else {
            try writer.print("{d}", .{f});
        }
    } else if (val.isNil()) {
        try writer.writeAll("nil");
    } else if (val.isBool()) {
        try writer.writeAll(if (val.asBool()) "true" else "false");
    } else if (val.isInt()) {
        try writer.print("{d}", .{val.asInt()});
    } else if (val.isAtom()) {
        try writer.writeByte(':');
        if (atom_names) |names| {
            const id = val.asAtom();
            if (id < names.len) {
                try writer.writeAll(names[id]);
            } else {
                try writer.print("{d}", .{id});
            }
        } else {
            try writer.print("{d}", .{val.asAtom()});
        }
    } else if (val.isObj()) {
        const obj_ptr = val.asObj();
        switch (obj_ptr.obj_type) {
            .string => {
                const str = ObjString.fromObj(obj_ptr);
                try writer.writeAll(str.bytes);
            },
            .bytes => {
                try writer.writeAll("<bytes>");
            },
            .int_big => {
                const big = obj_mod.ObjInt.fromObj(obj_ptr);
                try writer.print("{d}", .{big.value});
            },
            .range => {
                const r = obj_mod.ObjRange.fromObj(obj_ptr);
                try writer.print("range({d}, {d}, {d})", .{ r.start, r.end, r.step });
            },
            .function => {
                const func = obj_mod.ObjFunction.fromObj(obj_ptr);
                if (func.name) |name| {
                    try writer.print("<fn {s}>", .{name});
                } else {
                    try writer.writeAll("<fn>");
                }
            },
            .closure => {
                const clos = obj_mod.ObjClosure.fromObj(obj_ptr);
                if (clos.function.name) |name| {
                    try writer.print("<fn {s}>", .{name});
                } else {
                    try writer.writeAll("<fn>");
                }
            },
            .upvalue => {
                try writer.writeAll("<upvalue>");
            },
        }
    } else {
        try writer.writeAll("<unknown>");
    }

    return buf.toOwnedSlice(allocator);
}

// ── Built-in implementations ──────────────────────────────────────────

/// print(value) -- format value, write to stdout with newline. Returns nil.
fn builtinPrint(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = err_msg;
    const text = try formatValue(args[0], allocator, null);
    defer allocator.free(text);
    const stdout = std.fs.File.stdout();
    stdout.writeAll(text) catch {};
    stdout.writeAll("\n") catch {};
    return Value.nil;
}

/// str(value) -- convert any value to string representation.
fn builtinStr(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = err_msg;
    const text = try formatValue(args[0], allocator, null);
    defer allocator.free(text);
    const str_obj = try ObjString.create(allocator, text);
    return Value.fromObj(&str_obj.obj);
}

/// len(value) -- for strings: return byte length.
fn builtinLen(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = allocator;
    const val = args[0];
    if (val.isString()) {
        const str = ObjString.fromObj(val.asObj());
        return Value.fromInt(@intCast(str.bytes.len));
    }
    err_msg.* = "len() expects a string argument";
    return error.RuntimeError;
}

/// type_of(value) -- returns atom representing the type.
fn builtinTypeOf(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = allocator;
    _ = err_msg;
    const val = args[0];
    // Return atoms by convention: 0=int, 1=float, 2=bool, 3=nil, 4=string, 5=bytes, 6=atom
    // For Phase 1, we return integer atom IDs. The VM will map these to names.
    if (val.isInt() or val.isObjType(.int_big)) return Value.fromAtom(0); // :int
    if (val.isFloat()) return Value.fromAtom(1); // :float
    if (val.isBool()) return Value.fromAtom(2); // :bool
    if (val.isNil()) return Value.fromAtom(3); // :nil
    if (val.isString()) return Value.fromAtom(4); // :string
    if (val.isObjType(.bytes)) return Value.fromAtom(5); // :bytes
    if (val.isAtom()) return Value.fromAtom(6); // :atom
    if (val.isObjType(.range)) return Value.fromAtom(7); // :range
    if (val.isObjType(.closure) or val.isObjType(.function)) return Value.fromAtom(8); // :function
    return Value.fromAtom(3); // fallback: nil
}

/// assert(condition) -- runtime error if falsy. Returns nil.
fn builtinAssert(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = allocator;
    const val = args[0];
    if (isFalsy(val)) {
        err_msg.* = "assertion failed";
        return error.RuntimeError;
    }
    return Value.nil;
}

/// panic(message) -- always runtime error with message.
fn builtinPanic(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = allocator;
    const val = args[0];
    if (val.isString()) {
        const str = ObjString.fromObj(val.asObj());
        err_msg.* = str.bytes;
    } else {
        err_msg.* = "panic!";
    }
    return error.RuntimeError;
}

/// range(n) or range(start, end) or range(start, end, step).
/// Returns a heap-allocated ObjRange that the VM's op_for_iter can iterate.
fn builtinRange(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    const ObjRange = obj_mod.ObjRange;
    switch (args.len) {
        1 => {
            if (!args[0].isInt()) {
                err_msg.* = "range() expects integer arguments";
                return error.RuntimeError;
            }
            const r = try ObjRange.create(allocator, 0, args[0].asInt(), 1);
            return Value.fromObj(&r.obj);
        },
        2 => {
            if (!args[0].isInt() or !args[1].isInt()) {
                err_msg.* = "range() expects integer arguments";
                return error.RuntimeError;
            }
            const r = try ObjRange.create(allocator, args[0].asInt(), args[1].asInt(), 1);
            return Value.fromObj(&r.obj);
        },
        3 => {
            if (!args[0].isInt() or !args[1].isInt() or !args[2].isInt()) {
                err_msg.* = "range() expects integer arguments";
                return error.RuntimeError;
            }
            if (args[2].asInt() == 0) {
                err_msg.* = "range() step cannot be zero";
                return error.RuntimeError;
            }
            const r = try ObjRange.create(allocator, args[0].asInt(), args[1].asInt(), args[2].asInt());
            return Value.fromObj(&r.obj);
        },
        else => {
            err_msg.* = "range() takes 1 to 3 arguments";
            return error.RuntimeError;
        },
    }
}

/// show(value) -- like print but returns the value (for debugging).
fn builtinShow(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = err_msg;
    const text = try formatValue(args[0], allocator, null);
    defer allocator.free(text);
    // Print to stderr for show().
    const stderr = std.fs.File.stderr();
    stderr.writeAll(text) catch {};
    stderr.writeAll("\n") catch {};
    return args[0]; // return the value itself
}

// ── Helpers ───────────────────────────────────────────────────────────

/// Check if a value is "falsy" (nil or false).
pub fn isFalsy(val: Value) bool {
    if (val.isNil()) return true;
    if (val.isBool()) return !val.asBool();
    return false;
}

// ═══════════════════════════════════════════════════════════════════════
// ── Tests ──────────────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════

test "builtins: str converts int to string" {
    const allocator = std.testing.allocator;
    var err_msg: []const u8 = "";
    const result = try builtinStr(&[_]Value{Value.fromInt(42)}, allocator, &err_msg);
    // Result should be an ObjString "42"
    try std.testing.expect(result.isString());
    const str = ObjString.fromObj(result.asObj());
    try std.testing.expectEqualStrings("42", str.bytes);
    result.asObj().destroy(allocator);
}

test "builtins: str converts bool to string" {
    const allocator = std.testing.allocator;
    var err_msg: []const u8 = "";
    const result = try builtinStr(&[_]Value{Value.true_val}, allocator, &err_msg);
    try std.testing.expect(result.isString());
    const str = ObjString.fromObj(result.asObj());
    try std.testing.expectEqualStrings("true", str.bytes);
    result.asObj().destroy(allocator);
}

test "builtins: str converts nil to string" {
    const allocator = std.testing.allocator;
    var err_msg: []const u8 = "";
    const result = try builtinStr(&[_]Value{Value.nil}, allocator, &err_msg);
    try std.testing.expect(result.isString());
    const str = ObjString.fromObj(result.asObj());
    try std.testing.expectEqualStrings("nil", str.bytes);
    result.asObj().destroy(allocator);
}

test "builtins: len returns string length" {
    const allocator = std.testing.allocator;
    const s = try ObjString.create(allocator, "hello");
    defer s.obj.destroy(allocator);

    var err_msg: []const u8 = "";
    const result = try builtinLen(&[_]Value{Value.fromObj(&s.obj)}, allocator, &err_msg);
    try std.testing.expect(result.isInt());
    try std.testing.expectEqual(@as(i32, 5), result.asInt());
}

test "builtins: len errors on non-string" {
    const allocator = std.testing.allocator;
    var err_msg: []const u8 = "";
    const result = builtinLen(&[_]Value{Value.fromInt(42)}, allocator, &err_msg);
    try std.testing.expectError(error.RuntimeError, result);
    try std.testing.expectEqualStrings("len() expects a string argument", err_msg);
}

test "builtins: type_of returns correct atoms" {
    const allocator = std.testing.allocator;
    var err_msg: []const u8 = "";

    // int -> atom 0
    const t_int = try builtinTypeOf(&[_]Value{Value.fromInt(42)}, allocator, &err_msg);
    try std.testing.expect(t_int.isAtom());
    try std.testing.expectEqual(@as(u32, 0), t_int.asAtom()); // :int

    // float -> atom 1
    const t_float = try builtinTypeOf(&[_]Value{Value.fromFloat(3.14)}, allocator, &err_msg);
    try std.testing.expectEqual(@as(u32, 1), t_float.asAtom()); // :float

    // bool -> atom 2
    const t_bool = try builtinTypeOf(&[_]Value{Value.true_val}, allocator, &err_msg);
    try std.testing.expectEqual(@as(u32, 2), t_bool.asAtom()); // :bool

    // nil -> atom 3
    const t_nil = try builtinTypeOf(&[_]Value{Value.nil}, allocator, &err_msg);
    try std.testing.expectEqual(@as(u32, 3), t_nil.asAtom()); // :nil

    // string -> atom 4
    const s = try ObjString.create(allocator, "test");
    defer s.obj.destroy(allocator);
    const t_str = try builtinTypeOf(&[_]Value{Value.fromObj(&s.obj)}, allocator, &err_msg);
    try std.testing.expectEqual(@as(u32, 4), t_str.asAtom()); // :string

    // atom -> atom 6
    const t_atom = try builtinTypeOf(&[_]Value{Value.fromAtom(99)}, allocator, &err_msg);
    try std.testing.expectEqual(@as(u32, 6), t_atom.asAtom()); // :atom
}

test "builtins: assert passes on true" {
    const allocator = std.testing.allocator;
    var err_msg: []const u8 = "";
    const result = try builtinAssert(&[_]Value{Value.true_val}, allocator, &err_msg);
    try std.testing.expect(result.isNil());
}

test "builtins: assert fails on false" {
    const allocator = std.testing.allocator;
    var err_msg: []const u8 = "";
    const result = builtinAssert(&[_]Value{Value.false_val}, allocator, &err_msg);
    try std.testing.expectError(error.RuntimeError, result);
    try std.testing.expectEqualStrings("assertion failed", err_msg);
}

test "builtins: assert fails on nil" {
    const allocator = std.testing.allocator;
    var err_msg: []const u8 = "";
    const result = builtinAssert(&[_]Value{Value.nil}, allocator, &err_msg);
    try std.testing.expectError(error.RuntimeError, result);
}

test "builtins: panic always errors" {
    const allocator = std.testing.allocator;
    const s = try ObjString.create(allocator, "oh no");
    defer s.obj.destroy(allocator);

    var err_msg: []const u8 = "";
    const result = builtinPanic(&[_]Value{Value.fromObj(&s.obj)}, allocator, &err_msg);
    try std.testing.expectError(error.RuntimeError, result);
    try std.testing.expectEqualStrings("oh no", err_msg);
}

test "builtins: isFalsy" {
    try std.testing.expect(isFalsy(Value.nil));
    try std.testing.expect(isFalsy(Value.false_val));
    try std.testing.expect(!isFalsy(Value.true_val));
    try std.testing.expect(!isFalsy(Value.fromInt(0)));
    try std.testing.expect(!isFalsy(Value.fromInt(42)));
}

test "builtins: range with invalid args" {
    const allocator = std.testing.allocator;
    var err_msg: []const u8 = "";
    // Float argument should fail
    const result = builtinRange(&[_]Value{Value.fromFloat(3.14)}, allocator, &err_msg);
    try std.testing.expectError(error.RuntimeError, result);
}

test "builtins: range with step zero" {
    const allocator = std.testing.allocator;
    var err_msg: []const u8 = "";
    const result = builtinRange(&[_]Value{ Value.fromInt(0), Value.fromInt(10), Value.fromInt(0) }, allocator, &err_msg);
    try std.testing.expectError(error.RuntimeError, result);
    try std.testing.expectEqualStrings("range() step cannot be zero", err_msg);
}

test "builtins: range(n) returns ObjRange(0, n, 1)" {
    const allocator = std.testing.allocator;
    var err_msg: []const u8 = "";
    const result = try builtinRange(&[_]Value{Value.fromInt(5)}, allocator, &err_msg);
    try std.testing.expect(result.isObj());
    try std.testing.expect(result.isObjType(.range));
    const r = obj_mod.ObjRange.fromObj(result.asObj());
    try std.testing.expectEqual(@as(i32, 0), r.start);
    try std.testing.expectEqual(@as(i32, 5), r.end);
    try std.testing.expectEqual(@as(i32, 1), r.step);
    result.asObj().destroy(allocator);
}

test "builtins: range(start, end) returns ObjRange(start, end, 1)" {
    const allocator = std.testing.allocator;
    var err_msg: []const u8 = "";
    const result = try builtinRange(&[_]Value{ Value.fromInt(2), Value.fromInt(5) }, allocator, &err_msg);
    try std.testing.expect(result.isObjType(.range));
    const r = obj_mod.ObjRange.fromObj(result.asObj());
    try std.testing.expectEqual(@as(i32, 2), r.start);
    try std.testing.expectEqual(@as(i32, 5), r.end);
    try std.testing.expectEqual(@as(i32, 1), r.step);
    result.asObj().destroy(allocator);
}

test "builtins: range(start, end, step) returns ObjRange" {
    const allocator = std.testing.allocator;
    var err_msg: []const u8 = "";
    const result = try builtinRange(&[_]Value{ Value.fromInt(0), Value.fromInt(10), Value.fromInt(2) }, allocator, &err_msg);
    try std.testing.expect(result.isObjType(.range));
    const r = obj_mod.ObjRange.fromObj(result.asObj());
    try std.testing.expectEqual(@as(i32, 0), r.start);
    try std.testing.expectEqual(@as(i32, 10), r.end);
    try std.testing.expectEqual(@as(i32, 2), r.step);
    result.asObj().destroy(allocator);
}

test "builtins: formatValue for various types" {
    const allocator = std.testing.allocator;

    const int_str = try formatValue(Value.fromInt(42), allocator, null);
    defer allocator.free(int_str);
    try std.testing.expectEqualStrings("42", int_str);

    const bool_str = try formatValue(Value.true_val, allocator, null);
    defer allocator.free(bool_str);
    try std.testing.expectEqualStrings("true", bool_str);

    const nil_str = try formatValue(Value.nil, allocator, null);
    defer allocator.free(nil_str);
    try std.testing.expectEqualStrings("nil", nil_str);

    const float_str = try formatValue(Value.fromFloat(3.14), allocator, null);
    defer allocator.free(float_str);
    // Just check it contains "3.14"
    try std.testing.expect(std.mem.indexOf(u8, float_str, "3.14") != null);
}
