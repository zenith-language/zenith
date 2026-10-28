const std = @import("std");
const Allocator = std.mem.Allocator;
const value_mod = @import("value");
const Value = value_mod.Value;

/// Temporary GC roots for native code.
///
/// Builtins and stream operators hold heap values in Zig locals, which the
/// collector cannot see. Every object a builtin registers with the VM is
/// pinned here, and so is every value a closure returns to a builtin.
///
/// Pins are scoped like a stack: take a `mark()`, do some work, then
/// `restore()` to drop everything pinned since. The VM wraps each builtin
/// call in a scope, so a simple builtin needs nothing extra. Loops that
/// process many elements open a scope per element so that memory stays
/// bounded, and use `keep()` to carry the one value they return.
pub const RootStack = struct {
    items: std.ArrayListUnmanaged(Value) = .empty,
    allocator: Allocator,

    pub fn init(allocator: Allocator) RootStack {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *RootStack) void {
        self.items.deinit(self.allocator);
    }

    pub fn slice(self: *RootStack) []Value {
        return self.items.items;
    }
};

/// Root stack of the VM running on this thread. Null outside the VM
/// (unit tests, compiler), where every operation below is a no-op.
pub threadlocal var current: ?*RootStack = null;

pub inline fn mark() usize {
    const r = current orelse return 0;
    return r.items.items.len;
}

pub inline fn restore(m: usize) void {
    const r = current orelse return;
    std.debug.assert(m <= r.items.items.len);
    r.items.shrinkRetainingCapacity(m);
}

pub fn pin(v: Value) void {
    if (!v.isObj()) return;
    const r = current orelse return;
    r.items.append(r.allocator, v) catch @panic("out of memory while pinning a GC root");
}

/// Drop the pins taken since `m` and pin `v` in their place.
pub fn keep(m: usize, v: Value) Value {
    restore(m);
    pin(v);
    return v;
}

/// Reserve a root slot for a value that is replaced as a loop runs
/// (an accumulator). Returns the slot index for `set()`.
pub fn slot(v: Value) usize {
    const r = current orelse return 0;
    const idx = r.items.items.len;
    r.items.append(r.allocator, v) catch @panic("out of memory while pinning a GC root");
    return idx;
}

pub fn set(idx: usize, v: Value) void {
    const r = current orelse return;
    r.items.items[idx] = v;
}

test "mark, pin and restore" {
    var stack = RootStack.init(std.testing.allocator);
    defer stack.deinit();
    current = &stack;
    defer current = null;

    const m = mark();
    pin(Value.fromInt(1)); // not an object, ignored
    try std.testing.expectEqual(@as(usize, 0), stack.items.items.len);

    const s = slot(Value.nil);
    set(s, Value.fromInt(7));
    try std.testing.expectEqual(@as(usize, 1), stack.items.items.len);
    restore(m);
    try std.testing.expectEqual(@as(usize, 0), stack.items.items.len);
}

test "no-ops without a current stack" {
    current = null;
    const m = mark();
    pin(Value.nil);
    restore(m);
    try std.testing.expectEqual(@as(usize, 0), m);
}
