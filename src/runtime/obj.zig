const std = @import("std");
const Allocator = std.mem.Allocator;
const value_mod = @import("value");
const Value = value_mod.Value;
const chunk_mod = @import("chunk");
const Chunk = chunk_mod.Chunk;
const intern_mod = @import("intern");
const InternTable = intern_mod.InternTable;
const stream_mod = @import("stream");

/// Object type tag for heap-allocated values.
pub const ObjType = enum(u8) {
    string,
    bytes,
    int_big,
    range,
    function,
    closure,
    upvalue,
    list,
    map,
    tuple,
    record,
    adt,
    stream,
    fiber,
    channel,
};

/// Common header for all heap-allocated objects.
/// Every Obj-derived type embeds this as its first field so
/// that we can always recover the type tag from any `*Obj` pointer.
pub const Obj = struct {
    obj_type: ObjType,
    /// GC flags: bit 0 = mark, bit 1 = permanent, bit 2 = tracked.
    gc_flags: u8 = 0,
    /// Linked-list pointer for GC traversal.
    next: ?*Obj = null,

    pub const GC_MARK: u8 = 0x01;
    /// Compiler constants: never swept and never traced.
    pub const GC_PERM: u8 = 0x02;
    /// On one of the GC's object lists.
    pub const GC_TRACKED: u8 = 0x04;

    pub inline fn isMarked(self: *const Obj) bool {
        return (self.gc_flags & GC_MARK) != 0;
    }

    pub inline fn setMarked(self: *Obj, marked: bool) void {
        if (marked) {
            self.gc_flags |= GC_MARK;
        } else {
            self.gc_flags &= ~GC_MARK;
        }
    }

    pub inline fn isPermanent(self: *const Obj) bool {
        return (self.gc_flags & GC_PERM) != 0;
    }

    pub inline fn isTracked(self: *const Obj) bool {
        return (self.gc_flags & GC_TRACKED) != 0;
    }

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
            .list => {
                const lst = ObjList.fromObj(self);
                lst.items.deinit(allocator);
                allocator.destroy(lst);
            },
            .map => {
                const m = ObjMap.fromObj(self);
                m.entries.deinit(allocator);
                allocator.destroy(m);
            },
            .tuple => {
                const t = ObjTuple.fromObj(self);
                allocator.free(t.fields);
                allocator.destroy(t);
            },
            .record => {
                const rec = ObjRecord.fromObj(self);
                allocator.free(rec.field_names);
                allocator.free(rec.field_values);
                allocator.destroy(rec);
            },
            .adt => {
                const a = ObjAdt.fromObj(self);
                allocator.free(a.payload);
                allocator.destroy(a);
            },
            .stream => {
                const s = ObjStream.fromObj(self);
                s.state.deinit(allocator);
                allocator.destroy(s.state);
                allocator.destroy(s);
            },
            .fiber => {
                // Fiber destroy is handled by ObjFiber.destroy which frees
                // the native stack. Use the fiber module's destroy method.
                const fiber_mod = @import("fiber");
                const f = fiber_mod.ObjFiber.fromObj(self);
                f.destroy(allocator);
            },
            .channel => {
                const channel_mod = @import("channel");
                const ch = channel_mod.ObjChannel.fromObj(self);
                ch.destroy(allocator);
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
    /// If `intern_table` is non-null, deduplicates: returns existing interned
    /// string if one matches, otherwise allocates and inserts into the table.
    /// If `intern_table` is null, allocates without interning (for bootstrapping/tests).
    pub fn create(allocator: Allocator, source: []const u8, intern_table: ?*InternTable) !*ObjString {
        const hash = hashBytes(source);

        // Check intern table for existing string.
        if (intern_table) |table| {
            if (table.findByContent(source, hash)) |existing| {
                return existing;
            }
        }

        const copy = try allocator.dupe(u8, source);
        errdefer allocator.free(copy);

        const str = try allocator.create(ObjString);
        str.* = .{
            .obj = .{ .obj_type = .string },
            .bytes = copy,
            .hash = hash,
        };

        // Insert into intern table if provided.
        if (intern_table) |table| {
            try table.insert(str);
        }

        return str;
    }

    /// Recover the containing `ObjString` from an `*Obj` pointer.
    pub fn fromObj(obj: *Obj) *ObjString {
        return @fieldParentPtr("obj", obj);
    }

    pub fn hashBytes(data: []const u8) u32 {
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
    start: i64,
    end: i64,
    step: i64,

    pub fn create(allocator: Allocator, start: i64, end_val: i64, step: i64) !*ObjRange {
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

/// Heap-allocated dynamic list (ordered, mutable-length).
pub const ObjList = struct {
    obj: Obj,
    items: std.ArrayListUnmanaged(Value),

    pub fn create(allocator: Allocator) !*ObjList {
        const lst = try allocator.create(ObjList);
        lst.* = .{
            .obj = .{ .obj_type = .list },
            .items = .empty,
        };
        return lst;
    }

    pub fn fromObj(o: *Obj) *ObjList {
        return @fieldParentPtr("obj", o);
    }
};

/// Context for hashing and comparing Values as map keys.
pub const ValueContext = struct {
    pub fn hash(_: ValueContext, v: Value) u32 {
        // Hash the raw u64 bits using FNV-1a for non-string types.
        // For strings, hash the string bytes for content equality.
        if (v.isObj() and v.asObj().obj_type == .string) {
            const str = ObjString.fromObj(v.asObj());
            return ObjString.hashBytes(str.bytes);
        }
        // FNV-1a on the raw u64 bits.
        const bits = v.bits;
        var h: u32 = 2166136261;
        inline for (0..8) |i| {
            h ^= @as(u32, @truncate(bits >> @intCast(i * 8)));
            h *%= 16777619;
        }
        return h;
    }

    pub fn eql(_: ValueContext, a: Value, b: Value, _: usize) bool {
        return Value.eql(a, b);
    }
};

/// Context for hashing and comparing Values by content: strings by bytes,
/// numbers numerically (1 == 1.0), and lists, tuples, records, maps and
/// ADTs recursively. Records and maps ignore key order. Other objects
/// (closures, streams, fibers, channels) compare by identity.
pub const StructuralContext = struct {
    pub fn hash(_: StructuralContext, v: Value) u32 {
        return structuralHash(v);
    }

    pub fn eql(_: StructuralContext, a: Value, b: Value, _: usize) bool {
        return structuralEql(a, b);
    }
};

fn mix(h: u64, x: u64) u64 {
    return (h ^ x) *% 0x100000001b3;
}

fn hashU64(x: u64) u64 {
    var h = x *% 0x9E3779B97F4A7C15;
    h ^= h >> 32;
    return h;
}

fn structuralHash(v: Value) u32 {
    return @truncate(structuralHash64(v));
}

fn structuralHash64(v: Value) u64 {
    if (v.isInteger()) return hashU64(@bitCast(v.asI64()));
    if (v.isFloat()) {
        const f = v.asFloat();
        // Integral floats hash like the equal int.
        if (@trunc(f) == f and f >= -9.2e18 and f <= 9.2e18) {
            return hashU64(@bitCast(@as(i64, @intFromFloat(f))));
        }
        return hashU64(@bitCast(f));
    }
    if (!v.isObj()) return hashU64(v.bits);
    const o = v.asObj();
    switch (o.obj_type) {
        .string => return std.hash.Wyhash.hash(0, ObjString.fromObj(o).bytes),
        .list => {
            var h: u64 = 0x6c697374;
            for (ObjList.fromObj(o).items.items) |item| h = mix(h, structuralHash64(item));
            return h;
        },
        .tuple => {
            var h: u64 = 0x7475706c;
            for (ObjTuple.fromObj(o).fields) |item| h = mix(h, structuralHash64(item));
            return h;
        },
        .record => {
            // Order-independent: sum of per-field hashes.
            const rec = ObjRecord.fromObj(o);
            var h: u64 = 0x7265636f;
            for (rec.field_names, rec.field_values) |name, val| {
                h +%= mix(std.hash.Wyhash.hash(1, name), structuralHash64(val));
            }
            return h;
        },
        .map => {
            const m = ObjMap.fromObj(o);
            var h: u64 = 0x6d6170;
            var it = m.entries.iterator();
            while (it.next()) |entry| {
                h +%= mix(structuralHash64(entry.key_ptr.*), structuralHash64(entry.value_ptr.*));
            }
            return h;
        },
        .adt => {
            const a = ObjAdt.fromObj(o);
            var h: u64 = mix(@as(u64, a.type_id) << 16 | a.variant_idx, 0x616474);
            for (a.payload) |item| h = mix(h, structuralHash64(item));
            return h;
        },
        else => return hashU64(v.bits),
    }
}

fn structuralEql(a: Value, b: Value) bool {
    if (a.bits == b.bits) return !(a.isFloat() and std.math.isNan(a.asFloat()));
    if (isNumber(a) and isNumber(b)) {
        if (a.isInteger() and b.isInteger()) return a.asI64() == b.asI64();
        return toF64(a) == toF64(b);
    }
    if (!a.isObj() or !b.isObj()) return false;
    const oa = a.asObj();
    const ob = b.asObj();
    if (oa.obj_type != ob.obj_type) return false;
    switch (oa.obj_type) {
        .string => return std.mem.eql(u8, ObjString.fromObj(oa).bytes, ObjString.fromObj(ob).bytes),
        .list => return sliceEql(ObjList.fromObj(oa).items.items, ObjList.fromObj(ob).items.items),
        .tuple => return sliceEql(ObjTuple.fromObj(oa).fields, ObjTuple.fromObj(ob).fields),
        .record => {
            const ra = ObjRecord.fromObj(oa);
            const rb = ObjRecord.fromObj(ob);
            if (ra.field_names.len != rb.field_names.len) return false;
            outer: for (ra.field_names, ra.field_values) |name, val| {
                for (rb.field_names, rb.field_values) |other_name, other_val| {
                    if (std.mem.eql(u8, name, other_name)) {
                        if (!structuralEql(val, other_val)) return false;
                        continue :outer;
                    }
                }
                return false;
            }
            return true;
        },
        .map => {
            const ma = ObjMap.fromObj(oa);
            const mb = ObjMap.fromObj(ob);
            if (ma.entries.count() != mb.entries.count()) return false;
            var it = ma.entries.iterator();
            while (it.next()) |entry| {
                const other = mb.entries.get(entry.key_ptr.*) orelse return false;
                if (!structuralEql(entry.value_ptr.*, other)) return false;
            }
            return true;
        },
        .adt => {
            const aa = ObjAdt.fromObj(oa);
            const ab = ObjAdt.fromObj(ob);
            if (aa.type_id != ab.type_id or aa.variant_idx != ab.variant_idx) return false;
            return sliceEql(aa.payload, ab.payload);
        },
        else => return false,
    }
}

/// Total order over values, used by sorting, min and max. Numbers compare
/// numerically (NaN after every other number), strings by bytes, lists,
/// tuples and ADT payloads lexicographically. Values of different kinds
/// order by kind: nil, bool, number, string, atom, list, tuple, adt, other.
pub fn compareValues(a: Value, b: Value) std.math.Order {
    const ka = kindRank(a);
    const kb = kindRank(b);
    if (ka != kb) return std.math.order(ka, kb);
    switch (ka) {
        0 => return .eq,
        1 => return std.math.order(@intFromBool(a.asBool()), @intFromBool(b.asBool())),
        2 => {
            if (a.isInteger() and b.isInteger()) return std.math.order(a.asI64(), b.asI64());
            const fa = toF64(a);
            const fb = toF64(b);
            const na = std.math.isNan(fa);
            const nb = std.math.isNan(fb);
            if (na or nb) return std.math.order(@intFromBool(na), @intFromBool(nb));
            return std.math.order(fa, fb);
        },
        3 => return std.mem.order(u8, ObjString.fromObj(a.asObj()).bytes, ObjString.fromObj(b.asObj()).bytes),
        5 => return sliceOrder(ObjList.fromObj(a.asObj()).items.items, ObjList.fromObj(b.asObj()).items.items),
        6 => return sliceOrder(ObjTuple.fromObj(a.asObj()).fields, ObjTuple.fromObj(b.asObj()).fields),
        7 => {
            const aa = ObjAdt.fromObj(a.asObj());
            const ab = ObjAdt.fromObj(b.asObj());
            if (aa.type_id != ab.type_id) return std.math.order(aa.type_id, ab.type_id);
            if (aa.variant_idx != ab.variant_idx) return std.math.order(aa.variant_idx, ab.variant_idx);
            return sliceOrder(aa.payload, ab.payload);
        },
        else => return std.math.order(a.bits, b.bits),
    }
}

fn kindRank(v: Value) u8 {
    if (v.isNil()) return 0;
    if (v.isBool()) return 1;
    if (isNumber(v)) return 2;
    if (v.isString()) return 3;
    if (v.isAtom()) return 4;
    if (v.isObjType(.list)) return 5;
    if (v.isObjType(.tuple)) return 6;
    if (v.isObjType(.adt)) return 7;
    return 8;
}

fn sliceOrder(a: []const Value, b: []const Value) std.math.Order {
    for (a[0..@min(a.len, b.len)], b[0..@min(a.len, b.len)]) |x, y| {
        const o = compareValues(x, y);
        if (o != .eq) return o;
    }
    return std.math.order(a.len, b.len);
}

fn isNumber(v: Value) bool {
    return v.isFloat() or v.isInteger();
}

fn toF64(v: Value) f64 {
    return if (v.isFloat()) v.asFloat() else @floatFromInt(v.asI64());
}

fn sliceEql(a: []const Value, b: []const Value) bool {
    if (a.len != b.len) return false;
    for (a, b) |x, y| {
        if (!structuralEql(x, y)) return false;
    }
    return true;
}

/// Heap-allocated ordered hash map (preserves insertion order).
pub const ObjMap = struct {
    obj: Obj,
    entries: std.ArrayHashMapUnmanaged(Value, Value, ValueContext, true),

    pub fn create(allocator: Allocator) !*ObjMap {
        const m = try allocator.create(ObjMap);
        m.* = .{
            .obj = .{ .obj_type = .map },
            .entries = .{},
        };
        return m;
    }

    pub fn fromObj(o: *Obj) *ObjMap {
        return @fieldParentPtr("obj", o);
    }
};

/// Heap-allocated fixed-size tuple.
pub const ObjTuple = struct {
    obj: Obj,
    fields: []Value,

    pub fn create(allocator: Allocator, values: []const Value) !*ObjTuple {
        const fields = try allocator.alloc(Value, values.len);
        @memcpy(fields, values);

        const t = try allocator.create(ObjTuple);
        t.* = .{
            .obj = .{ .obj_type = .tuple },
            .fields = fields,
        };
        return t;
    }

    pub fn fromObj(o: *Obj) *ObjTuple {
        return @fieldParentPtr("obj", o);
    }
};

/// Heap-allocated record (named fields).
pub const ObjRecord = struct {
    obj: Obj,
    field_count: u16,
    field_names: []const []const u8,
    field_values: []Value,

    pub fn create(allocator: Allocator, names: []const []const u8, values: []const Value) !*ObjRecord {
        std.debug.assert(names.len == values.len);
        const n = try allocator.alloc([]const u8, names.len);
        @memcpy(n, names);
        const v = try allocator.alloc(Value, values.len);
        @memcpy(v, values);

        const rec = try allocator.create(ObjRecord);
        rec.* = .{
            .obj = .{ .obj_type = .record },
            .field_count = @intCast(names.len),
            .field_names = n,
            .field_values = v,
        };
        return rec;
    }

    pub fn fromObj(o: *Obj) *ObjRecord {
        return @fieldParentPtr("obj", o);
    }
};

/// Heap-allocated algebraic data type variant.
pub const ObjAdt = struct {
    obj: Obj,
    type_id: u16,
    variant_idx: u16,
    payload: []Value,

    pub fn create(allocator: Allocator, type_id: u16, variant_idx: u16, values: []const Value) !*ObjAdt {
        const payload = try allocator.alloc(Value, values.len);
        @memcpy(payload, values);

        const a = try allocator.create(ObjAdt);
        a.* = .{
            .obj = .{ .obj_type = .adt },
            .type_id = type_id,
            .variant_idx = variant_idx,
            .payload = payload,
        };
        return a;
    }

    pub fn fromObj(o: *Obj) *ObjAdt {
        return @fieldParentPtr("obj", o);
    }
};

/// Heap-allocated stream object (pull-based lazy iterator).
pub const ObjStream = struct {
    obj: Obj,
    state: *stream_mod.StreamState,

    pub fn create(allocator: Allocator, state: *stream_mod.StreamState) !*ObjStream {
        const s = try allocator.create(ObjStream);
        s.* = .{
            .obj = .{ .obj_type = .stream },
            .state = state,
        };
        return s;
    }

    pub fn fromObj(o: *Obj) *ObjStream {
        return @fieldParentPtr("obj", o);
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

test "ObjList create, append, and destroy lifecycle" {
    const allocator = std.testing.allocator;
    const lst = try ObjList.create(allocator);
    defer lst.obj.destroy(allocator);

    try lst.items.append(allocator, Value.fromInt(1));
    try lst.items.append(allocator, Value.fromInt(2));
    try lst.items.append(allocator, Value.fromInt(3));

    try std.testing.expectEqual(ObjType.list, lst.obj.obj_type);
    try std.testing.expectEqual(@as(usize, 3), lst.items.items.len);
    try std.testing.expectEqual(@as(i32, 2), lst.items.items[1].asInt());
}

test "ObjList fromObj recovers original" {
    const allocator = std.testing.allocator;
    const lst = try ObjList.create(allocator);
    defer lst.obj.destroy(allocator);

    try lst.items.append(allocator, Value.fromInt(42));
    const obj_ptr: *Obj = &lst.obj;
    const recovered = ObjList.fromObj(obj_ptr);
    try std.testing.expectEqual(@as(usize, 1), recovered.items.items.len);
}

test "ObjMap create and destroy lifecycle" {
    const allocator = std.testing.allocator;
    const m = try ObjMap.create(allocator);
    defer m.obj.destroy(allocator);

    try m.entries.put(allocator, Value.fromInt(1), Value.fromInt(10));
    try m.entries.put(allocator, Value.fromInt(2), Value.fromInt(20));

    try std.testing.expectEqual(ObjType.map, m.obj.obj_type);
    try std.testing.expectEqual(@as(u32, 2), m.entries.count());
}

test "ObjTuple create and destroy lifecycle" {
    const allocator = std.testing.allocator;
    const values = [_]Value{ Value.fromInt(1), Value.fromInt(2), Value.fromInt(3) };
    const t = try ObjTuple.create(allocator, &values);
    defer t.obj.destroy(allocator);

    try std.testing.expectEqual(ObjType.tuple, t.obj.obj_type);
    try std.testing.expectEqual(@as(usize, 3), t.fields.len);
    try std.testing.expectEqual(@as(i32, 2), t.fields[1].asInt());
}

test "ObjRecord create and destroy lifecycle" {
    const allocator = std.testing.allocator;
    const names = [_][]const u8{ "x", "y" };
    const values = [_]Value{ Value.fromInt(10), Value.fromInt(20) };
    const rec = try ObjRecord.create(allocator, &names, &values);
    defer rec.obj.destroy(allocator);

    try std.testing.expectEqual(ObjType.record, rec.obj.obj_type);
    try std.testing.expectEqual(@as(u16, 2), rec.field_count);
    try std.testing.expectEqualStrings("x", rec.field_names[0]);
    try std.testing.expectEqual(@as(i32, 20), rec.field_values[1].asInt());
}

test "ObjAdt create and destroy lifecycle" {
    const allocator = std.testing.allocator;
    const payload = [_]Value{Value.fromInt(42)};
    const a = try ObjAdt.create(allocator, 0, 1, &payload);
    defer a.obj.destroy(allocator);

    try std.testing.expectEqual(ObjType.adt, a.obj.obj_type);
    try std.testing.expectEqual(@as(u16, 0), a.type_id);
    try std.testing.expectEqual(@as(u16, 1), a.variant_idx);
    try std.testing.expectEqual(@as(usize, 1), a.payload.len);
    try std.testing.expectEqual(@as(i32, 42), a.payload[0].asInt());
}

test "ObjAdt nullary variant (empty payload)" {
    const allocator = std.testing.allocator;
    const a = try ObjAdt.create(allocator, 0, 0, &[_]Value{});
    defer a.obj.destroy(allocator);

    try std.testing.expectEqual(@as(usize, 0), a.payload.len);
}

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
    const str = try ObjString.create(allocator, "hello", null);
    defer str.obj.destroy(allocator);

    try std.testing.expectEqualStrings("hello", str.bytes);
    try std.testing.expectEqual(ObjType.string, str.obj.obj_type);
    try std.testing.expect(str.hash != 0);
}

test "ObjString empty string" {
    const allocator = std.testing.allocator;
    const str = try ObjString.create(allocator, "", null);
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
    const str = try ObjString.create(allocator, "test", null);
    defer str.obj.destroy(allocator);

    const obj_ptr: *Obj = &str.obj;
    const recovered = ObjString.fromObj(obj_ptr);
    try std.testing.expectEqualStrings("test", recovered.bytes);
}
