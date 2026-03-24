const std = @import("std");
const Allocator = std.mem.Allocator;
const value_mod = @import("value");
const Value = value_mod.Value;
const obj_mod = @import("obj");
const ObjString = obj_mod.ObjString;
const ObjList = obj_mod.ObjList;
const ObjMap = obj_mod.ObjMap;
const ObjTuple = obj_mod.ObjTuple;
const ObjAdt = obj_mod.ObjAdt;
const ObjRecord = obj_mod.ObjRecord;
const ObjStream = obj_mod.ObjStream;
const ObjRange = obj_mod.ObjRange;
const stream_mod = @import("stream");
const StreamState = stream_mod.StreamState;
const json_mod = @import("json");

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

// ── VM Callback Mechanism ─────────────────────────────────────────────
// Higher-order builtins (List.map, List.filter, etc.) need to invoke user
// closures. Since builtins.zig cannot import vm.zig (circular dependency),
// we use a callback function pointer that the VM registers before calling
// any builtin.

/// Callback type: invoke a closure Value with given arguments, return result.
/// Returns null on error (the VM will have already recorded the error).
pub const CallClosureFn = *const fn (vm_ptr: *anyopaque, closure_val: Value, args: []const Value) ?Value;

/// Callback type: register a heap object with the VM for cleanup.
pub const TrackObjFn = *const fn (vm_ptr: *anyopaque, obj: *obj_mod.Obj) void;

/// Callback type: trigger a full GC collection.
pub const TriggerGCFn = *const fn (vm_ptr: *anyopaque) void;

/// GC statistics returned by the getGCStats callback.
pub const GCStats = struct {
    nursery_collections: u64,
    oldgen_collections: u64,
    bytes_freed: u64,
    last_pause_ns: u64,
    heap_size: usize,
    nursery_size: usize,
};

/// Callback type: get GC statistics from the VM.
pub const GetGCStatsFn = *const fn (vm_ptr: *anyopaque) GCStats;

/// Callback type: pop last error message from the VM (for par_map_result).
pub const PopLastErrorFn = *const fn (vm_ptr: *anyopaque) ?[]const u8;

/// Module-level state set by the VM before calling builtins.
/// Threadlocal so each worker thread has its own copy in multi-threaded mode.
threadlocal var current_vm: ?*anyopaque = null;
threadlocal var call_closure_fn: ?CallClosureFn = null;
threadlocal var track_obj_fn: ?TrackObjFn = null;
threadlocal var trigger_gc_fn: ?TriggerGCFn = null;
threadlocal var get_gc_stats_fn: ?GetGCStatsFn = null;
threadlocal var pop_last_error_fn: ?PopLastErrorFn = null;
/// Atom name table, set by the VM for builtins that need atom resolution (e.g. Json.encode).
threadlocal var current_atom_names: ?[]const []const u8 = null;

/// Called by the VM before dispatching a builtin function.
pub fn setVM(vm_ptr: *anyopaque, closure_fn: CallClosureFn, track_fn: TrackObjFn) void {
    current_vm = vm_ptr;
    call_closure_fn = closure_fn;
    track_obj_fn = track_fn;
}

/// Set GC callback functions (called separately since not all VMs have GC).
pub fn setGCCallbacks(gc_fn: TriggerGCFn, stats_fn: GetGCStatsFn) void {
    trigger_gc_fn = gc_fn;
    get_gc_stats_fn = stats_fn;
}

/// Set pop-last-error callback (for stream par_map_result error capture).
pub fn setPopLastError(f: PopLastErrorFn) void {
    pop_last_error_fn = f;
}

/// Set atom names for builtins that need atom resolution (Json.encode).
pub fn setAtomNames(names: []const []const u8) void {
    current_atom_names = names;
}

/// Called by the VM after a builtin returns.
pub fn clearVM() void {
    current_vm = null;
    call_closure_fn = null;
    track_obj_fn = null;
    trigger_gc_fn = null;
    get_gc_stats_fn = null;
    pop_last_error_fn = null;
    current_atom_names = null;
}

/// Track an intermediate heap object with the VM (called by builtins that
/// create objects other than the final return value).
pub fn trackObj(obj: *obj_mod.Obj) void {
    if (current_vm) |vm_ptr| {
        if (track_obj_fn) |f| {
            f(vm_ptr, obj);
        }
    }
}

// ── ADT Type Name Resolution ─────────────────────────────────────────
// Module-level state for ADT type/variant name lookup during formatting.
// Set by the VM (or e2e runner) so that formatValue can print ADTs as
// "Color.Red" instead of "ADT(2.0)".

/// ADT type info for display purposes.
pub const AdtTypeInfo = struct {
    name: []const u8,
    variant_names: []const []const u8,
};

/// Module-level ADT type registry for formatValue.
threadlocal var adt_type_info: ?[]const AdtTypeInfo = null;

/// Set ADT type info for formatting. Called once after compilation.
pub fn setAdtTypes(info: []const AdtTypeInfo) void {
    adt_type_info = info;
}

/// Clear ADT type info.
pub fn clearAdtTypes() void {
    adt_type_info = null;
}

/// Internal helper: invoke a closure from within a builtin.
fn callClosure(closure_val: Value, args: []const Value) NativeError!Value {
    const vm_ptr = current_vm orelse return error.RuntimeError;
    const fn_ptr = call_closure_fn orelse return error.RuntimeError;
    return fn_ptr(vm_ptr, closure_val, args) orelse return error.RuntimeError;
}

// ── ADT Helper Functions ──────────────────────────────────────────────
// Option: type_id=0, Some=variant_idx 0 (arity 1), None=variant_idx 1 (arity 0)
// Result: type_id=1, Ok=variant_idx 0 (arity 1), Err=variant_idx 1 (arity 1)

fn makeNone(allocator: Allocator) NativeError!Value {
    const adt = try ObjAdt.create(allocator, 0, 1, &[_]Value{});
    return Value.fromObj(&adt.obj);
}

fn makeSome(val: Value, allocator: Allocator) NativeError!Value {
    const adt = try ObjAdt.create(allocator, 0, 0, &[_]Value{val});
    return Value.fromObj(&adt.obj);
}

fn makeOk(val: Value, allocator: Allocator) NativeError!Value {
    const adt = try ObjAdt.create(allocator, 1, 0, &[_]Value{val});
    return Value.fromObj(&adt.obj);
}

fn makeErr(val: Value, allocator: Allocator) NativeError!Value {
    const adt = try ObjAdt.create(allocator, 1, 1, &[_]Value{val});
    return Value.fromObj(&adt.obj);
}

/// Check if value is an ADT with given type_id and variant_idx.
fn isAdtVariant(val: Value, type_id: u16, variant_idx: u16) bool {
    if (!val.isObjType(.adt)) return false;
    const adt = ObjAdt.fromObj(val.asObj());
    return adt.type_id == type_id and adt.variant_idx == variant_idx;
}

/// Get payload from ADT at given index.
fn adtPayload(val: Value, idx: usize) Value {
    const adt = ObjAdt.fromObj(val.asObj());
    return adt.payload[idx];
}

/// All built-in functions.
pub const builtins = [_]BuiltinDesc{
    // ── Core builtins (indices 0-7) ──────────────────────────────────
    .{ .name = "print", .func = &builtinPrint, .arity_min = 1, .arity_max = 1 },
    .{ .name = "str", .func = &builtinStr, .arity_min = 1, .arity_max = 1 },
    .{ .name = "len", .func = &builtinLen, .arity_min = 1, .arity_max = 1 },
    .{ .name = "type_of", .func = &builtinTypeOf, .arity_min = 1, .arity_max = 1 },
    .{ .name = "assert", .func = &builtinAssert, .arity_min = 1, .arity_max = 1 },
    .{ .name = "panic", .func = &builtinPanic, .arity_min = 1, .arity_max = 1 },
    .{ .name = "range", .func = &builtinRange, .arity_min = 1, .arity_max = 3 },
    .{ .name = "show", .func = &builtinShow, .arity_min = 1, .arity_max = 1 },

    // ── List module (indices 8-19) ───────────────────────────────────
    .{ .name = "List.get", .func = &builtinListGet, .arity_min = 2, .arity_max = 2 },
    .{ .name = "List.set", .func = &builtinListSet, .arity_min = 3, .arity_max = 3 },
    .{ .name = "List.append", .func = &builtinListAppend, .arity_min = 2, .arity_max = 2 },
    .{ .name = "List.length", .func = &builtinListLength, .arity_min = 1, .arity_max = 1 },
    .{ .name = "List.map", .func = &builtinListMap, .arity_min = 2, .arity_max = 2 },
    .{ .name = "List.filter", .func = &builtinListFilter, .arity_min = 2, .arity_max = 2 },
    .{ .name = "List.reduce", .func = &builtinListReduce, .arity_min = 3, .arity_max = 3 },
    .{ .name = "List.sort", .func = &builtinListSort, .arity_min = 1, .arity_max = 1 },
    .{ .name = "List.reverse", .func = &builtinListReverse, .arity_min = 1, .arity_max = 1 },
    .{ .name = "List.zip", .func = &builtinListZip, .arity_min = 2, .arity_max = 2 },
    .{ .name = "List.flatten", .func = &builtinListFlatten, .arity_min = 1, .arity_max = 1 },
    .{ .name = "List.contains", .func = &builtinListContains, .arity_min = 2, .arity_max = 2 },

    // ── Map module (indices 20-27) ───────────────────────────────────
    .{ .name = "Map.get", .func = &builtinMapGet, .arity_min = 2, .arity_max = 2 },
    .{ .name = "Map.set", .func = &builtinMapSet, .arity_min = 3, .arity_max = 3 },
    .{ .name = "Map.delete", .func = &builtinMapDelete, .arity_min = 2, .arity_max = 2 },
    .{ .name = "Map.keys", .func = &builtinMapKeys, .arity_min = 1, .arity_max = 1 },
    .{ .name = "Map.values", .func = &builtinMapValues, .arity_min = 1, .arity_max = 1 },
    .{ .name = "Map.merge", .func = &builtinMapMerge, .arity_min = 2, .arity_max = 2 },
    .{ .name = "Map.contains", .func = &builtinMapContains, .arity_min = 2, .arity_max = 2 },
    .{ .name = "Map.length", .func = &builtinMapLength, .arity_min = 1, .arity_max = 1 },

    // ── Tuple module (indices 28-29) ─────────────────────────────────
    .{ .name = "Tuple.get", .func = &builtinTupleGet, .arity_min = 2, .arity_max = 2 },
    .{ .name = "Tuple.length", .func = &builtinTupleLength, .arity_min = 1, .arity_max = 1 },

    // ── String module (indices 30-39) ────────────────────────────────
    .{ .name = "String.split", .func = &builtinStringSplit, .arity_min = 2, .arity_max = 2 },
    .{ .name = "String.trim", .func = &builtinStringTrim, .arity_min = 1, .arity_max = 1 },
    .{ .name = "String.join", .func = &builtinStringJoin, .arity_min = 2, .arity_max = 2 },
    .{ .name = "String.contains", .func = &builtinStringContains, .arity_min = 2, .arity_max = 2 },
    .{ .name = "String.replace", .func = &builtinStringReplace, .arity_min = 3, .arity_max = 3 },
    .{ .name = "String.starts_with", .func = &builtinStringStartsWith, .arity_min = 2, .arity_max = 2 },
    .{ .name = "String.ends_with", .func = &builtinStringEndsWith, .arity_min = 2, .arity_max = 2 },
    .{ .name = "String.to_lower", .func = &builtinStringToLower, .arity_min = 1, .arity_max = 1 },
    .{ .name = "String.to_upper", .func = &builtinStringToUpper, .arity_min = 1, .arity_max = 1 },
    .{ .name = "String.length", .func = &builtinStringLength, .arity_min = 1, .arity_max = 1 },

    // ── Result module (indices 40-47) ────────────────────────────────
    .{ .name = "Result.Ok", .func = &builtinResultOk, .arity_min = 1, .arity_max = 1 },
    .{ .name = "Result.Err", .func = &builtinResultErr, .arity_min = 1, .arity_max = 1 },
    .{ .name = "Result.map_ok", .func = &builtinResultMapOk, .arity_min = 2, .arity_max = 2 },
    .{ .name = "Result.map_err", .func = &builtinResultMapErr, .arity_min = 2, .arity_max = 2 },
    .{ .name = "Result.then", .func = &builtinResultThen, .arity_min = 2, .arity_max = 2 },
    .{ .name = "Result.unwrap_or", .func = &builtinResultUnwrapOr, .arity_min = 2, .arity_max = 2 },
    .{ .name = "Result.is_ok", .func = &builtinResultIsOk, .arity_min = 1, .arity_max = 1 },
    .{ .name = "Result.is_err", .func = &builtinResultIsErr, .arity_min = 1, .arity_max = 1 },

    // ── Option module (indices 48-54) ────────────────────────────────
    .{ .name = "Option.Some", .func = &builtinOptionSome, .arity_min = 1, .arity_max = 1 },
    .{ .name = "Option.None", .func = &builtinOptionNone, .arity_min = 0, .arity_max = 0 },
    .{ .name = "Option.map", .func = &builtinOptionMap, .arity_min = 2, .arity_max = 2 },
    .{ .name = "Option.unwrap_or", .func = &builtinOptionUnwrapOr, .arity_min = 2, .arity_max = 2 },
    .{ .name = "Option.is_some", .func = &builtinOptionIsSome, .arity_min = 1, .arity_max = 1 },
    .{ .name = "Option.is_none", .func = &builtinOptionIsNone, .arity_min = 1, .arity_max = 1 },
    .{ .name = "Option.to_result", .func = &builtinOptionToResult, .arity_min = 2, .arity_max = 2 },

    // ── List.filter_map (index 55) ──────────────────────────────────
    .{ .name = "List.filter_map", .func = &builtinListFilterMap, .arity_min = 2, .arity_max = 2 },

    // ── GC (indices 56-57) ──────────────────────────────────────────
    .{ .name = "gc", .func = &builtinGC, .arity_min = 0, .arity_max = 0 },
    .{ .name = "gc_stats", .func = &builtinGCStats, .arity_min = 0, .arity_max = 0 },

    // ── Stream sources (indices 58-59) ────────────────────────────
    .{ .name = "repeat", .func = &builtinRepeat, .arity_min = 1, .arity_max = 1 },
    .{ .name = "iterate", .func = &builtinIterate, .arity_min = 2, .arity_max = 2 },

    // ── Stream transforms (indices 60-63) ─────────────────────────
    .{ .name = "map", .func = &builtinMap, .arity_min = 2, .arity_max = 2 },
    .{ .name = "filter", .func = &builtinFilter, .arity_min = 2, .arity_max = 2 },
    .{ .name = "take", .func = &builtinTake, .arity_min = 2, .arity_max = 2 },
    .{ .name = "drop", .func = &builtinDrop, .arity_min = 2, .arity_max = 2 },

    // ── Stream terminals (indices 64-65) ──────────────────────────
    .{ .name = "collect", .func = &builtinCollect, .arity_min = 1, .arity_max = 1 },
    .{ .name = "count", .func = &builtinCount, .arity_min = 1, .arity_max = 1 },

    // ── Stream transforms continued (indices 66-73) ─────────────
    .{ .name = "flat_map", .func = &builtinFlatMap, .arity_min = 2, .arity_max = 2 },
    .{ .name = "filter_map", .func = &builtinFilterMap, .arity_min = 2, .arity_max = 2 },
    .{ .name = "scan", .func = &builtinScan, .arity_min = 3, .arity_max = 3 },
    .{ .name = "distinct", .func = &builtinDistinct, .arity_min = 1, .arity_max = 1 },
    .{ .name = "zip", .func = &builtinZip, .arity_min = 2, .arity_max = 2 },
    .{ .name = "flatten", .func = &builtinFlatten, .arity_min = 1, .arity_max = 1 },
    .{ .name = "tap", .func = &builtinTap, .arity_min = 2, .arity_max = 2 },
    .{ .name = "batch", .func = &builtinBatch, .arity_min = 2, .arity_max = 2 },

    // ── Stream terminals continued (indices 74-80) ──────────────
    .{ .name = "sum", .func = &builtinSum, .arity_min = 1, .arity_max = 1 },
    .{ .name = "reduce", .func = &builtinReduce, .arity_min = 3, .arity_max = 3 },
    .{ .name = "first", .func = &builtinFirst, .arity_min = 1, .arity_max = 1 },
    .{ .name = "last", .func = &builtinLast, .arity_min = 1, .arity_max = 1 },
    .{ .name = "each", .func = &builtinEach, .arity_min = 2, .arity_max = 2 },
    .{ .name = "min", .func = &builtinMin, .arity_min = 1, .arity_max = 1 },
    .{ .name = "max", .func = &builtinMax, .arity_min = 1, .arity_max = 1 },

    // ── Stream error handling (index 81) ────────────────────────
    .{ .name = "partition_result", .func = &builtinPartitionResult, .arity_min = 1, .arity_max = 1 },

    // ── Json module (indices 82-83) ──────────────────────────────
    .{ .name = "Json.decode", .func = &builtinJsonDecode, .arity_min = 1, .arity_max = 1 },
    .{ .name = "Json.encode", .func = &builtinJsonEncode, .arity_min = 1, .arity_max = 1 },

    // ── File I/O (indices 84-85) ────────────────────────────────
    .{ .name = "source", .func = &builtinSource, .arity_min = 1, .arity_max = 2 },
    .{ .name = "sink", .func = &builtinSink, .arity_min = 2, .arity_max = 3 },

    // ── Concurrency stream operators (indices 86-89) ─────────
    .{ .name = "par_map", .func = &builtinParMap, .arity_min = 2, .arity_max = 3 },
    .{ .name = "par_map_unordered", .func = &builtinParMapUnordered, .arity_min = 2, .arity_max = 3 },
    .{ .name = "par_map_result", .func = &builtinParMapResult, .arity_min = 2, .arity_max = 3 },
    .{ .name = "tick", .func = &builtinTick, .arity_min = 1, .arity_max = 1 },
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
            .list => {
                const lst = ObjList.fromObj(obj_ptr);
                try writer.writeByte('[');
                for (lst.items.items, 0..) |item, i| {
                    if (i > 0) try writer.writeAll(", ");
                    const item_str = try formatValue(item, allocator, atom_names);
                    defer allocator.free(item_str);
                    try writer.writeAll(item_str);
                }
                try writer.writeByte(']');
            },
            .map => {
                const m = ObjMap.fromObj(obj_ptr);
                try writer.writeByte('{');
                var it = m.entries.iterator();
                var first = true;
                while (it.next()) |entry| {
                    if (!first) try writer.writeAll(", ");
                    first = false;
                    const k_str = try formatValue(entry.key_ptr.*, allocator, atom_names);
                    defer allocator.free(k_str);
                    try writer.writeAll(k_str);
                    try writer.writeAll(": ");
                    const v_str = try formatValue(entry.value_ptr.*, allocator, atom_names);
                    defer allocator.free(v_str);
                    try writer.writeAll(v_str);
                }
                try writer.writeByte('}');
            },
            .tuple => {
                const t = ObjTuple.fromObj(obj_ptr);
                try writer.writeByte('(');
                for (t.fields, 0..) |field, i| {
                    if (i > 0) try writer.writeAll(", ");
                    const f_str = try formatValue(field, allocator, atom_names);
                    defer allocator.free(f_str);
                    try writer.writeAll(f_str);
                }
                try writer.writeByte(')');
            },
            .record => {
                const rec = obj_mod.ObjRecord.fromObj(obj_ptr);
                try writer.writeAll("{");
                for (0..rec.field_count) |i| {
                    if (i > 0) try writer.writeAll(", ");
                    try writer.writeAll(rec.field_names[i]);
                    try writer.writeAll(": ");
                    const v_str = try formatValue(rec.field_values[i], allocator, atom_names);
                    defer allocator.free(v_str);
                    try writer.writeAll(v_str);
                }
                try writer.writeAll("}");
            },
            .adt => {
                const a = ObjAdt.fromObj(obj_ptr);
                // Try to resolve type and variant names from module-level registry.
                if (adt_type_info) |info| {
                    if (a.type_id < info.len) {
                        const meta = info[a.type_id];
                        try writer.writeAll(meta.name);
                        try writer.writeByte('.');
                        if (a.variant_idx < meta.variant_names.len) {
                            try writer.writeAll(meta.variant_names[a.variant_idx]);
                        } else {
                            try writer.print("{d}", .{a.variant_idx});
                        }
                    } else {
                        try writer.print("ADT({d}.{d})", .{ a.type_id, a.variant_idx });
                    }
                } else {
                    try writer.print("ADT({d}.{d})", .{ a.type_id, a.variant_idx });
                }
                if (a.payload.len > 0) {
                    try writer.writeByte('(');
                    for (a.payload, 0..) |p, i| {
                        if (i > 0) try writer.writeAll(", ");
                        const p_str = try formatValue(p, allocator, atom_names);
                        defer allocator.free(p_str);
                        try writer.writeAll(p_str);
                    }
                    try writer.writeByte(')');
                }
            },
            .stream => {
                try writer.writeAll("<stream>");
            },
            .fiber => {
                try writer.writeAll("<fiber>");
            },
            .channel => {
                try writer.writeAll("<channel>");
            },
        }
    } else {
        try writer.writeAll("<unknown>");
    }

    return buf.toOwnedSlice(allocator);
}

// ── Core built-in implementations ────────────────────────────────────

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
    const str_obj = try ObjString.create(allocator, text, null);
    return Value.fromObj(&str_obj.obj);
}

/// len(value) -- for strings, lists, maps, tuples, records: return element count.
fn builtinLen(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = allocator;
    const val = args[0];
    if (val.isString()) {
        const str = ObjString.fromObj(val.asObj());
        return Value.fromInt(@intCast(str.bytes.len));
    }
    if (val.isObjType(.list)) {
        const lst = ObjList.fromObj(val.asObj());
        return Value.fromInt(@intCast(lst.items.items.len));
    }
    if (val.isObjType(.map)) {
        const m = ObjMap.fromObj(val.asObj());
        return Value.fromInt(@intCast(m.entries.count()));
    }
    if (val.isObjType(.tuple)) {
        const t = ObjTuple.fromObj(val.asObj());
        return Value.fromInt(@intCast(t.fields.len));
    }
    if (val.isObjType(.record)) {
        const rec = obj_mod.ObjRecord.fromObj(val.asObj());
        return Value.fromInt(@intCast(rec.field_count));
    }
    err_msg.* = "len() expects a string, list, map, tuple, or record argument";
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
    if (val.isObjType(.list)) return Value.fromAtom(9); // :list
    if (val.isObjType(.map)) return Value.fromAtom(10); // :map
    if (val.isObjType(.tuple)) return Value.fromAtom(11); // :tuple
    if (val.isObjType(.record)) return Value.fromAtom(12); // :record
    if (val.isObjType(.adt)) return Value.fromAtom(13); // :adt
    if (val.isObjType(.stream)) return Value.fromAtom(14); // :stream
    if (val.isObjType(.fiber)) return Value.fromAtom(15); // :fiber
    if (val.isObjType(.channel)) return Value.fromAtom(16); // :channel
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

// ── List module implementations ──────────────────────────────────────

/// List.get(list, index) -> Option: return Some(element) or None if out of bounds.
fn builtinListGet(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    if (!args[0].isObjType(.list)) {
        err_msg.* = "List.get expects a list as first argument";
        return error.RuntimeError;
    }
    if (!args[1].isInt()) {
        err_msg.* = "List.get expects an integer index";
        return error.RuntimeError;
    }
    const lst = ObjList.fromObj(args[0].asObj());
    const idx = args[1].asInt();
    if (idx < 0 or idx >= @as(i32, @intCast(lst.items.items.len))) {
        return makeNone(allocator);
    }
    return makeSome(lst.items.items[@intCast(idx)], allocator);
}

/// List.set(list, index, value) -> List: return new list with element replaced.
fn builtinListSet(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    if (!args[0].isObjType(.list)) {
        err_msg.* = "List.set expects a list as first argument";
        return error.RuntimeError;
    }
    if (!args[1].isInt()) {
        err_msg.* = "List.set expects an integer index";
        return error.RuntimeError;
    }
    const src = ObjList.fromObj(args[0].asObj());
    const idx = args[1].asInt();
    if (idx < 0 or idx >= @as(i32, @intCast(src.items.items.len))) {
        err_msg.* = "List.set index out of bounds";
        return error.RuntimeError;
    }
    const new_list = try ObjList.create(allocator);
    try new_list.items.appendSlice(allocator, src.items.items);
    new_list.items.items[@intCast(idx)] = args[2];
    return Value.fromObj(&new_list.obj);
}

/// List.append(list, value) -> List: return new list with element appended.
fn builtinListAppend(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    if (!args[0].isObjType(.list)) {
        err_msg.* = "List.append expects a list as first argument";
        return error.RuntimeError;
    }
    const src = ObjList.fromObj(args[0].asObj());
    const new_list = try ObjList.create(allocator);
    try new_list.items.appendSlice(allocator, src.items.items);
    try new_list.items.append(allocator, args[1]);
    return Value.fromObj(&new_list.obj);
}

/// List.length(list) -> Int: return list length.
fn builtinListLength(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = allocator;
    if (!args[0].isObjType(.list)) {
        err_msg.* = "List.length expects a list argument";
        return error.RuntimeError;
    }
    const lst = ObjList.fromObj(args[0].asObj());
    return Value.fromInt(@intCast(lst.items.items.len));
}

/// List.map(list, fn) -> List: apply fn to each element, return new list.
fn builtinListMap(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    if (!args[0].isObjType(.list)) {
        err_msg.* = "List.map expects a list as first argument";
        return error.RuntimeError;
    }
    const src = ObjList.fromObj(args[0].asObj());
    const closure_val = args[1];
    const new_list = try ObjList.create(allocator);
    try new_list.items.ensureTotalCapacity(allocator, src.items.items.len);
    for (src.items.items) |item| {
        const result = try callClosure(closure_val, &[_]Value{item});
        new_list.items.appendAssumeCapacity(result);
    }
    return Value.fromObj(&new_list.obj);
}

/// List.filter(list, fn) -> List: keep elements where fn returns true.
fn builtinListFilter(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    if (!args[0].isObjType(.list)) {
        err_msg.* = "List.filter expects a list as first argument";
        return error.RuntimeError;
    }
    const src = ObjList.fromObj(args[0].asObj());
    const closure_val = args[1];
    const new_list = try ObjList.create(allocator);
    for (src.items.items) |item| {
        const result = try callClosure(closure_val, &[_]Value{item});
        if (!isFalsy(result)) {
            try new_list.items.append(allocator, item);
        }
    }
    return Value.fromObj(&new_list.obj);
}

/// List.reduce(list, init, fn) -> Value: fold with accumulator.
fn builtinListReduce(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = allocator;
    if (!args[0].isObjType(.list)) {
        err_msg.* = "List.reduce expects a list as first argument";
        return error.RuntimeError;
    }
    const src = ObjList.fromObj(args[0].asObj());
    var acc = args[1];
    const closure_val = args[2];
    for (src.items.items) |item| {
        acc = try callClosure(closure_val, &[_]Value{ acc, item });
    }
    return acc;
}

/// List.sort(list) -> List: return sorted list.
fn builtinListSort(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    if (!args[0].isObjType(.list)) {
        err_msg.* = "List.sort expects a list argument";
        return error.RuntimeError;
    }
    const src = ObjList.fromObj(args[0].asObj());
    const new_list = try ObjList.create(allocator);
    try new_list.items.appendSlice(allocator, src.items.items);
    std.mem.sort(Value, new_list.items.items, {}, valueCompare);
    return Value.fromObj(&new_list.obj);
}

/// List.reverse(list) -> List: return reversed list.
fn builtinListReverse(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    if (!args[0].isObjType(.list)) {
        err_msg.* = "List.reverse expects a list argument";
        return error.RuntimeError;
    }
    const src = ObjList.fromObj(args[0].asObj());
    const new_list = try ObjList.create(allocator);
    try new_list.items.ensureTotalCapacity(allocator, src.items.items.len);
    var i: usize = src.items.items.len;
    while (i > 0) {
        i -= 1;
        new_list.items.appendAssumeCapacity(src.items.items[i]);
    }
    return Value.fromObj(&new_list.obj);
}

/// List.zip(list1, list2) -> List: return list of tuples.
fn builtinListZip(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    if (!args[0].isObjType(.list) or !args[1].isObjType(.list)) {
        err_msg.* = "List.zip expects two list arguments";
        return error.RuntimeError;
    }
    const lst1 = ObjList.fromObj(args[0].asObj());
    const lst2 = ObjList.fromObj(args[1].asObj());
    const min_len = @min(lst1.items.items.len, lst2.items.items.len);
    const new_list = try ObjList.create(allocator);
    try new_list.items.ensureTotalCapacity(allocator, min_len);
    for (0..min_len) |i| {
        const pair = [_]Value{ lst1.items.items[i], lst2.items.items[i] };
        const tup = try ObjTuple.create(allocator, &pair);
        trackObj(&tup.obj); // Track intermediate tuple for GC
        new_list.items.appendAssumeCapacity(Value.fromObj(&tup.obj));
    }
    return Value.fromObj(&new_list.obj);
}

/// List.flatten(list) -> List: flatten one level of nested lists.
fn builtinListFlatten(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    if (!args[0].isObjType(.list)) {
        err_msg.* = "List.flatten expects a list argument";
        return error.RuntimeError;
    }
    const src = ObjList.fromObj(args[0].asObj());
    const new_list = try ObjList.create(allocator);
    for (src.items.items) |item| {
        if (item.isObjType(.list)) {
            const inner = ObjList.fromObj(item.asObj());
            try new_list.items.appendSlice(allocator, inner.items.items);
        } else {
            try new_list.items.append(allocator, item);
        }
    }
    return Value.fromObj(&new_list.obj);
}

/// List.contains(list, value) -> Bool: check if value exists in list.
fn builtinListContains(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = allocator;
    if (!args[0].isObjType(.list)) {
        err_msg.* = "List.contains expects a list as first argument";
        return error.RuntimeError;
    }
    const lst = ObjList.fromObj(args[0].asObj());
    for (lst.items.items) |item| {
        if (Value.eql(item, args[1])) return Value.true_val;
    }
    return Value.false_val;
}

/// List.filter_map(list, fn) -> List: apply fn to each element, collect non-None results.
/// fn must return Option (Some(x) or None). Collects the x values from Some, skipping None.
fn builtinListFilterMap(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    if (!args[0].isObjType(.list)) {
        err_msg.* = "List.filter_map expects a list as first argument";
        return error.RuntimeError;
    }
    const src = ObjList.fromObj(args[0].asObj());
    const closure_val = args[1];
    const new_list = try ObjList.create(allocator);

    for (src.items.items) |elem| {
        const result = try callClosure(closure_val, &[_]Value{elem});
        // Check if result is Some(x): ADT with type_id=0 (Option), variant_idx=0 (Some)
        if (isAdtVariant(result, 0, 0)) {
            const payload = adtPayload(result, 0);
            try new_list.items.append(allocator, payload);
        }
        // None (variant_idx=1) or non-ADT results are skipped.
    }

    return Value.fromObj(&new_list.obj);
}

// ── GC module implementations ────────────────────────────────────────

/// Convert a u64 stat value to a Value. Uses inline i32 for small values,
/// heap-allocated ObjInt for large values.
fn u64ToValue(v: u64, allocator: Allocator) !Value {
    if (v <= @as(u64, @intCast(std.math.maxInt(i32)))) {
        return Value.fromInt(@intCast(v));
    }
    // Value exceeds i32 range; use i64 path (heap-allocates ObjInt if > i32).
    if (v <= @as(u64, @intCast(std.math.maxInt(i64)))) {
        return Value.fromI64(@intCast(v), allocator);
    }
    // Extremely large u64; just return 0 (unlikely in practice).
    return Value.fromInt(0);
}

/// gc() -> nil: trigger a full garbage collection cycle.
fn builtinGC(_: []const Value, _: Allocator, _: *[]const u8) NativeError!Value {
    if (current_vm) |vm_ptr| {
        if (trigger_gc_fn) |f| {
            f(vm_ptr);
        }
    }
    return Value.nil;
}

/// gc_stats() -> record: return a record with GC statistics.
/// Fields: nursery_collections, oldgen_collections, bytes_freed,
///         last_pause_ns, heap_size, nursery_size.
fn builtinGCStats(_: []const Value, allocator: Allocator, _: *[]const u8) NativeError!Value {
    const stats: GCStats = blk: {
        if (current_vm) |vm_ptr| {
            if (get_gc_stats_fn) |f| {
                break :blk f(vm_ptr);
            }
        }
        // No GC available: return zeros.
        break :blk GCStats{
            .nursery_collections = 0,
            .oldgen_collections = 0,
            .bytes_freed = 0,
            .last_pause_ns = 0,
            .heap_size = 0,
            .nursery_size = 0,
        };
    };

    const field_names = [_][]const u8{
        "nursery_collections",
        "oldgen_collections",
        "bytes_freed",
        "last_pause_ns",
        "heap_size",
        "nursery_size",
    };
    const field_values = [_]Value{
        try u64ToValue(stats.nursery_collections, allocator),
        try u64ToValue(stats.oldgen_collections, allocator),
        try u64ToValue(stats.bytes_freed, allocator),
        try u64ToValue(stats.last_pause_ns, allocator),
        try u64ToValue(stats.heap_size, allocator),
        try u64ToValue(stats.nursery_size, allocator),
    };

    const rec = try ObjRecord.create(allocator, &field_names, &field_values);
    return Value.fromObj(&rec.obj);
}

// ── Map module implementations ───────────────────────────────────────

/// Map.get(map, key) -> Option: return Some(value) or None.
fn builtinMapGet(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    if (!args[0].isObjType(.map)) {
        err_msg.* = "Map.get expects a map as first argument";
        return error.RuntimeError;
    }
    const m = ObjMap.fromObj(args[0].asObj());
    if (m.entries.get(args[1])) |val| {
        return makeSome(val, allocator);
    }
    return makeNone(allocator);
}

/// Map.set(map, key, value) -> Map: return new map with key set.
fn builtinMapSet(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    if (!args[0].isObjType(.map)) {
        err_msg.* = "Map.set expects a map as first argument";
        return error.RuntimeError;
    }
    const src = ObjMap.fromObj(args[0].asObj());
    const new_map = try ObjMap.create(allocator);
    // Copy all existing entries.
    var it = src.entries.iterator();
    while (it.next()) |entry| {
        try new_map.entries.put(allocator, entry.key_ptr.*, entry.value_ptr.*);
    }
    // Set/overwrite the key.
    try new_map.entries.put(allocator, args[1], args[2]);
    return Value.fromObj(&new_map.obj);
}

/// Map.delete(map, key) -> Map: return new map without key.
fn builtinMapDelete(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    if (!args[0].isObjType(.map)) {
        err_msg.* = "Map.delete expects a map as first argument";
        return error.RuntimeError;
    }
    const src = ObjMap.fromObj(args[0].asObj());
    const new_map = try ObjMap.create(allocator);
    var it_src = src.entries.iterator();
    while (it_src.next()) |entry| {
        if (!Value.eql(entry.key_ptr.*, args[1])) {
            try new_map.entries.put(allocator, entry.key_ptr.*, entry.value_ptr.*);
        }
    }
    return Value.fromObj(&new_map.obj);
}

/// Map.keys(map) -> List: return list of keys.
fn builtinMapKeys(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    if (!args[0].isObjType(.map)) {
        err_msg.* = "Map.keys expects a map argument";
        return error.RuntimeError;
    }
    const m = ObjMap.fromObj(args[0].asObj());
    const new_list = try ObjList.create(allocator);
    var it = m.entries.iterator();
    while (it.next()) |entry| {
        try new_list.items.append(allocator, entry.key_ptr.*);
    }
    return Value.fromObj(&new_list.obj);
}

/// Map.values(map) -> List: return list of values.
fn builtinMapValues(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    if (!args[0].isObjType(.map)) {
        err_msg.* = "Map.values expects a map argument";
        return error.RuntimeError;
    }
    const m = ObjMap.fromObj(args[0].asObj());
    const new_list = try ObjList.create(allocator);
    var it = m.entries.iterator();
    while (it.next()) |entry| {
        try new_list.items.append(allocator, entry.value_ptr.*);
    }
    return Value.fromObj(&new_list.obj);
}

/// Map.merge(map1, map2) -> Map: return new map with entries from both (map2 wins on conflict).
fn builtinMapMerge(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    if (!args[0].isObjType(.map) or !args[1].isObjType(.map)) {
        err_msg.* = "Map.merge expects two map arguments";
        return error.RuntimeError;
    }
    const m1 = ObjMap.fromObj(args[0].asObj());
    const m2 = ObjMap.fromObj(args[1].asObj());
    const new_map = try ObjMap.create(allocator);
    // Copy m1.
    var it1 = m1.entries.iterator();
    while (it1.next()) |entry| {
        try new_map.entries.put(allocator, entry.key_ptr.*, entry.value_ptr.*);
    }
    // Copy m2 (overwrites m1 on conflict).
    var it2 = m2.entries.iterator();
    while (it2.next()) |entry| {
        try new_map.entries.put(allocator, entry.key_ptr.*, entry.value_ptr.*);
    }
    return Value.fromObj(&new_map.obj);
}

/// Map.contains(map, key) -> Bool: check if key exists.
fn builtinMapContains(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = allocator;
    if (!args[0].isObjType(.map)) {
        err_msg.* = "Map.contains expects a map as first argument";
        return error.RuntimeError;
    }
    const m = ObjMap.fromObj(args[0].asObj());
    return Value.fromBool(m.entries.get(args[1]) != null);
}

/// Map.length(map) -> Int: return entry count.
fn builtinMapLength(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = allocator;
    if (!args[0].isObjType(.map)) {
        err_msg.* = "Map.length expects a map argument";
        return error.RuntimeError;
    }
    const m = ObjMap.fromObj(args[0].asObj());
    return Value.fromInt(@intCast(m.entries.count()));
}

// ── Tuple module implementations ─────────────────────────────────────

/// Tuple.get(tuple, index) -> Option: return Some(element) or None if out of bounds.
fn builtinTupleGet(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    if (!args[0].isObjType(.tuple)) {
        err_msg.* = "Tuple.get expects a tuple as first argument";
        return error.RuntimeError;
    }
    if (!args[1].isInt()) {
        err_msg.* = "Tuple.get expects an integer index";
        return error.RuntimeError;
    }
    const t = ObjTuple.fromObj(args[0].asObj());
    const idx = args[1].asInt();
    if (idx < 0 or idx >= @as(i32, @intCast(t.fields.len))) {
        return makeNone(allocator);
    }
    return makeSome(t.fields[@intCast(idx)], allocator);
}

/// Tuple.length(tuple) -> Int: return tuple size.
fn builtinTupleLength(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = allocator;
    if (!args[0].isObjType(.tuple)) {
        err_msg.* = "Tuple.length expects a tuple argument";
        return error.RuntimeError;
    }
    const t = ObjTuple.fromObj(args[0].asObj());
    return Value.fromInt(@intCast(t.fields.len));
}

// ── String module implementations ────────────────────────────────────

/// String.split(str, sep) -> List: split string by separator.
fn builtinStringSplit(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    if (!args[0].isString() or !args[1].isString()) {
        err_msg.* = "String.split expects two string arguments";
        return error.RuntimeError;
    }
    const str_bytes = ObjString.fromObj(args[0].asObj()).bytes;
    const sep_bytes = ObjString.fromObj(args[1].asObj()).bytes;
    const new_list = try ObjList.create(allocator);

    if (sep_bytes.len == 0) {
        // Empty separator: split into individual characters.
        for (str_bytes) |byte| {
            const s = try ObjString.create(allocator, &[_]u8{byte}, null);
            trackObj(&s.obj);
            try new_list.items.append(allocator, Value.fromObj(&s.obj));
        }
    } else if (sep_bytes.len == 1) {
        // Single-byte separator: use splitScalar.
        var it = std.mem.splitScalar(u8, str_bytes, sep_bytes[0]);
        while (it.next()) |part| {
            const s = try ObjString.create(allocator, part, null);
            trackObj(&s.obj);
            try new_list.items.append(allocator, Value.fromObj(&s.obj));
        }
    } else {
        // Multi-byte separator: manual scan.
        var pos: usize = 0;
        var seg_start: usize = 0;
        while (pos + sep_bytes.len <= str_bytes.len) {
            if (std.mem.eql(u8, str_bytes[pos..][0..sep_bytes.len], sep_bytes)) {
                const s = try ObjString.create(allocator, str_bytes[seg_start..pos], null);
                trackObj(&s.obj);
                try new_list.items.append(allocator, Value.fromObj(&s.obj));
                pos += sep_bytes.len;
                seg_start = pos;
            } else {
                pos += 1;
            }
        }
        // Remaining segment.
        const s = try ObjString.create(allocator, str_bytes[seg_start..], null);
        trackObj(&s.obj);
        try new_list.items.append(allocator, Value.fromObj(&s.obj));
    }

    return Value.fromObj(&new_list.obj);
}

/// String.trim(str) -> String: trim leading/trailing whitespace.
fn builtinStringTrim(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    if (!args[0].isString()) {
        err_msg.* = "String.trim expects a string argument";
        return error.RuntimeError;
    }
    const str_bytes = ObjString.fromObj(args[0].asObj()).bytes;
    const trimmed = std.mem.trim(u8, str_bytes, " \t\n\r");
    const s = try ObjString.create(allocator, trimmed, null);
    return Value.fromObj(&s.obj);
}

/// String.join(list, sep) -> String: join list of strings with separator.
fn builtinStringJoin(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    if (!args[0].isObjType(.list)) {
        err_msg.* = "String.join expects a list as first argument";
        return error.RuntimeError;
    }
    if (!args[1].isString()) {
        err_msg.* = "String.join expects a string separator";
        return error.RuntimeError;
    }
    const lst = ObjList.fromObj(args[0].asObj());
    const sep = ObjString.fromObj(args[1].asObj()).bytes;

    var buf = std.ArrayListUnmanaged(u8){};
    for (lst.items.items, 0..) |item, i| {
        if (i > 0) try buf.appendSlice(allocator, sep);
        if (item.isString()) {
            try buf.appendSlice(allocator, ObjString.fromObj(item.asObj()).bytes);
        } else {
            const formatted = try formatValue(item, allocator, null);
            defer allocator.free(formatted);
            try buf.appendSlice(allocator, formatted);
        }
    }
    const result_bytes = try buf.toOwnedSlice(allocator);
    defer allocator.free(result_bytes);
    const s = try ObjString.create(allocator, result_bytes, null);
    return Value.fromObj(&s.obj);
}

/// String.contains(str, sub) -> Bool: check if substring exists.
fn builtinStringContains(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = allocator;
    if (!args[0].isString() or !args[1].isString()) {
        err_msg.* = "String.contains expects two string arguments";
        return error.RuntimeError;
    }
    const haystack = ObjString.fromObj(args[0].asObj()).bytes;
    const needle = ObjString.fromObj(args[1].asObj()).bytes;
    return Value.fromBool(std.mem.indexOf(u8, haystack, needle) != null);
}

/// String.replace(str, old, new) -> String: replace all occurrences.
fn builtinStringReplace(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    if (!args[0].isString() or !args[1].isString() or !args[2].isString()) {
        err_msg.* = "String.replace expects three string arguments";
        return error.RuntimeError;
    }
    const str_bytes = ObjString.fromObj(args[0].asObj()).bytes;
    const old_bytes = ObjString.fromObj(args[1].asObj()).bytes;
    const new_bytes = ObjString.fromObj(args[2].asObj()).bytes;

    if (old_bytes.len == 0) {
        // Empty old string: return original.
        const s = try ObjString.create(allocator, str_bytes, null);
        return Value.fromObj(&s.obj);
    }

    var buf = std.ArrayListUnmanaged(u8){};
    var pos: usize = 0;
    while (pos <= str_bytes.len) {
        if (pos + old_bytes.len <= str_bytes.len and
            std.mem.eql(u8, str_bytes[pos..][0..old_bytes.len], old_bytes))
        {
            try buf.appendSlice(allocator, new_bytes);
            pos += old_bytes.len;
        } else {
            if (pos < str_bytes.len) {
                try buf.append(allocator, str_bytes[pos]);
                pos += 1;
            } else {
                break;
            }
        }
    }
    const result_bytes = try buf.toOwnedSlice(allocator);
    defer allocator.free(result_bytes);
    const s = try ObjString.create(allocator, result_bytes, null);
    return Value.fromObj(&s.obj);
}

/// String.starts_with(str, prefix) -> Bool.
fn builtinStringStartsWith(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = allocator;
    if (!args[0].isString() or !args[1].isString()) {
        err_msg.* = "String.starts_with expects two string arguments";
        return error.RuntimeError;
    }
    const str_bytes = ObjString.fromObj(args[0].asObj()).bytes;
    const prefix = ObjString.fromObj(args[1].asObj()).bytes;
    return Value.fromBool(std.mem.startsWith(u8, str_bytes, prefix));
}

/// String.ends_with(str, suffix) -> Bool.
fn builtinStringEndsWith(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = allocator;
    if (!args[0].isString() or !args[1].isString()) {
        err_msg.* = "String.ends_with expects two string arguments";
        return error.RuntimeError;
    }
    const str_bytes = ObjString.fromObj(args[0].asObj()).bytes;
    const suffix = ObjString.fromObj(args[1].asObj()).bytes;
    return Value.fromBool(std.mem.endsWith(u8, str_bytes, suffix));
}

/// String.to_lower(str) -> String: lowercase ASCII characters.
fn builtinStringToLower(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    if (!args[0].isString()) {
        err_msg.* = "String.to_lower expects a string argument";
        return error.RuntimeError;
    }
    const str_bytes = ObjString.fromObj(args[0].asObj()).bytes;
    const lowered = try allocator.alloc(u8, str_bytes.len);
    defer allocator.free(lowered);
    for (str_bytes, 0..) |byte, i| {
        lowered[i] = std.ascii.toLower(byte);
    }
    const s = try ObjString.create(allocator, lowered, null);
    return Value.fromObj(&s.obj);
}

/// String.to_upper(str) -> String: uppercase ASCII characters.
fn builtinStringToUpper(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    if (!args[0].isString()) {
        err_msg.* = "String.to_upper expects a string argument";
        return error.RuntimeError;
    }
    const str_bytes = ObjString.fromObj(args[0].asObj()).bytes;
    const uppered = try allocator.alloc(u8, str_bytes.len);
    defer allocator.free(uppered);
    for (str_bytes, 0..) |byte, i| {
        uppered[i] = std.ascii.toUpper(byte);
    }
    const s = try ObjString.create(allocator, uppered, null);
    return Value.fromObj(&s.obj);
}

/// String.length(str) -> Int: byte length.
fn builtinStringLength(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = allocator;
    if (!args[0].isString()) {
        err_msg.* = "String.length expects a string argument";
        return error.RuntimeError;
    }
    const str_bytes = ObjString.fromObj(args[0].asObj()).bytes;
    return Value.fromInt(@intCast(str_bytes.len));
}

// ── Result module implementations ────────────────────────────────────

/// Result.Ok(value) -> Result: create Ok variant.
fn builtinResultOk(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = err_msg;
    return makeOk(args[0], allocator);
}

/// Result.Err(error_val) -> Result: create Err variant.
fn builtinResultErr(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = err_msg;
    return makeErr(args[0], allocator);
}

/// Result.map_ok(result, fn) -> Result: if Ok, apply fn to payload and wrap in Ok.
fn builtinResultMapOk(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    if (!args[0].isObjType(.adt)) {
        err_msg.* = "Result.map_ok expects a Result as first argument";
        return error.RuntimeError;
    }
    // Ok: type_id=1, variant_idx=0
    if (isAdtVariant(args[0], 1, 0)) {
        const payload = adtPayload(args[0], 0);
        const result = try callClosure(args[1], &[_]Value{payload});
        return makeOk(result, allocator);
    }
    // Err: return unchanged.
    return args[0];
}

/// Result.map_err(result, fn) -> Result: if Err, apply fn to error payload and wrap in Err.
fn builtinResultMapErr(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    if (!args[0].isObjType(.adt)) {
        err_msg.* = "Result.map_err expects a Result as first argument";
        return error.RuntimeError;
    }
    // Err: type_id=1, variant_idx=1
    if (isAdtVariant(args[0], 1, 1)) {
        const payload = adtPayload(args[0], 0);
        const result = try callClosure(args[1], &[_]Value{payload});
        return makeErr(result, allocator);
    }
    // Ok: return unchanged.
    return args[0];
}

/// Result.then(result, fn) -> Result: if Ok, apply fn to payload (fn must return a Result).
fn builtinResultThen(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = allocator;
    if (!args[0].isObjType(.adt)) {
        err_msg.* = "Result.then expects a Result as first argument";
        return error.RuntimeError;
    }
    if (isAdtVariant(args[0], 1, 0)) {
        const payload = adtPayload(args[0], 0);
        return callClosure(args[1], &[_]Value{payload});
    }
    // Err: return unchanged.
    return args[0];
}

/// Result.unwrap_or(result, default) -> Value: if Ok, return payload. If Err, return default.
fn builtinResultUnwrapOr(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = allocator;
    _ = err_msg;
    if (args[0].isObjType(.adt) and isAdtVariant(args[0], 1, 0)) {
        return adtPayload(args[0], 0);
    }
    return args[1];
}

/// Result.is_ok(result) -> Bool.
fn builtinResultIsOk(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = allocator;
    _ = err_msg;
    return Value.fromBool(args[0].isObjType(.adt) and isAdtVariant(args[0], 1, 0));
}

/// Result.is_err(result) -> Bool.
fn builtinResultIsErr(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = allocator;
    _ = err_msg;
    return Value.fromBool(args[0].isObjType(.adt) and isAdtVariant(args[0], 1, 1));
}

// ── Option module implementations ────────────────────────────────────

/// Option.Some(value) -> Option: create Some variant.
fn builtinOptionSome(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = err_msg;
    return makeSome(args[0], allocator);
}

/// Option.None -> Option: create None variant.
fn builtinOptionNone(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = args;
    _ = err_msg;
    return makeNone(allocator);
}

/// Option.map(option, fn) -> Option: if Some, apply fn and wrap in Some. If None, return None.
fn builtinOptionMap(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    if (!args[0].isObjType(.adt)) {
        err_msg.* = "Option.map expects an Option as first argument";
        return error.RuntimeError;
    }
    // Some: type_id=0, variant_idx=0
    if (isAdtVariant(args[0], 0, 0)) {
        const payload = adtPayload(args[0], 0);
        const result = try callClosure(args[1], &[_]Value{payload});
        return makeSome(result, allocator);
    }
    // None: return None.
    return makeNone(allocator);
}

/// Option.unwrap_or(option, default) -> Value: if Some, return payload. If None, return default.
fn builtinOptionUnwrapOr(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = allocator;
    _ = err_msg;
    if (args[0].isObjType(.adt) and isAdtVariant(args[0], 0, 0)) {
        return adtPayload(args[0], 0);
    }
    return args[1];
}

/// Option.is_some(option) -> Bool.
fn builtinOptionIsSome(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = allocator;
    _ = err_msg;
    return Value.fromBool(args[0].isObjType(.adt) and isAdtVariant(args[0], 0, 0));
}

/// Option.is_none(option) -> Bool.
fn builtinOptionIsNone(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = allocator;
    _ = err_msg;
    return Value.fromBool(args[0].isObjType(.adt) and isAdtVariant(args[0], 0, 1));
}

/// Option.to_result(option, error_val) -> Result: Some(v) -> Ok(v), None -> Err(error_val).
fn builtinOptionToResult(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = err_msg;
    if (args[0].isObjType(.adt) and isAdtVariant(args[0], 0, 0)) {
        return makeOk(adtPayload(args[0], 0), allocator);
    }
    return makeErr(args[1], allocator);
}

// ── Helpers ───────────────────────────────────────────────────────────

/// Check if a value is "falsy" (nil or false).
pub fn isFalsy(val: Value) bool {
    if (val.isNil()) return true;
    if (val.isBool()) return !val.asBool();
    return false;
}

/// Compare two Values for sorting. Returns true if a < b.
/// int/float are numeric, strings are lexicographic, other types by raw bits.
fn valueCompare(ctx: void, a: Value, b: Value) bool {
    _ = ctx;
    // Both ints.
    if (a.isInt() and b.isInt()) return a.asInt() < b.asInt();
    // Both floats.
    if (a.isFloat() and b.isFloat()) return a.asFloat() < b.asFloat();
    // Int vs float.
    if (a.isInt() and b.isFloat()) return @as(f64, @floatFromInt(a.asInt())) < b.asFloat();
    if (a.isFloat() and b.isInt()) return a.asFloat() < @as(f64, @floatFromInt(b.asInt()));
    // Both strings.
    if (a.isString() and b.isString()) {
        const sa = ObjString.fromObj(a.asObj()).bytes;
        const sb = ObjString.fromObj(b.asObj()).bytes;
        return std.mem.order(u8, sa, sb) == .lt;
    }
    // Fallback: compare raw bits.
    return a.bits < b.bits;
}

// ── Stream builtin implementations ────────────────────────────────────

/// Set up stream.zig callbacks from the builtins module-level state.
/// The stream module needs call_closure and track_obj to function.
fn setStreamCallbacks() void {
    if (current_vm) |vm_ptr| {
        const closure_fn = call_closure_fn orelse return;
        const tfn = track_obj_fn orelse return;
        stream_mod.setVM(vm_ptr, closure_fn, tfn);
        if (pop_last_error_fn) |f| {
            stream_mod.setPopLastError(f);
        }
    }
}

/// Helper: create an ObjStream with the given state, track it with the VM.
fn createStream(state: *StreamState, allocator: Allocator) NativeError!Value {
    const s = try ObjStream.create(allocator, state);
    trackObj(&s.obj);
    return Value.fromObj(&s.obj);
}

/// Helper: if value is a range, auto-wrap into a stream with range_iter state.
fn autoWrapRange(val: Value, allocator: Allocator) NativeError!Value {
    if (val.isObjType(.range)) {
        const r = ObjRange.fromObj(val.asObj());
        const state = try allocator.create(StreamState);
        state.* = .{ .range_iter = .{
            .current = r.start,
            .end = r.end,
            .step = r.step,
        } };
        return createStream(state, allocator);
    }
    return val;
}

/// repeat(value) -> Stream: infinite stream that always yields value.
fn builtinRepeat(args: []const Value, allocator: Allocator, _: *[]const u8) NativeError!Value {
    const state = try allocator.create(StreamState);
    state.* = .{ .repeat_iter = .{ .value = args[0] } };
    return createStream(state, allocator);
}

/// iterate(init, fn) -> Stream: stream where each element is fn applied to previous.
fn builtinIterate(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    if (!args[1].isObjType(.closure)) {
        err_msg.* = "iterate() expects a function as second argument";
        return error.RuntimeError;
    }
    const state = try allocator.create(StreamState);
    state.* = .{ .iterate_iter = .{
        .current = args[0],
        .fn_val = args[1],
        .started = false,
    } };
    return createStream(state, allocator);
}

/// map(stream_or_list, fn) -> Stream or List: overloaded dispatch.
/// For streams: creates a lazy map_op. For lists: delegates to List.map.
fn builtinMap(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    // Auto-wrap range to stream if needed.
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }

    if (first.isObjType(.stream)) {
        if (!args[1].isObjType(.closure)) {
            err_msg.* = "map() expects a function as second argument";
            return error.RuntimeError;
        }
        const state = try allocator.create(StreamState);
        state.* = .{ .map_op = .{
            .upstream = first,
            .fn_val = args[1],
        } };
        return createStream(state, allocator);
    }
    if (first.isObjType(.list)) {
        // Delegate to existing List.map.
        return builtinListMap(args, allocator, err_msg);
    }
    err_msg.* = "map() expects a stream or list as first argument";
    return error.RuntimeError;
}

/// filter(stream_or_list, fn) -> Stream or List: overloaded dispatch.
fn builtinFilter(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    // Auto-wrap range to stream if needed.
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }

    if (first.isObjType(.stream)) {
        if (!args[1].isObjType(.closure)) {
            err_msg.* = "filter() expects a function as second argument";
            return error.RuntimeError;
        }
        const state = try allocator.create(StreamState);
        state.* = .{ .filter_op = .{
            .upstream = first,
            .fn_val = args[1],
        } };
        return createStream(state, allocator);
    }
    if (first.isObjType(.list)) {
        // Delegate to existing List.filter.
        return builtinListFilter(args, allocator, err_msg);
    }
    err_msg.* = "filter() expects a stream or list as first argument";
    return error.RuntimeError;
}

/// take(stream, n) -> Stream: take first n elements from stream.
fn builtinTake(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    // Auto-wrap range to stream if needed.
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }

    if (!first.isObjType(.stream)) {
        err_msg.* = "take() expects a stream as first argument";
        return error.RuntimeError;
    }
    if (!args[1].isInt()) {
        err_msg.* = "take() expects an integer as second argument";
        return error.RuntimeError;
    }
    const state = try allocator.create(StreamState);
    state.* = .{ .take_op = .{
        .upstream = first,
        .remaining = args[1].asInt(),
    } };
    return createStream(state, allocator);
}

/// drop(stream, n) -> Stream: skip first n elements from stream.
fn builtinDrop(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    // Auto-wrap range to stream if needed.
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }

    if (!first.isObjType(.stream)) {
        err_msg.* = "drop() expects a stream as first argument";
        return error.RuntimeError;
    }
    if (!args[1].isInt()) {
        err_msg.* = "drop() expects an integer as second argument";
        return error.RuntimeError;
    }
    const state = try allocator.create(StreamState);
    state.* = .{ .drop_op = .{
        .upstream = first,
        .remaining = args[1].asInt(),
        .started = false,
    } };
    return createStream(state, allocator);
}

/// collect(stream) -> List: terminal that pulls all elements into a list.
fn builtinCollect(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    // Auto-wrap range to stream if needed.
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }

    if (!first.isObjType(.stream)) {
        err_msg.* = "collect() expects a stream as first argument";
        return error.RuntimeError;
    }

    // Set stream module callbacks to match our current VM callbacks.
    setStreamCallbacks();
    defer stream_mod.clearVM();

    const stream_obj = ObjStream.fromObj(first.asObj());
    const result_list = try ObjList.create(allocator);
    trackObj(&result_list.obj);

    while (true) {
        const val = try stream_obj.state.next(allocator);
        // Check if None (Option type_id=0, variant_idx=1).
        if (isAdtVariant(val, 0, 1)) break;
        // Extract from Some(x).
        const inner = adtPayload(val, 0);
        try result_list.items.append(allocator, inner);
    }

    return Value.fromObj(&result_list.obj);
}

/// count(stream) -> Int: terminal that counts elements in the stream.
fn builtinCount(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    // Auto-wrap range to stream if needed.
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }

    if (!first.isObjType(.stream)) {
        err_msg.* = "count() expects a stream as first argument";
        return error.RuntimeError;
    }

    // Set stream module callbacks.
    setStreamCallbacks();
    defer stream_mod.clearVM();

    const stream_obj = ObjStream.fromObj(first.asObj());
    var n: i32 = 0;

    while (true) {
        const val = try stream_obj.state.next(allocator);
        if (isAdtVariant(val, 0, 1)) break;
        n += 1;
    }

    return Value.fromInt(n);
}

// ═══════════════════════════════════════════════════════════════════════
// ── Stream transforms (continued) ──────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════

/// flat_map(stream, fn) -> Stream: apply fn to each element (fn returns stream/list), flatten results.
fn builtinFlatMap(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }
    if (!first.isObjType(.stream)) {
        err_msg.* = "flat_map() expects a stream as first argument";
        return error.RuntimeError;
    }
    if (!args[1].isObjType(.closure)) {
        err_msg.* = "flat_map() expects a function as second argument";
        return error.RuntimeError;
    }
    const state = try allocator.create(StreamState);
    state.* = .{ .flat_map_op = .{
        .upstream = first,
        .fn_val = args[1],
        .inner = Value.nil,
    } };
    return createStream(state, allocator);
}

/// filter_map(stream_or_list, fn) -> Stream or List: overloaded dispatch.
/// For streams: apply fn, keep only Some values. For lists: delegate to List.filter_map.
fn builtinFilterMap(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }
    if (first.isObjType(.stream)) {
        if (!args[1].isObjType(.closure)) {
            err_msg.* = "filter_map() expects a function as second argument";
            return error.RuntimeError;
        }
        const state = try allocator.create(StreamState);
        state.* = .{ .filter_map_op = .{
            .upstream = first,
            .fn_val = args[1],
        } };
        return createStream(state, allocator);
    }
    if (first.isObjType(.list)) {
        return builtinListFilterMap(args, allocator, err_msg);
    }
    err_msg.* = "filter_map() expects a stream or list as first argument";
    return error.RuntimeError;
}

/// scan(stream, init, fn) -> Stream: accumulate state across elements.
fn builtinScan(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }
    if (!first.isObjType(.stream)) {
        err_msg.* = "scan() expects a stream as first argument";
        return error.RuntimeError;
    }
    if (!args[2].isObjType(.closure)) {
        err_msg.* = "scan() expects a function as third argument";
        return error.RuntimeError;
    }
    const state = try allocator.create(StreamState);
    state.* = .{ .scan_op = .{
        .upstream = first,
        .acc = args[1],
        .fn_val = args[2],
    } };
    return createStream(state, allocator);
}

/// distinct(stream) -> Stream: remove duplicate elements.
fn builtinDistinct(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }
    if (!first.isObjType(.stream)) {
        err_msg.* = "distinct() expects a stream as first argument";
        return error.RuntimeError;
    }
    const state = try allocator.create(StreamState);
    state.* = .{ .distinct_op = .{
        .upstream = first,
        .seen = .{},
    } };
    return createStream(state, allocator);
}

/// zip(stream_or_list, stream_or_list) -> Stream or List: overloaded dispatch.
/// For streams: zip element-wise into tuples. For lists: delegate to List.zip.
fn builtinZip(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }
    if (first.isObjType(.stream)) {
        var second = args[1];
        if (second.isObjType(.range)) {
            second = try autoWrapRange(second, allocator);
        }
        if (!second.isObjType(.stream)) {
            err_msg.* = "zip() expects a stream as second argument when first is stream";
            return error.RuntimeError;
        }
        const state = try allocator.create(StreamState);
        state.* = .{ .zip_op = .{
            .upstream_a = first,
            .upstream_b = second,
        } };
        return createStream(state, allocator);
    }
    if (first.isObjType(.list)) {
        return builtinListZip(args, allocator, err_msg);
    }
    err_msg.* = "zip() expects a stream or list as first argument";
    return error.RuntimeError;
}

/// flatten(stream_or_list) -> Stream or List: overloaded dispatch.
/// For streams: flatten stream of lists/streams. For lists: delegate to List.flatten.
fn builtinFlatten(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }
    if (first.isObjType(.stream)) {
        const state = try allocator.create(StreamState);
        state.* = .{ .flatten_op = .{
            .upstream = first,
            .inner_list = Value.nil,
            .inner_idx = 0,
            .inner_stream = Value.nil,
        } };
        return createStream(state, allocator);
    }
    if (first.isObjType(.list)) {
        return builtinListFlatten(args, allocator, err_msg);
    }
    err_msg.* = "flatten() expects a stream or list as first argument";
    return error.RuntimeError;
}

/// tap(stream, fn) -> Stream: invoke side-effect function without altering elements.
fn builtinTap(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }
    if (!first.isObjType(.stream)) {
        err_msg.* = "tap() expects a stream as first argument";
        return error.RuntimeError;
    }
    if (!args[1].isObjType(.closure)) {
        err_msg.* = "tap() expects a function as second argument";
        return error.RuntimeError;
    }
    const state = try allocator.create(StreamState);
    state.* = .{ .tap_op = .{
        .upstream = first,
        .fn_val = args[1],
    } };
    return createStream(state, allocator);
}

/// batch(stream, size) -> Stream: group elements into fixed-size lists.
fn builtinBatch(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }
    if (!first.isObjType(.stream)) {
        err_msg.* = "batch() expects a stream as first argument";
        return error.RuntimeError;
    }
    if (!args[1].isInt()) {
        err_msg.* = "batch() expects an integer as second argument";
        return error.RuntimeError;
    }
    const size = args[1].asInt();
    if (size <= 0) {
        err_msg.* = "batch() size must be positive";
        return error.RuntimeError;
    }
    const state = try allocator.create(StreamState);
    state.* = .{ .batch_op = .{
        .upstream = first,
        .size = size,
        .exhausted = false,
    } };
    return createStream(state, allocator);
}

// ═══════════════════════════════════════════════════════════════════════
// ── Stream terminals (continued) ───────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════

/// sum(stream) -> Int/Float: terminal that sums all elements.
fn builtinSum(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }
    if (!first.isObjType(.stream)) {
        err_msg.* = "sum() expects a stream as first argument";
        return error.RuntimeError;
    }

    setStreamCallbacks();
    defer stream_mod.clearVM();

    const stream_obj = ObjStream.fromObj(first.asObj());
    var int_sum: i64 = 0;
    var has_float = false;
    var float_sum: f64 = 0.0;

    while (true) {
        const val = try stream_obj.state.next(allocator);
        if (isAdtVariant(val, 0, 1)) break;
        const elem = adtPayload(val, 0);
        if (elem.isInt()) {
            if (has_float) {
                float_sum += @as(f64, @floatFromInt(elem.asInt()));
            } else {
                int_sum += @as(i64, elem.asInt());
            }
        } else if (elem.isFloat()) {
            if (!has_float) {
                has_float = true;
                float_sum = @as(f64, @floatFromInt(int_sum));
            }
            float_sum += elem.asFloat();
        }
    }

    if (has_float) {
        return Value.fromFloat(float_sum);
    }
    // Try to return as i32 if it fits, otherwise use i64.
    if (int_sum >= @as(i64, std.math.minInt(i32)) and int_sum <= @as(i64, std.math.maxInt(i32))) {
        return Value.fromInt(@intCast(int_sum));
    }
    return Value.fromI64(int_sum, allocator);
}

/// reduce(stream_or_list, init, fn) -> value: overloaded dispatch.
/// For streams: fold with accumulator. For lists: delegate to List.reduce.
fn builtinReduce(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }
    if (first.isObjType(.stream)) {
        if (!args[2].isObjType(.closure)) {
            err_msg.* = "reduce() expects a function as third argument";
            return error.RuntimeError;
        }
        setStreamCallbacks();
        defer stream_mod.clearVM();

        const stream_obj = ObjStream.fromObj(first.asObj());
        var acc = args[1];
        const closure_val = args[2];

        while (true) {
            const val = try stream_obj.state.next(allocator);
            if (isAdtVariant(val, 0, 1)) break;
            const elem = adtPayload(val, 0);
            acc = try callClosure(closure_val, &[_]Value{ acc, elem });
        }
        return acc;
    }
    if (first.isObjType(.list)) {
        return builtinListReduce(args, allocator, err_msg);
    }
    err_msg.* = "reduce() expects a stream or list as first argument";
    return error.RuntimeError;
}

/// first(stream) -> Option: terminal that returns Some(first element) or None.
fn builtinFirst(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }
    if (!first.isObjType(.stream)) {
        err_msg.* = "first() expects a stream as first argument";
        return error.RuntimeError;
    }

    setStreamCallbacks();
    defer stream_mod.clearVM();

    const stream_obj = ObjStream.fromObj(first.asObj());
    const val = try stream_obj.state.next(allocator);
    if (isAdtVariant(val, 0, 1)) {
        // Empty stream -> None.
        return makeNone(allocator);
    }
    const elem = adtPayload(val, 0);
    return makeSome(elem, allocator);
}

/// last(stream) -> Option: terminal that returns Some(last element) or None.
fn builtinLast(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }
    if (!first.isObjType(.stream)) {
        err_msg.* = "last() expects a stream as first argument";
        return error.RuntimeError;
    }

    setStreamCallbacks();
    defer stream_mod.clearVM();

    const stream_obj = ObjStream.fromObj(first.asObj());
    var last_val: ?Value = null;

    while (true) {
        const val = try stream_obj.state.next(allocator);
        if (isAdtVariant(val, 0, 1)) break;
        last_val = adtPayload(val, 0);
    }

    if (last_val) |lv| {
        return makeSome(lv, allocator);
    }
    return makeNone(allocator);
}

/// each(stream, fn) -> nil: terminal that calls fn on each element for side effects.
fn builtinEach(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }
    if (!first.isObjType(.stream)) {
        err_msg.* = "each() expects a stream as first argument";
        return error.RuntimeError;
    }
    if (!args[1].isObjType(.closure)) {
        err_msg.* = "each() expects a function as second argument";
        return error.RuntimeError;
    }

    setStreamCallbacks();
    defer stream_mod.clearVM();

    const stream_obj = ObjStream.fromObj(first.asObj());
    const closure_val = args[1];

    while (true) {
        const val = try stream_obj.state.next(allocator);
        if (isAdtVariant(val, 0, 1)) break;
        const elem = adtPayload(val, 0);
        _ = try callClosure(closure_val, &[_]Value{elem});
    }

    return Value.nil;
}

/// min(stream) -> Option: terminal that returns Some(minimum) or None.
fn builtinMin(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }
    if (!first.isObjType(.stream)) {
        err_msg.* = "min() expects a stream as first argument";
        return error.RuntimeError;
    }

    setStreamCallbacks();
    defer stream_mod.clearVM();

    const stream_obj = ObjStream.fromObj(first.asObj());
    var min_val: ?Value = null;

    while (true) {
        const val = try stream_obj.state.next(allocator);
        if (isAdtVariant(val, 0, 1)) break;
        const elem = adtPayload(val, 0);
        if (min_val) |current_min| {
            if (valueLessThan(elem, current_min)) {
                min_val = elem;
            }
        } else {
            min_val = elem;
        }
    }

    if (min_val) |mv| {
        return makeSome(mv, allocator);
    }
    return makeNone(allocator);
}

/// max(stream) -> Option: terminal that returns Some(maximum) or None.
fn builtinMax(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }
    if (!first.isObjType(.stream)) {
        err_msg.* = "max() expects a stream as first argument";
        return error.RuntimeError;
    }

    setStreamCallbacks();
    defer stream_mod.clearVM();

    const stream_obj = ObjStream.fromObj(first.asObj());
    var max_val: ?Value = null;

    while (true) {
        const val = try stream_obj.state.next(allocator);
        if (isAdtVariant(val, 0, 1)) break;
        const elem = adtPayload(val, 0);
        if (max_val) |current_max| {
            if (valueLessThan(current_max, elem)) {
                max_val = elem;
            }
        } else {
            max_val = elem;
        }
    }

    if (max_val) |mv| {
        return makeSome(mv, allocator);
    }
    return makeNone(allocator);
}

/// Helper: compare two numeric values for less-than ordering.
fn valueLessThan(a: Value, b: Value) bool {
    if (a.isInt() and b.isInt()) return a.asInt() < b.asInt();
    // Promote to float for mixed comparisons.
    const fa: f64 = if (a.isInt()) @floatFromInt(a.asInt()) else if (a.isFloat()) a.asFloat() else return false;
    const fb: f64 = if (b.isInt()) @floatFromInt(b.asInt()) else if (b.isFloat()) b.asFloat() else return false;
    return fa < fb;
}

// ═══════════════════════════════════════════════════════════════════════
// ── Stream error handling ──────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════

/// partition_result(stream) -> Record {ok: Stream, err: Stream}: split stream of Results.
fn builtinPartitionResult(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }
    if (!first.isObjType(.stream)) {
        err_msg.* = "partition_result() expects a stream as first argument";
        return error.RuntimeError;
    }

    // Create shared partition state.
    const shared = try allocator.create(StreamState.PartitionState);
    shared.* = .{
        .upstream = first,
        .ok_queue = .{},
        .err_queue = .{},
        .ref_count = 2,
    };

    // Create ok stream.
    const ok_state = try allocator.create(StreamState);
    ok_state.* = .{ .partition_ok = .{ .shared = shared } };
    const ok_stream = try ObjStream.create(allocator, ok_state);
    trackObj(&ok_stream.obj);

    // Create err stream.
    const err_state = try allocator.create(StreamState);
    err_state.* = .{ .partition_err = .{ .shared = shared } };
    const err_stream = try ObjStream.create(allocator, err_state);
    trackObj(&err_stream.obj);

    // Create record {ok: Stream, err: Stream}.
    const names = [_][]const u8{ "ok", "err" };
    const values = [_]Value{ Value.fromObj(&ok_stream.obj), Value.fromObj(&err_stream.obj) };
    const record = try ObjRecord.create(allocator, &names, &values);
    trackObj(&record.obj);

    return Value.fromObj(&record.obj);
}

// ═══════════════════════════════════════════════════════════════════════
// ── Json module ────────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════

fn builtinJsonDecode(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    if (!args[0].isString()) {
        err_msg.* = "Json.decode expects a string argument";
        return error.RuntimeError;
    }
    const str = ObjString.fromObj(args[0].asObj());
    if (current_vm) |vm_ptr| {
        if (track_obj_fn) |tfn| {
            json_mod.setVM(vm_ptr, tfn);
        }
    }
    defer json_mod.clearVM();
    const result = json_mod.parse(str.bytes, allocator);
    switch (result) {
        .ok => |val| {
            return makeOk(val, allocator);
        },
        .err => |e| {
            const msg_str = try ObjString.create(allocator, e.message, null);
            trackObj(&msg_str.obj);
            const names = [_][]const u8{ "message", "position" };
            const values = [_]Value{
                Value.fromObj(&msg_str.obj),
                Value.fromInt(@intCast(@min(e.position, @as(usize, @intCast(std.math.maxInt(i32)))))),
            };
            const record = try ObjRecord.create(allocator, &names, &values);
            trackObj(&record.obj);
            return makeErr(Value.fromObj(&record.obj), allocator);
        },
    }
}

fn builtinJsonEncode(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = err_msg;
    if (current_vm) |vm_ptr| {
        if (track_obj_fn) |tfn| {
            json_mod.setVM(vm_ptr, tfn);
        }
    }
    defer json_mod.clearVM();
    if (current_atom_names) |names| {
        json_mod.setAtomNames(names);
    }
    const result = json_mod.emit(args[0], allocator);
    switch (result) {
        .ok => |json_bytes| {
            const json_str = ObjString.create(allocator, json_bytes, null) catch {
                allocator.free(json_bytes);
                return error.OutOfMemory;
            };
            if (json_str.bytes.ptr != json_bytes.ptr) {
                allocator.free(json_bytes);
            }
            trackObj(&json_str.obj);
            return makeOk(Value.fromObj(&json_str.obj), allocator);
        },
        .err => |msg| {
            const err_str = try ObjString.create(allocator, msg, null);
            trackObj(&err_str.obj);
            return makeErr(Value.fromObj(&err_str.obj), allocator);
        },
    }
}

// ═══════════════════════════════════════════════════════════════════════
// ── File I/O: source() and sink() ──────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════

/// Resolve an atom value to its name string using the current atom table.
fn atomName(val: Value) ?[]const u8 {
    if (!val.isAtom()) return null;
    const names = current_atom_names orelse return null;
    const id = val.asAtom();
    if (id < names.len) return names[id];
    return null;
}

/// source(path_or_atom, format?) -> Stream
/// source("file.txt") creates a stream of lines from a plain text file.
/// source("file.jsonl", format: :jsonl) creates Stream(Result(Map, ParseError)).
/// source(:stdin) reads from standard input.
fn builtinSource(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    const first = args[0];

    // Check for atom :stdin.
    if (first.isAtom()) {
        if (atomName(first)) |name| {
            if (std.mem.eql(u8, name, "stdin")) {
                const stdin_file = std.fs.File.stdin();
                const frs = StreamState.FileReaderState.create(allocator, stdin_file, true) catch {
                    err_msg.* = "failed to create stdin reader";
                    return error.RuntimeError;
                };
                const state = try allocator.create(StreamState);
                state.* = .{ .stdin_reader = .{ .frs = frs } };
                return createStream(state, allocator);
            }
        }
        err_msg.* = "source() expects a string path or :stdin atom";
        return error.RuntimeError;
    }

    // Must be a string path.
    if (!first.isString()) {
        err_msg.* = "source() expects a string path or :stdin atom as first argument";
        return error.RuntimeError;
    }

    const path_str = ObjString.fromObj(first.asObj());
    const path = path_str.bytes;

    // Open the file.
    const file = std.fs.cwd().openFile(path, .{}) catch |err| {
        err_msg.* = switch (err) {
            error.FileNotFound => "file not found",
            error.AccessDenied => "permission denied",
            else => "failed to open file",
        };
        return error.RuntimeError;
    };

    // Determine format: if arity == 2 and args[1] is atom :jsonl, use JSONL reader.
    const is_jsonl = if (args.len > 1) blk: {
        if (atomName(args[1])) |name| {
            break :blk std.mem.eql(u8, name, "jsonl");
        }
        break :blk false;
    } else false;

    const frs = StreamState.FileReaderState.create(allocator, file, false) catch {
        file.close();
        err_msg.* = "failed to create file reader";
        return error.RuntimeError;
    };

    const state = try allocator.create(StreamState);
    if (is_jsonl) {
        state.* = .{ .jsonl_reader = .{ .frs = frs } };
    } else {
        state.* = .{ .file_reader = .{ .frs = frs } };
    }
    return createStream(state, allocator);
}

/// sink(stream, path_or_atom, format?) -> Nil
/// Consumes the stream and writes elements to a file.
/// sink(stream, "out.txt") writes each element as a line.
/// sink(stream, "out.jsonl", format: :jsonl) writes each element as JSON per line.
/// sink(stream, :stdout) writes to standard output.
/// sink(stream, :stderr) writes to standard error.
fn builtinSink(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    // First arg must be a stream (or range, auto-wrapped).
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }
    if (!first.isObjType(.stream)) {
        err_msg.* = "sink() expects a stream as first argument";
        return error.RuntimeError;
    }

    const dest = args[1];

    // Determine format.
    const is_jsonl = if (args.len > 2) blk: {
        if (atomName(args[2])) |name| {
            break :blk std.mem.eql(u8, name, "jsonl");
        }
        break :blk false;
    } else false;

    // Determine output target.
    var out_file: std.fs.File = undefined;
    var is_std_handle = false;

    if (dest.isAtom()) {
        if (atomName(dest)) |name| {
            if (std.mem.eql(u8, name, "stdout")) {
                out_file = std.fs.File.stdout();
                is_std_handle = true;
            } else if (std.mem.eql(u8, name, "stderr")) {
                out_file = std.fs.File.stderr();
                is_std_handle = true;
            } else {
                err_msg.* = "sink() expects :stdout, :stderr, or a string path";
                return error.RuntimeError;
            }
        } else {
            err_msg.* = "sink() expects :stdout, :stderr, or a string path";
            return error.RuntimeError;
        }
    } else if (dest.isString()) {
        const path_str = ObjString.fromObj(dest.asObj());
        out_file = std.fs.cwd().createFile(path_str.bytes, .{}) catch |err| {
            err_msg.* = switch (err) {
                error.AccessDenied => "permission denied",
                else => "failed to create file",
            };
            return error.RuntimeError;
        };
    } else {
        err_msg.* = "sink() expects a string path or :stdout/:stderr atom as second argument";
        return error.RuntimeError;
    }
    defer {
        if (!is_std_handle) out_file.close();
    }

    // Set up buffered writer.
    var write_buf: [64 * 1024]u8 = undefined;
    var bw = out_file.writer(&write_buf);

    // Set stream module callbacks for pulling.
    setStreamCallbacks();
    defer stream_mod.clearVM();

    const stream_obj = ObjStream.fromObj(first.asObj());

    // Pull loop: consume all elements from the stream.
    while (true) {
        const val = try stream_obj.state.next(allocator);
        if (isAdtVariant(val, 0, 1)) break; // None
        const elem = adtPayload(val, 0);

        if (is_jsonl) {
            // JSON encode each element.
            if (current_vm) |vm_ptr| {
                if (track_obj_fn) |tfn| {
                    json_mod.setVM(vm_ptr, tfn);
                }
            }
            if (current_atom_names) |names| {
                json_mod.setAtomNames(names);
            }
            const emit_result = json_mod.emit(elem, allocator);
            json_mod.clearVM();
            switch (emit_result) {
                .ok => |json_bytes| {
                    bw.interface.writeAll(json_bytes) catch {
                        allocator.free(json_bytes);
                        err_msg.* = "failed to write to file";
                        return error.RuntimeError;
                    };
                    allocator.free(json_bytes);
                },
                .err => |msg| {
                    err_msg.* = msg;
                    return error.RuntimeError;
                },
            }
        } else {
            // Plain text: format as string, write as line.
            const formatted = formatValue(elem, allocator, current_atom_names) catch {
                err_msg.* = "failed to format value for sink";
                return error.RuntimeError;
            };
            defer allocator.free(formatted);
            bw.interface.writeAll(formatted) catch {
                err_msg.* = "failed to write to file";
                return error.RuntimeError;
            };
        }

        // Write newline after each element.
        bw.interface.writeByte('\n') catch {
            err_msg.* = "failed to write newline to file";
            return error.RuntimeError;
        };
    }

    // CRITICAL: flush buffered writer before closing.
    bw.interface.flush() catch {
        err_msg.* = "failed to flush output file";
        return error.RuntimeError;
    };

    return Value.nil;
}

// ── Concurrency stream operators ─────────────────────────────────────

/// par_map(stream, fn) or par_map(stream, N, fn)
/// Parallel map with order preservation. In v1 (single-threaded), processes sequentially.
fn builtinParMap(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    // Auto-wrap range to stream if needed.
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }

    if (!first.isObjType(.stream)) {
        err_msg.* = "par_map() expects a stream as first argument";
        return error.RuntimeError;
    }

    var concurrency: u32 = getCpuCount();
    var fn_val: Value = undefined;

    if (args.len == 3) {
        // par_map(stream, N, fn)
        if (!args[1].isInt() or args[1].asInt() <= 0) {
            err_msg.* = "par_map() concurrency argument must be a positive integer";
            return error.RuntimeError;
        }
        concurrency = @intCast(args[1].asInt());
        fn_val = args[2];
    } else {
        // par_map(stream, fn)
        fn_val = args[1];
    }

    if (!fn_val.isObjType(.closure)) {
        err_msg.* = "par_map() expects a function argument";
        return error.RuntimeError;
    }

    const state = try allocator.create(StreamState);
    state.* = .{ .par_map = .{
        .upstream = first,
        .transform_fn = fn_val,
        .concurrency = concurrency,
    } };
    return createStream(state, allocator);
}

/// par_map_unordered(stream, fn) or par_map_unordered(stream, N, fn)
/// Parallel map emitting results in completion order.
fn builtinParMapUnordered(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }

    if (!first.isObjType(.stream)) {
        err_msg.* = "par_map_unordered() expects a stream as first argument";
        return error.RuntimeError;
    }

    var concurrency: u32 = getCpuCount();
    var fn_val: Value = undefined;

    if (args.len == 3) {
        if (!args[1].isInt() or args[1].asInt() <= 0) {
            err_msg.* = "par_map_unordered() concurrency argument must be a positive integer";
            return error.RuntimeError;
        }
        concurrency = @intCast(args[1].asInt());
        fn_val = args[2];
    } else {
        fn_val = args[1];
    }

    if (!fn_val.isObjType(.closure)) {
        err_msg.* = "par_map_unordered() expects a function argument";
        return error.RuntimeError;
    }

    const state = try allocator.create(StreamState);
    state.* = .{ .par_map_unordered = .{
        .upstream = first,
        .transform_fn = fn_val,
        .concurrency = concurrency,
    } };
    return createStream(state, allocator);
}

/// par_map_result(stream, fn) or par_map_result(stream, N, fn)
/// Parallel map wrapping outputs in Result. Errors wrapped in Result.Err.
fn builtinParMapResult(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }

    if (!first.isObjType(.stream)) {
        err_msg.* = "par_map_result() expects a stream as first argument";
        return error.RuntimeError;
    }

    var concurrency: u32 = getCpuCount();
    var fn_val: Value = undefined;

    if (args.len == 3) {
        if (!args[1].isInt() or args[1].asInt() <= 0) {
            err_msg.* = "par_map_result() concurrency argument must be a positive integer";
            return error.RuntimeError;
        }
        concurrency = @intCast(args[1].asInt());
        fn_val = args[2];
    } else {
        fn_val = args[1];
    }

    if (!fn_val.isObjType(.closure)) {
        err_msg.* = "par_map_result() expects a function argument";
        return error.RuntimeError;
    }

    const state = try allocator.create(StreamState);
    state.* = .{ .par_map_result = .{
        .upstream = first,
        .transform_fn = fn_val,
        .concurrency = concurrency,
    } };
    return createStream(state, allocator);
}

/// tick(interval_ms) -> Stream: generates incrementing integers at regular intervals.
fn builtinTick(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    if (!args[0].isInt() or args[0].asInt() <= 0) {
        err_msg.* = "tick() expects a positive integer interval in milliseconds";
        return error.RuntimeError;
    }
    const interval_ms: u64 = @intCast(args[0].asInt());

    const state = try allocator.create(StreamState);
    state.* = .{ .tick = .{
        .interval_ms = interval_ms,
        .counter = 0,
        .last_emit = 0,
    } };
    return createStream(state, allocator);
}

/// Get the CPU core count, defaulting to 4 if unavailable.
fn getCpuCount() u32 {
    return @intCast(std.Thread.getCpuCount() catch 4);
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
    const s = try ObjString.create(allocator, "hello", null);
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
    try std.testing.expectEqualStrings("len() expects a string, list, map, tuple, or record argument", err_msg);
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
    const s = try ObjString.create(allocator, "test", null);
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
    const s = try ObjString.create(allocator, "oh no", null);
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

// ── List module tests ────────────────────────────────────────────────

test "builtins: List.get returns Some for valid index" {
    const allocator = std.testing.allocator;
    const lst = try ObjList.create(allocator);
    defer lst.obj.destroy(allocator);
    try lst.items.append(allocator, Value.fromInt(10));
    try lst.items.append(allocator, Value.fromInt(20));
    try lst.items.append(allocator, Value.fromInt(30));

    var err_msg: []const u8 = "";
    const result = try builtinListGet(&[_]Value{ Value.fromObj(&lst.obj), Value.fromInt(1) }, allocator, &err_msg);
    // Should be Some(20) = ADT(0.0)(20)
    try std.testing.expect(result.isObjType(.adt));
    const adt = ObjAdt.fromObj(result.asObj());
    try std.testing.expectEqual(@as(u16, 0), adt.type_id); // Option
    try std.testing.expectEqual(@as(u16, 0), adt.variant_idx); // Some
    try std.testing.expectEqual(@as(i32, 20), adt.payload[0].asInt());
    result.asObj().destroy(allocator);
}

test "builtins: List.get returns None for out of bounds" {
    const allocator = std.testing.allocator;
    const lst = try ObjList.create(allocator);
    defer lst.obj.destroy(allocator);
    try lst.items.append(allocator, Value.fromInt(10));

    var err_msg: []const u8 = "";
    const result = try builtinListGet(&[_]Value{ Value.fromObj(&lst.obj), Value.fromInt(5) }, allocator, &err_msg);
    // Should be None = ADT(0.1)
    try std.testing.expect(result.isObjType(.adt));
    const adt = ObjAdt.fromObj(result.asObj());
    try std.testing.expectEqual(@as(u16, 0), adt.type_id); // Option
    try std.testing.expectEqual(@as(u16, 1), adt.variant_idx); // None
    result.asObj().destroy(allocator);
}

test "builtins: List.append creates new list" {
    const allocator = std.testing.allocator;
    const lst = try ObjList.create(allocator);
    defer lst.obj.destroy(allocator);
    try lst.items.append(allocator, Value.fromInt(1));

    var err_msg: []const u8 = "";
    const result = try builtinListAppend(&[_]Value{ Value.fromObj(&lst.obj), Value.fromInt(2) }, allocator, &err_msg);
    const new_lst = ObjList.fromObj(result.asObj());
    try std.testing.expectEqual(@as(usize, 2), new_lst.items.items.len);
    try std.testing.expectEqual(@as(i32, 1), new_lst.items.items[0].asInt());
    try std.testing.expectEqual(@as(i32, 2), new_lst.items.items[1].asInt());
    // Original unchanged.
    try std.testing.expectEqual(@as(usize, 1), lst.items.items.len);
    result.asObj().destroy(allocator);
}

test "builtins: List.reverse creates reversed list" {
    const allocator = std.testing.allocator;
    const lst = try ObjList.create(allocator);
    defer lst.obj.destroy(allocator);
    try lst.items.append(allocator, Value.fromInt(1));
    try lst.items.append(allocator, Value.fromInt(2));
    try lst.items.append(allocator, Value.fromInt(3));

    var err_msg: []const u8 = "";
    const result = try builtinListReverse(&[_]Value{Value.fromObj(&lst.obj)}, allocator, &err_msg);
    const rev = ObjList.fromObj(result.asObj());
    try std.testing.expectEqual(@as(i32, 3), rev.items.items[0].asInt());
    try std.testing.expectEqual(@as(i32, 2), rev.items.items[1].asInt());
    try std.testing.expectEqual(@as(i32, 1), rev.items.items[2].asInt());
    result.asObj().destroy(allocator);
}

test "builtins: List.contains finds element" {
    const allocator = std.testing.allocator;
    const lst = try ObjList.create(allocator);
    defer lst.obj.destroy(allocator);
    try lst.items.append(allocator, Value.fromInt(10));
    try lst.items.append(allocator, Value.fromInt(20));

    var err_msg: []const u8 = "";
    const found = try builtinListContains(&[_]Value{ Value.fromObj(&lst.obj), Value.fromInt(20) }, allocator, &err_msg);
    try std.testing.expect(found.asBool() == true);
    const not_found = try builtinListContains(&[_]Value{ Value.fromObj(&lst.obj), Value.fromInt(99) }, allocator, &err_msg);
    try std.testing.expect(not_found.asBool() == false);
}

test "builtins: List.sort sorts integers" {
    const allocator = std.testing.allocator;
    const lst = try ObjList.create(allocator);
    defer lst.obj.destroy(allocator);
    try lst.items.append(allocator, Value.fromInt(3));
    try lst.items.append(allocator, Value.fromInt(1));
    try lst.items.append(allocator, Value.fromInt(2));

    var err_msg: []const u8 = "";
    const result = try builtinListSort(&[_]Value{Value.fromObj(&lst.obj)}, allocator, &err_msg);
    const sorted = ObjList.fromObj(result.asObj());
    try std.testing.expectEqual(@as(i32, 1), sorted.items.items[0].asInt());
    try std.testing.expectEqual(@as(i32, 2), sorted.items.items[1].asInt());
    try std.testing.expectEqual(@as(i32, 3), sorted.items.items[2].asInt());
    result.asObj().destroy(allocator);
}

// ── Map module tests ─────────────────────────────────────────────────

test "builtins: Map.get returns Some for existing key" {
    const allocator = std.testing.allocator;
    const m = try ObjMap.create(allocator);
    defer m.obj.destroy(allocator);
    try m.entries.put(allocator, Value.fromInt(1), Value.fromInt(100));

    var err_msg: []const u8 = "";
    const result = try builtinMapGet(&[_]Value{ Value.fromObj(&m.obj), Value.fromInt(1) }, allocator, &err_msg);
    try std.testing.expect(isAdtVariant(result, 0, 0)); // Some
    try std.testing.expectEqual(@as(i32, 100), adtPayload(result, 0).asInt());
    result.asObj().destroy(allocator);
}

test "builtins: Map.get returns None for missing key" {
    const allocator = std.testing.allocator;
    const m = try ObjMap.create(allocator);
    defer m.obj.destroy(allocator);

    var err_msg: []const u8 = "";
    const result = try builtinMapGet(&[_]Value{ Value.fromObj(&m.obj), Value.fromInt(1) }, allocator, &err_msg);
    try std.testing.expect(isAdtVariant(result, 0, 1)); // None
    result.asObj().destroy(allocator);
}

test "builtins: Map.set creates new map with entry" {
    const allocator = std.testing.allocator;
    const m = try ObjMap.create(allocator);
    defer m.obj.destroy(allocator);
    try m.entries.put(allocator, Value.fromInt(1), Value.fromInt(10));

    var err_msg: []const u8 = "";
    const result = try builtinMapSet(&[_]Value{ Value.fromObj(&m.obj), Value.fromInt(2), Value.fromInt(20) }, allocator, &err_msg);
    const new_m = ObjMap.fromObj(result.asObj());
    try std.testing.expectEqual(@as(u32, 2), new_m.entries.count());
    // Original unchanged.
    try std.testing.expectEqual(@as(u32, 1), m.entries.count());
    result.asObj().destroy(allocator);
}

test "builtins: Map.contains checks key existence" {
    const allocator = std.testing.allocator;
    const m = try ObjMap.create(allocator);
    defer m.obj.destroy(allocator);
    try m.entries.put(allocator, Value.fromInt(42), Value.true_val);

    var err_msg: []const u8 = "";
    const found = try builtinMapContains(&[_]Value{ Value.fromObj(&m.obj), Value.fromInt(42) }, allocator, &err_msg);
    try std.testing.expect(found.asBool() == true);
    const not_found = try builtinMapContains(&[_]Value{ Value.fromObj(&m.obj), Value.fromInt(99) }, allocator, &err_msg);
    try std.testing.expect(not_found.asBool() == false);
}

// ── String module tests ──────────────────────────────────────────────

test "builtins: String.split splits by separator" {
    const allocator = std.testing.allocator;
    const s = try ObjString.create(allocator, "a,b,c", null);
    defer s.obj.destroy(allocator);
    const sep = try ObjString.create(allocator, ",", null);
    defer sep.obj.destroy(allocator);

    var err_msg: []const u8 = "";
    const result = try builtinStringSplit(&[_]Value{ Value.fromObj(&s.obj), Value.fromObj(&sep.obj) }, allocator, &err_msg);
    const lst = ObjList.fromObj(result.asObj());
    try std.testing.expectEqual(@as(usize, 3), lst.items.items.len);
    try std.testing.expectEqualStrings("a", ObjString.fromObj(lst.items.items[0].asObj()).bytes);
    try std.testing.expectEqualStrings("b", ObjString.fromObj(lst.items.items[1].asObj()).bytes);
    try std.testing.expectEqualStrings("c", ObjString.fromObj(lst.items.items[2].asObj()).bytes);
    // Clean up all created objects.
    for (lst.items.items) |item| item.asObj().destroy(allocator);
    result.asObj().destroy(allocator);
}

test "builtins: String.trim removes whitespace" {
    const allocator = std.testing.allocator;
    const s = try ObjString.create(allocator, "  hello  ", null);
    defer s.obj.destroy(allocator);

    var err_msg: []const u8 = "";
    const result = try builtinStringTrim(&[_]Value{Value.fromObj(&s.obj)}, allocator, &err_msg);
    try std.testing.expectEqualStrings("hello", ObjString.fromObj(result.asObj()).bytes);
    result.asObj().destroy(allocator);
}

test "builtins: String.contains checks substring" {
    const allocator = std.testing.allocator;
    const s = try ObjString.create(allocator, "hello world", null);
    defer s.obj.destroy(allocator);
    const sub = try ObjString.create(allocator, "world", null);
    defer sub.obj.destroy(allocator);
    const missing = try ObjString.create(allocator, "xyz", null);
    defer missing.obj.destroy(allocator);

    var err_msg: []const u8 = "";
    const found = try builtinStringContains(&[_]Value{ Value.fromObj(&s.obj), Value.fromObj(&sub.obj) }, allocator, &err_msg);
    try std.testing.expect(found.asBool() == true);
    const not_found = try builtinStringContains(&[_]Value{ Value.fromObj(&s.obj), Value.fromObj(&missing.obj) }, allocator, &err_msg);
    try std.testing.expect(not_found.asBool() == false);
}

test "builtins: String.to_lower lowercases" {
    const allocator = std.testing.allocator;
    const s = try ObjString.create(allocator, "Hello WORLD", null);
    defer s.obj.destroy(allocator);

    var err_msg: []const u8 = "";
    const result = try builtinStringToLower(&[_]Value{Value.fromObj(&s.obj)}, allocator, &err_msg);
    try std.testing.expectEqualStrings("hello world", ObjString.fromObj(result.asObj()).bytes);
    result.asObj().destroy(allocator);
}

test "builtins: String.to_upper uppercases" {
    const allocator = std.testing.allocator;
    const s = try ObjString.create(allocator, "Hello world", null);
    defer s.obj.destroy(allocator);

    var err_msg: []const u8 = "";
    const result = try builtinStringToUpper(&[_]Value{Value.fromObj(&s.obj)}, allocator, &err_msg);
    try std.testing.expectEqualStrings("HELLO WORLD", ObjString.fromObj(result.asObj()).bytes);
    result.asObj().destroy(allocator);
}

test "builtins: String.replace replaces all occurrences" {
    const allocator = std.testing.allocator;
    const s = try ObjString.create(allocator, "aXbXc", null);
    defer s.obj.destroy(allocator);
    const old = try ObjString.create(allocator, "X", null);
    defer old.obj.destroy(allocator);
    const new_str = try ObjString.create(allocator, "--", null);
    defer new_str.obj.destroy(allocator);

    var err_msg: []const u8 = "";
    const result = try builtinStringReplace(&[_]Value{ Value.fromObj(&s.obj), Value.fromObj(&old.obj), Value.fromObj(&new_str.obj) }, allocator, &err_msg);
    try std.testing.expectEqualStrings("a--b--c", ObjString.fromObj(result.asObj()).bytes);
    result.asObj().destroy(allocator);
}

test "builtins: String.starts_with and String.ends_with" {
    const allocator = std.testing.allocator;
    const s = try ObjString.create(allocator, "hello world", null);
    defer s.obj.destroy(allocator);
    const prefix = try ObjString.create(allocator, "hello", null);
    defer prefix.obj.destroy(allocator);
    const suffix = try ObjString.create(allocator, "world", null);
    defer suffix.obj.destroy(allocator);

    var err_msg: []const u8 = "";
    const sw = try builtinStringStartsWith(&[_]Value{ Value.fromObj(&s.obj), Value.fromObj(&prefix.obj) }, allocator, &err_msg);
    try std.testing.expect(sw.asBool() == true);
    const ew = try builtinStringEndsWith(&[_]Value{ Value.fromObj(&s.obj), Value.fromObj(&suffix.obj) }, allocator, &err_msg);
    try std.testing.expect(ew.asBool() == true);
}

// ── Result module tests ──────────────────────────────────────────────

test "builtins: Result.Ok and Result.is_ok" {
    const allocator = std.testing.allocator;
    var err_msg: []const u8 = "";
    const ok_val = try builtinResultOk(&[_]Value{Value.fromInt(42)}, allocator, &err_msg);
    defer ok_val.asObj().destroy(allocator);
    try std.testing.expect(isAdtVariant(ok_val, 1, 0));
    try std.testing.expectEqual(@as(i32, 42), adtPayload(ok_val, 0).asInt());

    const is_ok = try builtinResultIsOk(&[_]Value{ok_val}, allocator, &err_msg);
    try std.testing.expect(is_ok.asBool() == true);
    const is_err = try builtinResultIsErr(&[_]Value{ok_val}, allocator, &err_msg);
    try std.testing.expect(is_err.asBool() == false);
}

test "builtins: Result.Err and Result.is_err" {
    const allocator = std.testing.allocator;
    var err_msg: []const u8 = "";
    const err_str = try ObjString.create(allocator, "oops", null);
    defer err_str.obj.destroy(allocator);
    const err_val = try builtinResultErr(&[_]Value{Value.fromObj(&err_str.obj)}, allocator, &err_msg);
    defer err_val.asObj().destroy(allocator);
    try std.testing.expect(isAdtVariant(err_val, 1, 1));

    const is_err = try builtinResultIsErr(&[_]Value{err_val}, allocator, &err_msg);
    try std.testing.expect(is_err.asBool() == true);
    const is_ok = try builtinResultIsOk(&[_]Value{err_val}, allocator, &err_msg);
    try std.testing.expect(is_ok.asBool() == false);
}

test "builtins: Result.unwrap_or returns payload for Ok" {
    const allocator = std.testing.allocator;
    var err_msg: []const u8 = "";
    const ok_val = try builtinResultOk(&[_]Value{Value.fromInt(42)}, allocator, &err_msg);
    defer ok_val.asObj().destroy(allocator);

    const unwrapped = try builtinResultUnwrapOr(&[_]Value{ ok_val, Value.fromInt(0) }, allocator, &err_msg);
    try std.testing.expectEqual(@as(i32, 42), unwrapped.asInt());
}

test "builtins: Result.unwrap_or returns default for Err" {
    const allocator = std.testing.allocator;
    var err_msg: []const u8 = "";
    const err_val = try builtinResultErr(&[_]Value{Value.fromInt(0)}, allocator, &err_msg);
    defer err_val.asObj().destroy(allocator);

    const unwrapped = try builtinResultUnwrapOr(&[_]Value{ err_val, Value.fromInt(99) }, allocator, &err_msg);
    try std.testing.expectEqual(@as(i32, 99), unwrapped.asInt());
}

// ── Option module tests ──────────────────────────────────────────────

test "builtins: Option.Some and Option.is_some" {
    const allocator = std.testing.allocator;
    var err_msg: []const u8 = "";
    const some_val = try builtinOptionSome(&[_]Value{Value.fromInt(42)}, allocator, &err_msg);
    defer some_val.asObj().destroy(allocator);
    try std.testing.expect(isAdtVariant(some_val, 0, 0));

    const is_some = try builtinOptionIsSome(&[_]Value{some_val}, allocator, &err_msg);
    try std.testing.expect(is_some.asBool() == true);
}

test "builtins: Option.None and Option.is_none" {
    const allocator = std.testing.allocator;
    var err_msg: []const u8 = "";
    const none_val = try builtinOptionNone(&[_]Value{}, allocator, &err_msg);
    defer none_val.asObj().destroy(allocator);
    try std.testing.expect(isAdtVariant(none_val, 0, 1));

    const is_none = try builtinOptionIsNone(&[_]Value{none_val}, allocator, &err_msg);
    try std.testing.expect(is_none.asBool() == true);
}

test "builtins: Option.unwrap_or returns payload for Some" {
    const allocator = std.testing.allocator;
    var err_msg: []const u8 = "";
    const some_val = try builtinOptionSome(&[_]Value{Value.fromInt(42)}, allocator, &err_msg);
    defer some_val.asObj().destroy(allocator);

    const unwrapped = try builtinOptionUnwrapOr(&[_]Value{ some_val, Value.fromInt(0) }, allocator, &err_msg);
    try std.testing.expectEqual(@as(i32, 42), unwrapped.asInt());
}

test "builtins: Option.unwrap_or returns default for None" {
    const allocator = std.testing.allocator;
    var err_msg: []const u8 = "";
    const none_val = try builtinOptionNone(&[_]Value{}, allocator, &err_msg);
    defer none_val.asObj().destroy(allocator);

    const unwrapped = try builtinOptionUnwrapOr(&[_]Value{ none_val, Value.fromInt(99) }, allocator, &err_msg);
    try std.testing.expectEqual(@as(i32, 99), unwrapped.asInt());
}

test "builtins: Option.to_result converts Some to Ok" {
    const allocator = std.testing.allocator;
    var err_msg: []const u8 = "";
    const some_val = try builtinOptionSome(&[_]Value{Value.fromInt(42)}, allocator, &err_msg);
    defer some_val.asObj().destroy(allocator);

    const result = try builtinOptionToResult(&[_]Value{ some_val, Value.fromInt(0) }, allocator, &err_msg);
    defer result.asObj().destroy(allocator);
    try std.testing.expect(isAdtVariant(result, 1, 0)); // Ok
    try std.testing.expectEqual(@as(i32, 42), adtPayload(result, 0).asInt());
}

test "builtins: Option.to_result converts None to Err" {
    const allocator = std.testing.allocator;
    var err_msg: []const u8 = "";
    const none_val = try builtinOptionNone(&[_]Value{}, allocator, &err_msg);
    defer none_val.asObj().destroy(allocator);

    const err_str = try ObjString.create(allocator, "missing", null);
    defer err_str.obj.destroy(allocator);
    const result = try builtinOptionToResult(&[_]Value{ none_val, Value.fromObj(&err_str.obj) }, allocator, &err_msg);
    defer result.asObj().destroy(allocator);
    try std.testing.expect(isAdtVariant(result, 1, 1)); // Err
}

// ── Additional Task 2 tests ──────────────────────────────────────────

test "builtins: String.join joins list elements" {
    const allocator = std.testing.allocator;
    const lst = try ObjList.create(allocator);
    defer lst.obj.destroy(allocator);
    const s1 = try ObjString.create(allocator, "hello", null);
    try lst.items.append(allocator, Value.fromObj(&s1.obj));
    const s2 = try ObjString.create(allocator, "world", null);
    try lst.items.append(allocator, Value.fromObj(&s2.obj));
    const sep = try ObjString.create(allocator, " ", null);
    defer sep.obj.destroy(allocator);

    var err_msg: []const u8 = "";
    const result = try builtinStringJoin(&[_]Value{ Value.fromObj(&lst.obj), Value.fromObj(&sep.obj) }, allocator, &err_msg);
    try std.testing.expectEqualStrings("hello world", ObjString.fromObj(result.asObj()).bytes);
    result.asObj().destroy(allocator);
    s1.obj.destroy(allocator);
    s2.obj.destroy(allocator);
}

test "builtins: String.split with multi-byte separator" {
    const allocator = std.testing.allocator;
    const s = try ObjString.create(allocator, "a::b::c", null);
    defer s.obj.destroy(allocator);
    const sep = try ObjString.create(allocator, "::", null);
    defer sep.obj.destroy(allocator);

    var err_msg: []const u8 = "";
    const result = try builtinStringSplit(&[_]Value{ Value.fromObj(&s.obj), Value.fromObj(&sep.obj) }, allocator, &err_msg);
    const lst = ObjList.fromObj(result.asObj());
    try std.testing.expectEqual(@as(usize, 3), lst.items.items.len);
    try std.testing.expectEqualStrings("a", ObjString.fromObj(lst.items.items[0].asObj()).bytes);
    try std.testing.expectEqualStrings("b", ObjString.fromObj(lst.items.items[1].asObj()).bytes);
    try std.testing.expectEqualStrings("c", ObjString.fromObj(lst.items.items[2].asObj()).bytes);
    for (lst.items.items) |item| item.asObj().destroy(allocator);
    result.asObj().destroy(allocator);
}

test "builtins: String.replace with empty old returns original" {
    const allocator = std.testing.allocator;
    const s = try ObjString.create(allocator, "hello", null);
    defer s.obj.destroy(allocator);
    const old = try ObjString.create(allocator, "", null);
    defer old.obj.destroy(allocator);
    const new_s = try ObjString.create(allocator, "X", null);
    defer new_s.obj.destroy(allocator);

    var err_msg: []const u8 = "";
    const result = try builtinStringReplace(&[_]Value{ Value.fromObj(&s.obj), Value.fromObj(&old.obj), Value.fromObj(&new_s.obj) }, allocator, &err_msg);
    try std.testing.expectEqualStrings("hello", ObjString.fromObj(result.asObj()).bytes);
    result.asObj().destroy(allocator);
}

test "builtins: String.length returns byte count" {
    const allocator = std.testing.allocator;
    const s = try ObjString.create(allocator, "hello", null);
    defer s.obj.destroy(allocator);

    var err_msg: []const u8 = "";
    const result = try builtinStringLength(&[_]Value{Value.fromObj(&s.obj)}, allocator, &err_msg);
    try std.testing.expectEqual(@as(i32, 5), result.asInt());
}

test "builtins: Tuple.get returns Option" {
    const allocator = std.testing.allocator;
    const values = [_]Value{ Value.fromInt(10), Value.fromInt(20) };
    const t = try ObjTuple.create(allocator, &values);
    defer t.obj.destroy(allocator);

    var err_msg: []const u8 = "";
    const some = try builtinTupleGet(&[_]Value{ Value.fromObj(&t.obj), Value.fromInt(0) }, allocator, &err_msg);
    try std.testing.expect(isAdtVariant(some, 0, 0)); // Some
    try std.testing.expectEqual(@as(i32, 10), adtPayload(some, 0).asInt());
    some.asObj().destroy(allocator);

    const none = try builtinTupleGet(&[_]Value{ Value.fromObj(&t.obj), Value.fromInt(5) }, allocator, &err_msg);
    try std.testing.expect(isAdtVariant(none, 0, 1)); // None
    none.asObj().destroy(allocator);
}

test "builtins: Tuple.length returns size" {
    const allocator = std.testing.allocator;
    const values = [_]Value{ Value.fromInt(1), Value.fromInt(2), Value.fromInt(3) };
    const t = try ObjTuple.create(allocator, &values);
    defer t.obj.destroy(allocator);

    var err_msg: []const u8 = "";
    const result = try builtinTupleLength(&[_]Value{Value.fromObj(&t.obj)}, allocator, &err_msg);
    try std.testing.expectEqual(@as(i32, 3), result.asInt());
}

test "builtins: List.set creates modified copy" {
    const allocator = std.testing.allocator;
    const lst = try ObjList.create(allocator);
    defer lst.obj.destroy(allocator);
    try lst.items.append(allocator, Value.fromInt(1));
    try lst.items.append(allocator, Value.fromInt(2));
    try lst.items.append(allocator, Value.fromInt(3));

    var err_msg: []const u8 = "";
    const result = try builtinListSet(&[_]Value{ Value.fromObj(&lst.obj), Value.fromInt(1), Value.fromInt(99) }, allocator, &err_msg);
    const new_lst = ObjList.fromObj(result.asObj());
    try std.testing.expectEqual(@as(i32, 99), new_lst.items.items[1].asInt());
    // Original unchanged.
    try std.testing.expectEqual(@as(i32, 2), lst.items.items[1].asInt());
    result.asObj().destroy(allocator);
}

test "builtins: List.flatten flattens one level" {
    const allocator = std.testing.allocator;
    const inner1 = try ObjList.create(allocator);
    defer inner1.obj.destroy(allocator);
    try inner1.items.append(allocator, Value.fromInt(1));
    try inner1.items.append(allocator, Value.fromInt(2));

    const inner2 = try ObjList.create(allocator);
    defer inner2.obj.destroy(allocator);
    try inner2.items.append(allocator, Value.fromInt(3));

    const outer = try ObjList.create(allocator);
    defer outer.obj.destroy(allocator);
    try outer.items.append(allocator, Value.fromObj(&inner1.obj));
    try outer.items.append(allocator, Value.fromObj(&inner2.obj));
    try outer.items.append(allocator, Value.fromInt(4));

    var err_msg: []const u8 = "";
    const result = try builtinListFlatten(&[_]Value{Value.fromObj(&outer.obj)}, allocator, &err_msg);
    const flat = ObjList.fromObj(result.asObj());
    try std.testing.expectEqual(@as(usize, 4), flat.items.items.len);
    try std.testing.expectEqual(@as(i32, 1), flat.items.items[0].asInt());
    try std.testing.expectEqual(@as(i32, 2), flat.items.items[1].asInt());
    try std.testing.expectEqual(@as(i32, 3), flat.items.items[2].asInt());
    try std.testing.expectEqual(@as(i32, 4), flat.items.items[3].asInt());
    result.asObj().destroy(allocator);
}

test "builtins: Map.delete removes key" {
    const allocator = std.testing.allocator;
    const m = try ObjMap.create(allocator);
    defer m.obj.destroy(allocator);
    try m.entries.put(allocator, Value.fromInt(1), Value.fromInt(10));
    try m.entries.put(allocator, Value.fromInt(2), Value.fromInt(20));

    var err_msg: []const u8 = "";
    const result = try builtinMapDelete(&[_]Value{ Value.fromObj(&m.obj), Value.fromInt(1) }, allocator, &err_msg);
    const new_m = ObjMap.fromObj(result.asObj());
    try std.testing.expectEqual(@as(u32, 1), new_m.entries.count());
    try std.testing.expect(new_m.entries.get(Value.fromInt(2)) != null);
    try std.testing.expect(new_m.entries.get(Value.fromInt(1)) == null);
    // Original unchanged.
    try std.testing.expectEqual(@as(u32, 2), m.entries.count());
    result.asObj().destroy(allocator);
}

test "builtins: Map.keys and Map.values" {
    const allocator = std.testing.allocator;
    const m = try ObjMap.create(allocator);
    defer m.obj.destroy(allocator);
    try m.entries.put(allocator, Value.fromInt(1), Value.fromInt(10));
    try m.entries.put(allocator, Value.fromInt(2), Value.fromInt(20));

    var err_msg: []const u8 = "";
    const keys_result = try builtinMapKeys(&[_]Value{Value.fromObj(&m.obj)}, allocator, &err_msg);
    const keys_list = ObjList.fromObj(keys_result.asObj());
    try std.testing.expectEqual(@as(usize, 2), keys_list.items.items.len);
    keys_result.asObj().destroy(allocator);

    const vals_result = try builtinMapValues(&[_]Value{Value.fromObj(&m.obj)}, allocator, &err_msg);
    const vals_list = ObjList.fromObj(vals_result.asObj());
    try std.testing.expectEqual(@as(usize, 2), vals_list.items.items.len);
    vals_result.asObj().destroy(allocator);
}

test "builtins: Map.merge combines two maps" {
    const allocator = std.testing.allocator;
    const m1 = try ObjMap.create(allocator);
    defer m1.obj.destroy(allocator);
    try m1.entries.put(allocator, Value.fromInt(1), Value.fromInt(10));
    try m1.entries.put(allocator, Value.fromInt(2), Value.fromInt(20));

    const m2 = try ObjMap.create(allocator);
    defer m2.obj.destroy(allocator);
    try m2.entries.put(allocator, Value.fromInt(2), Value.fromInt(200)); // overwrite
    try m2.entries.put(allocator, Value.fromInt(3), Value.fromInt(30));

    var err_msg: []const u8 = "";
    const result = try builtinMapMerge(&[_]Value{ Value.fromObj(&m1.obj), Value.fromObj(&m2.obj) }, allocator, &err_msg);
    const merged = ObjMap.fromObj(result.asObj());
    try std.testing.expectEqual(@as(u32, 3), merged.entries.count());
    // Key 2 should have m2's value (200).
    try std.testing.expectEqual(@as(i32, 200), merged.entries.get(Value.fromInt(2)).?.asInt());
    result.asObj().destroy(allocator);
}

test "builtins: List.zip creates tuples" {
    const allocator = std.testing.allocator;
    const lst1 = try ObjList.create(allocator);
    defer lst1.obj.destroy(allocator);
    try lst1.items.append(allocator, Value.fromInt(1));
    try lst1.items.append(allocator, Value.fromInt(2));

    const lst2 = try ObjList.create(allocator);
    defer lst2.obj.destroy(allocator);
    try lst2.items.append(allocator, Value.fromInt(10));
    try lst2.items.append(allocator, Value.fromInt(20));
    try lst2.items.append(allocator, Value.fromInt(30)); // extra element ignored

    var err_msg: []const u8 = "";
    const result = try builtinListZip(&[_]Value{ Value.fromObj(&lst1.obj), Value.fromObj(&lst2.obj) }, allocator, &err_msg);
    const zipped = ObjList.fromObj(result.asObj());
    try std.testing.expectEqual(@as(usize, 2), zipped.items.items.len);
    // First element should be tuple (1, 10).
    const tup0 = ObjTuple.fromObj(zipped.items.items[0].asObj());
    try std.testing.expectEqual(@as(i32, 1), tup0.fields[0].asInt());
    try std.testing.expectEqual(@as(i32, 10), tup0.fields[1].asInt());
    // Second element should be tuple (2, 20).
    const tup1 = ObjTuple.fromObj(zipped.items.items[1].asObj());
    try std.testing.expectEqual(@as(i32, 2), tup1.fields[0].asInt());
    try std.testing.expectEqual(@as(i32, 20), tup1.fields[1].asInt());
    // Clean up tuples (they were heap allocated by zip).
    for (zipped.items.items) |item| item.asObj().destroy(allocator);
    result.asObj().destroy(allocator);
}
