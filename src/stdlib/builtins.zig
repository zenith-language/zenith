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
const uri_mod = @import("uri");
const aws_sig = @import("aws_sig");
const azure_sig = @import("azure_sig");
const auth_mod = @import("auth");

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
/// Scheduler pointer for par_map fiber dispatch (null in single-threaded mode).
threadlocal var current_scheduler_ptr: ?*anyopaque = null;

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

/// Set scheduler pointer for par_map fiber dispatch.
pub fn setSchedulerPtr(sched: ?*anyopaque) void {
    current_scheduler_ptr = sched;
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
    current_scheduler_ptr = null;
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

    // ── List module (indices 8-20) ───────────────────────────────────
    .{ .name = "List.get", .func = &builtinListGet, .arity_min = 2, .arity_max = 2 },
    .{ .name = "List.set", .func = &builtinListSet, .arity_min = 3, .arity_max = 3 },
    .{ .name = "List.append", .func = &builtinListAppend, .arity_min = 2, .arity_max = 2 },
    .{ .name = "List.length", .func = &builtinListLength, .arity_min = 1, .arity_max = 1 },
    .{ .name = "List.map", .func = &builtinListMap, .arity_min = 2, .arity_max = 2 },
    .{ .name = "List.filter", .func = &builtinListFilter, .arity_min = 2, .arity_max = 2 },
    .{ .name = "List.reduce", .func = &builtinListReduce, .arity_min = 3, .arity_max = 3 },
    .{ .name = "List.sort", .func = &builtinListSort, .arity_min = 1, .arity_max = 1 },
    .{ .name = "List.sort_by", .func = &builtinListSortBy, .arity_min = 2, .arity_max = 2 },
    .{ .name = "List.reverse", .func = &builtinListReverse, .arity_min = 1, .arity_max = 1 },
    .{ .name = "List.zip", .func = &builtinListZip, .arity_min = 2, .arity_max = 2 },
    .{ .name = "List.flatten", .func = &builtinListFlatten, .arity_min = 1, .arity_max = 1 },
    .{ .name = "List.contains", .func = &builtinListContains, .arity_min = 2, .arity_max = 2 },

    // ── Map module (indices 21-28) ───────────────────────────────────
    .{ .name = "Map.get", .func = &builtinMapGet, .arity_min = 2, .arity_max = 2 },
    .{ .name = "Map.set", .func = &builtinMapSet, .arity_min = 3, .arity_max = 3 },
    .{ .name = "Map.delete", .func = &builtinMapDelete, .arity_min = 2, .arity_max = 2 },
    .{ .name = "Map.keys", .func = &builtinMapKeys, .arity_min = 1, .arity_max = 1 },
    .{ .name = "Map.values", .func = &builtinMapValues, .arity_min = 1, .arity_max = 1 },
    .{ .name = "Map.merge", .func = &builtinMapMerge, .arity_min = 2, .arity_max = 2 },
    .{ .name = "Map.contains", .func = &builtinMapContains, .arity_min = 2, .arity_max = 2 },
    .{ .name = "Map.length", .func = &builtinMapLength, .arity_min = 1, .arity_max = 1 },

    // ── Tuple module (indices 29-30) ─────────────────────────────────
    .{ .name = "Tuple.get", .func = &builtinTupleGet, .arity_min = 2, .arity_max = 2 },
    .{ .name = "Tuple.length", .func = &builtinTupleLength, .arity_min = 1, .arity_max = 1 },

    // ── String module (indices 31-40) ────────────────────────────────
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

    // ── Result module (indices 41-48) ────────────────────────────────
    .{ .name = "Result.Ok", .func = &builtinResultOk, .arity_min = 1, .arity_max = 1 },
    .{ .name = "Result.Err", .func = &builtinResultErr, .arity_min = 1, .arity_max = 1 },
    .{ .name = "Result.map_ok", .func = &builtinResultMapOk, .arity_min = 2, .arity_max = 2 },
    .{ .name = "Result.map_err", .func = &builtinResultMapErr, .arity_min = 2, .arity_max = 2 },
    .{ .name = "Result.then", .func = &builtinResultThen, .arity_min = 2, .arity_max = 2 },
    .{ .name = "Result.unwrap_or", .func = &builtinResultUnwrapOr, .arity_min = 2, .arity_max = 2 },
    .{ .name = "Result.is_ok", .func = &builtinResultIsOk, .arity_min = 1, .arity_max = 1 },
    .{ .name = "Result.is_err", .func = &builtinResultIsErr, .arity_min = 1, .arity_max = 1 },

    // ── Option module (indices 49-55) ────────────────────────────────
    .{ .name = "Option.Some", .func = &builtinOptionSome, .arity_min = 1, .arity_max = 1 },
    .{ .name = "Option.None", .func = &builtinOptionNone, .arity_min = 0, .arity_max = 0 },
    .{ .name = "Option.map", .func = &builtinOptionMap, .arity_min = 2, .arity_max = 2 },
    .{ .name = "Option.unwrap_or", .func = &builtinOptionUnwrapOr, .arity_min = 2, .arity_max = 2 },
    .{ .name = "Option.is_some", .func = &builtinOptionIsSome, .arity_min = 1, .arity_max = 1 },
    .{ .name = "Option.is_none", .func = &builtinOptionIsNone, .arity_min = 1, .arity_max = 1 },
    .{ .name = "Option.to_result", .func = &builtinOptionToResult, .arity_min = 2, .arity_max = 2 },

    // ── List.filter_map (index 56) ──────────────────────────────────
    .{ .name = "List.filter_map", .func = &builtinListFilterMap, .arity_min = 2, .arity_max = 2 },

    // ── GC (indices 57-58) ──────────────────────────────────────────
    .{ .name = "gc", .func = &builtinGC, .arity_min = 0, .arity_max = 0 },
    .{ .name = "gc_stats", .func = &builtinGCStats, .arity_min = 0, .arity_max = 0 },

    // ── Stream sources (indices 59-60) ────────────────────────────
    .{ .name = "repeat", .func = &builtinRepeat, .arity_min = 1, .arity_max = 1 },
    .{ .name = "iterate", .func = &builtinIterate, .arity_min = 2, .arity_max = 2 },

    // ── Stream transforms (indices 61-64) ─────────────────────────
    .{ .name = "map", .func = &builtinMap, .arity_min = 2, .arity_max = 2 },
    .{ .name = "filter", .func = &builtinFilter, .arity_min = 2, .arity_max = 2 },
    .{ .name = "take", .func = &builtinTake, .arity_min = 2, .arity_max = 2 },
    .{ .name = "drop", .func = &builtinDrop, .arity_min = 2, .arity_max = 2 },

    // ── Stream terminals (indices 65-66) ──────────────────────────
    .{ .name = "collect", .func = &builtinCollect, .arity_min = 1, .arity_max = 1 },
    .{ .name = "count", .func = &builtinCount, .arity_min = 1, .arity_max = 1 },

    // ── Stream transforms continued (indices 67-75) ─────────────
    .{ .name = "flat_map", .func = &builtinFlatMap, .arity_min = 2, .arity_max = 2 },
    .{ .name = "filter_map", .func = &builtinFilterMap, .arity_min = 2, .arity_max = 2 },
    .{ .name = "filter_ok", .func = &builtinFilterOk, .arity_min = 1, .arity_max = 1 },
    .{ .name = "filter_err", .func = &builtinFilterErr, .arity_min = 1, .arity_max = 1 },
    .{ .name = "scan", .func = &builtinScan, .arity_min = 3, .arity_max = 3 },
    .{ .name = "distinct", .func = &builtinDistinct, .arity_min = 1, .arity_max = 1 },
    .{ .name = "zip", .func = &builtinZip, .arity_min = 2, .arity_max = 2 },
    .{ .name = "flatten", .func = &builtinFlatten, .arity_min = 1, .arity_max = 1 },
    .{ .name = "tap", .func = &builtinTap, .arity_min = 2, .arity_max = 2 },
    .{ .name = "batch", .func = &builtinBatch, .arity_min = 2, .arity_max = 2 },
    .{ .name = "sort_by", .func = &builtinStreamSortBy, .arity_min = 2, .arity_max = 3 },

    // ── Stream terminals continued (indices 76-82) ──────────────
    .{ .name = "sum", .func = &builtinSum, .arity_min = 1, .arity_max = 1 },
    .{ .name = "reduce", .func = &builtinReduce, .arity_min = 3, .arity_max = 3 },
    .{ .name = "first", .func = &builtinFirst, .arity_min = 1, .arity_max = 1 },
    .{ .name = "last", .func = &builtinLast, .arity_min = 1, .arity_max = 1 },
    .{ .name = "each", .func = &builtinEach, .arity_min = 2, .arity_max = 2 },
    .{ .name = "min", .func = &builtinMin, .arity_min = 1, .arity_max = 1 },
    .{ .name = "max", .func = &builtinMax, .arity_min = 1, .arity_max = 1 },

    // ── Stream error handling (index 83) ────────────────────────
    .{ .name = "partition_result", .func = &builtinPartitionResult, .arity_min = 1, .arity_max = 1 },

    // ── Json module (indices 84-85) ──────────────────────────────
    .{ .name = "Json.decode", .func = &builtinJsonDecode, .arity_min = 1, .arity_max = 1 },
    .{ .name = "Json.encode", .func = &builtinJsonEncode, .arity_min = 1, .arity_max = 1 },

    // ── I/O: source/sink (indices 86-87) ──────────────────────
    .{ .name = "source", .func = &builtinSource, .arity_min = 1, .arity_max = 3 },
    .{ .name = "sink", .func = &builtinSink, .arity_min = 2, .arity_max = 5 },

    // ── Concurrency stream operators (indices 88-91) ─────────
    .{ .name = "par_map", .func = &builtinParMap, .arity_min = 2, .arity_max = 3 },
    .{ .name = "par_map_unordered", .func = &builtinParMapUnordered, .arity_min = 2, .arity_max = 3 },
    .{ .name = "par_map_result", .func = &builtinParMapResult, .arity_min = 2, .arity_max = 3 },
    .{ .name = "tick", .func = &builtinTick, .arity_min = 1, .arity_max = 1 },

    // ── Rate limiting / buffering (indices 92-93) ───────────
    .{ .name = "throttle", .func = &builtinThrottle, .arity_min = 3, .arity_max = 3 },
    .{ .name = "buffer", .func = &builtinBuffer, .arity_min = 2, .arity_max = 2 },

    // ── Result-aware combinators (indices 94-96) ───────────
    .{ .name = "single", .func = &builtinSingle, .arity_min = 1, .arity_max = 1 },
    .{ .name = "ok_or", .func = &builtinOkOr, .arity_min = 2, .arity_max = 2 },
    .{ .name = "tap_err", .func = &builtinTapErr, .arity_min = 2, .arity_max = 2 },

    // ── Standalone Result/Option helpers (indices 97-99) ──
    .{ .name = "unwrap", .func = &builtinUnwrap, .arity_min = 1, .arity_max = 1 },
    .{ .name = "unwrap_or", .func = &builtinUnwrapOr, .arity_min = 2, .arity_max = 2 },
    .{ .name = "avg", .func = &builtinAvg, .arity_min = 1, .arity_max = 1 },

    // ── Environment (index 100) ────────────────────────────
    .{ .name = "env", .func = &builtinEnv, .arity_min = 1, .arity_max = 2 },
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

/// List.sort_by(list, fn) -> List: sort using a key function.
/// fn(element) -> comparable value. Elements are sorted by the extracted key.
fn builtinListSortBy(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    if (!args[0].isObjType(.list)) {
        err_msg.* = "List.sort_by expects a list as first argument";
        return error.RuntimeError;
    }
    const src = ObjList.fromObj(args[0].asObj());
    const key_fn = args[1];

    // Pre-compute keys for each element to avoid calling the closure inside sort.
    const len = src.items.items.len;
    const keys = allocator.alloc(Value, len) catch return error.OutOfMemory;
    defer allocator.free(keys);

    for (src.items.items, 0..) |item, i| {
        keys[i] = callClosure(key_fn, &[_]Value{item}) catch {
            err_msg.* = "List.sort_by: key function failed";
            return error.RuntimeError;
        };
    }

    // Build index array and sort by keys (Schwartzian transform).
    const indices = allocator.alloc(usize, len) catch return error.OutOfMemory;
    defer allocator.free(indices);
    for (0..len) |i| indices[i] = i;

    std.mem.sort(usize, indices, keys, struct {
        fn lessThan(k: []Value, a: usize, b: usize) bool {
            return valueCompare({}, k[a], k[b]);
        }
    }.lessThan);

    // Build result list in sorted order.
    const new_list = try ObjList.create(allocator);
    try new_list.items.ensureTotalCapacity(allocator, len);
    for (indices) |idx| {
        new_list.items.appendAssumeCapacity(src.items.items[idx]);
    }
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
        // Option.Some(x): unwrap and keep.
        if (isAdtVariant(result, 0, 0)) {
            const payload = adtPayload(result, 0);
            try new_list.items.append(allocator, payload);
        } else if (isAdtVariant(result, 0, 1)) {
            // Option.None: skip.
        } else if (result.isNil()) {
            // nil: skip (convenient for ?. chaining).
        } else {
            // Non-Option, non-nil value: keep as-is.
            try new_list.items.append(allocator, result);
        }
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
        stream_mod.setScheduler(current_scheduler_ptr);
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

fn autoWrapList(val: Value, allocator: Allocator) NativeError!Value {
    if (val.isObjType(.list)) {
        const state = try allocator.create(StreamState);
        state.* = .{ .flatten_op = .{
            .upstream = Value.nil,
            .inner_list = val,
            .inner_idx = 0,
            .inner_stream = Value.nil,
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

    // If already a list, return as-is (idempotent).
    if (first.isObjType(.list)) {
        return first;
    }

    if (!first.isObjType(.stream)) {
        err_msg.* = "collect() expects a stream or list as first argument";
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

///// filter_ok(stream) -> Stream: keep only Result.Ok payloads, skip Err.
fn builtinFilterOk(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }
    if (!first.isObjType(.stream)) {
        err_msg.* = "filter_ok() expects a stream as first argument";
        return error.RuntimeError;
    }
    const state = try allocator.create(StreamState);
    state.* = .{ .filter_ok_op = .{ .upstream = first } };
    return createStream(state, allocator);
}

/// filter_err(stream) -> Stream: keep only Result.Err payloads, skip Ok.
fn builtinFilterErr(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }
    if (!first.isObjType(.stream)) {
        err_msg.* = "filter_err() expects a stream as first argument";
        return error.RuntimeError;
    }
    const state = try allocator.create(StreamState);
    state.* = .{ .filter_err_op = .{ .upstream = first } };
    return createStream(state, allocator);
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

/// sort_by(stream, key_fn) -> Stream: collect, sort by key, re-emit as stream.
/// sort_by(stream, key_fn) or sort_by(stream, key_fn, :desc)
fn builtinStreamSortBy(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    const descending = if (args.len > 2) blk: {
        if (atomName(args[2])) |name| {
            if (std.mem.eql(u8, name, "desc")) break :blk true;
            if (std.mem.eql(u8, name, "asc")) break :blk false;
        }
        err_msg.* = "sort_by() direction must be :asc or :desc";
        return error.RuntimeError;
    } else false;
    return streamSortByImpl(args, allocator, err_msg, descending);
}

fn streamSortByImpl(args: []const Value, allocator: Allocator, err_msg: *[]const u8, descending: bool) NativeError!Value {
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }
    if (!first.isObjType(.stream)) {
        err_msg.* = "sort_by() expects a stream as first argument";
        return error.RuntimeError;
    }
    const key_fn = args[1];
    const state = try allocator.create(StreamState);
    state.* = .{ .sort_by_op = .{
        .upstream = first,
        .key_fn = key_fn,
        .sorted = null,
        .idx = 0,
        .descending = descending,
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

const SourceFormat = enum { text, jsonl, json, csv };

/// Detect source format from optional second atom argument.
fn detectSourceFormat(args: []const Value) SourceFormat {
    if (args.len > 1) {
        if (atomName(args[1])) |name| {
            if (std.mem.eql(u8, name, "jsonl")) return .jsonl;
            if (std.mem.eql(u8, name, "json")) return .json;
            if (std.mem.eql(u8, name, "csv")) return .csv;
        }
    }
    return .text;
}

// ── Sink format & options ────────────────────────────────────────────

const SinkFormat = enum { text, jsonl, json, json_pretty, csv, tsv, table, markdown };

const Compression = enum { none, gzip };

const SinkOptions = struct {
    append: bool = false,
    header: bool = true,
    delimiter: u8 = ',',
    indent: u8 = 2,
    compress: Compression = .none,
    retry: u8 = 0,
    timeout: u32 = 0, // 0 = default
};

fn parseSinkFormat(args: []const Value) SinkFormat {
    if (args.len > 2) {
        if (atomName(args[2])) |name| {
            if (std.mem.eql(u8, name, "jsonl")) return .jsonl;
            if (std.mem.eql(u8, name, "json")) return .json;
            if (std.mem.eql(u8, name, "json_pretty")) return .json_pretty;
            if (std.mem.eql(u8, name, "csv")) return .csv;
            if (std.mem.eql(u8, name, "tsv")) return .tsv;
            if (std.mem.eql(u8, name, "table")) return .table;
            if (std.mem.eql(u8, name, "markdown")) return .markdown;
        }
    }
    return .text;
}

fn parseSinkOptions(args: []const Value) SinkOptions {
    var opts = SinkOptions{};
    if (args.len <= 3) return opts;
    const ov = args[3];

    // Handle record syntax: {header: false, append: true}
    if (ov.isObjType(.record)) {
        const rec = ObjRecord.fromObj(ov.asObj());
        for (0..rec.field_count) |i| {
            const kn = rec.field_names[i];
            const val = rec.field_values[i];
            applySinkOption(&opts, kn, val);
        }
        return opts;
    }

    // Handle map syntax: {"header": false}
    if (ov.isObjType(.map)) {
        const m = ObjMap.fromObj(ov.asObj());
        var it = m.entries.iterator();
        while (it.next()) |entry| {
            const key_name = blk: {
                const k = entry.key_ptr.*;
                if (k.isAtom()) {
                    break :blk atomName(k);
                } else if (k.isObj() and k.asObj().obj_type == .string) {
                    break :blk @as(?[]const u8, ObjString.fromObj(k.asObj()).bytes);
                }
                break :blk @as(?[]const u8, null);
            };
            if (key_name) |kn| {
                applySinkOption(&opts, kn, entry.value_ptr.*);
            }
        }
    }
    return opts;
}

fn applySinkOption(opts: *SinkOptions, kn: []const u8, val: Value) void {
    if (std.mem.eql(u8, kn, "append")) {
        if (val.isBool()) opts.append = val.asBool();
    } else if (std.mem.eql(u8, kn, "header")) {
        if (val.isBool()) opts.header = val.asBool();
    } else if (std.mem.eql(u8, kn, "delimiter")) {
        if (val.isObj() and val.asObj().obj_type == .string) {
            const s = ObjString.fromObj(val.asObj());
            if (s.bytes.len > 0) opts.delimiter = s.bytes[0];
        }
    } else if (std.mem.eql(u8, kn, "indent")) {
        if (val.isInt()) {
            const iv = val.asInt();
            if (iv >= 0 and iv <= 16) opts.indent = @intCast(@as(u32, @bitCast(iv)));
        }
    } else if (std.mem.eql(u8, kn, "compress")) {
        if (val.isAtom()) {
            if (atomName(val)) |name| {
                if (std.mem.eql(u8, name, "gzip")) opts.compress = .gzip;
            }
        }
    } else if (std.mem.eql(u8, kn, "retry")) {
        if (val.isInt()) {
            const iv = val.asInt();
            if (iv >= 0 and iv <= 10) opts.retry = @intCast(@as(u32, @bitCast(iv)));
        }
    } else if (std.mem.eql(u8, kn, "timeout")) {
        if (val.isInt()) {
            const iv = val.asInt();
            if (iv > 0) opts.timeout = @intCast(@as(u32, @bitCast(iv)));
        }
    }
}

// ── Sink helpers: tabular data ───────────────────────────────────────

const SinkHeaders = struct {
    names: []const []const u8,
    owned: bool, // true if caller must free `names` slice
};

fn sinkExtractHeaders(value: Value, allocator: Allocator, err_msg: *[]const u8) ?SinkHeaders {
    if (value.isObjType(.record)) {
        const rec = ObjRecord.fromObj(value.asObj());
        return .{ .names = rec.field_names, .owned = false };
    }
    if (value.isObjType(.map)) {
        const m = ObjMap.fromObj(value.asObj());
        var names = std.ArrayListUnmanaged([]const u8){};
        var it = m.entries.iterator();
        while (it.next()) |entry| {
            const k = entry.key_ptr.*;
            const name: ?[]const u8 = if (k.isAtom())
                atomName(k)
            else if (k.isObj() and k.asObj().obj_type == .string)
                ObjString.fromObj(k.asObj()).bytes
            else
                null;
            if (name) |n| {
                names.append(allocator, n) catch {
                    err_msg.* = "out of memory";
                    return null;
                };
            }
        }
        if (names.items.len > 0) {
            const owned = names.toOwnedSlice(allocator) catch {
                err_msg.* = "out of memory";
                return null;
            };
            return .{ .names = owned, .owned = true };
        }
        names.deinit(allocator);
    }
    err_msg.* = "sink() :csv/:tsv/:table/:markdown expects stream of maps or records";
    return null;
}

fn sinkFreeHeaders(hdr: SinkHeaders, allocator: Allocator) void {
    if (hdr.owned) {
        allocator.free(hdr.names);
    }
}

fn sinkLookupField(value: Value, key_name: []const u8) ?Value {
    if (value.isObjType(.record)) {
        const rec = ObjRecord.fromObj(value.asObj());
        for (rec.field_names, 0..) |fn_name, i| {
            if (std.mem.eql(u8, fn_name, key_name)) return rec.field_values[i];
        }
        return null;
    }
    if (value.isObjType(.map)) {
        const m = ObjMap.fromObj(value.asObj());
        var it = m.entries.iterator();
        while (it.next()) |entry| {
            const k = entry.key_ptr.*;
            const name: ?[]const u8 = if (k.isAtom())
                atomName(k)
            else if (k.isObj() and k.asObj().obj_type == .string)
                ObjString.fromObj(k.asObj()).bytes
            else
                null;
            if (name) |n| {
                if (std.mem.eql(u8, n, key_name)) return entry.value_ptr.*;
            }
        }
        return null;
    }
    return null;
}

fn sinkCsvQuoteField(field: []const u8, delimiter: u8, bw: anytype) !void {
    var needs_quote = false;
    for (field) |c| {
        if (c == delimiter or c == '"' or c == '\n' or c == '\r') {
            needs_quote = true;
            break;
        }
    }
    if (needs_quote) {
        try bw.interface.writeByte('"');
        for (field) |c| {
            if (c == '"') {
                try bw.interface.writeAll("\"\"");
            } else {
                try bw.interface.writeByte(c);
            }
        }
        try bw.interface.writeByte('"');
    } else {
        try bw.interface.writeAll(field);
    }
}

fn sinkWritePadding(bw: anytype, count: usize) void {
    var i: usize = 0;
    while (i < count) : (i += 1) {
        bw.interface.writeByte(' ') catch return;
    }
}

fn sinkWriteDashes(bw: anytype, count: usize) void {
    var i: usize = 0;
    while (i < count) : (i += 1) {
        bw.interface.writeByte('-') catch return;
    }
}

/// Fetch HTTP(S) URL body. Returns an ArrayList; caller must call deinit().
fn httpFetch(url: []const u8, allocator: Allocator) !std.ArrayListUnmanaged(u8) {
    var client: std.http.Client = .{ .allocator = allocator };
    defer client.deinit();

    var aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer aw.deinit();

    const result = client.fetch(.{
        .location = .{ .url = url },
        .response_writer = &aw.writer,
    }) catch return error.HttpFetchFailed;

    if (result.status != .ok) {
        aw.deinit();
        return error.HttpStatusError;
    }

    return aw.toArrayList();
}

/// Parse JSON content and create a json_array_iter stream.
/// If the parsed value is an array, each element is wrapped in Result.Ok.
/// If it's a single value, a one-element stream with Result.Ok is created.
/// On parse error, a one-element stream with Result.Err is created.
fn createJsonStream(content: []const u8, allocator: Allocator, _: *[]const u8) NativeError!Value {
    // Set up JSON module callbacks.
    if (current_vm) |vm_ptr| {
        if (track_obj_fn) |tfn| {
            json_mod.setVM(vm_ptr, tfn);
        }
    }
    defer json_mod.clearVM();

    const items_list = try ObjList.create(allocator);
    trackObj(&items_list.obj);

    const parse_result = json_mod.parse(content, allocator);
    switch (parse_result) {
        .ok => |val| {
            // If parsed value is an array, stream each element as Result.Ok.
            if (val.isObjType(.list)) {
                const parsed_list = ObjList.fromObj(val.asObj());
                try items_list.items.ensureTotalCapacity(allocator, parsed_list.items.items.len);
                for (parsed_list.items.items) |elem| {
                    const ok_adt = try ObjAdt.create(allocator, 1, 0, &[_]Value{elem});
                    trackObj(&ok_adt.obj);
                    items_list.items.appendAssumeCapacity(Value.fromObj(&ok_adt.obj));
                }
            } else {
                // Single value: one-element stream.
                const ok_adt = try ObjAdt.create(allocator, 1, 0, &[_]Value{val});
                trackObj(&ok_adt.obj);
                try items_list.items.append(allocator, Value.fromObj(&ok_adt.obj));
            }
        },
        .err => |e| {
            // Parse error: single Result.Err element.
            const msg_str = try ObjString.create(allocator, e.message, null);
            trackObj(&msg_str.obj);
            const field_names = [_][]const u8{ "message", "position" };
            const field_values = [_]Value{
                Value.fromObj(&msg_str.obj),
                Value.fromInt(@intCast(@min(e.position, @as(usize, @intCast(std.math.maxInt(i32)))))),
            };
            const record = try ObjRecord.create(allocator, &field_names, &field_values);
            trackObj(&record.obj);
            const err_adt = try ObjAdt.create(allocator, 1, 1, &[_]Value{Value.fromObj(&record.obj)});
            trackObj(&err_adt.obj);
            try items_list.items.append(allocator, Value.fromObj(&err_adt.obj));
        },
    }

    const state = try allocator.create(StreamState);
    state.* = .{ .json_array_iter = .{
        .items = Value.fromObj(&items_list.obj),
        .idx = 0,
    } };
    return createStream(state, allocator);
}

/// Parse CSV content into a stream of records.
/// First row = headers, subsequent rows = records with those field names.
fn createCsvStream(content: []const u8, delimiter: u8, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    const items_list = try ObjList.create(allocator);
    trackObj(&items_list.obj);

    var lines = std.mem.splitScalar(u8, content, '\n');

    // First line = headers.
    const header_line = lines.next() orelse {
        // Empty file — return empty stream.
        const state = try allocator.create(StreamState);
        state.* = .{ .json_array_iter = .{ .items = Value.fromObj(&items_list.obj), .idx = 0 } };
        return createStream(state, allocator);
    };

    var raw_headers = std.ArrayListUnmanaged([]const u8){};
    defer raw_headers.deinit(allocator);
    try csvSplitFields(header_line, delimiter, &raw_headers, allocator);

    if (raw_headers.items.len == 0) {
        err_msg.* = "CSV source: no headers found in first row";
        return error.RuntimeError;
    }

    // Dupe header names so they outlive the content buffer.
    // ObjRecord.create copies the pointer array but not the strings themselves,
    // so duped strings must stay alive as long as the records exist (GC-managed).
    const field_names = try allocator.alloc([]const u8, raw_headers.items.len);
    for (raw_headers.items, 0..) |h, i| {
        field_names[i] = try allocator.dupe(u8, h);
    }

    // Parse data rows.
    var row_num: usize = 1;
    while (lines.next()) |raw_line| {
        // Strip trailing \r.
        const line = if (raw_line.len > 0 and raw_line[raw_line.len - 1] == '\r')
            raw_line[0 .. raw_line.len - 1]
        else
            raw_line;

        // Skip empty lines.
        if (line.len == 0) continue;

        row_num += 1;

        var fields = std.ArrayListUnmanaged([]const u8){};
        defer fields.deinit(allocator);
        csvSplitFields(line, delimiter, &fields, allocator) catch {
            err_msg.* = "CSV parse error";
            return error.RuntimeError;
        };

        // Build field values — match number of headers.
        const field_values = try allocator.alloc(Value, field_names.len);
        defer allocator.free(field_values);

        for (0..field_names.len) |i| {
            if (i < fields.items.len) {
                const str = try ObjString.create(allocator, fields.items[i], null);
                trackObj(&str.obj);
                field_values[i] = Value.fromObj(&str.obj);
            } else {
                field_values[i] = Value.nil;
            }
        }

        const record = try ObjRecord.create(allocator, field_names, field_values);
        trackObj(&record.obj);

        // Wrap in Result.Ok.
        const ok_adt = try ObjAdt.create(allocator, 1, 0, &[_]Value{Value.fromObj(&record.obj)});
        trackObj(&ok_adt.obj);
        try items_list.items.append(allocator, Value.fromObj(&ok_adt.obj));
    }

    const state = try allocator.create(StreamState);
    state.* = .{ .json_array_iter = .{ .items = Value.fromObj(&items_list.obj), .idx = 0 } };
    return createStream(state, allocator);
}

/// Split a CSV line into fields, handling quoted fields.
fn csvSplitFields(line: []const u8, delimiter: u8, fields: *std.ArrayListUnmanaged([]const u8), allocator: Allocator) !void {
    if (line.len == 0) return;
    var pos: usize = 0;
    while (true) {
        if (pos < line.len and line[pos] == '"') {
            // Quoted field.
            pos += 1;
            var field_buf = std.ArrayListUnmanaged(u8){};
            errdefer field_buf.deinit(allocator);

            while (pos < line.len) {
                if (line[pos] == '"') {
                    if (pos + 1 < line.len and line[pos + 1] == '"') {
                        try field_buf.append(allocator, '"');
                        pos += 2;
                    } else {
                        pos += 1;
                        break;
                    }
                } else {
                    try field_buf.append(allocator, line[pos]);
                    pos += 1;
                }
            }
            const owned = try allocator.dupe(u8, field_buf.items);
            field_buf.deinit(allocator);
            try fields.append(allocator, owned);
        } else {
            // Unquoted field.
            const start = pos;
            while (pos < line.len and line[pos] != delimiter) : (pos += 1) {}
            try fields.append(allocator, line[start..pos]);
        }
        // After field: expect delimiter or end of line.
        if (pos >= line.len) break;
        if (line[pos] == delimiter) {
            pos += 1;
            // If delimiter is last char, emit empty trailing field.
            if (pos >= line.len) {
                try fields.append(allocator, "");
                break;
            }
        } else {
            break;
        }
    }
}

/// source(uri, format?, auth?) -> Stream
/// URI-based transport dispatch. Supported schemes:
///   file://, fs://, bare paths → local file
///   http://, https://         → HTTP fetch
///   s3://, gs://, az://       → cloud
///   :stdin atom               → standard input
/// Format: :text (default), :jsonl, :json, :csv
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

    // Must be a string path or URL.
    if (!first.isString()) {
        err_msg.* = "source() expects a string URI, path, or :stdin atom as first argument";
        return error.RuntimeError;
    }

    const raw_str = ObjString.fromObj(first.asObj());
    const raw = raw_str.bytes;
    const format = detectSourceFormat(args);

    // Parse URI to determine transport scheme.
    const uri = uri_mod.parse(raw) catch {
        err_msg.* = "unsupported URI scheme";
        return error.RuntimeError;
    };

    // Extract optional auth parameter (args[2] if present).
    const auth_val: ?Value = if (args.len > 2) args[2] else null;

    switch (uri.scheme) {
        .file => return sourceFile(uri.path, format, allocator, err_msg),
        .http, .https => return sourceHttp(raw, format, allocator, err_msg),
        .s3 => return sourceS3(uri, format, auth_val, allocator, err_msg),
        .gs => return sourceGcs(uri, format, auth_val, allocator, err_msg),
        .az => return sourceAzure(uri, format, auth_val, allocator, err_msg),
    }
}

/// Local file source: reads from the filesystem.
fn sourceFile(path: []const u8, format: SourceFormat, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    // CSV: read entire file, parse into stream of records.
    if (format == .csv) {
        const content = std.fs.cwd().readFileAlloc(allocator, path, 64 * 1024 * 1024) catch |err| {
            err_msg.* = switch (err) {
                error.FileNotFound => "file not found",
                error.AccessDenied => "permission denied",
                else => "failed to read file",
            };
            return error.RuntimeError;
        };
        defer allocator.free(content);
        return createCsvStream(content, ',', allocator, err_msg);
    }

    // JSON format reads entire file and parses.
    if (format == .json) {
        const content = std.fs.cwd().readFileAlloc(allocator, path, 64 * 1024 * 1024) catch |err| {
            err_msg.* = switch (err) {
                error.FileNotFound => "file not found",
                error.AccessDenied => "permission denied",
                else => "failed to read file",
            };
            return error.RuntimeError;
        };
        defer allocator.free(content);
        return createJsonStream(content, allocator, err_msg);
    }

    // Text or JSONL: streaming line-by-line reader.
    const file = std.fs.cwd().openFile(path, .{}) catch |err| {
        err_msg.* = switch (err) {
            error.FileNotFound => "file not found",
            error.AccessDenied => "permission denied",
            else => "failed to open file",
        };
        return error.RuntimeError;
    };

    const frs = StreamState.FileReaderState.create(allocator, file, false) catch {
        file.close();
        err_msg.* = "failed to create file reader";
        return error.RuntimeError;
    };

    const state = try allocator.create(StreamState);
    if (format == .jsonl) {
        state.* = .{ .jsonl_reader = .{ .frs = frs } };
    } else {
        state.* = .{ .file_reader = .{ .frs = frs } };
    }
    return createStream(state, allocator);
}

/// HTTP(S) source: fetches URL and creates a stream based on format.
fn sourceHttp(url: []const u8, format: SourceFormat, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    var body_list = httpFetch(url, allocator) catch {
        err_msg.* = "HTTP request failed";
        return error.RuntimeError;
    };

    if (format == .json) {
        defer body_list.deinit(allocator);
        return createJsonStream(body_list.items, allocator, err_msg);
    }
    if (format == .csv) {
        defer body_list.deinit(allocator);
        return createCsvStream(body_list.items, ',', allocator, err_msg);
    }

    // For text and JSONL: transfer ownership of body to a memory reader.
    const owned = body_list.toOwnedSlice(allocator) catch {
        body_list.deinit(allocator);
        err_msg.* = "failed to allocate HTTP response buffer";
        return error.RuntimeError;
    };

    const state = try allocator.create(StreamState);
    state.* = .{ .memory_reader = .{
        .data = owned,
        .cursor = 0,
        .is_jsonl = (format == .jsonl),
        .line_number = 0,
        .allocator = allocator,
    } };
    return createStream(state, allocator);
}

/// S3 source: fetches an object from S3 using AWS Signature V4.
/// Supports single-key fetch and glob patterns (e.g. "s3://bucket/prefix/*.jsonl").
fn sourceS3(
    uri: uri_mod.ParsedUri,
    format: SourceFormat,
    auth_val: ?Value,
    allocator: Allocator,
    err_msg: *[]const u8,
) NativeError!Value {
    const bucket = uri.host orelse {
        err_msg.* = "S3 URI must include bucket name: s3://bucket/key";
        return error.RuntimeError;
    };
    const key = uri.path;

    // Check for glob pattern — if key contains '*' or '?', use ListObjectsV2.
    if (std.mem.indexOfAny(u8, key, "*?")) |_| {
        return sourceS3Glob(bucket, key, format, auth_val, allocator, err_msg);
    }

    // Resolve credentials.
    var creds = auth_mod.resolveAwsCredentials(auth_val, null, &atomName, allocator) catch |err| {
        err_msg.* = switch (err) {
            error.MissingCredentials => "S3 credentials not found. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or pass auth: parameter",
            error.InvalidAuthValue => "auth: parameter must be an atom (named profile) or record {access_key:, secret_key:, region:}",
            error.InvalidProfile => "AWS profile not found in ~/.aws/credentials",
            error.ConfigReadError => "failed to read AWS credentials file",
        };
        return error.RuntimeError;
    };
    defer creds.deinit(allocator);

    // Fetch S3 object.
    var body = s3GetObject(bucket, key, creds, allocator) catch {
        err_msg.* = "S3 GetObject request failed";
        return error.RuntimeError;
    };

    if (format == .json) {
        defer body.deinit(allocator);
        return createJsonStream(body.items, allocator, err_msg);
    }
    if (format == .csv) {
        defer body.deinit(allocator);
        return createCsvStream(body.items, ',', allocator, err_msg);
    }

    // Transfer ownership to memory reader for text/jsonl streaming.
    const owned = body.toOwnedSlice(allocator) catch {
        body.deinit(allocator);
        err_msg.* = "failed to allocate S3 response buffer";
        return error.RuntimeError;
    };

    const state = try allocator.create(StreamState);
    state.* = .{ .memory_reader = .{
        .data = owned,
        .cursor = 0,
        .is_jsonl = (format == .jsonl),
        .line_number = 0,
        .allocator = allocator,
    } };
    return createStream(state, allocator);
}

/// Make a signed S3 GetObject HTTP request.
fn s3GetObject(
    bucket: []const u8,
    key: []const u8,
    creds: auth_mod.AwsCredentials,
    allocator: Allocator,
) !std.ArrayListUnmanaged(u8) {
    // Build URL: https://{bucket}.s3.{region}.amazonaws.com/{key}
    var url_buf = std.ArrayListUnmanaged(u8){};
    defer url_buf.deinit(allocator);
    const uw = url_buf.writer(allocator);
    try uw.writeAll("https://");
    try uw.writeAll(bucket);
    try uw.writeAll(".s3.");
    try uw.writeAll(creds.region);
    try uw.writeAll(".amazonaws.com/");
    try uw.writeAll(key);
    const url = url_buf.items;

    // Build host header value.
    var host_buf: [512]u8 = undefined;
    const host = std.fmt.bufPrint(&host_buf, "{s}.s3.{s}.amazonaws.com", .{ bucket, creds.region }) catch
        return error.HttpFetchFailed;

    // Build canonical URI path: /{key}
    var path_buf: [2048]u8 = undefined;
    const uri_path = std.fmt.bufPrint(&path_buf, "/{s}", .{key}) catch
        return error.HttpFetchFailed;

    // Sign the request.
    const ts = aws_sig.currentTimestamp();
    const payload_hash = aws_sig.sha256Hex("");

    const sig_creds = aws_sig.Credentials{
        .access_key = creds.access_key,
        .secret_key = creds.secret_key,
        .session_token = creds.session_token,
        .region = creds.region,
    };

    const auth_header = aws_sig.signRequest(
        "GET",
        uri_path,
        "",
        host,
        &payload_hash,
        sig_creds,
        ts,
        allocator,
    ) catch return error.HttpFetchFailed;
    defer allocator.free(auth_header);

    // Make the HTTP request.
    return s3HttpRequest(.GET, url, null, auth_header, &ts, &payload_hash, creds.session_token, allocator);
}

/// S3 ListObjectsV2 + multi-key streaming for glob patterns.
fn sourceS3Glob(
    bucket: []const u8,
    key_pattern: []const u8,
    format: SourceFormat,
    auth_val: ?Value,
    allocator: Allocator,
    err_msg: *[]const u8,
) NativeError!Value {
    // Extract prefix (everything before the first wildcard).
    const wildcard_pos = std.mem.indexOfAny(u8, key_pattern, "*?") orelse 0;
    // Find the last '/' before the wildcard to get the directory prefix.
    const prefix_end = if (wildcard_pos > 0)
        (std.mem.lastIndexOfScalar(u8, key_pattern[0..wildcard_pos], '/') orelse 0)
    else
        0;
    const prefix = if (prefix_end > 0) key_pattern[0 .. prefix_end + 1] else "";

    // Resolve credentials.
    var creds = auth_mod.resolveAwsCredentials(auth_val, null, &atomName, allocator) catch |err| {
        err_msg.* = switch (err) {
            error.MissingCredentials => "S3 credentials not found",
            error.InvalidAuthValue => "invalid auth: parameter",
            error.InvalidProfile => "AWS profile not found",
            error.ConfigReadError => "failed to read AWS credentials file",
        };
        return error.RuntimeError;
    };
    defer creds.deinit(allocator);

    // List objects matching the prefix.
    var keys = s3ListObjects(bucket, prefix, creds, allocator) catch {
        err_msg.* = "S3 ListObjectsV2 request failed";
        return error.RuntimeError;
    };
    defer {
        for (keys.items) |k| allocator.free(k);
        keys.deinit(allocator);
    }

    // Filter keys by glob pattern.
    var matched_keys = std.ArrayListUnmanaged([]const u8){};
    defer matched_keys.deinit(allocator);

    for (keys.items) |obj_key| {
        if (globMatch(key_pattern, obj_key)) {
            const dup = allocator.dupe(u8, obj_key) catch {
                err_msg.* = "out of memory";
                return error.OutOfMemory;
            };
            matched_keys.append(allocator, dup) catch {
                allocator.free(dup);
                err_msg.* = "out of memory";
                return error.OutOfMemory;
            };
        }
    }

    if (matched_keys.items.len == 0) {
        // No matching keys — return empty stream.
        return createEmptyStream(allocator);
    }

    // For each matched key, fetch and concatenate into one buffer.
    // This is simpler than creating a multi-stream variant.
    var combined = std.ArrayListUnmanaged(u8){};
    errdefer combined.deinit(allocator);

    // Re-resolve creds for each fetch (env creds are still valid).
    var fetch_creds = auth_mod.resolveAwsCredentials(auth_val, null, &atomName, allocator) catch {
        err_msg.* = "S3 credentials not found";
        return error.RuntimeError;
    };
    defer fetch_creds.deinit(allocator);

    for (matched_keys.items) |obj_key| {
        var body = s3GetObject(bucket, obj_key, fetch_creds, allocator) catch {
            err_msg.* = "S3 GetObject request failed during glob fetch";
            return error.RuntimeError;
        };
        defer body.deinit(allocator);

        combined.appendSlice(allocator, body.items) catch {
            err_msg.* = "out of memory";
            return error.OutOfMemory;
        };

        // Ensure newline separator between files.
        if (body.items.len > 0 and body.items[body.items.len - 1] != '\n') {
            combined.append(allocator, '\n') catch {
                err_msg.* = "out of memory";
                return error.OutOfMemory;
            };
        }
    }

    // Free matched keys now.
    for (matched_keys.items) |k| allocator.free(k);
    matched_keys.clearRetainingCapacity();

    if (format == .json) {
        defer combined.deinit(allocator);
        return createJsonStream(combined.items, allocator, err_msg);
    }
    if (format == .csv) {
        defer combined.deinit(allocator);
        return createCsvStream(combined.items, ',', allocator, err_msg);
    }

    const owned = combined.toOwnedSlice(allocator) catch {
        combined.deinit(allocator);
        err_msg.* = "failed to allocate S3 glob buffer";
        return error.RuntimeError;
    };

    const state = try allocator.create(StreamState);
    state.* = .{ .memory_reader = .{
        .data = owned,
        .cursor = 0,
        .is_jsonl = (format == .jsonl),
        .line_number = 0,
        .allocator = allocator,
    } };
    return createStream(state, allocator);
}

/// Make a signed S3 ListObjectsV2 HTTP request.
/// Returns list of object keys matching the prefix.
fn s3ListObjects(
    bucket: []const u8,
    prefix: []const u8,
    creds: auth_mod.AwsCredentials,
    allocator: Allocator,
) !std.ArrayListUnmanaged([]const u8) {
    var all_keys = std.ArrayListUnmanaged([]const u8){};
    errdefer {
        for (all_keys.items) |k| allocator.free(k);
        all_keys.deinit(allocator);
    }

    var continuation_token: ?[]u8 = null;
    defer if (continuation_token) |t| allocator.free(t);

    while (true) {
        // Build query string: list-type=2&prefix=...
        var query_buf = std.ArrayListUnmanaged(u8){};
        defer query_buf.deinit(allocator);
        const qw = query_buf.writer(allocator);
        try qw.writeAll("list-type=2&prefix=");
        try uriEncode(qw, prefix);
        if (continuation_token) |token| {
            try qw.writeAll("&continuation-token=");
            try uriEncode(qw, token);
        }

        // Build URL.
        var url_buf = std.ArrayListUnmanaged(u8){};
        defer url_buf.deinit(allocator);
        const uw = url_buf.writer(allocator);
        try uw.writeAll("https://");
        try uw.writeAll(bucket);
        try uw.writeAll(".s3.");
        try uw.writeAll(creds.region);
        try uw.writeAll(".amazonaws.com/?");
        try uw.writeAll(query_buf.items);

        var host_buf: [512]u8 = undefined;
        const host = std.fmt.bufPrint(&host_buf, "{s}.s3.{s}.amazonaws.com", .{ bucket, creds.region }) catch
            return error.HttpFetchFailed;

        const ts = aws_sig.currentTimestamp();
        const payload_hash = aws_sig.sha256Hex("");

        const sig_creds = aws_sig.Credentials{
            .access_key = creds.access_key,
            .secret_key = creds.secret_key,
            .session_token = creds.session_token,
            .region = creds.region,
        };

        const auth_header = aws_sig.signRequest(
            "GET",
            "/",
            query_buf.items,
            host,
            &payload_hash,
            sig_creds,
            ts,
            allocator,
        ) catch return error.HttpFetchFailed;
        defer allocator.free(auth_header);

        var body_list = s3HttpRequest(.GET, url_buf.items, null, auth_header, &ts, &payload_hash, creds.session_token, allocator) catch
            return error.HttpFetchFailed;
        defer body_list.deinit(allocator);
        const xml = body_list.items;

        // Parse XML response for <Key> elements.
        try parseListObjectKeys(xml, &all_keys, allocator);

        // Check for truncation (<IsTruncated>true</IsTruncated>)
        if (std.mem.indexOf(u8, xml, "<IsTruncated>true</IsTruncated>")) |_| {
            // Extract <NextContinuationToken>...</NextContinuationToken>
            if (continuation_token) |old| allocator.free(old);
            continuation_token = extractXmlTag(xml, "NextContinuationToken", allocator) catch null;
            if (continuation_token == null) break;
        } else {
            break;
        }
    }

    return all_keys;
}

/// Extract <Key>...</Key> values from S3 ListObjectsV2 XML response.
fn parseListObjectKeys(
    xml: []const u8,
    keys: *std.ArrayListUnmanaged([]const u8),
    allocator: Allocator,
) !void {
    var pos: usize = 0;
    while (std.mem.indexOfPos(u8, xml, pos, "<Key>")) |start| {
        const content_start = start + 5; // len("<Key>")
        if (std.mem.indexOfPos(u8, xml, content_start, "</Key>")) |end| {
            const key_text = xml[content_start..end];
            const dup = try allocator.dupe(u8, key_text);
            try keys.append(allocator, dup);
            pos = end + 6; // len("</Key>")
        } else {
            break;
        }
    }
}

/// Extract content of an XML tag. Caller owns returned slice.
fn extractXmlTag(xml: []const u8, tag: []const u8, allocator: Allocator) ![]u8 {
    // Build "<tag>" and "</tag>" patterns.
    var open_buf: [128]u8 = undefined;
    var close_buf: [128]u8 = undefined;
    const open = std.fmt.bufPrint(&open_buf, "<{s}>", .{tag}) catch return error.OutOfMemory;
    const close = std.fmt.bufPrint(&close_buf, "</{s}>", .{tag}) catch return error.OutOfMemory;

    if (std.mem.indexOf(u8, xml, open)) |start| {
        const content_start = start + open.len;
        if (std.mem.indexOfPos(u8, xml, content_start, close)) |end| {
            return try allocator.dupe(u8, xml[content_start..end]);
        }
    }
    return error.OutOfMemory; // tag not found
}

/// Simple glob pattern matching (supports * and ? wildcards).
fn globMatch(pattern: []const u8, text: []const u8) bool {
    var pi: usize = 0;
    var ti: usize = 0;
    var star_pi: ?usize = null;
    var star_ti: usize = 0;

    while (ti < text.len) {
        if (pi < pattern.len and (pattern[pi] == '?' or pattern[pi] == text[ti])) {
            pi += 1;
            ti += 1;
        } else if (pi < pattern.len and pattern[pi] == '*') {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if (star_pi) |sp| {
            pi = sp + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }

    while (pi < pattern.len and pattern[pi] == '*') : (pi += 1) {}
    return pi == pattern.len;
}

/// Percent-encode a string for URI query parameters.
fn uriEncode(writer: anytype, input: []const u8) !void {
    for (input) |c| {
        if (std.ascii.isAlphanumeric(c) or c == '-' or c == '_' or c == '.' or c == '~') {
            try writer.writeByte(c);
        } else {
            try writer.writeByte('%');
            const hex = std.fmt.bytesToHex(&[_]u8{c}, .upper);
            try writer.writeAll(&hex);
        }
    }
}

/// Create an empty stream that immediately returns None.
fn createEmptyStream(allocator: Allocator) NativeError!Value {
    const items_list = try ObjList.create(allocator);
    trackObj(&items_list.obj);
    const state = try allocator.create(StreamState);
    state.* = .{ .json_array_iter = .{
        .items = Value.fromObj(&items_list.obj),
        .idx = 0,
    } };
    return createStream(state, allocator);
}

/// sink(stream, path_or_atom, format?, options?) -> Nil
/// Consumes the stream and writes elements to a file.
/// sink(stream, "out.txt") writes each element as a line.
/// sink(stream, "out.jsonl", :jsonl) writes each element as JSON per line.
/// sink(stream, "out.csv", :csv) writes as CSV with headers.
/// sink(stream, "out.tsv", :tsv) writes as TSV with headers.
/// sink(stream, "out.json", :json_pretty) writes indented JSON.
/// sink(stream, :stdout, :table) writes a pretty-printed table.
/// sink(stream, :stdout, :markdown) writes a markdown table.
/// sink(stream, "out.csv", :csv, {header: false, delimiter: ";"}) options map.
/// sink(stream, "log.txt", :text, {append: true}) append mode.
fn builtinSink(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    // First arg must be a stream (or range/list, auto-wrapped).
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    } else if (first.isObjType(.list)) {
        first = try autoWrapList(first, allocator);
    }
    const is_stream = first.isObjType(.stream);

    const dest = args[1];

    // Determine format and options.
    var sink_format = parseSinkFormat(args);
    const opts = parseSinkOptions(args);

    // Handle {pretty: true} on :json format.
    if (sink_format == .json and args.len > 3) {
        const ov = args[3];
        if (ov.isObjType(.record)) {
            const rec = ObjRecord.fromObj(ov.asObj());
            for (0..rec.field_count) |i| {
                if (std.mem.eql(u8, rec.field_names[i], "pretty")) {
                    if (rec.field_values[i].isBool() and rec.field_values[i].asBool())
                        sink_format = .json_pretty;
                }
            }
        } else if (ov.isObjType(.map)) {
            const m = ObjMap.fromObj(ov.asObj());
            var it = m.entries.iterator();
            while (it.next()) |entry| {
                const kn = blk: {
                    const k = entry.key_ptr.*;
                    if (k.isAtom()) break :blk atomName(k);
                    if (k.isObj() and k.asObj().obj_type == .string)
                        break :blk @as(?[]const u8, ObjString.fromObj(k.asObj()).bytes);
                    break :blk @as(?[]const u8, null);
                };
                if (kn) |n| {
                    if (std.mem.eql(u8, n, "pretty")) {
                        const v = entry.value_ptr.*;
                        if (v.isBool() and v.asBool()) sink_format = .json_pretty;
                    }
                }
            }
        }
    }

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
        const dest_str = ObjString.fromObj(dest.asObj());
        const uri = uri_mod.parse(dest_str.bytes) catch {
            err_msg.* = "unsupported URI scheme";
            return error.RuntimeError;
        };
        switch (uri.scheme) {
            .file => {},
            .http, .https => {
                err_msg.* = "HTTP sink not supported";
                return error.RuntimeError;
            },
            .s3 => {
                // S3 sink: buffer all output to memory, then PutObject.
                return sinkS3(first, is_stream, uri, sink_format, opts, args, allocator, err_msg);
            },
            .gs => {
                return sinkGcs(first, is_stream, uri, sink_format, opts, args, allocator, err_msg);
            },
            .az => {
                return sinkAzure(first, is_stream, uri, sink_format, opts, args, allocator, err_msg);
            },
        }
        if (opts.append) {
            // Append mode: create without truncation.
            out_file = std.fs.cwd().createFile(uri.path, .{ .truncate = false }) catch |err| {
                err_msg.* = switch (err) {
                    error.AccessDenied => "permission denied",
                    else => "failed to create file",
                };
                return error.RuntimeError;
            };
        } else {
            out_file = std.fs.cwd().createFile(uri.path, .{}) catch |err| {
                err_msg.* = switch (err) {
                    error.AccessDenied => "permission denied",
                    else => "failed to create file",
                };
                return error.RuntimeError;
            };
        }
    } else {
        err_msg.* = "sink() expects a string URI, path, or :stdout/:stderr atom as second argument";
        return error.RuntimeError;
    }
    defer {
        if (!is_std_handle) out_file.close();
    }

    // Gzip compression for local files: buffer → compress → write.
    if (opts.compress == .gzip and !is_std_handle) {
        var out_buf = std.ArrayListUnmanaged(u8){};
        defer out_buf.deinit(allocator);
        sinkFormatToBuffer(first, is_stream, sink_format, opts, &out_buf, allocator, err_msg) catch |err| return err;

        const compressed = gzipCompress(out_buf.items, allocator) catch {
            err_msg.* = "gzip compression failed";
            return error.RuntimeError;
        };
        defer allocator.free(compressed);

        out_file.writeAll(compressed) catch {
            err_msg.* = "failed to write compressed data";
            return error.RuntimeError;
        };
        return Value.nil;
    }

    // Set up buffered writer.
    var write_buf: [64 * 1024]u8 = undefined;
    var bw = out_file.writer(&write_buf);

    // For append mode, start writing at the end of the file.
    if (opts.append and !is_std_handle) {
        bw.pos = out_file.getEndPos() catch 0;
    }

    // Non-stream value: write directly and return.
    if (!is_stream) {
        if ((sink_format == .csv or sink_format == .tsv or
            sink_format == .table or sink_format == .markdown) and
            (first.isObjType(.map) or first.isObjType(.record)))
        {
            // Single map/record: write tabular output directly.
            const hdr = sinkExtractHeaders(first, allocator, err_msg) orelse
                return error.RuntimeError;
            defer sinkFreeHeaders(hdr, allocator);
            const headers = hdr.names;

            if (sink_format == .csv or sink_format == .tsv) {
                const delim: u8 = if (sink_format == .tsv) '\t' else opts.delimiter;
                if (opts.header) {
                    for (headers, 0..) |h, i| {
                        if (i > 0) bw.interface.writeByte(delim) catch {};
                        if (sink_format == .csv) {
                            sinkCsvQuoteField(h, delim, &bw) catch {};
                        } else {
                            bw.interface.writeAll(h) catch {};
                        }
                    }
                    bw.interface.writeByte('\n') catch {};
                }
                for (headers, 0..) |h, i| {
                    if (i > 0) bw.interface.writeByte(delim) catch {};
                    if (sinkLookupField(first, h)) |fv| {
                        const fmt = formatValue(fv, allocator, current_atom_names) catch continue;
                        defer allocator.free(fmt);
                        if (sink_format == .csv) {
                            sinkCsvQuoteField(fmt, delim, &bw) catch {};
                        } else {
                            for (fmt) |c| {
                                bw.interface.writeByte(if (c == '\t' or c == '\n' or c == '\r') ' ' else c) catch {};
                            }
                        }
                    }
                }
                bw.interface.writeByte('\n') catch {};
            } else {
                // table / markdown: compute column widths from single row.
                var col_widths_buf: [64]usize = undefined;
                const col_widths = col_widths_buf[0..headers.len];
                var cell_buf: [64][]const u8 = undefined;
                const cells = cell_buf[0..headers.len];
                for (headers, 0..) |h, i| {
                    cells[i] = if (sinkLookupField(first, h)) |fv|
                        (formatValue(fv, allocator, current_atom_names) catch "")
                    else
                        "";
                    col_widths[i] = @max(h.len, cells[i].len);
                }
                defer for (cells) |c| {
                    if (c.len > 0) allocator.free(c);
                };
                if (sink_format == .markdown) {
                    bw.interface.writeByte('|') catch {};
                    for (headers, 0..) |h, i| {
                        bw.interface.writeByte(' ') catch {};
                        bw.interface.writeAll(h) catch {};
                        sinkWritePadding(&bw, col_widths[i] - h.len);
                        bw.interface.writeAll(" |") catch {};
                    }
                    bw.interface.writeByte('\n') catch {};
                    bw.interface.writeByte('|') catch {};
                    for (col_widths) |w| {
                        bw.interface.writeByte(' ') catch {};
                        sinkWriteDashes(&bw, w);
                        bw.interface.writeAll(" |") catch {};
                    }
                    bw.interface.writeByte('\n') catch {};
                    bw.interface.writeByte('|') catch {};
                    for (cells, 0..) |cell, i| {
                        bw.interface.writeByte(' ') catch {};
                        bw.interface.writeAll(cell) catch {};
                        sinkWritePadding(&bw, col_widths[i] - cell.len);
                        bw.interface.writeAll(" |") catch {};
                    }
                    bw.interface.writeByte('\n') catch {};
                } else {
                    for (headers, 0..) |h, i| {
                        if (i > 0) bw.interface.writeAll("  ") catch {};
                        bw.interface.writeAll(h) catch {};
                        sinkWritePadding(&bw, col_widths[i] - h.len);
                    }
                    bw.interface.writeByte('\n') catch {};
                    for (cells, 0..) |cell, i| {
                        if (i > 0) bw.interface.writeAll("  ") catch {};
                        bw.interface.writeAll(cell) catch {};
                        sinkWritePadding(&bw, col_widths[i] - cell.len);
                    }
                    bw.interface.writeByte('\n') catch {};
                }
            }
            bw.interface.flush() catch {};
            return Value.nil;
        }

        if (sink_format == .json or sink_format == .json_pretty) {
            if (sink_format == .json_pretty) {
                try sinkWriteJsonPrettyValue(first, &bw, allocator, opts.indent, err_msg);
            } else {
                try sinkWriteJsonValue(first, &bw, allocator, err_msg);
            }
            bw.interface.writeByte('\n') catch {};
        } else {
            const formatted = formatValue(first, allocator, current_atom_names) catch {
                err_msg.* = "failed to format value for sink";
                return error.RuntimeError;
            };
            defer allocator.free(formatted);
            bw.interface.writeAll(formatted) catch {};
            bw.interface.writeByte('\n') catch {};
        }
        bw.interface.flush() catch {};
        return Value.nil;
    }

    // Set stream module callbacks for pulling.
    setStreamCallbacks();
    defer stream_mod.clearVM();

    const stream_obj = ObjStream.fromObj(first.asObj());

    switch (sink_format) {
        .json => {
            // :json — write elements as a JSON array: [elem1,elem2,...]\n
            bw.interface.writeByte('[') catch {
                err_msg.* = "failed to write to file";
                return error.RuntimeError;
            };

            var first_elem = true;
            while (true) {
                const val = try stream_obj.state.next(allocator);
                if (isAdtVariant(val, 0, 1)) break;
                const elem = adtPayload(val, 0);

                if (!first_elem) {
                    bw.interface.writeByte(',') catch {
                        err_msg.* = "failed to write to file";
                        return error.RuntimeError;
                    };
                }
                first_elem = false;

                try sinkWriteJsonValue(elem, &bw, allocator, err_msg);
            }

            bw.interface.writeAll("]\n") catch {
                err_msg.* = "failed to write to file";
                return error.RuntimeError;
            };
        },

        .json_pretty => {
            // :json_pretty — write elements as an indented JSON array.
            const indent = opts.indent;
            bw.interface.writeAll("[\n") catch {
                err_msg.* = "failed to write to file";
                return error.RuntimeError;
            };

            var first_elem = true;
            while (true) {
                const val = try stream_obj.state.next(allocator);
                if (isAdtVariant(val, 0, 1)) break;
                const elem = adtPayload(val, 0);

                if (!first_elem) {
                    bw.interface.writeAll(",\n") catch {
                        err_msg.* = "failed to write to file";
                        return error.RuntimeError;
                    };
                }
                first_elem = false;

                sinkWritePadding(&bw, indent);
                try sinkWriteJsonPrettyValueAtDepth(elem, &bw, allocator, indent, 1, err_msg);
            }

            bw.interface.writeAll("\n]\n") catch {
                err_msg.* = "failed to write to file";
                return error.RuntimeError;
            };
        },

        .jsonl => {
            while (true) {
                const val = try stream_obj.state.next(allocator);
                if (isAdtVariant(val, 0, 1)) break;
                const elem = adtPayload(val, 0);

                if (elem.isObjType(.list)) {
                    const lst = ObjList.fromObj(elem.asObj());
                    for (lst.items.items) |item| {
                        try sinkWriteJsonlLine(item, &bw, allocator, err_msg);
                    }
                } else {
                    try sinkWriteJsonlLine(elem, &bw, allocator, err_msg);
                }
            }
        },

        .csv, .tsv => {
            const delim: u8 = if (sink_format == .tsv) '\t' else opts.delimiter;
            var hdr_info: ?SinkHeaders = null;
            defer if (hdr_info) |hi| sinkFreeHeaders(hi, allocator);
            var first_elem = true;

            while (true) {
                const val = try stream_obj.state.next(allocator);
                if (isAdtVariant(val, 0, 1)) break;
                const elem = adtPayload(val, 0);

                if (first_elem) {
                    hdr_info = sinkExtractHeaders(elem, allocator, err_msg);
                    if (hdr_info == null) return error.RuntimeError;

                    if (opts.header) {
                        // Write header row.
                        for (hdr_info.?.names, 0..) |h, i| {
                            if (i > 0) bw.interface.writeByte(delim) catch {};
                            if (sink_format == .csv) {
                                sinkCsvQuoteField(h, delim, &bw) catch {};
                            } else {
                                bw.interface.writeAll(h) catch {};
                            }
                        }
                        bw.interface.writeByte('\n') catch {};
                    }
                    first_elem = false;
                }

                // Write data row.
                if (hdr_info) |hi| {
                    for (hi.names, 0..) |h, i| {
                        if (i > 0) bw.interface.writeByte(delim) catch {};
                        if (sinkLookupField(elem, h)) |field_val| {
                            const formatted = formatValue(field_val, allocator, current_atom_names) catch continue;
                            defer allocator.free(formatted);
                            if (sink_format == .csv) {
                                sinkCsvQuoteField(formatted, delim, &bw) catch {};
                            } else {
                                // TSV: replace tabs/newlines with spaces.
                                for (formatted) |c| {
                                    if (c == '\t' or c == '\n' or c == '\r') {
                                        bw.interface.writeByte(' ') catch {};
                                    } else {
                                        bw.interface.writeByte(c) catch {};
                                    }
                                }
                            }
                        }
                    }
                    bw.interface.writeByte('\n') catch {};
                }
            }
        },

        .table, .markdown => {
            // Buffer all elements to compute column widths.
            var elements = std.ArrayListUnmanaged(Value){};
            defer elements.deinit(allocator);

            while (true) {
                const val = try stream_obj.state.next(allocator);
                if (isAdtVariant(val, 0, 1)) break;
                elements.append(allocator, adtPayload(val, 0)) catch {
                    err_msg.* = "out of memory";
                    return error.RuntimeError;
                };
            }

            if (elements.items.len == 0) {
                bw.interface.flush() catch {};
                return Value.nil;
            }

            const hdr = sinkExtractHeaders(elements.items[0], allocator, err_msg) orelse
                return error.RuntimeError;
            defer sinkFreeHeaders(hdr, allocator);
            const headers = hdr.names;

            // Format all cells and compute max column widths.
            var col_widths_buf: [64]usize = undefined;
            const col_widths = col_widths_buf[0..headers.len];
            for (headers, 0..) |h, i| {
                col_widths[i] = h.len;
            }

            // We'll store formatted cell strings in a flat list.
            var cell_storage = std.ArrayListUnmanaged([]const u8){};
            defer {
                for (cell_storage.items) |s| {
                    if (s.len > 0) allocator.free(s);
                }
                cell_storage.deinit(allocator);
            }

            for (elements.items) |elem| {
                for (headers, 0..) |h, col| {
                    const cell_str: []const u8 = if (sinkLookupField(elem, h)) |fv|
                        (formatValue(fv, allocator, current_atom_names) catch "")
                    else
                        "";
                    cell_storage.append(allocator, cell_str) catch {};
                    if (cell_str.len > col_widths[col]) col_widths[col] = cell_str.len;
                }
            }

            const ncols = headers.len;

            if (sink_format == .markdown) {
                // Header: | col1 | col2 |
                bw.interface.writeByte('|') catch {};
                for (headers, 0..) |h, i| {
                    bw.interface.writeByte(' ') catch {};
                    bw.interface.writeAll(h) catch {};
                    sinkWritePadding(&bw, col_widths[i] - h.len);
                    bw.interface.writeAll(" |") catch {};
                }
                bw.interface.writeByte('\n') catch {};
                // Separator: | --- | --- |
                bw.interface.writeByte('|') catch {};
                for (col_widths) |w| {
                    bw.interface.writeByte(' ') catch {};
                    sinkWriteDashes(&bw, w);
                    bw.interface.writeAll(" |") catch {};
                }
                bw.interface.writeByte('\n') catch {};
                // Data rows.
                for (0..elements.items.len) |row| {
                    bw.interface.writeByte('|') catch {};
                    for (0..ncols) |col| {
                        const cell = cell_storage.items[row * ncols + col];
                        bw.interface.writeByte(' ') catch {};
                        bw.interface.writeAll(cell) catch {};
                        sinkWritePadding(&bw, col_widths[col] - cell.len);
                        bw.interface.writeAll(" |") catch {};
                    }
                    bw.interface.writeByte('\n') catch {};
                }
            } else {
                // Plain table: header row, then data rows (space-padded).
                for (headers, 0..) |h, i| {
                    if (i > 0) bw.interface.writeAll("  ") catch {};
                    bw.interface.writeAll(h) catch {};
                    sinkWritePadding(&bw, col_widths[i] - h.len);
                }
                bw.interface.writeByte('\n') catch {};
                for (0..elements.items.len) |row| {
                    for (0..ncols) |col| {
                        if (col > 0) bw.interface.writeAll("  ") catch {};
                        const cell = cell_storage.items[row * ncols + col];
                        bw.interface.writeAll(cell) catch {};
                        sinkWritePadding(&bw, col_widths[col] - cell.len);
                    }
                    bw.interface.writeByte('\n') catch {};
                }
            }
        },

        .text => {
            while (true) {
                const val = try stream_obj.state.next(allocator);
                if (isAdtVariant(val, 0, 1)) break;
                const elem = adtPayload(val, 0);

                const formatted = formatValue(elem, allocator, current_atom_names) catch {
                    err_msg.* = "failed to format value for sink";
                    return error.RuntimeError;
                };
                defer allocator.free(formatted);
                bw.interface.writeAll(formatted) catch {
                    err_msg.* = "failed to write to file";
                    return error.RuntimeError;
                };
                bw.interface.writeByte('\n') catch {
                    err_msg.* = "failed to write newline to file";
                    return error.RuntimeError;
                };
            }
        },
    }

    // CRITICAL: flush buffered writer before closing.
    bw.interface.flush() catch {
        err_msg.* = "failed to flush output file";
        return error.RuntimeError;
    };

    return Value.nil;
}

/// Write a single value as JSON (no trailing newline).
fn sinkWriteJsonValue(
    value: Value,
    bw: anytype,
    allocator: Allocator,
    err_msg: *[]const u8,
) NativeError!void {
    if (current_vm) |vm_ptr| {
        if (track_obj_fn) |tfn| {
            json_mod.setVM(vm_ptr, tfn);
        }
    }
    if (current_atom_names) |names| {
        json_mod.setAtomNames(names);
    }
    const emit_result = json_mod.emit(value, allocator);
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
}

/// Write a single value as one JSONL line (JSON + newline).
fn sinkWriteJsonlLine(
    value: Value,
    bw: anytype,
    allocator: Allocator,
    err_msg: *[]const u8,
) NativeError!void {
    if (current_vm) |vm_ptr| {
        if (track_obj_fn) |tfn| {
            json_mod.setVM(vm_ptr, tfn);
        }
    }
    if (current_atom_names) |names| {
        json_mod.setAtomNames(names);
    }
    const emit_result = json_mod.emit(value, allocator);
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
    bw.interface.writeByte('\n') catch {
        err_msg.* = "failed to write newline to file";
        return error.RuntimeError;
    };
}

/// Write a single value as pretty-printed JSON (no trailing newline).
fn sinkWriteJsonPrettyValue(
    value: Value,
    bw: anytype,
    allocator: Allocator,
    indent: u8,
    err_msg: *[]const u8,
) NativeError!void {
    return sinkWriteJsonPrettyValueAtDepth(value, bw, allocator, indent, 0, err_msg);
}

fn sinkWriteJsonPrettyValueAtDepth(
    value: Value,
    bw: anytype,
    allocator: Allocator,
    indent: u8,
    depth: u16,
    err_msg: *[]const u8,
) NativeError!void {
    if (current_vm) |vm_ptr| {
        if (track_obj_fn) |tfn| {
            json_mod.setVM(vm_ptr, tfn);
        }
    }
    if (current_atom_names) |names| {
        json_mod.setAtomNames(names);
    }
    const emit_result = json_mod.emitPrettyAtDepth(value, allocator, indent, depth);
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
}

// ── Concurrency stream operators ─────────────────────────────────────

/// par_map(stream, fn) or par_map(stream, N, fn)
/// Parallel map with order preservation and fail-fast error propagation.
/// Dispatches batch items to fibers via scheduler when available.
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
        .result_buf = null,
        .input_buf = null,
        .batch_size = 0,
        .next_emit = 0,
        .upstream_done = false,
        .had_error = false,
        .error_message = null,
    } };
    return createStream(state, allocator);
}

/// par_map_unordered(stream, fn) or par_map_unordered(stream, N, fn)
/// Parallel map emitting results in completion order with fail-fast.
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
        .result_buf = null,
        .input_buf = null,
        .batch_size = 0,
        .next_emit = 0,
        .upstream_done = false,
        .had_error = false,
        .error_message = null,
    } };
    return createStream(state, allocator);
}

/// par_map_result(stream, fn) or par_map_result(stream, N, fn)
/// Parallel map wrapping outputs in Result (no fail-fast). Errors wrapped in Result.Err.
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
        .result_buf = null,
        .input_buf = null,
        .batch_size = 0,
        .next_emit = 0,
        .upstream_done = false,
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

/// throttle(stream, rate, time_unit) -> Stream
/// Rate-limits the stream using token bucket algorithm.
/// time_unit: :per_second, :per_minute, :per_hour
fn builtinThrottle(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }
    if (!first.isObjType(.stream)) {
        err_msg.* = "throttle() expects a stream as first argument";
        return error.RuntimeError;
    }
    // Parse rate (int or float).
    var rate: f64 = undefined;
    if (args[1].isInt()) {
        rate = @floatFromInt(args[1].asInt());
    } else if (args[1].isFloat()) {
        rate = args[1].asFloat();
    } else {
        err_msg.* = "throttle() rate must be a number";
        return error.RuntimeError;
    }
    if (rate <= 0) {
        err_msg.* = "throttle() rate must be positive";
        return error.RuntimeError;
    }
    // Parse time unit atom.
    const interval_ms: f64 = if (atomName(args[2])) |name| blk: {
        if (std.mem.eql(u8, name, "per_second")) break :blk 1000.0;
        if (std.mem.eql(u8, name, "per_minute")) break :blk 60000.0;
        if (std.mem.eql(u8, name, "per_hour")) break :blk 3600000.0;
        err_msg.* = "throttle() time unit must be :per_second, :per_minute, or :per_hour";
        return error.RuntimeError;
    } else {
        err_msg.* = "throttle() third argument must be a time unit atom";
        return error.RuntimeError;
    };
    const state = try allocator.create(StreamState);
    state.* = .{ .throttle_op = .{
        .upstream = first,
        .rate = rate,
        .interval_ms = interval_ms,
        .tokens = rate,
        .last_refill = 0,
        .started = false,
    } };
    return createStream(state, allocator);
}

/// buffer(stream, capacity) -> Stream
/// Prefetch buffer that reads ahead from upstream.
fn builtinBuffer(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }
    if (!first.isObjType(.stream)) {
        err_msg.* = "buffer() expects a stream as first argument";
        return error.RuntimeError;
    }
    if (!args[1].isInt() or args[1].asInt() <= 0) {
        err_msg.* = "buffer() capacity must be a positive integer";
        return error.RuntimeError;
    }
    const capacity: u32 = @intCast(args[1].asInt());
    const state = try allocator.create(StreamState);
    state.* = .{ .buffer_op = .{
        .upstream = first,
        .capacity = capacity,
        .buf = .{},
        .read_idx = 0,
        .exhausted = false,
    } };
    return createStream(state, allocator);
}

/// single(stream) -> value: expects exactly one element from stream.
/// 0 elements -> Result.Err("empty stream")
/// 1 element -> that element AS-IS
/// >1 elements -> Result.Err("expected single element")
fn builtinSingle(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }
    if (!first.isObjType(.stream)) {
        err_msg.* = "single() expects a stream as first argument";
        return error.RuntimeError;
    }

    setStreamCallbacks();
    defer stream_mod.clearVM();

    const stream_obj = ObjStream.fromObj(first.asObj());

    // Pull first element.
    const val1 = try stream_obj.state.next(allocator);
    if (isAdtVariant(val1, 0, 1)) {
        // Empty stream.
        const msg = try ObjString.create(allocator, "empty stream", null);
        trackObj(&msg.obj);
        return makeErr(Value.fromObj(&msg.obj), allocator);
    }
    const elem = adtPayload(val1, 0);

    // Pull second to verify singularity.
    const val2 = try stream_obj.state.next(allocator);
    if (isAdtVariant(val2, 0, 1)) {
        // Exactly one element.
        return elem;
    }

    // More than one element.
    const msg = try ObjString.create(allocator, "expected single element", null);
    trackObj(&msg.obj);
    return makeErr(Value.fromObj(&msg.obj), allocator);
}

/// ok_or(value, default) -> value: if Result.Ok(v) return v, else return default.
fn builtinOkOr(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = allocator;
    _ = err_msg;
    if (args[0].isObjType(.adt)) {
        const adt = ObjAdt.fromObj(args[0].asObj());
        if (adt.type_id == 1 and adt.variant_idx == 0) {
            return adt.payload[0];
        }
    }
    return args[1];
}

/// tap_err(stream, fn) -> Stream: invoke fn on Result.Err payloads for side effects.
fn builtinTapErr(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    var first = args[0];
    if (first.isObjType(.range)) {
        first = try autoWrapRange(first, allocator);
    }
    if (!first.isObjType(.stream)) {
        err_msg.* = "tap_err() expects a stream as first argument";
        return error.RuntimeError;
    }
    if (!args[1].isObjType(.closure)) {
        err_msg.* = "tap_err() expects a function as second argument";
        return error.RuntimeError;
    }
    const state = try allocator.create(StreamState);
    state.* = .{ .tap_err_op = .{
        .upstream = first,
        .fn_val = args[1],
    } };
    return createStream(state, allocator);
}

/// unwrap(value) -> payload: panics if Result.Err or Option.None.
fn builtinUnwrap(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = allocator;
    if (args[0].isObjType(.adt)) {
        const adt = ObjAdt.fromObj(args[0].asObj());
        // Result.Ok (type_id=1, variant_idx=0) or Option.Some (type_id=0, variant_idx=0)
        if (adt.variant_idx == 0 and (adt.type_id == 0 or adt.type_id == 1)) {
            return adt.payload[0];
        }
        // Result.Err or Option.None
        err_msg.* = "unwrap called on Err or None";
        return error.RuntimeError;
    }
    err_msg.* = "unwrap expects a Result or Option";
    return error.RuntimeError;
}

/// unwrap_or(value, default) -> payload or default. Works with both Result and Option.
fn builtinUnwrapOr(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    _ = allocator;
    _ = err_msg;
    if (args[0].isObjType(.adt)) {
        const adt = ObjAdt.fromObj(args[0].asObj());
        // Result.Ok or Option.Some → return payload
        if (adt.variant_idx == 0 and (adt.type_id == 0 or adt.type_id == 1)) {
            return adt.payload[0];
        }
    }
    return args[1];
}

/// avg(list) -> Option: average of a list of numbers. Returns Option.None for empty list.
fn builtinAvg(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    if (!args[0].isObjType(.list)) {
        err_msg.* = "avg() expects a list";
        return error.RuntimeError;
    }
    const list = ObjList.fromObj(args[0].asObj());
    if (list.items.items.len == 0) {
        return makeNone(allocator);
    }
    var sum: f64 = 0;
    for (list.items.items) |v| {
        if (v.isInt()) {
            sum += @floatFromInt(v.asInt());
        } else if (v.isFloat()) {
            sum += v.asFloat();
        } else {
            err_msg.* = "avg() expects a list of numbers";
            return error.RuntimeError;
        }
    }
    const result = sum / @as(f64, @floatFromInt(list.items.items.len));
    return makeSome(Value.fromFloat(result), allocator);
}

/// S3 sink: buffer formatted output to a temp file, then PutObject.
/// Reuses the existing sink formatting by writing to a temp file first.
fn sinkS3(
    stream_val: Value,
    is_stream: bool,
    uri: uri_mod.ParsedUri,
    sink_format: SinkFormat,
    opts: SinkOptions,
    args: []const Value,
    allocator: Allocator,
    err_msg: *[]const u8,
) NativeError!Value {
    const bucket = uri.host orelse {
        err_msg.* = "S3 URI must include bucket name: s3://bucket/key";
        return error.RuntimeError;
    };
    const key = uri.path;

    // Extract auth parameter. For sink, auth is at args[4] if present
    // (args = [stream, dest, format?, options?, auth?]).
    // But we also check args[2] and args[3] for atoms/records.
    const auth_val: ?Value = blk: {
        // Walk through optional args looking for auth value.
        // Format is an atom like :jsonl. Options is a record/map.
        // Auth is an atom (profile) or record (explicit creds).
        // We look at args past format and options.
        var idx: usize = 2;
        // Skip format atom if present.
        if (idx < args.len and args[idx].isAtom()) idx += 1;
        // Skip options map/record if present.
        if (idx < args.len and (args[idx].isObjType(.record) or args[idx].isObjType(.map))) idx += 1;
        // Next value (if any) is auth.
        if (idx < args.len) break :blk args[idx];
        break :blk null;
    };

    // Resolve credentials.
    var creds = auth_mod.resolveAwsCredentials(auth_val, null, &atomName, allocator) catch |err| {
        err_msg.* = switch (err) {
            error.MissingCredentials => "S3 credentials not found. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY",
            error.InvalidAuthValue => "invalid auth: parameter for S3 sink",
            error.InvalidProfile => "AWS profile not found in ~/.aws/credentials",
            error.ConfigReadError => "failed to read AWS credentials file",
        };
        return error.RuntimeError;
    };
    defer creds.deinit(allocator);

    var out_buf = std.ArrayListUnmanaged(u8){};
    defer out_buf.deinit(allocator);
    sinkFormatToBuffer(stream_val, is_stream, sink_format, opts, &out_buf, allocator, err_msg) catch |err| return err;

    // Apply compression if requested.
    const upload = maybeCompress(out_buf.items, opts, allocator) catch {
        err_msg.* = "gzip compression failed";
        return error.RuntimeError;
    };
    defer if (upload.owned) allocator.free(@constCast(upload.buf));

    // Upload with retry.
    var attempts: u8 = 0;
    while (true) {
        if (s3PutObject(bucket, key, upload.buf, creds, allocator)) {
            break;
        } else |_| {
            if (attempts < opts.retry) {
                attempts += 1;
                continue;
            }
            err_msg.* = "S3 PutObject request failed";
            return error.RuntimeError;
        }
    }

    return Value.nil;
}

/// Format stream/value output into a memory buffer (for S3 upload).
/// Supports text, jsonl, and json formats.
fn sinkFormatToBuffer(
    stream_val: Value,
    is_stream: bool,
    sink_format: SinkFormat,
    opts: SinkOptions,
    out_buf: *std.ArrayListUnmanaged(u8),
    allocator: Allocator,
    err_msg: *[]const u8,
) NativeError!void {
    _ = opts;
    if (!is_stream) {
        // Non-stream single value.
        const formatted = formatValue(stream_val, allocator, current_atom_names) catch {
            err_msg.* = "failed to format value";
            return error.RuntimeError;
        };
        defer allocator.free(formatted);
        out_buf.appendSlice(allocator, formatted) catch return error.OutOfMemory;
        out_buf.append(allocator, '\n') catch return error.OutOfMemory;
        return;
    }

    const stream_obj = ObjStream.fromObj(stream_val.asObj());

    if (sink_format == .json or sink_format == .json_pretty) {
        // Collect all elements and format as JSON array.
        var items = std.ArrayListUnmanaged(Value){};
        defer items.deinit(allocator);

        while (true) {
            const val = stream_obj.state.next(allocator) catch {
                err_msg.* = "stream error during S3 sink";
                return error.RuntimeError;
            };
            // Option.None = type_id=0, variant_idx=1
            if (isAdtVariant(val, 0, 1)) break;
            const inner = adtPayload(val, 0);
            items.append(allocator, inner) catch return error.OutOfMemory;
        }

        const w = out_buf.writer(allocator);
        w.writeByte('[') catch return error.OutOfMemory;
        for (items.items, 0..) |item, i| {
            if (i > 0) w.writeByte(',') catch return error.OutOfMemory;
            if (sink_format == .json_pretty) w.writeByte('\n') catch return error.OutOfMemory;
            switch (json_mod.emit(item, allocator)) {
                .ok => |s| {
                    defer allocator.free(s);
                    w.writeAll(s) catch return error.OutOfMemory;
                },
                .err => continue,
            }
        }
        if (sink_format == .json_pretty and items.items.len > 0) {
            w.writeByte('\n') catch return error.OutOfMemory;
        }
        w.writeByte(']') catch return error.OutOfMemory;
        w.writeByte('\n') catch return error.OutOfMemory;
        return;
    }

    // Text and JSONL: line-by-line.
    while (true) {
        const val = stream_obj.state.next(allocator) catch {
            err_msg.* = "stream error during S3 sink";
            return error.RuntimeError;
        };
        if (isAdtVariant(val, 0, 1)) break;
        const inner = adtPayload(val, 0);

        if (sink_format == .jsonl) {
            switch (json_mod.emit(inner, allocator)) {
                .ok => |s| {
                    defer allocator.free(s);
                    out_buf.appendSlice(allocator, s) catch return error.OutOfMemory;
                },
                .err => continue,
            }
        } else {
            // Text format.
            const formatted = formatValue(inner, allocator, current_atom_names) catch continue;
            defer allocator.free(formatted);
            out_buf.appendSlice(allocator, formatted) catch return error.OutOfMemory;
        }
        out_buf.append(allocator, '\n') catch return error.OutOfMemory;
    }
}

/// Make a signed S3 PutObject HTTP request.
fn s3PutObject(
    bucket: []const u8,
    key: []const u8,
    body: []const u8,
    creds: auth_mod.AwsCredentials,
    allocator: Allocator,
) !void {
    // Build URL.
    var url_buf = std.ArrayListUnmanaged(u8){};
    defer url_buf.deinit(allocator);
    const uw = url_buf.writer(allocator);
    try uw.writeAll("https://");
    try uw.writeAll(bucket);
    try uw.writeAll(".s3.");
    try uw.writeAll(creds.region);
    try uw.writeAll(".amazonaws.com/");
    try uw.writeAll(key);

    var host_buf: [512]u8 = undefined;
    const host = std.fmt.bufPrint(&host_buf, "{s}.s3.{s}.amazonaws.com", .{ bucket, creds.region }) catch
        return error.HttpFetchFailed;

    var path_buf: [2048]u8 = undefined;
    const uri_path = std.fmt.bufPrint(&path_buf, "/{s}", .{key}) catch
        return error.HttpFetchFailed;

    const payload_hash = aws_sig.sha256Hex(body);
    const ts = aws_sig.currentTimestamp();

    const sig_creds = aws_sig.Credentials{
        .access_key = creds.access_key,
        .secret_key = creds.secret_key,
        .session_token = creds.session_token,
        .region = creds.region,
    };

    const auth_header = aws_sig.signRequest(
        "PUT",
        uri_path,
        "",
        host,
        &payload_hash,
        sig_creds,
        ts,
        allocator,
    ) catch return error.HttpFetchFailed;
    defer allocator.free(auth_header);

    _ = s3HttpRequest(.PUT, url_buf.items, body, auth_header, &ts, &payload_hash, creds.session_token, allocator) catch
        return error.HttpFetchFailed;
}

/// Shared helper for S3 HTTP requests with Sig V4 auth.
/// For GET: returns response body. For PUT: sends body and returns empty list.
fn s3HttpRequest(
    method: std.http.Method,
    url: []const u8,
    body: ?[]const u8,
    auth_header: []const u8,
    ts: *const aws_sig.AwsTimestamp,
    payload_hash: *const [64]u8,
    session_token: ?[]const u8,
    allocator: Allocator,
) !std.ArrayListUnmanaged(u8) {
    var client: std.http.Client = .{ .allocator = allocator };
    defer client.deinit();

    var extra_headers_buf: [4]std.http.Header = undefined;
    var header_count: usize = 0;
    extra_headers_buf[header_count] = .{ .name = "x-amz-date", .value = &ts.datetime };
    header_count += 1;
    extra_headers_buf[header_count] = .{ .name = "x-amz-content-sha256", .value = payload_hash };
    header_count += 1;
    if (session_token) |token| {
        extra_headers_buf[header_count] = .{ .name = "x-amz-security-token", .value = token };
        header_count += 1;
    }

    if (method == .PUT) {
        // PUT request: send body, don't collect response.
        const result = client.fetch(.{
            .location = .{ .url = url },
            .method = .PUT,
            .payload = body,
            .headers = .{
                .authorization = .{ .override = auth_header },
            },
            .extra_headers = extra_headers_buf[0..header_count],
        }) catch return error.HttpFetchFailed;

        if (result.status != .ok) return error.HttpStatusError;
        return std.ArrayListUnmanaged(u8){};
    }

    // GET request: collect response body.
    var aw: std.Io.Writer.Allocating = .init(allocator);

    const result = client.fetch(.{
        .location = .{ .url = url },
        .method = .GET,
        .headers = .{
            .authorization = .{ .override = auth_header },
        },
        .extra_headers = extra_headers_buf[0..header_count],
        .response_writer = &aw.writer,
    }) catch {
        // On fetch failure, free any partial buffer.
        if (aw.writer.buffer.len > 0) allocator.free(aw.writer.buffer);
        return error.HttpFetchFailed;
    };

    if (result.status != .ok) {
        aw.deinit();
        return error.HttpStatusError;
    }

    return aw.toArrayList();
}

// ═══════════════════════════════════════════════════════════════════════
// ── GCS Transport ────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════

const GCS_HOST = "storage.googleapis.com";

/// GCS source: fetches an object from Google Cloud Storage.
/// Supports HMAC (S3-compat Sig V4) and Bearer token auth modes.
fn sourceGcs(
    uri: uri_mod.ParsedUri,
    format: SourceFormat,
    auth_val: ?Value,
    allocator: Allocator,
    err_msg: *[]const u8,
) NativeError!Value {
    const bucket = uri.host orelse {
        err_msg.* = "GCS URI must include bucket name: gs://bucket/key";
        return error.RuntimeError;
    };
    const key = uri.path;

    // Check for glob pattern.
    if (std.mem.indexOfAny(u8, key, "*?")) |_| {
        return sourceGcsGlob(bucket, key, format, auth_val, allocator, err_msg);
    }

    var creds = auth_mod.resolveGcsCredentials(auth_val, &atomName, allocator) catch |err| {
        err_msg.* = switch (err) {
            error.MissingCredentials => "GCS credentials not found. Set GCS_HMAC_ACCESS_KEY/GCS_HMAC_SECRET_KEY or GOOGLE_BEARER_TOKEN, or pass auth: parameter",
            error.InvalidAuthValue => "auth: parameter must be an atom (profile) or record {access_key:, secret_key:} / {token:}",
            error.InvalidProfile => "GCS profile not found in credentials file",
            error.ConfigReadError => "failed to read credentials file",
        };
        return error.RuntimeError;
    };
    defer creds.deinit(allocator);

    var body = gcsGetObject(bucket, key, creds, allocator) catch {
        err_msg.* = "GCS GetObject request failed";
        return error.RuntimeError;
    };

    if (format == .json) {
        defer body.deinit(allocator);
        return createJsonStream(body.items, allocator, err_msg);
    }

    const owned = body.toOwnedSlice(allocator) catch {
        body.deinit(allocator);
        err_msg.* = "failed to allocate GCS response buffer";
        return error.RuntimeError;
    };

    const state = try allocator.create(StreamState);
    state.* = .{ .memory_reader = .{
        .data = owned,
        .cursor = 0,
        .is_jsonl = (format == .jsonl),
        .line_number = 0,
        .allocator = allocator,
    } };
    return createStream(state, allocator);
}

/// GCS sink: buffer formatted output, then PutObject.
fn sinkGcs(
    stream_val: Value,
    is_stream: bool,
    uri: uri_mod.ParsedUri,
    sink_format: SinkFormat,
    opts: SinkOptions,
    args: []const Value,
    allocator: Allocator,
    err_msg: *[]const u8,
) NativeError!Value {
    const bucket = uri.host orelse {
        err_msg.* = "GCS URI must include bucket name: gs://bucket/key";
        return error.RuntimeError;
    };
    const key = uri.path;

    // Extract auth — walk past format atom and options record.
    const auth_val: ?Value = blk: {
        var idx: usize = 2;
        if (idx < args.len and args[idx].isAtom()) idx += 1;
        if (idx < args.len and (args[idx].isObjType(.record) or args[idx].isObjType(.map))) idx += 1;
        if (idx < args.len) break :blk args[idx];
        break :blk null;
    };

    var creds = auth_mod.resolveGcsCredentials(auth_val, &atomName, allocator) catch |err| {
        err_msg.* = switch (err) {
            error.MissingCredentials => "GCS credentials not found. Set GCS_HMAC_ACCESS_KEY/GCS_HMAC_SECRET_KEY or GOOGLE_BEARER_TOKEN",
            error.InvalidAuthValue => "invalid auth: parameter for GCS sink",
            error.InvalidProfile => "GCS profile not found",
            error.ConfigReadError => "failed to read credentials file",
        };
        return error.RuntimeError;
    };
    defer creds.deinit(allocator);

    var out_buf = std.ArrayListUnmanaged(u8){};
    defer out_buf.deinit(allocator);

    sinkFormatToBuffer(stream_val, is_stream, sink_format, opts, &out_buf, allocator, err_msg) catch |err| return err;

    const upload = maybeCompress(out_buf.items, opts, allocator) catch {
        err_msg.* = "gzip compression failed";
        return error.RuntimeError;
    };
    defer if (upload.owned) allocator.free(@constCast(upload.buf));

    var attempts: u8 = 0;
    while (true) {
        if (gcsPutObject(bucket, key, upload.buf, creds, allocator)) {
            break;
        } else |_| {
            if (attempts < opts.retry) { attempts += 1; continue; }
            err_msg.* = "GCS PutObject request failed";
            return error.RuntimeError;
        }
    }

    return Value.nil;
}

/// Fetch a GCS object. Dispatches to HMAC or Bearer mode.
fn gcsGetObject(
    bucket: []const u8,
    key: []const u8,
    creds: auth_mod.GcsCredentials,
    allocator: Allocator,
) !std.ArrayListUnmanaged(u8) {
    // Build URL: https://storage.googleapis.com/{bucket}/{key}
    var url_buf = std.ArrayListUnmanaged(u8){};
    defer url_buf.deinit(allocator);
    const uw = url_buf.writer(allocator);
    try uw.writeAll("https://");
    try uw.writeAll(GCS_HOST);
    try uw.writeByte('/');
    try uw.writeAll(bucket);
    try uw.writeByte('/');
    try uw.writeAll(key);

    switch (creds.mode) {
        .hmac => {
            // Canonical URI: /{bucket}/{key}
            var path_buf: [2048]u8 = undefined;
            const uri_path = std.fmt.bufPrint(&path_buf, "/{s}/{s}", .{ bucket, key }) catch
                return error.HttpFetchFailed;

            const ts = aws_sig.currentTimestamp();
            const payload_hash = aws_sig.sha256Hex("");
            const sig_creds = aws_sig.Credentials{
                .access_key = creds.access_key,
                .secret_key = creds.secret_key,
                .region = creds.region,
            };
            const auth_header = aws_sig.signRequest("GET", uri_path, "", GCS_HOST, &payload_hash, sig_creds, ts, allocator) catch
                return error.HttpFetchFailed;
            defer allocator.free(auth_header);

            return s3HttpRequest(.GET, url_buf.items, null, auth_header, &ts, &payload_hash, null, allocator);
        },
        .bearer => {
            return gcsBearerRequest(.GET, url_buf.items, null, creds.token, allocator);
        },
    }
}

/// Upload an object to GCS. Dispatches to HMAC or Bearer mode.
fn gcsPutObject(
    bucket: []const u8,
    key: []const u8,
    body: []const u8,
    creds: auth_mod.GcsCredentials,
    allocator: Allocator,
) !void {
    var url_buf = std.ArrayListUnmanaged(u8){};
    defer url_buf.deinit(allocator);
    const uw = url_buf.writer(allocator);
    try uw.writeAll("https://");
    try uw.writeAll(GCS_HOST);
    try uw.writeByte('/');
    try uw.writeAll(bucket);
    try uw.writeByte('/');
    try uw.writeAll(key);

    switch (creds.mode) {
        .hmac => {
            var path_buf: [2048]u8 = undefined;
            const uri_path = std.fmt.bufPrint(&path_buf, "/{s}/{s}", .{ bucket, key }) catch
                return error.HttpFetchFailed;

            const payload_hash = aws_sig.sha256Hex(body);
            const ts = aws_sig.currentTimestamp();
            const sig_creds = aws_sig.Credentials{
                .access_key = creds.access_key,
                .secret_key = creds.secret_key,
                .region = creds.region,
            };
            const auth_header = aws_sig.signRequest("PUT", uri_path, "", GCS_HOST, &payload_hash, sig_creds, ts, allocator) catch
                return error.HttpFetchFailed;
            defer allocator.free(auth_header);

            _ = s3HttpRequest(.PUT, url_buf.items, body, auth_header, &ts, &payload_hash, null, allocator) catch
                return error.HttpFetchFailed;
        },
        .bearer => {
            _ = gcsBearerRequest(.PUT, url_buf.items, body, creds.token, allocator) catch
                return error.HttpFetchFailed;
        },
    }
}

/// GCS glob: list objects and fetch matching keys.
fn sourceGcsGlob(
    bucket: []const u8,
    key_pattern: []const u8,
    format: SourceFormat,
    auth_val: ?Value,
    allocator: Allocator,
    err_msg: *[]const u8,
) NativeError!Value {
    const wildcard_pos = std.mem.indexOfAny(u8, key_pattern, "*?") orelse 0;
    const prefix_end = if (wildcard_pos > 0)
        (std.mem.lastIndexOfScalar(u8, key_pattern[0..wildcard_pos], '/') orelse 0)
    else
        0;
    const prefix = if (prefix_end > 0) key_pattern[0 .. prefix_end + 1] else "";

    var creds = auth_mod.resolveGcsCredentials(auth_val, &atomName, allocator) catch |err| {
        err_msg.* = switch (err) {
            error.MissingCredentials => "GCS credentials not found",
            error.InvalidAuthValue => "invalid auth: parameter",
            error.InvalidProfile => "GCS profile not found",
            error.ConfigReadError => "failed to read credentials file",
        };
        return error.RuntimeError;
    };
    defer creds.deinit(allocator);

    // List objects.
    var keys = gcsListObjects(bucket, prefix, creds, allocator) catch {
        err_msg.* = "GCS list objects request failed";
        return error.RuntimeError;
    };
    defer {
        for (keys.items) |k| allocator.free(k);
        keys.deinit(allocator);
    }

    // Filter by glob pattern.
    var matched_keys = std.ArrayListUnmanaged([]const u8){};
    defer matched_keys.deinit(allocator);

    for (keys.items) |obj_key| {
        if (globMatch(key_pattern, obj_key)) {
            const dup = allocator.dupe(u8, obj_key) catch return error.OutOfMemory;
            matched_keys.append(allocator, dup) catch {
                allocator.free(dup);
                return error.OutOfMemory;
            };
        }
    }

    if (matched_keys.items.len == 0) {
        return createEmptyStream(allocator);
    }

    // Fetch and concatenate.
    var combined = std.ArrayListUnmanaged(u8){};
    errdefer combined.deinit(allocator);

    var fetch_creds = auth_mod.resolveGcsCredentials(auth_val, &atomName, allocator) catch {
        err_msg.* = "GCS credentials not found";
        return error.RuntimeError;
    };
    defer fetch_creds.deinit(allocator);

    for (matched_keys.items) |obj_key| {
        var body = gcsGetObject(bucket, obj_key, fetch_creds, allocator) catch {
            err_msg.* = "GCS GetObject request failed during glob fetch";
            return error.RuntimeError;
        };
        defer body.deinit(allocator);
        combined.appendSlice(allocator, body.items) catch return error.OutOfMemory;
        if (body.items.len > 0 and body.items[body.items.len - 1] != '\n') {
            combined.append(allocator, '\n') catch return error.OutOfMemory;
        }
    }

    for (matched_keys.items) |k| allocator.free(k);
    matched_keys.clearRetainingCapacity();

    if (format == .json) {
        defer combined.deinit(allocator);
        return createJsonStream(combined.items, allocator, err_msg);
    }
    if (format == .csv) {
        defer combined.deinit(allocator);
        return createCsvStream(combined.items, ',', allocator, err_msg);
    }

    const owned = combined.toOwnedSlice(allocator) catch {
        combined.deinit(allocator);
        err_msg.* = "failed to allocate GCS glob buffer";
        return error.RuntimeError;
    };

    const state = try allocator.create(StreamState);
    state.* = .{ .memory_reader = .{
        .data = owned,
        .cursor = 0,
        .is_jsonl = (format == .jsonl),
        .line_number = 0,
        .allocator = allocator,
    } };
    return createStream(state, allocator);
}

/// List objects in a GCS bucket with a prefix.
fn gcsListObjects(
    bucket: []const u8,
    prefix: []const u8,
    creds: auth_mod.GcsCredentials,
    allocator: Allocator,
) !std.ArrayListUnmanaged([]const u8) {
    var all_keys = std.ArrayListUnmanaged([]const u8){};
    errdefer {
        for (all_keys.items) |k| allocator.free(k);
        all_keys.deinit(allocator);
    }

    switch (creds.mode) {
        .hmac => {
            // Use S3-compat ListObjectsV2 API.
            var continuation_token: ?[]u8 = null;
            defer if (continuation_token) |t| allocator.free(t);

            while (true) {
                var query_buf = std.ArrayListUnmanaged(u8){};
                defer query_buf.deinit(allocator);
                const qw = query_buf.writer(allocator);
                try qw.writeAll("list-type=2&prefix=");
                try uriEncode(qw, prefix);
                if (continuation_token) |token| {
                    try qw.writeAll("&continuation-token=");
                    try uriEncode(qw, token);
                }

                var url_buf = std.ArrayListUnmanaged(u8){};
                defer url_buf.deinit(allocator);
                const uw = url_buf.writer(allocator);
                try uw.writeAll("https://");
                try uw.writeAll(GCS_HOST);
                try uw.writeByte('/');
                try uw.writeAll(bucket);
                try uw.writeAll("/?");
                try uw.writeAll(query_buf.items);

                var path_buf: [2048]u8 = undefined;
                const uri_path = std.fmt.bufPrint(&path_buf, "/{s}/", .{bucket}) catch
                    return error.HttpFetchFailed;

                const ts = aws_sig.currentTimestamp();
                const payload_hash = aws_sig.sha256Hex("");
                const sig_creds = aws_sig.Credentials{
                    .access_key = creds.access_key,
                    .secret_key = creds.secret_key,
                    .region = creds.region,
                };
                const auth_header = aws_sig.signRequest("GET", uri_path, query_buf.items, GCS_HOST, &payload_hash, sig_creds, ts, allocator) catch
                    return error.HttpFetchFailed;
                defer allocator.free(auth_header);

                var body_list = s3HttpRequest(.GET, url_buf.items, null, auth_header, &ts, &payload_hash, null, allocator) catch
                    return error.HttpFetchFailed;
                defer body_list.deinit(allocator);

                try parseListObjectKeys(body_list.items, &all_keys, allocator);

                if (std.mem.indexOf(u8, body_list.items, "<IsTruncated>true</IsTruncated>")) |_| {
                    if (continuation_token) |old| allocator.free(old);
                    continuation_token = extractXmlTag(body_list.items, "NextContinuationToken", allocator) catch null;
                    if (continuation_token == null) break;
                } else break;
            }
        },
        .bearer => {
            // Use GCS JSON API: GET /storage/v1/b/{bucket}/o?prefix={prefix}
            var page_token: ?[]u8 = null;
            defer if (page_token) |t| allocator.free(t);

            while (true) {
                var url_buf = std.ArrayListUnmanaged(u8){};
                defer url_buf.deinit(allocator);
                const uw = url_buf.writer(allocator);
                try uw.writeAll("https://");
                try uw.writeAll(GCS_HOST);
                try uw.writeAll("/storage/v1/b/");
                try uw.writeAll(bucket);
                try uw.writeAll("/o?prefix=");
                try uriEncode(uw, prefix);
                if (page_token) |pt| {
                    try uw.writeAll("&pageToken=");
                    try uriEncode(uw, pt);
                }

                var body_list = gcsBearerRequest(.GET, url_buf.items, null, creds.token, allocator) catch
                    return error.HttpFetchFailed;
                defer body_list.deinit(allocator);

                // Parse JSON response for items[].name
                try parseGcsJsonListKeys(body_list.items, &all_keys, allocator);

                // Check for nextPageToken
                if (page_token) |old| allocator.free(old);
                page_token = extractJsonStringField(body_list.items, "nextPageToken", allocator) catch null;
                if (page_token == null) break;
            }
        },
    }

    return all_keys;
}

/// Parse GCS JSON API list response for object names.
/// Extracts "name" fields from "items" array.
fn parseGcsJsonListKeys(
    json_body: []const u8,
    keys: *std.ArrayListUnmanaged([]const u8),
    allocator: Allocator,
) !void {
    // Simple extraction: find "name": "..." patterns after "items"
    var pos: usize = 0;
    const name_pattern = "\"name\":";
    while (std.mem.indexOfPos(u8, json_body, pos, name_pattern)) |start| {
        const after = start + name_pattern.len;
        // Skip whitespace.
        var i = after;
        while (i < json_body.len and (json_body[i] == ' ' or json_body[i] == '\t')) : (i += 1) {}
        if (i >= json_body.len or json_body[i] != '"') {
            pos = after;
            continue;
        }
        i += 1; // skip opening quote
        const name_start = i;
        while (i < json_body.len and json_body[i] != '"') : (i += 1) {}
        if (i >= json_body.len) break;
        const name = json_body[name_start..i];
        // Skip folder markers (keys ending with /).
        if (name.len > 0 and name[name.len - 1] != '/') {
            const dup = try allocator.dupe(u8, name);
            try keys.append(allocator, dup);
        }
        pos = i + 1;
    }
}

/// Extract a string field value from JSON by field name.
fn extractJsonStringField(json_body: []const u8, field: []const u8, allocator: Allocator) ![]u8 {
    // Build pattern: "field":
    var pattern_buf: [128]u8 = undefined;
    const pattern = std.fmt.bufPrint(&pattern_buf, "\"{s}\":", .{field}) catch return error.OutOfMemory;

    if (std.mem.indexOf(u8, json_body, pattern)) |start| {
        var i = start + pattern.len;
        while (i < json_body.len and (json_body[i] == ' ' or json_body[i] == '\t')) : (i += 1) {}
        if (i >= json_body.len or json_body[i] != '"') return error.OutOfMemory;
        i += 1;
        const val_start = i;
        while (i < json_body.len and json_body[i] != '"') : (i += 1) {}
        if (i >= json_body.len) return error.OutOfMemory;
        return try allocator.dupe(u8, json_body[val_start..i]);
    }
    return error.OutOfMemory;
}

/// HTTP request with Bearer token authentication.
fn gcsBearerRequest(
    method: std.http.Method,
    url: []const u8,
    body: ?[]const u8,
    token: []const u8,
    allocator: Allocator,
) !std.ArrayListUnmanaged(u8) {
    var client: std.http.Client = .{ .allocator = allocator };
    defer client.deinit();

    // Build "Bearer <token>" header value.
    var auth_buf = std.ArrayListUnmanaged(u8){};
    defer auth_buf.deinit(allocator);
    const aw_auth = auth_buf.writer(allocator);
    try aw_auth.writeAll("Bearer ");
    try aw_auth.writeAll(token);

    if (method == .PUT) {
        const result = client.fetch(.{
            .location = .{ .url = url },
            .method = .PUT,
            .payload = body,
            .headers = .{
                .authorization = .{ .override = auth_buf.items },
            },
        }) catch return error.HttpFetchFailed;

        if (result.status != .ok) return error.HttpStatusError;
        return std.ArrayListUnmanaged(u8){};
    }

    // GET request.
    var aw: std.Io.Writer.Allocating = .init(allocator);

    const result = client.fetch(.{
        .location = .{ .url = url },
        .method = .GET,
        .headers = .{
            .authorization = .{ .override = auth_buf.items },
        },
        .response_writer = &aw.writer,
    }) catch {
        if (aw.writer.buffer.len > 0) allocator.free(aw.writer.buffer);
        return error.HttpFetchFailed;
    };

    if (result.status != .ok) {
        aw.deinit();
        return error.HttpStatusError;
    }

    return aw.toArrayList();
}

// ═══════════════════════════════════════════════════════════════════════
// ── Azure Blob Transport ─────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════

/// Azure Blob source: fetches a blob from Azure Blob Storage.
fn sourceAzure(
    uri: uri_mod.ParsedUri,
    format: SourceFormat,
    auth_val: ?Value,
    allocator: Allocator,
    err_msg: *[]const u8,
) NativeError!Value {
    const container = uri.host orelse {
        err_msg.* = "Azure URI must include container name: az://container/blob";
        return error.RuntimeError;
    };
    const blob = uri.path;

    if (std.mem.indexOfAny(u8, blob, "*?")) |_| {
        return sourceAzureGlob(container, blob, format, auth_val, allocator, err_msg);
    }

    var creds = auth_mod.resolveAzureCredentials(auth_val, &atomName, allocator) catch |err| {
        err_msg.* = switch (err) {
            error.MissingCredentials => "Azure credentials not found. Set AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_KEY, or pass auth: parameter",
            error.InvalidAuthValue => "auth: must be an atom (profile) or record {account:, account_key:}",
            error.InvalidProfile => "Azure profile not found in credentials file",
            error.ConfigReadError => "failed to read credentials file",
        };
        return error.RuntimeError;
    };
    defer creds.deinit(allocator);

    var body = azureGetBlob(container, blob, creds, allocator) catch {
        err_msg.* = "Azure GetBlob request failed";
        return error.RuntimeError;
    };

    if (format == .json) {
        defer body.deinit(allocator);
        return createJsonStream(body.items, allocator, err_msg);
    }

    const owned = body.toOwnedSlice(allocator) catch {
        body.deinit(allocator);
        err_msg.* = "failed to allocate Azure response buffer";
        return error.RuntimeError;
    };

    const state = try allocator.create(StreamState);
    state.* = .{ .memory_reader = .{
        .data = owned,
        .cursor = 0,
        .is_jsonl = (format == .jsonl),
        .line_number = 0,
        .allocator = allocator,
    } };
    return createStream(state, allocator);
}

/// Azure Blob sink: buffer formatted output, then PutBlob.
fn sinkAzure(
    stream_val: Value,
    is_stream: bool,
    uri: uri_mod.ParsedUri,
    sink_format: SinkFormat,
    opts: SinkOptions,
    args: []const Value,
    allocator: Allocator,
    err_msg: *[]const u8,
) NativeError!Value {
    const container = uri.host orelse {
        err_msg.* = "Azure URI must include container name: az://container/blob";
        return error.RuntimeError;
    };
    const blob = uri.path;

    const auth_val: ?Value = blk: {
        var idx: usize = 2;
        if (idx < args.len and args[idx].isAtom()) idx += 1;
        if (idx < args.len and (args[idx].isObjType(.record) or args[idx].isObjType(.map))) idx += 1;
        if (idx < args.len) break :blk args[idx];
        break :blk null;
    };

    var creds = auth_mod.resolveAzureCredentials(auth_val, &atomName, allocator) catch |err| {
        err_msg.* = switch (err) {
            error.MissingCredentials => "Azure credentials not found. Set AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_KEY",
            error.InvalidAuthValue => "invalid auth: parameter for Azure sink",
            error.InvalidProfile => "Azure profile not found",
            error.ConfigReadError => "failed to read credentials file",
        };
        return error.RuntimeError;
    };
    defer creds.deinit(allocator);

    var out_buf = std.ArrayListUnmanaged(u8){};
    defer out_buf.deinit(allocator);
    sinkFormatToBuffer(stream_val, is_stream, sink_format, opts, &out_buf, allocator, err_msg) catch |err| return err;

    const upload = maybeCompress(out_buf.items, opts, allocator) catch {
        err_msg.* = "gzip compression failed";
        return error.RuntimeError;
    };
    defer if (upload.owned) allocator.free(@constCast(upload.buf));

    var attempts: u8 = 0;
    while (true) {
        if (azurePutBlob(container, blob, upload.buf, creds, allocator)) {
            break;
        } else |_| {
            if (attempts < opts.retry) { attempts += 1; continue; }
            err_msg.* = "Azure PutBlob request failed";
            return error.RuntimeError;
        }
    }

    return Value.nil;
}

/// Azure GetBlob HTTP request with Shared Key auth.
fn azureGetBlob(
    container: []const u8,
    blob: []const u8,
    creds: auth_mod.AzureCredentials,
    allocator: Allocator,
) !std.ArrayListUnmanaged(u8) {
    var url_buf = std.ArrayListUnmanaged(u8){};
    defer url_buf.deinit(allocator);
    const uw = url_buf.writer(allocator);
    try uw.writeAll("https://");
    try uw.writeAll(creds.account);
    try uw.writeAll(".blob.core.windows.net/");
    try uw.writeAll(container);
    try uw.writeByte('/');
    try uw.writeAll(blob);

    // Canonicalized resource: /{account}/{container}/{blob}
    var res_buf: [2048]u8 = undefined;
    const canon_res = std.fmt.bufPrint(&res_buf, "/{s}/{s}/{s}", .{ creds.account, container, blob }) catch
        return error.HttpFetchFailed;

    const date = azure_sig.currentRfc1123();
    const auth_header = azure_sig.signRequest("GET", 0, "", &date, azure_sig.API_VERSION, canon_res, creds.account, creds.account_key, allocator) catch
        return error.HttpFetchFailed;
    defer allocator.free(auth_header);

    return azureHttpRequest(.GET, url_buf.items, null, auth_header, &date, allocator);
}

/// Azure PutBlob HTTP request with Shared Key auth.
fn azurePutBlob(
    container: []const u8,
    blob: []const u8,
    body: []const u8,
    creds: auth_mod.AzureCredentials,
    allocator: Allocator,
) !void {
    var url_buf = std.ArrayListUnmanaged(u8){};
    defer url_buf.deinit(allocator);
    const uw = url_buf.writer(allocator);
    try uw.writeAll("https://");
    try uw.writeAll(creds.account);
    try uw.writeAll(".blob.core.windows.net/");
    try uw.writeAll(container);
    try uw.writeByte('/');
    try uw.writeAll(blob);

    var res_buf: [2048]u8 = undefined;
    const canon_res = std.fmt.bufPrint(&res_buf, "/{s}/{s}/{s}", .{ creds.account, container, blob }) catch
        return error.HttpFetchFailed;

    const date = azure_sig.currentRfc1123();
    const auth_header = azure_sig.signRequest("PUT", body.len, "application/octet-stream", &date, azure_sig.API_VERSION, canon_res, creds.account, creds.account_key, allocator) catch
        return error.HttpFetchFailed;
    defer allocator.free(auth_header);

    _ = azureHttpRequest(.PUT, url_buf.items, body, auth_header, &date, allocator) catch
        return error.HttpFetchFailed;
}

/// Azure glob: list blobs matching a pattern.
fn sourceAzureGlob(
    container: []const u8,
    blob_pattern: []const u8,
    format: SourceFormat,
    auth_val: ?Value,
    allocator: Allocator,
    err_msg: *[]const u8,
) NativeError!Value {
    const wildcard_pos = std.mem.indexOfAny(u8, blob_pattern, "*?") orelse 0;
    const prefix_end = if (wildcard_pos > 0)
        (std.mem.lastIndexOfScalar(u8, blob_pattern[0..wildcard_pos], '/') orelse 0)
    else
        0;
    const prefix = if (prefix_end > 0) blob_pattern[0 .. prefix_end + 1] else "";

    var creds = auth_mod.resolveAzureCredentials(auth_val, &atomName, allocator) catch |err| {
        err_msg.* = switch (err) {
            error.MissingCredentials => "Azure credentials not found",
            error.InvalidAuthValue => "invalid auth: parameter",
            error.InvalidProfile => "Azure profile not found",
            error.ConfigReadError => "failed to read credentials file",
        };
        return error.RuntimeError;
    };
    defer creds.deinit(allocator);

    var keys = azureListBlobs(container, prefix, creds, allocator) catch {
        err_msg.* = "Azure list blobs request failed";
        return error.RuntimeError;
    };
    defer {
        for (keys.items) |k| allocator.free(k);
        keys.deinit(allocator);
    }

    var matched_keys = std.ArrayListUnmanaged([]const u8){};
    defer matched_keys.deinit(allocator);
    for (keys.items) |obj_key| {
        if (globMatch(blob_pattern, obj_key)) {
            const dup = allocator.dupe(u8, obj_key) catch return error.OutOfMemory;
            matched_keys.append(allocator, dup) catch { allocator.free(dup); return error.OutOfMemory; };
        }
    }

    if (matched_keys.items.len == 0) return createEmptyStream(allocator);

    var combined = std.ArrayListUnmanaged(u8){};
    errdefer combined.deinit(allocator);

    var fetch_creds = auth_mod.resolveAzureCredentials(auth_val, &atomName, allocator) catch {
        err_msg.* = "Azure credentials not found";
        return error.RuntimeError;
    };
    defer fetch_creds.deinit(allocator);

    for (matched_keys.items) |obj_key| {
        var body = azureGetBlob(container, obj_key, fetch_creds, allocator) catch {
            err_msg.* = "Azure GetBlob failed during glob fetch";
            return error.RuntimeError;
        };
        defer body.deinit(allocator);
        combined.appendSlice(allocator, body.items) catch return error.OutOfMemory;
        if (body.items.len > 0 and body.items[body.items.len - 1] != '\n')
            combined.append(allocator, '\n') catch return error.OutOfMemory;
    }

    for (matched_keys.items) |k| allocator.free(k);
    matched_keys.clearRetainingCapacity();

    if (format == .json) {
        defer combined.deinit(allocator);
        return createJsonStream(combined.items, allocator, err_msg);
    }
    if (format == .csv) {
        defer combined.deinit(allocator);
        return createCsvStream(combined.items, ',', allocator, err_msg);
    }

    const owned = combined.toOwnedSlice(allocator) catch {
        combined.deinit(allocator);
        return error.RuntimeError;
    };

    const state = try allocator.create(StreamState);
    state.* = .{ .memory_reader = .{
        .data = owned,
        .cursor = 0,
        .is_jsonl = (format == .jsonl),
        .line_number = 0,
        .allocator = allocator,
    } };
    return createStream(state, allocator);
}

/// Azure List Blobs API with Shared Key auth.
fn azureListBlobs(
    container: []const u8,
    prefix: []const u8,
    creds: auth_mod.AzureCredentials,
    allocator: Allocator,
) !std.ArrayListUnmanaged([]const u8) {
    var all_keys = std.ArrayListUnmanaged([]const u8){};
    errdefer {
        for (all_keys.items) |k| allocator.free(k);
        all_keys.deinit(allocator);
    }

    var marker: ?[]u8 = null;
    defer if (marker) |m| allocator.free(m);

    while (true) {
        var url_buf = std.ArrayListUnmanaged(u8){};
        defer url_buf.deinit(allocator);
        const uw = url_buf.writer(allocator);
        try uw.writeAll("https://");
        try uw.writeAll(creds.account);
        try uw.writeAll(".blob.core.windows.net/");
        try uw.writeAll(container);
        try uw.writeAll("?restype=container&comp=list&prefix=");
        try uriEncode(uw, prefix);
        if (marker) |m| {
            try uw.writeAll("&marker=");
            try uriEncode(uw, m);
        }

        var res_buf: [2048]u8 = undefined;
        const canon_res = std.fmt.bufPrint(&res_buf, "/{s}/{s}\ncomp:list\nprefix:{s}\nrestype:container", .{ creds.account, container, prefix }) catch
            return error.HttpFetchFailed;

        const date = azure_sig.currentRfc1123();
        const auth_header = azure_sig.signRequest("GET", 0, "", &date, azure_sig.API_VERSION, canon_res, creds.account, creds.account_key, allocator) catch
            return error.HttpFetchFailed;
        defer allocator.free(auth_header);

        var body_list = azureHttpRequest(.GET, url_buf.items, null, auth_header, &date, allocator) catch
            return error.HttpFetchFailed;
        defer body_list.deinit(allocator);

        // Parse XML: extract <Name>...</Name> within <Blob> elements
        try parseAzureBlobNames(body_list.items, &all_keys, allocator);

        // Check for <NextMarker>...</NextMarker>
        if (marker) |old| allocator.free(old);
        marker = extractXmlTag(body_list.items, "NextMarker", allocator) catch null;
        if (marker == null or marker.?.len == 0) {
            if (marker) |m| allocator.free(m);
            marker = null;
            break;
        }
    }

    return all_keys;
}

/// Parse Azure List Blobs XML for blob names.
fn parseAzureBlobNames(xml: []const u8, keys: *std.ArrayListUnmanaged([]const u8), allocator: Allocator) !void {
    // Azure wraps blob names in <Blob><Name>...</Name></Blob>
    // We look for <Name>...</Name> within the response.
    var pos: usize = 0;
    while (std.mem.indexOfPos(u8, xml, pos, "<Name>")) |start| {
        const content_start = start + 6;
        if (std.mem.indexOfPos(u8, xml, content_start, "</Name>")) |end| {
            const name = xml[content_start..end];
            const dup = try allocator.dupe(u8, name);
            try keys.append(allocator, dup);
            pos = end + 7;
        } else break;
    }
}

/// Azure HTTP request with Shared Key auth headers.
fn azureHttpRequest(
    method: std.http.Method,
    url: []const u8,
    body: ?[]const u8,
    auth_header: []const u8,
    date: []const u8,
    allocator: Allocator,
) !std.ArrayListUnmanaged(u8) {
    var client: std.http.Client = .{ .allocator = allocator };
    defer client.deinit();

    const extra_headers = [_]std.http.Header{
        .{ .name = "x-ms-date", .value = date },
        .{ .name = "x-ms-version", .value = azure_sig.API_VERSION },
        .{ .name = "x-ms-blob-type", .value = "BlockBlob" },
    };

    // For PUT we need fewer headers, for GET only date+version matter.
    const header_count: usize = if (method == .PUT) 3 else 2;

    if (method == .PUT) {
        const result = client.fetch(.{
            .location = .{ .url = url },
            .method = .PUT,
            .payload = body,
            .headers = .{
                .authorization = .{ .override = auth_header },
                .content_type = .{ .override = "application/octet-stream" },
            },
            .extra_headers = extra_headers[0..header_count],
        }) catch return error.HttpFetchFailed;

        if (result.status != .created and result.status != .ok)
            return error.HttpStatusError;
        return std.ArrayListUnmanaged(u8){};
    }

    var aw: std.Io.Writer.Allocating = .init(allocator);
    const result = client.fetch(.{
        .location = .{ .url = url },
        .method = .GET,
        .headers = .{
            .authorization = .{ .override = auth_header },
        },
        .extra_headers = extra_headers[0..header_count],
        .response_writer = &aw.writer,
    }) catch {
        if (aw.writer.buffer.len > 0) allocator.free(aw.writer.buffer);
        return error.HttpFetchFailed;
    };

    if (result.status != .ok) {
        aw.deinit();
        return error.HttpStatusError;
    }

    return aw.toArrayList();
}

// ═══════════════════════════════════════════════════════════════════════
// ── Compression & retry helpers ──────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════

/// Gzip-wrap data using stored (uncompressed) deflate blocks.
/// Produces a valid gzip file that any standard tool can decompress.
/// Uses stored blocks (no actual compression) since Zig 0.15 flate is incomplete.
fn gzipCompress(data: []const u8, allocator: Allocator) ![]u8 {
    // Gzip format: 10-byte header + deflate stored blocks + 8-byte footer (CRC32 + ISIZE)
    // Stored block: 5-byte header (BFINAL, BTYPE=00, LEN, NLEN) + raw data
    // Max stored block size: 65535 bytes

    const max_block: usize = 65535;
    const num_blocks = if (data.len == 0) 1 else (data.len + max_block - 1) / max_block;
    const total_size = 10 + (num_blocks * 5) + data.len + 8;

    const out = try allocator.alloc(u8, total_size);
    errdefer allocator.free(out);

    var pos: usize = 0;

    // Gzip header (10 bytes): magic, method=deflate, flags=0, mtime=0, xfl=0, os=0xff
    out[pos] = 0x1f; pos += 1; // ID1
    out[pos] = 0x8b; pos += 1; // ID2
    out[pos] = 0x08; pos += 1; // CM = deflate
    out[pos] = 0x00; pos += 1; // FLG = 0
    out[pos] = 0; pos += 1; // MTIME (4 bytes, 0)
    out[pos] = 0; pos += 1;
    out[pos] = 0; pos += 1;
    out[pos] = 0; pos += 1;
    out[pos] = 0x00; pos += 1; // XFL
    out[pos] = 0xff; pos += 1; // OS = unknown

    // Deflate stored blocks.
    var data_pos: usize = 0;
    while (data_pos < data.len or data_pos == 0) {
        const remaining = data.len - data_pos;
        const block_len: u16 = @intCast(@min(remaining, max_block));
        const is_final: u8 = if (data_pos + block_len >= data.len) 1 else 0;

        out[pos] = is_final; pos += 1; // BFINAL | BTYPE=00 (stored)
        // LEN (little-endian u16)
        out[pos] = @intCast(block_len & 0xff); pos += 1;
        out[pos] = @intCast((block_len >> 8) & 0xff); pos += 1;
        // NLEN (one's complement of LEN)
        const nlen = ~block_len;
        out[pos] = @intCast(nlen & 0xff); pos += 1;
        out[pos] = @intCast((nlen >> 8) & 0xff); pos += 1;
        // Raw data.
        @memcpy(out[pos .. pos + block_len], data[data_pos .. data_pos + block_len]);
        pos += block_len;
        data_pos += block_len;

        if (block_len == 0) break; // empty data case
    }

    // Footer: CRC32 + ISIZE (original size mod 2^32)
    const crc = crc32(data);
    out[pos] = @intCast(crc & 0xff); pos += 1;
    out[pos] = @intCast((crc >> 8) & 0xff); pos += 1;
    out[pos] = @intCast((crc >> 16) & 0xff); pos += 1;
    out[pos] = @intCast((crc >> 24) & 0xff); pos += 1;
    const orig_size: u32 = @intCast(data.len & 0xffffffff);
    out[pos] = @intCast(orig_size & 0xff); pos += 1;
    out[pos] = @intCast((orig_size >> 8) & 0xff); pos += 1;
    out[pos] = @intCast((orig_size >> 16) & 0xff); pos += 1;
    out[pos] = @intCast((orig_size >> 24) & 0xff); pos += 1;

    return out[0..pos];
}

/// CRC-32 (IEEE 802.3) for gzip footer.
fn crc32(data: []const u8) u32 {
    // Using std.hash.crc
    return std.hash.crc.Crc32IsoHdlc.hash(data);
}

/// Apply gzip compression to a buffer if opts.compress == .gzip.
/// Returns owned compressed data (caller must free) or the original data.
fn maybeCompress(data: []const u8, opts: SinkOptions, allocator: Allocator) !struct { buf: []const u8, owned: bool } {
    if (opts.compress == .gzip) {
        const compressed = try gzipCompress(data, allocator);
        return .{ .buf = compressed, .owned = true };
    }
    return .{ .buf = data, .owned = false };
}

/// Get the CPU core count, defaulting to 4 if unavailable.
fn getCpuCount() u32 {
    return @intCast(std.Thread.getCpuCount() catch 4);
}

// ═══════════════════════════════════════════════════════════════════════
// ── Environment: env() ───────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════

/// env(key) → string | Result.Err(:undefined)
/// env(key, default) → string
/// Reads an environment variable by name.
fn builtinEnv(args: []const Value, allocator: Allocator, err_msg: *[]const u8) NativeError!Value {
    const key_val = args[0];
    if (!key_val.isString()) {
        err_msg.* = "env() expects a string key as first argument";
        return error.RuntimeError;
    }
    const key_str = ObjString.fromObj(key_val.asObj());

    if (std.posix.getenv(key_str.bytes)) |val| {
        const result = try ObjString.create(allocator, val, null);
        trackObj(&result.obj);
        return Value.fromObj(&result.obj);
    }

    // Not found: if default provided, return it; otherwise return Result.Err(:undefined).
    if (args.len > 1) {
        return args[1];
    }

    // Create Result.Err(:undefined) — look up the :undefined atom or use a string message.
    const msg = try ObjString.create(allocator, "undefined environment variable", null);
    trackObj(&msg.obj);
    const err_adt = try ObjAdt.create(allocator, 1, 1, &[_]Value{Value.fromObj(&msg.obj)});
    trackObj(&err_adt.obj);
    return Value.fromObj(&err_adt.obj);
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
