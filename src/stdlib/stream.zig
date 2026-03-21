const std = @import("std");
const Allocator = std.mem.Allocator;
const value_mod = @import("value");
const Value = value_mod.Value;
const obj_mod = @import("obj");
const Obj = obj_mod.Obj;
const ObjAdt = obj_mod.ObjAdt;
const ObjList = obj_mod.ObjList;
const ObjRange = obj_mod.ObjRange;

/// Pull-based stream state. Each variant represents a different stream
/// operator (generator, transform, or terminal helper). The `next()`
/// function pulls one element at a time, returning Some(val) or None
/// (as ObjAdt Option type).
pub const StreamState = union(enum) {
    range_iter: RangeIter,
    repeat_iter: RepeatIter,
    iterate_iter: IterateIter,
    map_op: MapOp,
    filter_op: FilterOp,
    take_op: TakeOp,
    drop_op: DropOp,

    pub const RangeIter = struct {
        current: i32,
        end: i32,
        step: i32,
    };

    pub const RepeatIter = struct {
        value: Value,
    };

    pub const IterateIter = struct {
        current: Value,
        fn_val: Value,
        started: bool,
    };

    pub const MapOp = struct {
        upstream: Value, // NaN-boxed ObjStream pointer
        fn_val: Value,
    };

    pub const FilterOp = struct {
        upstream: Value, // NaN-boxed ObjStream pointer
        fn_val: Value,
    };

    pub const TakeOp = struct {
        upstream: Value, // NaN-boxed ObjStream pointer
        remaining: i32,
    };

    pub const DropOp = struct {
        upstream: Value, // NaN-boxed ObjStream pointer
        remaining: i32,
        started: bool,
    };

    /// Pull the next element from this stream.
    /// Returns Some(val) or None as ObjAdt Option values.
    pub fn next(self: *StreamState, allocator: Allocator) !Value {
        switch (self.*) {
            .range_iter => |*s| {
                if (s.step > 0) {
                    if (s.current >= s.end) return makeNone(allocator);
                } else {
                    if (s.current <= s.end) return makeNone(allocator);
                }
                const val = Value.fromInt(s.current);
                s.current += s.step;
                return makeSome(val, allocator);
            },
            .repeat_iter => |s| {
                return makeSome(s.value, allocator);
            },
            .iterate_iter => |*s| {
                if (!s.started) {
                    s.started = true;
                    return makeSome(s.current, allocator);
                }
                // Apply function to current value to get next.
                const result = try callClosure(s.fn_val, &[_]Value{s.current});
                s.current = result;
                return makeSome(result, allocator);
            },
            .map_op => |s| {
                const upstream_stream = obj_mod.ObjStream.fromObj(s.upstream.asObj());
                const upstream_val = try upstream_stream.state.next(allocator);
                // If upstream returned None, pass it through.
                if (isNone(upstream_val)) return upstream_val;
                // Extract the value from Some(val).
                const inner = adtPayload(upstream_val, 0);
                const mapped = try callClosure(s.fn_val, &[_]Value{inner});
                return makeSome(mapped, allocator);
            },
            .filter_op => |s| {
                const upstream_stream = obj_mod.ObjStream.fromObj(s.upstream.asObj());
                // Keep pulling until we find a matching element or upstream is exhausted.
                while (true) {
                    const upstream_val = try upstream_stream.state.next(allocator);
                    if (isNone(upstream_val)) return upstream_val;
                    const inner = adtPayload(upstream_val, 0);
                    const predicate_result = try callClosure(s.fn_val, &[_]Value{inner});
                    if (!isFalsy(predicate_result)) {
                        return makeSome(inner, allocator);
                    }
                }
            },
            .take_op => |*s| {
                if (s.remaining <= 0) return makeNone(allocator);
                const upstream_stream = obj_mod.ObjStream.fromObj(s.upstream.asObj());
                const upstream_val = try upstream_stream.state.next(allocator);
                if (isNone(upstream_val)) return upstream_val;
                s.remaining -= 1;
                return upstream_val;
            },
            .drop_op => |*s| {
                const upstream_stream = obj_mod.ObjStream.fromObj(s.upstream.asObj());
                if (!s.started) {
                    // Discard `remaining` elements.
                    var dropped: i32 = 0;
                    while (dropped < s.remaining) : (dropped += 1) {
                        const upstream_val = try upstream_stream.state.next(allocator);
                        if (isNone(upstream_val)) return upstream_val;
                    }
                    s.started = true;
                }
                return upstream_stream.state.next(allocator);
            },
        }
    }

    /// Free any owned memory for this stream state.
    pub fn deinit(self: *StreamState, allocator: Allocator) void {
        _ = self;
        _ = allocator;
        // StreamState variants don't own heap memory beyond the StreamState
        // allocation itself (freed by ObjStream.destroy). GC-managed references
        // (closures, upstream streams) are freed by the GC.
    }

    /// Trace GC references in this stream state for nursery collection.
    /// All Value fields that might hold object references must be traced.
    pub fn traceGCRefs(self: *StreamState, nursery: anytype, gc: anytype) !void {
        switch (self.*) {
            .range_iter => {},
            .repeat_iter => |*s| {
                try nursery.processValue(&s.value, gc);
            },
            .iterate_iter => |*s| {
                try nursery.processValue(&s.current, gc);
                try nursery.processValue(&s.fn_val, gc);
            },
            .map_op => |*s| {
                try nursery.processValue(&s.upstream, gc);
                try nursery.processValue(&s.fn_val, gc);
            },
            .filter_op => |*s| {
                try nursery.processValue(&s.upstream, gc);
                try nursery.processValue(&s.fn_val, gc);
            },
            .take_op => |*s| {
                try nursery.processValue(&s.upstream, gc);
            },
            .drop_op => |*s| {
                try nursery.processValue(&s.upstream, gc);
            },
        }
    }

    /// Trace GC references for old-gen collection.
    pub fn traceGCRefsOldGen(self: *StreamState, oldgen: anytype, gc: anytype) !void {
        switch (self.*) {
            .range_iter => {},
            .repeat_iter => |*s| {
                try oldgen.processValue(&s.value, gc);
            },
            .iterate_iter => |*s| {
                try oldgen.processValue(&s.current, gc);
                try oldgen.processValue(&s.fn_val, gc);
            },
            .map_op => |*s| {
                try oldgen.processValue(&s.upstream, gc);
                try oldgen.processValue(&s.fn_val, gc);
            },
            .filter_op => |*s| {
                try oldgen.processValue(&s.upstream, gc);
                try oldgen.processValue(&s.fn_val, gc);
            },
            .take_op => |*s| {
                try oldgen.processValue(&s.upstream, gc);
            },
            .drop_op => |*s| {
                try oldgen.processValue(&s.upstream, gc);
            },
        }
    }
};

// ── VM Callback Interface ─────────────────────────────────────────────
// Stream.next() needs to invoke closures (for iterate, map, filter).
// Uses the same callback pattern as builtins.zig -- the VM sets these
// before running any stream terminal.

/// Error type matching builtins.zig NativeError.
pub const NativeError = error{
    RuntimeError,
} || Allocator.Error;

/// Callback type: invoke a closure Value with given arguments.
pub const CallClosureFn = *const fn (vm_ptr: *anyopaque, closure_val: Value, args: []const Value) ?Value;

/// Callback type: register a heap object with the VM for cleanup.
pub const TrackObjFn = *const fn (vm_ptr: *anyopaque, o: *Obj) void;

/// Module-level callback state (set by builtins.zig before terminal execution).
var current_vm: ?*anyopaque = null;
var call_closure_fn: ?CallClosureFn = null;
var track_obj_fn: ?TrackObjFn = null;

/// Set VM callbacks for stream operations.
pub fn setVM(vm_ptr: *anyopaque, closure_fn: CallClosureFn, track_fn: TrackObjFn) void {
    current_vm = vm_ptr;
    call_closure_fn = closure_fn;
    track_obj_fn = track_fn;
}

/// Clear VM callbacks.
pub fn clearVM() void {
    current_vm = null;
    call_closure_fn = null;
    track_obj_fn = null;
}

/// Track an intermediate heap object with the VM.
fn trackObj(o: *Obj) void {
    if (current_vm) |vm_ptr| {
        if (track_obj_fn) |f| {
            f(vm_ptr, o);
        }
    }
}

/// Internal helper: invoke a closure from within a stream operation.
fn callClosure(closure_val: Value, args: []const Value) NativeError!Value {
    const vm_ptr = current_vm orelse return error.RuntimeError;
    const fn_ptr = call_closure_fn orelse return error.RuntimeError;
    return fn_ptr(vm_ptr, closure_val, args) orelse return error.RuntimeError;
}

// ── ADT Helper Functions ──────────────────────────────────────────────
// Option: type_id=0, Some=variant_idx 0 (arity 1), None=variant_idx 1 (arity 0)

fn makeNone(allocator: Allocator) NativeError!Value {
    const adt = try ObjAdt.create(allocator, 0, 1, &[_]Value{});
    trackObj(&adt.obj);
    return Value.fromObj(&adt.obj);
}

fn makeSome(val: Value, allocator: Allocator) NativeError!Value {
    const adt = try ObjAdt.create(allocator, 0, 0, &[_]Value{val});
    trackObj(&adt.obj);
    return Value.fromObj(&adt.obj);
}

/// Check if a value is None (Option type_id=0, variant_idx=1).
fn isNone(val: Value) bool {
    if (!val.isObjType(.adt)) return false;
    const adt = ObjAdt.fromObj(val.asObj());
    return adt.type_id == 0 and adt.variant_idx == 1;
}

/// Get payload from ADT at given index.
fn adtPayload(val: Value, idx: usize) Value {
    const adt = ObjAdt.fromObj(val.asObj());
    return adt.payload[idx];
}

/// Check if value is falsy (nil or false).
fn isFalsy(val: Value) bool {
    if (val.isNil()) return true;
    if (val.isBool()) return !val.asBool();
    return false;
}
