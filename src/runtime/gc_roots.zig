const std = @import("std");
const obj_mod = @import("obj");
const ObjUpvalue = obj_mod.ObjUpvalue;
const value_mod = @import("value");
const Value = value_mod.Value;
const gc_mod = @import("gc");
const GC = gc_mod.GC;
const gc_nursery_mod = @import("gc_nursery");
const NurseryCollector = gc_nursery_mod.NurseryCollector;
const gc_oldgen_mod = @import("gc_oldgen");
const OldGenCollector = gc_oldgen_mod.OldGenCollector;
const arena_mod = @import("arena");
const StageArena = arena_mod.StageArena;
const vm_mod = @import("vm");
const VM = vm_mod.VM;

/// Scan all GC roots from the VM state.
///
/// Roots are values that are directly reachable without tracing through
/// other objects. For the current single-threaded VM, roots include:
///
/// 1. **Value stack**: All values currently on the operand stack.
/// 2. **Call frame closures**: The closure in each active call frame.
/// 3. **Open upvalue list**: All open upvalues that reference stack slots.
///
/// This function is designed to be extended in Phase 7 to also scan
/// fiber stacks, channel buffers, and deque entries.
pub fn scanRoots(nursery: *NurseryCollector, gc: *GC, vm: *VM) !void {
    // 1. Value stack: scan all active stack slots.
    for (vm.stack[0..vm.stack_top]) |*val| {
        try nursery.processValue(val, gc);
    }

    // 2. Call frame closures: each active frame holds a closure reference.
    for (vm.frames[0..vm.frame_count]) |*frame| {
        if (!frame.closure.obj.isOldGen()) {
            try nursery.markNurseryObj(&frame.closure.obj, gc);
        }
    }

    // 3. Open upvalue list: upvalues that reference live stack slots.
    var uv = vm.open_upvalues;
    while (uv) |u| {
        if (!u.obj.isOldGen()) {
            try nursery.markNurseryObj(&u.obj, gc);
        }
        // If the upvalue is closed (location points to its own closed field),
        // scan the closed value as well.
        if (@intFromPtr(u.location) == @intFromPtr(&u.closed)) {
            try nursery.processValue(&u.closed, gc);
        }
        uv = u.next;
    }

    // 4. Arena GC references: arena objects may hold references to GC objects.
    for (gc.arenas.items) |arena| {
        for (arena.gc_refs.items) |*ref| {
            try nursery.processValue(ref, gc);
        }
    }
}

/// Scan all GC roots for old-gen collection.
///
/// Same root set as nursery scanning, but marks old-gen objects via
/// OldGenCollector.markObj() instead of promoting nursery objects.
/// This is used during full old-gen mark-sweep collection.
pub fn scanRootsForOldGen(oldgen: *OldGenCollector, gc: *GC, vm: *VM) !void {
    // 1. Value stack: scan all active stack slots.
    for (vm.stack[0..vm.stack_top]) |*val| {
        try oldgen.processValue(val, gc);
    }

    // 2. Call frame closures: each active frame holds a closure reference.
    for (vm.frames[0..vm.frame_count]) |*frame| {
        try oldgen.markObj(&frame.closure.obj, gc);
    }

    // 3. Open upvalue list: upvalues that reference live stack slots.
    var uv = vm.open_upvalues;
    while (uv) |u| {
        try oldgen.markObj(&u.obj, gc);
        // If the upvalue is closed, scan the closed value.
        if (@intFromPtr(u.location) == @intFromPtr(&u.closed)) {
            try oldgen.processValue(&u.closed, gc);
        }
        uv = u.next;
    }

    // 4. Arena GC references: arena objects may hold references to GC objects.
    for (gc.arenas.items) |arena| {
        for (arena.gc_refs.items) |*ref| {
            try oldgen.processValue(ref, gc);
        }
    }
}

// ── Tests ──────────────────────────────────────────────────────────────

test "scanRoots promotes objects on value stack" {
    const allocator = std.testing.allocator;
    var gc = try GC.init(allocator);
    defer gc.deinit();

    var nursery = NurseryCollector.init();
    defer nursery.deinit(allocator);

    // Create a string and put it on the VM stack.
    const str = try obj_mod.ObjString.create(allocator, "stack_val", null);
    gc.trackObject(&str.obj);

    // Set up a minimal VM-like state.
    var vm = VM.init(undefined, allocator);
    vm.stack[0] = Value.fromObj(&str.obj);
    vm.stack_top = 1;

    try scanRoots(&nursery, &gc, &vm);
    try nursery.processGrayStack(&gc);

    try std.testing.expect(str.obj.isOldGen());
}
