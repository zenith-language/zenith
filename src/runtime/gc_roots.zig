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
const fiber_mod = @import("fiber");
const ObjFiber = fiber_mod.ObjFiber;
const scheduler_mod = @import("scheduler");
const Scheduler = scheduler_mod.Scheduler;

/// Scan all GC roots from the VM state.
///
/// When a scheduler is present, scans ALL fibers in the global fiber list.
/// When in legacy mode (no scheduler), scans the VM's own stack/frames/upvalues.
///
/// Root categories:
/// 1. **Fiber stacks**: All values on each fiber's operand stack.
/// 2. **Fiber call frames**: The closure in each active call frame per fiber.
/// 3. **Fiber open upvalues**: All open upvalues per fiber.
/// 4. **Fiber fields**: result, waiting_on, waiters for each fiber.
/// 5. **Arena GC references**: arena objects may hold references to GC objects.
pub fn scanRoots(nursery: *NurseryCollector, gc: *GC, vm: *VM) !void {
    if (vm.scheduler) |sched_ptr| {
        // Multi-fiber mode: scan ALL fibers via scheduler's all_fibers list.
        const sched: *Scheduler = @ptrCast(@alignCast(sched_ptr));
        sched.all_fibers_mutex.lock();
        defer sched.all_fibers_mutex.unlock();
        for (sched.all_fibers.items) |fiber| {
            try scanFiberStackNursery(nursery, gc, fiber);
            try scanFiberFramesNursery(nursery, gc, fiber);
            try scanFiberUpvaluesNursery(nursery, gc, fiber);
            try scanFiberFieldsNursery(nursery, gc, fiber);
        }
    } else {
        // Legacy single-fiber mode: scan VM's own stack/frames/upvalues.
        try scanVMStackNursery(nursery, gc, vm);
        try scanVMFramesNursery(nursery, gc, vm);
        try scanVMUpvaluesNursery(nursery, gc, vm);
    }

    // Arena GC references: always scanned regardless of mode.
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
pub fn scanRootsForOldGen(oldgen: *OldGenCollector, gc: *GC, vm: *VM) !void {
    if (vm.scheduler) |sched_ptr| {
        // Multi-fiber mode: scan ALL fibers.
        const sched: *Scheduler = @ptrCast(@alignCast(sched_ptr));
        sched.all_fibers_mutex.lock();
        defer sched.all_fibers_mutex.unlock();
        for (sched.all_fibers.items) |fiber| {
            try scanFiberStackOldGen(oldgen, gc, fiber);
            try scanFiberFramesOldGen(oldgen, gc, fiber);
            try scanFiberUpvaluesOldGen(oldgen, gc, fiber);
            try scanFiberFieldsOldGen(oldgen, gc, fiber);
        }
    } else {
        // Legacy single-fiber mode.
        try scanVMStackOldGen(oldgen, gc, vm);
        try scanVMFramesOldGen(oldgen, gc, vm);
        try scanVMUpvaluesOldGen(oldgen, gc, vm);
    }

    // Arena GC references.
    for (gc.arenas.items) |arena| {
        for (arena.gc_refs.items) |*ref| {
            try oldgen.processValue(ref, gc);
        }
    }
}

// ── Fiber scanning helpers (nursery) ─────────────────────────────────

fn scanFiberStackNursery(nursery: *NurseryCollector, gc: *GC, fiber: *ObjFiber) !void {
    for (fiber.stack[0..fiber.stack_top]) |*val| {
        try nursery.processValue(val, gc);
    }
}

fn scanFiberFramesNursery(nursery: *NurseryCollector, gc: *GC, fiber: *ObjFiber) !void {
    for (fiber.frames[0..fiber.frame_count]) |*frame| {
        if (!frame.closure.obj.isOldGen()) {
            try nursery.markNurseryObj(&frame.closure.obj, gc);
        }
    }
}

fn scanFiberUpvaluesNursery(nursery: *NurseryCollector, gc: *GC, fiber: *ObjFiber) !void {
    var uv = fiber.open_upvalues;
    while (uv) |u| {
        if (!u.obj.isOldGen()) {
            try nursery.markNurseryObj(&u.obj, gc);
        }
        if (@intFromPtr(u.location) == @intFromPtr(&u.closed)) {
            try nursery.processValue(&u.closed, gc);
        }
        uv = u.next;
    }
}

fn scanFiberFieldsNursery(nursery: *NurseryCollector, gc: *GC, fiber: *ObjFiber) !void {
    // Scan result value if present.
    if (fiber.result) |*r| {
        try nursery.processValue(r, gc);
    }
    // Scan the fiber object header itself.
    if (!fiber.obj.isOldGen()) {
        try nursery.markNurseryObj(&fiber.obj, gc);
    }
    // Scan waiting_on fiber reference.
    if (fiber.waiting_on) |w| {
        if (!w.obj.isOldGen()) {
            try nursery.markNurseryObj(&w.obj, gc);
        }
    }
    // Scan waiters linked list.
    var waiter = fiber.waiters;
    while (waiter) |w| {
        if (!w.obj.isOldGen()) {
            try nursery.markNurseryObj(&w.obj, gc);
        }
        waiter = w.next_waiter;
    }
}

// ── Fiber scanning helpers (old-gen) ─────────────────────────────────

fn scanFiberStackOldGen(oldgen: *OldGenCollector, gc: *GC, fiber: *ObjFiber) !void {
    for (fiber.stack[0..fiber.stack_top]) |*val| {
        try oldgen.processValue(val, gc);
    }
}

fn scanFiberFramesOldGen(oldgen: *OldGenCollector, gc: *GC, fiber: *ObjFiber) !void {
    for (fiber.frames[0..fiber.frame_count]) |*frame| {
        try oldgen.markObj(&frame.closure.obj, gc);
    }
}

fn scanFiberUpvaluesOldGen(oldgen: *OldGenCollector, gc: *GC, fiber: *ObjFiber) !void {
    var uv = fiber.open_upvalues;
    while (uv) |u| {
        try oldgen.markObj(&u.obj, gc);
        if (@intFromPtr(u.location) == @intFromPtr(&u.closed)) {
            try oldgen.processValue(&u.closed, gc);
        }
        uv = u.next;
    }
}

fn scanFiberFieldsOldGen(oldgen: *OldGenCollector, gc: *GC, fiber: *ObjFiber) !void {
    if (fiber.result) |*r| {
        try oldgen.processValue(r, gc);
    }
    try oldgen.markObj(&fiber.obj, gc);
    if (fiber.waiting_on) |w| {
        try oldgen.markObj(&w.obj, gc);
    }
    var waiter = fiber.waiters;
    while (waiter) |w| {
        try oldgen.markObj(&w.obj, gc);
        waiter = w.next_waiter;
    }
}

// ── Legacy VM scanning helpers (nursery) ─────────────────────────────

fn scanVMStackNursery(nursery: *NurseryCollector, gc: *GC, vm: *VM) !void {
    for (vm.stack[0..vm.stack_top]) |*val| {
        try nursery.processValue(val, gc);
    }
}

fn scanVMFramesNursery(nursery: *NurseryCollector, gc: *GC, vm: *VM) !void {
    for (vm.frames[0..vm.frame_count]) |*frame| {
        if (!frame.closure.obj.isOldGen()) {
            try nursery.markNurseryObj(&frame.closure.obj, gc);
        }
    }
}

fn scanVMUpvaluesNursery(nursery: *NurseryCollector, gc: *GC, vm: *VM) !void {
    var uv = vm.open_upvalues;
    while (uv) |u| {
        if (!u.obj.isOldGen()) {
            try nursery.markNurseryObj(&u.obj, gc);
        }
        if (@intFromPtr(u.location) == @intFromPtr(&u.closed)) {
            try nursery.processValue(&u.closed, gc);
        }
        uv = u.next;
    }
}

// ── Legacy VM scanning helpers (old-gen) ─────────────────────────────

fn scanVMStackOldGen(oldgen: *OldGenCollector, gc: *GC, vm: *VM) !void {
    for (vm.stack[0..vm.stack_top]) |*val| {
        try oldgen.processValue(val, gc);
    }
}

fn scanVMFramesOldGen(oldgen: *OldGenCollector, gc: *GC, vm: *VM) !void {
    for (vm.frames[0..vm.frame_count]) |*frame| {
        try oldgen.markObj(&frame.closure.obj, gc);
    }
}

fn scanVMUpvaluesOldGen(oldgen: *OldGenCollector, gc: *GC, vm: *VM) !void {
    var uv = vm.open_upvalues;
    while (uv) |u| {
        try oldgen.markObj(&u.obj, gc);
        if (@intFromPtr(u.location) == @intFromPtr(&u.closed)) {
            try oldgen.processValue(&u.closed, gc);
        }
        uv = u.next;
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
