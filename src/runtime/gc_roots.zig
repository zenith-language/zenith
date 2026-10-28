const std = @import("std");
const obj_mod = @import("obj");
const value_mod = @import("value");
const Value = value_mod.Value;
const gc_mod = @import("gc");
const GC = gc_mod.GC;
const Marker = gc_mod.Marker;
const vm_mod = @import("vm");
const VM = vm_mod.VM;
const scheduler_mod = @import("scheduler");
const Scheduler = scheduler_mod.Scheduler;

/// Mark everything directly reachable from the GC roots:
///
/// 1. The VM's own stack, call frames and open upvalues.
/// 2. Every fiber known to the scheduler, or the VM's current fiber when
///    running without one.
/// 3. Arena references.
/// 4. Values pinned by native code on the temp root stack.
pub fn scanRoots(marker: *Marker, gc: *GC, vm: *VM) !void {
    for (vm.stack[0..vm.stack_top]) |*val| {
        try marker.processValue(val, gc);
    }
    for (vm.frames[0..vm.frame_count]) |*frame| {
        try marker.markObj(&frame.closure.obj, gc);
    }
    var uv = vm.open_upvalues;
    while (uv) |u| : (uv = u.next) {
        try marker.markObj(&u.obj, gc);
    }

    if (vm.scheduler) |sched_ptr| {
        const sched: *Scheduler = @ptrCast(@alignCast(sched_ptr));
        sched.all_fibers_mutex.lock();
        defer sched.all_fibers_mutex.unlock();
        for (sched.all_fibers.items) |fiber| {
            try marker.markObj(&fiber.obj, gc);
            try marker.scanFiber(fiber, gc);
        }
    } else if (vm.current_fiber) |fiber| {
        try marker.markObj(&fiber.obj, gc);
        try marker.scanFiber(fiber, gc);
    }

    for (gc.arenas.items) |arena| {
        for (arena.gc_refs.items) |*ref| {
            try marker.processValue(ref, gc);
        }
    }

    for (gc.temp_roots.slice()) |*val| {
        try marker.processValue(val, gc);
    }
}

test "scanRoots marks objects on the value stack" {
    const allocator = std.testing.allocator;
    var gc = try GC.init(allocator);
    defer gc.deinit();

    const str = try obj_mod.ObjString.create(allocator, "stack_val", null);
    gc.trackObject(&str.obj);

    var vm = VM.init(undefined, allocator);
    vm.stack[0] = Value.fromObj(&str.obj);
    vm.stack_top = 1;

    var marker = Marker{};
    defer marker.deinit(allocator);
    try scanRoots(&marker, &gc, &vm);
    try marker.drain(&gc);

    try std.testing.expect(str.obj.isMarked());
    str.obj.setMarked(false);
}
