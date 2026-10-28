const std = @import("std");
const Allocator = std.mem.Allocator;
const Alignment = std.mem.Alignment;
const obj_mod = @import("obj");
const Obj = obj_mod.Obj;
const ObjFunction = obj_mod.ObjFunction;
const ObjClosure = obj_mod.ObjClosure;
const ObjUpvalue = obj_mod.ObjUpvalue;
const ObjList = obj_mod.ObjList;
const ObjMap = obj_mod.ObjMap;
const ObjTuple = obj_mod.ObjTuple;
const ObjRecord = obj_mod.ObjRecord;
const ObjAdt = obj_mod.ObjAdt;
const ObjStream = obj_mod.ObjStream;
const value_mod = @import("value");
const Value = value_mod.Value;
const intern_mod = @import("intern");
const InternTable = intern_mod.InternTable;
const roots_mod = @import("roots");
const RootStack = roots_mod.RootStack;
const gc_roots_mod = @import("gc_roots");
const arena_mod = @import("arena");
const StageArena = arena_mod.StageArena;
const fiber_mod = @import("fiber");
const ObjFiber = fiber_mod.ObjFiber;
const channel_mod = @import("channel");
const ObjChannel = channel_mod.ObjChannel;
const vm_mod = @import("vm");
const VM = vm_mod.VM;
const scheduler_mod = @import("scheduler");
const Scheduler = scheduler_mod.Scheduler;

/// Non-moving mark-sweep garbage collector.
///
/// Every heap object sits on one intrusive list. A collection marks what is
/// reachable from the roots, frees the rest, and schedules the next
/// collection at twice the surviving heap, so collection work stays
/// proportional to allocation. Objects never move and there are no write
/// barriers: native code may mutate any object it holds.
///
/// Roots are the VM and fiber stacks, frames and open upvalues, registered
/// arenas, and `temp_roots`, where native code pins the values it holds in
/// Zig locals (see roots.zig).
pub const GC = struct {
    // ── Heap accounting ──────────────────────────────────────────────
    bytes_allocated: usize = 0,
    next_gc: usize,
    min_heap: usize,

    // ── Statistics ───────────────────────────────────────────────────
    collections: u64 = 0,
    objects_freed: u64 = 0,
    bytes_freed: u64 = 0,
    last_pause_ns: u64 = 0,
    object_count: usize = 0,

    intern_table: InternTable,

    /// Objects subject to collection.
    objects: ?*Obj = null,
    /// Compiler constants, freed only by deinit.
    permanent_objects: ?*Obj = null,

    backing_allocator: Allocator,
    marker: Marker = .{},
    temp_roots: RootStack,
    arenas: std.ArrayListUnmanaged(*StageArena) = .empty,

    /// VM whose roots are scanned (set when the VM starts).
    vm: ?*VM = null,

    /// ZENITH_GC_LOG=1 prints a line per collection to stderr.
    log_enabled: bool,

    collecting: bool = false,

    pub const DEFAULT_MIN_HEAP: usize = 8 * 1024 * 1024;
    const GROWTH_FACTOR: usize = 2;

    pub fn init(backing: Allocator) !GC {
        return .{
            .backing_allocator = backing,
            .intern_table = try InternTable.init(backing, 256),
            .temp_roots = RootStack.init(backing),
            .min_heap = DEFAULT_MIN_HEAP,
            .next_gc = DEFAULT_MIN_HEAP,
            .log_enabled = envFlag("ZENITH_GC_LOG"),
        };
    }

    fn envFlag(name: [:0]const u8) bool {
        const val = std.posix.getenv(name) orelse return false;
        return std.mem.eql(u8, val, "1");
    }

    pub fn deinit(self: *GC) void {
        self.marker.deinit(self.backing_allocator);
        self.temp_roots.deinit();
        self.arenas.deinit(self.backing_allocator);
        freeList(self.objects, self.backing_allocator);
        self.objects = null;
        freeList(self.permanent_objects, self.backing_allocator);
        self.permanent_objects = null;
        self.intern_table.deinit();
    }

    fn freeList(head: ?*Obj, backing: Allocator) void {
        var obj = head;
        while (obj) |o| {
            const next = o.next;
            o.destroy(backing);
            obj = next;
        }
    }

    /// Allocator that counts bytes and triggers collections.
    pub fn allocator(self: *GC) Allocator {
        return .{ .ptr = @ptrCast(self), .vtable = &vtable };
    }

    /// Put a newly allocated object under GC management. Idempotent.
    pub fn trackObject(self: *GC, obj: *Obj) void {
        if (obj.isTracked()) return;
        obj.gc_flags |= Obj.GC_TRACKED;
        obj.next = self.objects;
        self.objects = obj;
        self.object_count += 1;
    }

    /// Register a compiler constant: kept alive until deinit and skipped
    /// by marking. Constants only reference other constants.
    pub fn trackPermanent(self: *GC, obj: *Obj) void {
        if (obj.isTracked()) return;
        obj.gc_flags |= Obj.GC_TRACKED | Obj.GC_PERM;
        obj.next = self.permanent_objects;
        self.permanent_objects = obj;
    }

    /// Register a stage arena with the GC for root scanning.
    pub fn registerArena(self: *GC, arena: *StageArena) !void {
        try self.arenas.append(self.backing_allocator, arena);
    }

    /// Unregister a stage arena from the GC.
    pub fn unregisterArena(self: *GC, arena: *StageArena) void {
        for (self.arenas.items, 0..) |a, i| {
            if (a == arena) {
                _ = self.arenas.swapRemove(i);
                return;
            }
        }
    }

    /// Get total heap size (bytes currently allocated).
    pub fn heapSize(self: *const GC) usize {
        return self.bytes_allocated;
    }

    /// Run a full collection. No-op until a VM is attached.
    pub fn collect(self: *GC) !void {
        if (self.collecting) return;
        const vm = self.vm orelse return;
        self.collecting = true;
        defer self.collecting = false;

        // Safepoint coordination: if scheduler exists, pause all workers.
        var sched: ?*Scheduler = null;
        if (vm.scheduler) |sched_ptr| {
            sched = @ptrCast(@alignCast(sched_ptr));
            sched.?.requestSafepoint();
            sched.?.waitForSafepoint();
        }
        defer {
            if (sched) |s| {
                s.releaseSafepoint();
            }
        }

        const timer_start = std.time.nanoTimestamp();
        const bytes_before = self.bytes_allocated;

        try gc_roots_mod.scanRoots(&self.marker, self, vm);
        try self.marker.drain(self);

        // The intern table holds weak references: drop dead strings first.
        _ = self.intern_table.removeUnmarked();

        const freed = self.sweep();
        self.marker.clearUntracked();

        self.collections += 1;
        self.objects_freed += freed;
        self.bytes_freed += bytes_before -| self.bytes_allocated;
        self.next_gc = @max(self.min_heap, self.bytes_allocated * GROWTH_FACTOR);

        const timer_end = std.time.nanoTimestamp();
        self.last_pause_ns = @intCast(@as(i128, timer_end) - @as(i128, timer_start));

        if (self.log_enabled) {
            self.logCollection(freed, bytes_before);
        }
    }

    /// Free unmarked objects and clear the marks of the survivors.
    fn sweep(self: *GC) usize {
        const alloc = self.allocator();
        var freed: usize = 0;
        var prev: ?*Obj = null;
        var current = self.objects;
        while (current) |obj| {
            const next = obj.next;
            if (obj.isMarked()) {
                obj.setMarked(false);
                prev = obj;
            } else {
                if (prev) |p| {
                    p.next = next;
                } else {
                    self.objects = next;
                }
                obj.destroy(alloc);
                freed += 1;
            }
            current = next;
        }
        self.object_count -= freed;
        return freed;
    }

    fn logCollection(self: *const GC, freed: usize, bytes_before: usize) void {
        const stderr = std.fs.File.stderr();
        var buf: [256]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "[GC] #{d}: {d} freed, {d} live, heap {d}B -> {d}B, next {d}B, pause {d}ns\n", .{
            self.collections,
            freed,
            self.object_count,
            bytes_before,
            self.bytes_allocated,
            self.next_gc,
            self.last_pause_ns,
        }) catch return;
        stderr.writeAll(msg) catch {};
    }

    // ── Allocator vtable ─────────────────────────────────────────────

    const vtable = Allocator.VTable{
        .alloc = gcAlloc,
        .resize = gcResize,
        .remap = gcRemap,
        .free = gcFree,
    };

    fn gcAlloc(ctx: *anyopaque, len: usize, alignment: Alignment, ret_addr: usize) ?[*]u8 {
        const gc: *GC = @ptrCast(@alignCast(ctx));
        if (gc.bytes_allocated + len > gc.next_gc) {
            gc.collect() catch {};
        }
        const result = gc.backing_allocator.vtable.alloc(gc.backing_allocator.ptr, len, alignment, ret_addr);
        if (result != null) {
            gc.bytes_allocated += len;
        }
        return result;
    }

    fn gcResize(ctx: *anyopaque, memory: []u8, alignment: Alignment, new_len: usize, ret_addr: usize) bool {
        const gc: *GC = @ptrCast(@alignCast(ctx));
        const ok = gc.backing_allocator.vtable.resize(gc.backing_allocator.ptr, memory, alignment, new_len, ret_addr);
        if (ok) gc.adjust(memory.len, new_len);
        return ok;
    }

    fn gcRemap(ctx: *anyopaque, memory: []u8, alignment: Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const gc: *GC = @ptrCast(@alignCast(ctx));
        const result = gc.backing_allocator.vtable.remap(gc.backing_allocator.ptr, memory, alignment, new_len, ret_addr);
        if (result != null) gc.adjust(memory.len, new_len);
        return result;
    }

    fn gcFree(ctx: *anyopaque, memory: []u8, alignment: Alignment, ret_addr: usize) void {
        const gc: *GC = @ptrCast(@alignCast(ctx));
        // Saturating: some objects were allocated before the GC existed.
        gc.bytes_allocated -|= memory.len;
        gc.backing_allocator.vtable.free(gc.backing_allocator.ptr, memory, alignment, ret_addr);
    }

    fn adjust(self: *GC, old_len: usize, new_len: usize) void {
        if (new_len > old_len) {
            self.bytes_allocated += new_len - old_len;
        } else {
            self.bytes_allocated -|= old_len - new_len;
        }
    }
};

/// Kept for callers that wrap the GC explicitly; same as `GC.allocator()`.
pub const GCAllocator = struct {
    gc: *GC,

    pub fn allocator(self: *GCAllocator) Allocator {
        return self.gc.allocator();
    }
};

/// Mark phase: gray-stack tracing from the roots.
pub const Marker = struct {
    gray_stack: std.ArrayListUnmanaged(*Obj) = .empty,
    /// Marked objects that are on no GC list (e.g. compiler functions, or
    /// objects a builtin has not registered yet). Their marks are cleared
    /// after the sweep, which never visits them.
    untracked: std.ArrayListUnmanaged(*Obj) = .empty,

    pub fn deinit(self: *Marker, allocator: Allocator) void {
        self.gray_stack.deinit(allocator);
        self.untracked.deinit(allocator);
    }

    pub fn markObj(self: *Marker, obj: *Obj, gc: *GC) !void {
        if (obj.isPermanent() or obj.isMarked()) return;
        obj.setMarked(true);
        if (!obj.isTracked()) try self.untracked.append(gc.backing_allocator, obj);
        try self.gray_stack.append(gc.backing_allocator, obj);
    }

    pub fn processValue(self: *Marker, val: *Value, gc: *GC) !void {
        if (val.isObj()) try self.markObj(val.asObj(), gc);
    }

    pub fn drain(self: *Marker, gc: *GC) !void {
        while (self.gray_stack.pop()) |obj| {
            try self.scanObject(obj, gc);
        }
    }

    fn clearUntracked(self: *Marker) void {
        for (self.untracked.items) |obj| obj.setMarked(false);
        self.untracked.clearRetainingCapacity();
    }

    fn scanObject(self: *Marker, obj: *Obj, gc: *GC) !void {
        switch (obj.obj_type) {
            .string, .bytes, .int_big, .range => {},
            .function => {
                const func = ObjFunction.fromObj(obj);
                for (func.chunk.constants.items) |*val| {
                    try self.processValue(val, gc);
                }
            },
            .closure => {
                const clos = ObjClosure.fromObj(obj);
                try self.markObj(&clos.function.obj, gc);
                for (clos.upvalues) |maybe_uv| {
                    if (maybe_uv) |uv| try self.markObj(&uv.obj, gc);
                }
            },
            .upvalue => {
                // Open upvalues point into stacks, which are roots.
                const uv = ObjUpvalue.fromObj(obj);
                try self.processValue(&uv.closed, gc);
            },
            .list => {
                const lst = ObjList.fromObj(obj);
                for (lst.items.items) |*val| {
                    try self.processValue(val, gc);
                }
            },
            .map => {
                const m = ObjMap.fromObj(obj);
                var it = m.entries.iterator();
                while (it.next()) |entry| {
                    try self.processValue(entry.key_ptr, gc);
                    try self.processValue(entry.value_ptr, gc);
                }
            },
            .tuple => {
                const t = ObjTuple.fromObj(obj);
                for (t.fields) |*val| {
                    try self.processValue(val, gc);
                }
            },
            .record => {
                const rec = ObjRecord.fromObj(obj);
                for (rec.field_values) |*val| {
                    try self.processValue(val, gc);
                }
            },
            .adt => {
                const a = ObjAdt.fromObj(obj);
                for (a.payload) |*val| {
                    try self.processValue(val, gc);
                }
            },
            .stream => {
                const s = ObjStream.fromObj(obj);
                try s.state.traceGCRefs(self, gc);
            },
            .fiber => try self.scanFiber(ObjFiber.fromObj(obj), gc),
            .channel => {
                const ch = ObjChannel.fromObj(obj);
                if (ch.buffer) |buf| {
                    var i: u32 = 0;
                    while (i < ch.count) : (i += 1) {
                        try self.processValue(&buf[(ch.head + i) % ch.capacity], gc);
                    }
                }
                if (ch.handoff) |*h| try self.processValue(h, gc);
                var sender = ch.senders.head;
                while (sender) |s| : (sender = s.next_waiter) {
                    try self.markObj(&s.obj, gc);
                }
                var receiver = ch.receivers.head;
                while (receiver) |r| : (receiver = r.next_waiter) {
                    try self.markObj(&r.obj, gc);
                }
            },
        }
    }

    pub fn scanFiber(self: *Marker, fiber: *ObjFiber, gc: *GC) !void {
        for (fiber.stack[0..fiber.stack_top]) |*val| {
            try self.processValue(val, gc);
        }
        for (fiber.frames[0..fiber.frame_count]) |*f| {
            try self.markObj(&f.closure.obj, gc);
        }
        var uv = fiber.open_upvalues;
        while (uv) |u| : (uv = u.next) {
            try self.markObj(&u.obj, gc);
        }
        if (fiber.result) |*r| try self.processValue(r, gc);
        if (fiber.waiting_on) |w| try self.markObj(&w.obj, gc);
        var waiter = fiber.waiters;
        while (waiter) |w| : (waiter = w.next_waiter) {
            try self.markObj(&w.obj, gc);
        }
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

test "allocator tracks bytes_allocated" {
    var gc = try GC.init(std.testing.allocator);
    defer gc.deinit();
    const alloc = gc.allocator();

    const s1 = try alloc.alloc(u8, 64);
    const s2 = try alloc.alloc(u8, 256);
    try std.testing.expectEqual(@as(usize, 320), gc.heapSize());
    alloc.free(s1);
    try std.testing.expectEqual(@as(usize, 256), gc.heapSize());
    alloc.free(s2);
    try std.testing.expectEqual(@as(usize, 0), gc.heapSize());
}

test "trackObject is idempotent" {
    var gc = try GC.init(std.testing.allocator);
    defer gc.deinit();

    const str = try obj_mod.ObjString.create(gc.allocator(), "s", null);
    gc.trackObject(&str.obj);
    gc.trackObject(&str.obj);
    try std.testing.expectEqual(@as(usize, 1), gc.object_count);
    try std.testing.expect(str.obj.next == null);
}

test "collect frees unreachable objects and keeps pinned ones" {
    var gc = try GC.init(std.testing.allocator);
    defer gc.deinit();
    const alloc = gc.allocator();

    var vm = VM.init(undefined, std.testing.allocator);
    vm.stack_top = 0;
    gc.vm = &vm;

    const live = try obj_mod.ObjString.create(alloc, "live", null);
    gc.trackObject(&live.obj);
    const lst = try ObjList.create(alloc);
    gc.trackObject(&lst.obj);
    try lst.items.append(alloc, Value.fromObj(&live.obj));
    const dead = try obj_mod.ObjString.create(alloc, "dead", null);
    gc.trackObject(&dead.obj);

    roots_mod.current = &gc.temp_roots;
    defer roots_mod.current = null;
    roots_mod.pin(Value.fromObj(&lst.obj));

    try gc.collect();
    try std.testing.expectEqual(@as(usize, 2), gc.object_count);
    try std.testing.expectEqual(@as(u64, 1), gc.objects_freed);
    try std.testing.expect(!live.obj.isMarked());

    roots_mod.restore(0);
    try gc.collect();
    try std.testing.expectEqual(@as(usize, 0), gc.object_count);
}
