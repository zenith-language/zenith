const std = @import("std");
const Allocator = std.mem.Allocator;
const Alignment = std.mem.Alignment;
const obj_mod = @import("obj");
const Obj = obj_mod.Obj;
const value_mod = @import("value");
const Value = value_mod.Value;
const intern_mod = @import("intern");
const InternTable = intern_mod.InternTable;
const gc_nursery_mod = @import("gc_nursery");
const NurseryCollector = gc_nursery_mod.NurseryCollector;
const gc_oldgen_mod = @import("gc_oldgen");
const OldGenCollector = gc_oldgen_mod.OldGenCollector;
const WriteBarrier = gc_oldgen_mod.WriteBarrier;
const gc_roots_mod = @import("gc_roots");
const vm_mod = @import("vm");
const VM = vm_mod.VM;

/// Central GC state for generational garbage collection.
///
/// Manages allocation tracking, collection statistics, the string intern
/// table, and object lists by generation. The nursery uses a semi-space
/// copying algorithm (Cheney's), while the old generation uses mark-sweep.
pub const GC = struct {
    // ── Allocation tracking ──────────────────────────────────────────
    bytes_allocated: usize = 0,
    next_nursery_gc: usize,

    // ── Collection statistics ────────────────────────────────────────
    nursery_count: u64 = 0,
    oldgen_count: u64 = 0,
    total_bytes_freed: u64 = 0,
    last_pause_ns: u64 = 0,

    // ── Intern table ─────────────────────────────────────────────────
    intern_table: InternTable,

    // ── Object lists by generation ───────────────────────────────────
    nursery_objects: ?*Obj = null,
    old_objects: ?*Obj = null,

    // ── Backing allocator ────────────────────────────────────────────
    backing_allocator: Allocator,

    // ── Nursery collector ─────────────────────────────────────────────
    nursery_collector: NurseryCollector = NurseryCollector.init(),

    // ── Old-gen collector ─────────────────────────────────────────────
    oldgen_collector: OldGenCollector = OldGenCollector.init(),

    // ── Write barrier (remembered set) ────────────────────────────────
    write_barrier: WriteBarrier = WriteBarrier.init(),

    // ── Old-gen sizing ────────────────────────────────────────────────
    old_gen_size: usize = 0, // count of objects in old gen
    old_gen_threshold: usize = 2048, // trigger threshold (initial: 2048 objects)
    old_gen_last_size: usize = 0, // size after last old-gen collection

    // ── VM reference (set when VM starts execution) ──────────────────
    vm: ?*VM = null,

    // ── Nursery config ───────────────────────────────────────────────
    nursery_capacity: usize,
    min_nursery: usize = 256 * 1024, // 256KB min
    max_nursery: usize = 16 * 1024 * 1024, // 16MB max

    // ── Adaptive nursery tracking ────────────────────────────────────
    low_survival_streak: u8 = 0,

    // ── GC logging ───────────────────────────────────────────────────
    log_enabled: bool,

    // ── Collection lock (prevent re-entrant GC) ──────────────────────
    collecting: bool = false,

    const DEFAULT_NURSERY_CAPACITY: usize = 1024 * 1024; // 1MB

    pub fn init(backing: Allocator) !GC {
        const log_enabled = blk: {
            if (std.posix.getenv("ZENITH_GC_LOG")) |val| {
                break :blk std.mem.eql(u8, val, "1");
            }
            break :blk false;
        };

        return .{
            .backing_allocator = backing,
            .intern_table = try InternTable.init(backing, 256),
            .nursery_capacity = DEFAULT_NURSERY_CAPACITY,
            .next_nursery_gc = DEFAULT_NURSERY_CAPACITY,
            .log_enabled = log_enabled,
        };
    }

    pub fn deinit(self: *GC) void {
        // Free collector resources.
        self.nursery_collector.deinit(self.backing_allocator);
        self.oldgen_collector.deinit(self.backing_allocator);
        self.write_barrier.deinit(self.backing_allocator);

        // Free all nursery objects.
        var obj = self.nursery_objects;
        while (obj) |o| {
            const next = o.next;
            o.destroy(self.backing_allocator);
            obj = next;
        }
        self.nursery_objects = null;

        // Free all old-gen objects.
        obj = self.old_objects;
        while (obj) |o| {
            const next = o.next;
            o.destroy(self.backing_allocator);
            obj = next;
        }
        self.old_objects = null;

        self.intern_table.deinit();
    }

    /// Track a newly allocated object (add to nursery list).
    pub fn trackObject(self: *GC, obj: *Obj) void {
        obj.next = self.nursery_objects;
        self.nursery_objects = obj;
    }

    /// Track an object as old-gen (for long-lived compiler objects).
    pub fn trackOldObject(self: *GC, obj: *Obj) void {
        obj.promoteToOld();
        obj.next = self.old_objects;
        self.old_objects = obj;
        self.old_gen_size += 1;
    }

    /// Get total heap size (bytes currently allocated).
    pub fn heapSize(self: *const GC) usize {
        return self.bytes_allocated;
    }

    /// Run nursery collection: scan roots, promote live objects to old-gen,
    /// sweep dead nursery objects.
    pub fn collectNursery(self: *GC) !void {
        // Prevent re-entrant collection (allocation during GC).
        if (self.collecting) return;
        self.collecting = true;
        defer self.collecting = false;

        // Need a VM to scan roots.
        const vm = self.vm orelse return;

        const timer_start = std.time.nanoTimestamp();

        // Count nursery objects before collection (for survival rate).
        var nursery_before: usize = 0;
        {
            var obj = self.nursery_objects;
            while (obj) |o| : (obj = o.next) {
                nursery_before += 1;
            }
        }

        // 1. Scan roots: marks reachable nursery objects by promoting them.
        try gc_roots_mod.scanRoots(&self.nursery_collector, self, vm);

        // 1b. Scan dirty old-gen objects from write barrier for additional
        //     nursery roots (old-to-young references).
        try self.write_barrier.scanDirtyObjects(&self.nursery_collector, self);
        self.write_barrier.clear();

        // 2. Process gray stack: scan promoted objects' references.
        try self.nursery_collector.processGrayStack(self);

        // 3. Sweep intern table: remove entries for dead nursery strings.
        //    Must happen before sweepNursery destroys the strings.
        _ = self.intern_table.removeUnmarked();

        // 4. Migrate promoted objects from nursery list to old-gen list.
        //    Count how many get promoted to update old_gen_size.
        const old_gen_before = self.old_gen_size;
        NurseryCollector.migratePromoted(self);
        // Count promoted objects by counting new old_objects.
        {
            var count: usize = 0;
            var obj = self.old_objects;
            while (obj) |o| : (obj = o.next) {
                count += 1;
            }
            self.old_gen_size = count;
        }
        const promoted_to_old = self.old_gen_size - old_gen_before;
        _ = promoted_to_old;

        // 5. Sweep remaining nursery objects (unreachable).
        const freed_count = NurseryCollector.sweepNursery(self);

        // 6. Update statistics.
        self.nursery_count += 1;
        self.total_bytes_freed += freed_count;

        const timer_end = std.time.nanoTimestamp();
        self.last_pause_ns = @intCast(@as(i128, timer_end) - @as(i128, timer_start));

        // 7. Adaptive nursery sizing.
        const promoted: usize = if (nursery_before > freed_count) nursery_before - freed_count else 0;
        self.adaptNurserySize(nursery_before, promoted);

        // 8. Log if enabled.
        if (self.log_enabled) {
            self.logCollection(nursery_before, promoted, freed_count);
        }

        // 9. Clear mark bits on old-gen objects (used by intern table sweep).
        self.clearOldGenMarks();
    }

    /// Adjust nursery capacity based on survival rate.
    /// - If survival > 50%, double capacity (up to max).
    /// - If survival < 10% for 3 consecutive collections, halve (down to min).
    fn adaptNurserySize(self: *GC, total: usize, promoted: usize) void {
        if (total == 0) return;

        const survival_pct = (promoted * 100) / total;

        if (survival_pct > 50) {
            // High survival: nursery too small, double it.
            const new_cap = @min(self.nursery_capacity * 2, self.max_nursery);
            self.nursery_capacity = new_cap;
            self.next_nursery_gc = self.bytes_allocated + new_cap;
            self.low_survival_streak = 0;
        } else if (survival_pct < 10) {
            self.low_survival_streak += 1;
            if (self.low_survival_streak >= 3) {
                // Consistently low survival: nursery too large, halve it.
                const new_cap = @max(self.nursery_capacity / 2, self.min_nursery);
                self.nursery_capacity = new_cap;
                self.next_nursery_gc = self.bytes_allocated + new_cap;
                self.low_survival_streak = 0;
            } else {
                self.next_nursery_gc = self.bytes_allocated + self.nursery_capacity;
            }
        } else {
            self.low_survival_streak = 0;
            self.next_nursery_gc = self.bytes_allocated + self.nursery_capacity;
        }
    }

    /// Log collection details to stderr.
    fn logCollection(self: *const GC, total: usize, promoted: usize, freed: usize) void {
        const stderr = std.fs.File.stderr();
        var buf: [256]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "[GC] nursery #{d}: {d} total, {d} promoted, {d} freed, pause {d}ns, heap {d}B\n", .{
            self.nursery_count,
            total,
            promoted,
            freed,
            self.last_pause_ns,
            self.bytes_allocated,
        }) catch return;
        stderr.writeAll(msg) catch {};
    }

    /// Clear mark bits on all old-gen objects after collection.
    fn clearOldGenMarks(self: *GC) void {
        var obj = self.old_objects;
        while (obj) |o| : (obj = o.next) {
            o.setMarked(false);
        }
    }

    // ── Old-gen collection ─────────────────────────────────────────────

    /// Run old-gen mark-sweep collection.
    /// Orchestrates: mark -> sweep intern table -> sweep old gen.
    pub fn collectOldGen(self: *GC) !void {
        // Prevent re-entrant collection.
        if (self.collecting) return;
        self.collecting = true;
        defer self.collecting = false;

        const vm = self.vm orelse return;

        const timer_start = std.time.nanoTimestamp();

        // Run old-gen mark-sweep (handles mark, intern sweep, old-gen sweep).
        const freed = try self.oldgen_collector.collect(self, vm);

        // Update statistics.
        self.oldgen_count += 1;
        self.total_bytes_freed += freed;

        const timer_end = std.time.nanoTimestamp();
        self.last_pause_ns = @intCast(@as(i128, timer_end) - @as(i128, timer_start));

        // Update threshold: 2x post-collection size (per user decision).
        self.old_gen_last_size = self.old_gen_size;
        self.old_gen_threshold = if (self.old_gen_size > 0) self.old_gen_size * 2 else 2048;

        // Log if enabled.
        if (self.log_enabled) {
            self.logOldGenCollection(freed);
        }
    }

    /// Run a full collection: nursery first, then old-gen.
    pub fn collectFull(self: *GC) !void {
        try self.collectNursery();
        try self.collectOldGen();
    }

    /// Write barrier: called when storing a reference value into a container object.
    /// If the container is old-gen and the value references a nursery object,
    /// records the container in the remembered set so nursery collection
    /// will scan it for additional roots.
    ///
    /// Write barrier sites in the VM (as of Phase 4):
    /// - closeUpvalues: copies stack value into uv.closed
    /// - op_set_upvalue: stores value into upvalue location
    ///
    /// Note: All Zenith collections are immutable (per design decision).
    /// List.append, Map.set, etc. return NEW collections, never mutate.
    /// Therefore no write barriers are needed in builtins.
    /// If mutable collection operations are added in the future,
    /// write barriers must be inserted at those sites.
    pub fn writeBarrier(self: *GC, container: *Obj, val: Value) void {
        // Only needed if container is old-gen and value is a nursery object.
        if (!container.isOldGen()) return;
        if (!val.isObj()) return;
        const ref_obj = val.asObj();
        if (ref_obj.isOldGen()) return;

        // Container is old-gen, value is a nursery object reference.
        self.write_barrier.recordStore(container, self.backing_allocator) catch {};
    }

    /// Check if old-gen size exceeds threshold and trigger collection if so.
    pub fn checkOldGenThreshold(self: *GC) void {
        if (self.old_gen_size > self.old_gen_threshold) {
            self.collectOldGen() catch {};
        }
    }

    /// Log old-gen collection details to stderr.
    fn logOldGenCollection(self: *const GC, freed: usize) void {
        const stderr = std.fs.File.stderr();
        var buf: [256]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "[GC] oldgen #{d}: {d} freed, old_size {d}, threshold {d}, pause {d}ns\n", .{
            self.oldgen_count,
            freed,
            self.old_gen_size,
            self.old_gen_threshold,
            self.last_pause_ns,
        }) catch return;
        stderr.writeAll(msg) catch {};
    }
};

/// GC-aware allocator that wraps a backing allocator and tracks
/// allocation metrics. Implements the std.mem.Allocator VTable so it
/// can be used transparently throughout the runtime.
pub const GCAllocator = struct {
    gc: *GC,

    pub fn allocator(self: *GCAllocator) Allocator {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    const vtable = Allocator.VTable{
        .alloc = gcAlloc,
        .resize = gcResize,
        .remap = gcRemap,
        .free = gcFree,
    };

    fn gcAlloc(ctx: *anyopaque, len: usize, alignment: Alignment, ret_addr: usize) ?[*]u8 {
        const self: *GCAllocator = @ptrCast(@alignCast(ctx));
        const gc = self.gc;

        // Trigger nursery collection if threshold exceeded.
        if (gc.bytes_allocated + len > gc.next_nursery_gc) {
            gc.collectNursery() catch {};
        }

        const result = gc.backing_allocator.vtable.alloc(gc.backing_allocator.ptr, len, alignment, ret_addr);
        if (result != null) {
            gc.bytes_allocated += len;
        }
        return result;
    }

    fn gcResize(ctx: *anyopaque, memory: []u8, alignment: Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *GCAllocator = @ptrCast(@alignCast(ctx));
        const gc = self.gc;

        const old_len = memory.len;
        const ok = gc.backing_allocator.vtable.resize(gc.backing_allocator.ptr, memory, alignment, new_len, ret_addr);
        if (ok) {
            if (new_len > old_len) {
                gc.bytes_allocated += (new_len - old_len);
            } else {
                gc.bytes_allocated -= (old_len - new_len);
            }
        }
        return ok;
    }

    fn gcRemap(ctx: *anyopaque, memory: []u8, alignment: Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *GCAllocator = @ptrCast(@alignCast(ctx));
        const gc = self.gc;

        const old_len = memory.len;
        const result = gc.backing_allocator.vtable.remap(gc.backing_allocator.ptr, memory, alignment, new_len, ret_addr);
        if (result != null) {
            if (new_len > old_len) {
                gc.bytes_allocated += (new_len - old_len);
            } else {
                gc.bytes_allocated -= (old_len - new_len);
            }
        }
        return result;
    }

    fn gcFree(ctx: *anyopaque, memory: []u8, alignment: Alignment, ret_addr: usize) void {
        const self: *GCAllocator = @ptrCast(@alignCast(ctx));
        const gc = self.gc;

        gc.bytes_allocated -= memory.len;
        gc.backing_allocator.vtable.free(gc.backing_allocator.ptr, memory, alignment, ret_addr);
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

test "GC init/deinit lifecycle" {
    const backing = std.testing.allocator;
    var gc = try GC.init(backing);
    defer gc.deinit();

    try std.testing.expectEqual(@as(usize, 0), gc.bytes_allocated);
    try std.testing.expectEqual(@as(u64, 0), gc.nursery_count);
    try std.testing.expectEqual(@as(u64, 0), gc.oldgen_count);
    try std.testing.expect(gc.nursery_objects == null);
    try std.testing.expect(gc.old_objects == null);
    try std.testing.expectEqual(GC.DEFAULT_NURSERY_CAPACITY, gc.nursery_capacity);
}

test "GCAllocator tracks bytes_allocated correctly" {
    const backing = std.testing.allocator;
    var gc = try GC.init(backing);
    defer gc.deinit();

    var gc_alloc = GCAllocator{ .gc = &gc };
    const alloc = gc_alloc.allocator();

    // Allocate some memory.
    const slice = try alloc.alloc(u8, 128);
    try std.testing.expectEqual(@as(usize, 128), gc.bytes_allocated);

    // Free it -- bytes_allocated should return to 0.
    alloc.free(slice);
    try std.testing.expectEqual(@as(usize, 0), gc.bytes_allocated);
}

test "GCAllocator tracks multiple allocations" {
    const backing = std.testing.allocator;
    var gc = try GC.init(backing);
    defer gc.deinit();

    var gc_alloc = GCAllocator{ .gc = &gc };
    const alloc = gc_alloc.allocator();

    const s1 = try alloc.alloc(u8, 64);
    const s2 = try alloc.alloc(u8, 256);
    try std.testing.expectEqual(@as(usize, 320), gc.bytes_allocated);

    alloc.free(s1);
    try std.testing.expectEqual(@as(usize, 256), gc.bytes_allocated);

    alloc.free(s2);
    try std.testing.expectEqual(@as(usize, 0), gc.bytes_allocated);
}

test "trackObject adds to nursery_objects list" {
    const backing = std.testing.allocator;
    var gc = try GC.init(backing);
    defer gc.deinit();

    var gc_alloc = GCAllocator{ .gc = &gc };
    const alloc = gc_alloc.allocator();

    // Create objects using the GC allocator.
    const obj_mod_local = @import("obj");
    const str1 = try obj_mod_local.ObjString.create(alloc, "first", null);
    gc.trackObject(&str1.obj);
    const str2 = try obj_mod_local.ObjString.create(alloc, "second", null);
    gc.trackObject(&str2.obj);

    // Verify linked list: str2 -> str1 -> null (last tracked is head).
    try std.testing.expectEqual(&str2.obj, gc.nursery_objects.?);
    try std.testing.expectEqual(&str1.obj, str2.obj.next.?);
    try std.testing.expect(str1.obj.next == null);
}

test "heapSize reports bytes_allocated" {
    const backing = std.testing.allocator;
    var gc = try GC.init(backing);
    defer gc.deinit();

    try std.testing.expectEqual(@as(usize, 0), gc.heapSize());

    var gc_alloc = GCAllocator{ .gc = &gc };
    const alloc = gc_alloc.allocator();

    const s = try alloc.alloc(u8, 512);
    try std.testing.expectEqual(@as(usize, 512), gc.heapSize());

    alloc.free(s);
    try std.testing.expectEqual(@as(usize, 0), gc.heapSize());
}
