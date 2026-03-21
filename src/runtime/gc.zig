const std = @import("std");
const Allocator = std.mem.Allocator;
const Alignment = std.mem.Alignment;
const obj_mod = @import("obj");
const Obj = obj_mod.Obj;
const intern_mod = @import("intern");
const InternTable = intern_mod.InternTable;

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

    // ── Nursery config ───────────────────────────────────────────────
    nursery_capacity: usize,
    min_nursery: usize = 256 * 1024, // 256KB min
    max_nursery: usize = 16 * 1024 * 1024, // 16MB max

    // ── GC logging ───────────────────────────────────────────────────
    log_enabled: bool,

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

    /// Get total heap size (bytes currently allocated).
    pub fn heapSize(self: *const GC) usize {
        return self.bytes_allocated;
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

        // TODO(plan-02): trigger nursery collection here when
        // gc.bytes_allocated + len > gc.next_nursery_gc

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
