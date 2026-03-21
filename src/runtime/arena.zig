const std = @import("std");
const Allocator = std.mem.Allocator;
const Alignment = std.mem.Alignment;
const value_mod = @import("value");
const Value = value_mod.Value;

/// Stage arena allocator: a bump allocator that allocates from large blocks
/// and supports bulk deallocation. Used by stream pipeline stages (Phase 5)
/// for efficient temporary allocation with O(blocks) cleanup instead of
/// O(objects).
///
/// Arena objects can hold references to GC-managed objects. These references
/// are tracked in `gc_refs` and scanned as GC roots during collection,
/// ensuring that GC-managed objects referenced by arena objects survive
/// collection.
pub const StageArena = struct {
    blocks: std.ArrayListUnmanaged([]u8),
    current_block: ?[]u8 = null,
    offset: usize = 0,
    block_size: usize,
    backing: Allocator,
    /// References from arena objects to GC-managed objects.
    /// Scanned as roots during GC.
    gc_refs: std.ArrayListUnmanaged(Value),

    const DEFAULT_BLOCK_SIZE: usize = 64 * 1024; // 64KB

    pub fn init(backing: Allocator, block_size: usize) StageArena {
        return .{
            .blocks = .empty,
            .current_block = null,
            .offset = 0,
            .block_size = block_size,
            .backing = backing,
            .gc_refs = .empty,
        };
    }

    /// Return a std.mem.Allocator that bump-allocates from this arena.
    pub fn allocator(self: *StageArena) Allocator {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    /// Register a GC object reference held by an arena-allocated object.
    /// Called when arena objects store references to GC-managed objects.
    pub fn trackGCRef(self: *StageArena, val: Value) !void {
        if (val.isObj()) {
            try self.gc_refs.append(self.backing, val);
        }
    }

    /// Bulk-free all arena memory. O(blocks) not O(objects).
    pub fn freeAll(self: *StageArena) void {
        for (self.blocks.items) |block| {
            self.backing.free(block);
        }
        self.blocks.clearRetainingCapacity();
        self.current_block = null;
        self.offset = 0;
        self.gc_refs.clearRetainingCapacity();
    }

    /// Full cleanup including the blocks list itself.
    pub fn deinit(self: *StageArena) void {
        self.freeAll();
        self.blocks.deinit(self.backing);
        self.gc_refs.deinit(self.backing);
    }

    // ── VTable implementation ───────────────────────────────────────────

    const vtable = Allocator.VTable{
        .alloc = arenaAlloc,
        .resize = arenaResize,
        .remap = arenaRemap,
        .free = arenaFree,
    };

    fn arenaAlloc(ctx: *anyopaque, len: usize, alignment: Alignment, _: usize) ?[*]u8 {
        const self: *StageArena = @ptrCast(@alignCast(ctx));

        // Try to bump-allocate from the current block.
        if (self.current_block) |block| {
            if (self.tryBumpAlloc(block, len, alignment)) |ptr| {
                return ptr;
            }
        }

        // Current block doesn't fit; allocate a new one.
        const align_bytes = alignment.toByteUnits();
        const new_size = @max(self.block_size, len + align_bytes);
        const new_block = self.backing.alloc(u8, new_size) catch return null;
        self.blocks.append(self.backing, new_block) catch {
            self.backing.free(new_block);
            return null;
        };
        self.current_block = new_block;
        self.offset = 0;

        return self.tryBumpAlloc(new_block, len, alignment);
    }

    fn tryBumpAlloc(self: *StageArena, block: []u8, len: usize, alignment: Alignment) ?[*]u8 {
        const align_bytes = alignment.toByteUnits();
        const base_addr = @intFromPtr(block.ptr) + self.offset;
        const aligned_addr = std.mem.alignForward(usize, base_addr, align_bytes);
        const padding = aligned_addr - base_addr;
        const total = padding + len;

        if (self.offset + total > block.len) return null;

        self.offset += total;
        return @ptrFromInt(aligned_addr);
    }

    fn arenaResize(_: *anyopaque, _: []u8, _: Alignment, _: usize, _: usize) bool {
        // Arenas don't support individual resize.
        return false;
    }

    fn arenaRemap(_: *anyopaque, _: []u8, _: Alignment, _: usize, _: usize) ?[*]u8 {
        // Arenas don't support individual remap.
        return null;
    }

    fn arenaFree(_: *anyopaque, _: []u8, _: Alignment, _: usize) void {
        // No-op: arena bulk-frees. Individual free not supported.
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

test "StageArena bump allocation from a block" {
    var arena = StageArena.init(std.testing.allocator, 1024);
    defer arena.deinit();

    const alloc = arena.allocator();
    const slice = try alloc.alloc(u8, 64);
    try std.testing.expectEqual(@as(usize, 64), slice.len);

    // Write to verify memory is accessible.
    @memset(slice, 0xAB);
    try std.testing.expectEqual(@as(u8, 0xAB), slice[0]);
    try std.testing.expectEqual(@as(u8, 0xAB), slice[63]);
}

test "StageArena allocates new block when current is full" {
    // Small block size to force a new block allocation.
    var arena = StageArena.init(std.testing.allocator, 64);
    defer arena.deinit();

    const alloc = arena.allocator();

    // First allocation fits in the first block.
    const s1 = try alloc.alloc(u8, 32);
    try std.testing.expectEqual(@as(usize, 1), arena.blocks.items.len);

    // Second allocation exceeds remaining space, forces new block.
    const s2 = try alloc.alloc(u8, 48);
    try std.testing.expectEqual(@as(usize, 2), arena.blocks.items.len);

    // Both slices are usable.
    @memset(s1, 0x01);
    @memset(s2, 0x02);
    try std.testing.expectEqual(@as(u8, 0x01), s1[0]);
    try std.testing.expectEqual(@as(u8, 0x02), s2[0]);
}

test "StageArena freeAll deallocates all blocks" {
    var arena = StageArena.init(std.testing.allocator, 64);
    defer arena.deinit();

    const alloc = arena.allocator();
    _ = try alloc.alloc(u8, 32);
    _ = try alloc.alloc(u8, 48);
    try std.testing.expectEqual(@as(usize, 2), arena.blocks.items.len);

    arena.freeAll();
    try std.testing.expectEqual(@as(usize, 0), arena.blocks.items.len);
    try std.testing.expect(arena.current_block == null);
    try std.testing.expectEqual(@as(usize, 0), arena.offset);
}

test "StageArena trackGCRef stores object references" {
    var arena = StageArena.init(std.testing.allocator, 1024);
    defer arena.deinit();

    // Non-object values should not be tracked.
    try arena.trackGCRef(Value.fromInt(42));
    try std.testing.expectEqual(@as(usize, 0), arena.gc_refs.items.len);

    try arena.trackGCRef(Value.nil);
    try std.testing.expectEqual(@as(usize, 0), arena.gc_refs.items.len);

    // Create a real object to track.
    const obj_mod = @import("obj");
    const str = try obj_mod.ObjString.create(std.testing.allocator, "test_gc_ref", null);
    defer str.obj.destroy(std.testing.allocator);

    const obj_val = Value.fromObj(&str.obj);
    try arena.trackGCRef(obj_val);
    try std.testing.expectEqual(@as(usize, 1), arena.gc_refs.items.len);
    try std.testing.expectEqual(obj_val.bits, arena.gc_refs.items[0].bits);
}

test "StageArena allocator VTable returns aligned memory" {
    var arena = StageArena.init(std.testing.allocator, 4096);
    defer arena.deinit();

    const alloc = arena.allocator();

    // Allocate with various sizes/alignments.
    const s1 = try alloc.alloc(u64, 8);
    try std.testing.expect(@intFromPtr(s1.ptr) % @alignOf(u64) == 0);

    const s2 = try alloc.alloc(u32, 4);
    try std.testing.expect(@intFromPtr(s2.ptr) % @alignOf(u32) == 0);

    const s3 = try alloc.alloc(u8, 3);
    try std.testing.expectEqual(@as(usize, 3), s3.len);
}

test "StageArena synthetic workload: many small objects, bulk free" {
    var arena = StageArena.init(std.testing.allocator, 4096);
    defer arena.deinit();

    const alloc = arena.allocator();

    // Allocate many small objects.
    var i: usize = 0;
    while (i < 1000) : (i += 1) {
        const s = try alloc.alloc(u8, 16);
        @memset(s, @intCast(i & 0xFF));
    }

    // Verify multiple blocks were used.
    try std.testing.expect(arena.blocks.items.len > 1);

    // Bulk free.
    arena.freeAll();
    try std.testing.expectEqual(@as(usize, 0), arena.blocks.items.len);
    try std.testing.expect(arena.current_block == null);

    // Can allocate again after freeAll.
    const s = try alloc.alloc(u8, 32);
    @memset(s, 0xFF);
    try std.testing.expectEqual(@as(u8, 0xFF), s[0]);
}

test "StageArena freeAll clears gc_refs" {
    var arena = StageArena.init(std.testing.allocator, 1024);
    defer arena.deinit();

    const obj_mod = @import("obj");
    const str = try obj_mod.ObjString.create(std.testing.allocator, "ref", null);
    defer str.obj.destroy(std.testing.allocator);

    try arena.trackGCRef(Value.fromObj(&str.obj));
    try std.testing.expectEqual(@as(usize, 1), arena.gc_refs.items.len);

    arena.freeAll();
    try std.testing.expectEqual(@as(usize, 0), arena.gc_refs.items.len);
}

test "StageArena large allocation exceeding block_size" {
    var arena = StageArena.init(std.testing.allocator, 64);
    defer arena.deinit();

    const alloc = arena.allocator();

    // Request larger than block_size -- should allocate a block big enough.
    const big = try alloc.alloc(u8, 256);
    try std.testing.expectEqual(@as(usize, 256), big.len);
    @memset(big, 0xCC);
    try std.testing.expectEqual(@as(u8, 0xCC), big[0]);
}
