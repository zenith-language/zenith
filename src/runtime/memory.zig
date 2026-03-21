const std = @import("std");
const builtin = @import("builtin");
const gc_mod = @import("gc");
const GC = gc_mod.GC;
const GCAllocator = gc_mod.GCAllocator;

/// Create the allocator appropriate for the current build mode.
///
/// - In Debug and test builds: wraps `std.heap.page_allocator` with a
///   `DebugAllocator` that catches leaks, use-after-free, and double-free.
/// - In release builds: returns `std.heap.page_allocator` directly.
///
/// This is the legacy allocator for backwards compatibility during
/// the transition to GC-aware allocation. New code should prefer
/// `createGC()` which routes through the GCAllocator for byte tracking.
pub fn create() Allocator {
    if (builtin.mode == .Debug or builtin.is_test) {
        return debug_allocator.allocator();
    }
    return std.heap.page_allocator;
}

/// Return a GC-aware allocator backed by the given GC state.
/// The returned allocator tracks bytes_allocated and will eventually
/// trigger nursery collection when the threshold is reached.
pub fn createGC(gc_alloc: *GCAllocator) Allocator {
    return gc_alloc.allocator();
}

var debug_allocator: std.heap.DebugAllocator(.{}) = .init;

const Allocator = std.mem.Allocator;

// ── Tests ──────────────────────────────────────────────────────────────

test "create returns a usable allocator" {
    const allocator = create();
    const slice = try allocator.alloc(u8, 64);
    defer allocator.free(slice);
    try std.testing.expectEqual(@as(usize, 64), slice.len);
}

test "allocator can allocate and free without leaks" {
    // Use std.testing.allocator which is itself a DebugAllocator.
    const allocator = std.testing.allocator;
    var list: std.ArrayListUnmanaged(u32) = .empty;
    defer list.deinit(allocator);
    try list.append(allocator, 42);
    try list.append(allocator, 99);
    try std.testing.expectEqual(@as(u32, 42), list.items[0]);
    try std.testing.expectEqual(@as(u32, 99), list.items[1]);
}
