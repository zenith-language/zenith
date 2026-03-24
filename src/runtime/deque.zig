/// Lock-free Chase-Lev work-stealing deque.
///
/// Implements the algorithm from Le et al. "Correct and Efficient
/// Work-Stealing for Weak Memory Models" with appropriate memory
/// orderings for ARM64/x86-64 correctness.
///
/// In Zig 0.15.2, standalone fence instructions are not available.
/// Instead, the required ordering constraints are encoded directly
/// into the atomic load/store operations:
/// - push: `release` store of bottom replaces release-fence + monotonic-store
/// - pop: `seq_cst` store of bottom replaces monotonic-store + seq_cst-fence
/// - steal: `seq_cst` load of top replaces acquire-load + seq_cst-fence
///
/// The owner thread pushes and pops from the bottom (LIFO).
/// Stealer threads steal from the top (FIFO).

const std = @import("std");

/// Fixed-capacity power-of-two Chase-Lev work-stealing deque.
///
/// `T` is the element type stored in the deque.
/// Capacity is fixed at initialization to a power-of-two value.
pub fn ChaseLevDeque(comptime T: type) type {
    return struct {
        const Self = @This();

        /// Default capacity: 1024 entries (ample for fiber count).
        pub const DEFAULT_CAPACITY: usize = 1024;

        /// Storage buffer using atomic values for safe concurrent access.
        buffer: []std.atomic.Value(T),
        /// Capacity of the buffer (must be power of two).
        capacity: usize,
        /// Bitmask for fast modulo (capacity - 1).
        mask: usize,
        /// Top index -- stealers CAS this (FIFO end).
        top: std.atomic.Value(i64),
        /// Bottom index -- owner only (LIFO end).
        bottom: std.atomic.Value(i64),

        /// Initialize a deque with the given capacity (must be power of two).
        /// Uses the provided allocator for the internal buffer.
        pub fn init(allocator: std.mem.Allocator, capacity: usize) !Self {
            std.debug.assert(capacity > 0 and (capacity & (capacity - 1)) == 0); // power of two
            const buffer = try allocator.alloc(std.atomic.Value(T), capacity);
            return Self{
                .buffer = buffer,
                .capacity = capacity,
                .mask = capacity - 1,
                .top = std.atomic.Value(i64).init(0),
                .bottom = std.atomic.Value(i64).init(0),
            };
        }

        /// Free the internal buffer.
        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            allocator.free(self.buffer);
        }

        /// Owner pushes an item to the bottom of the deque.
        /// Panics if the deque is full (for v1, 1024 entries is ample).
        pub fn push(self: *Self, item: T) void {
            const b = self.bottom.load(.monotonic);
            const t = self.top.load(.acquire);

            // Check capacity.
            const size = b - t;
            if (size >= @as(i64, @intCast(self.capacity))) {
                @panic("ChaseLevDeque overflow: capacity exceeded");
            }

            // Store item at bottom position.
            self.buffer[@intCast(@as(u64, @bitCast(b)) & self.mask)].store(item, .unordered);

            // Release store ensures the item store above is visible before
            // the bottom increment is visible to stealers.
            self.bottom.store(b + 1, .release);
        }

        /// Owner pops an item from the bottom of the deque (LIFO).
        /// Returns null if the deque is empty.
        pub fn pop(self: *Self) ?T {
            // Decrement bottom with seq_cst to create the required
            // ordering between the bottom store and the top load.
            const b = self.bottom.load(.monotonic) - 1;
            self.bottom.store(b, .seq_cst);

            const t = self.top.load(.seq_cst);

            if (t <= b) {
                // Non-empty: at least one element.
                const item = self.buffer[@intCast(@as(u64, @bitCast(b)) & self.mask)].load(.unordered);

                if (t == b) {
                    // Last element -- race with stealers.
                    // Try to claim it via CAS on top.
                    if (self.top.cmpxchgStrong(t, t + 1, .seq_cst, .monotonic) != null) {
                        // Lost the race -- stealer got it.
                        self.bottom.store(t + 1, .monotonic);
                        return null;
                    }
                    self.bottom.store(t + 1, .monotonic);
                }

                return item;
            } else {
                // Empty deque.
                self.bottom.store(t, .monotonic);
                return null;
            }
        }

        /// Stealer takes an item from the top of the deque (FIFO).
        /// Returns null if the deque is empty or if CAS failed (retry externally).
        pub fn steal(self: *Self) ?T {
            // seq_cst load of top ensures ordering with the bottom load.
            const t = self.top.load(.seq_cst);
            const b = self.bottom.load(.acquire);

            if (t >= b) {
                // Empty.
                return null;
            }

            // Load the item at top.
            const item = self.buffer[@intCast(@as(u64, @bitCast(t)) & self.mask)].load(.unordered);

            // Try to advance top via CAS.
            if (self.top.cmpxchgStrong(t, t + 1, .seq_cst, .monotonic) != null) {
                // CAS failed -- another stealer got it. Return null (caller retries).
                return null;
            }

            return item;
        }

        /// Returns the approximate number of items in the deque.
        /// This is not exact under concurrent access -- use for debugging only.
        pub fn len(self: *const Self) usize {
            const b = self.bottom.load(.monotonic);
            const t = self.top.load(.monotonic);
            if (b > t) {
                return @intCast(b - t);
            }
            return 0;
        }
    };
}

// ── Tests ──────────────────────────────────────────────────────────────

test "single-threaded push/pop LIFO ordering" {
    const Deque = ChaseLevDeque(u32);
    var deque = try Deque.init(std.testing.allocator, Deque.DEFAULT_CAPACITY);
    defer deque.deinit(std.testing.allocator);

    deque.push(1);
    deque.push(2);
    deque.push(3);

    // Pop returns LIFO order: 3, 2, 1.
    try std.testing.expectEqual(@as(u32, 3), deque.pop().?);
    try std.testing.expectEqual(@as(u32, 2), deque.pop().?);
    try std.testing.expectEqual(@as(u32, 1), deque.pop().?);
}

test "push until full, then pop all" {
    const Deque = ChaseLevDeque(u32);
    const cap: usize = 16;
    var deque = try Deque.init(std.testing.allocator, cap);
    defer deque.deinit(std.testing.allocator);

    // Fill to capacity.
    for (0..cap) |i| {
        deque.push(@intCast(i));
    }

    try std.testing.expectEqual(cap, deque.len());

    // Pop all in reverse order.
    var i: u32 = @intCast(cap);
    while (i > 0) {
        i -= 1;
        try std.testing.expectEqual(i, deque.pop().?);
    }

    try std.testing.expectEqual(@as(usize, 0), deque.len());
}

test "steal returns FIFO order" {
    const Deque = ChaseLevDeque(u32);
    var deque = try Deque.init(std.testing.allocator, Deque.DEFAULT_CAPACITY);
    defer deque.deinit(std.testing.allocator);

    deque.push(10);
    deque.push(20);
    deque.push(30);

    // Steal returns FIFO order: 10, 20, 30.
    try std.testing.expectEqual(@as(u32, 10), deque.steal().?);
    try std.testing.expectEqual(@as(u32, 20), deque.steal().?);
    try std.testing.expectEqual(@as(u32, 30), deque.steal().?);
}

test "empty deque returns null for pop and steal" {
    const Deque = ChaseLevDeque(u32);
    var deque = try Deque.init(std.testing.allocator, Deque.DEFAULT_CAPACITY);
    defer deque.deinit(std.testing.allocator);

    try std.testing.expect(deque.pop() == null);
    try std.testing.expect(deque.steal() == null);
}

test "interleaved push/pop/steal correctness" {
    const Deque = ChaseLevDeque(u32);
    var deque = try Deque.init(std.testing.allocator, Deque.DEFAULT_CAPACITY);
    defer deque.deinit(std.testing.allocator);

    // Push 1, 2, 3.
    deque.push(1);
    deque.push(2);
    deque.push(3);

    // Steal from top: gets 1.
    try std.testing.expectEqual(@as(u32, 1), deque.steal().?);

    // Push 4.
    deque.push(4);

    // Pop from bottom: gets 4 (last pushed).
    try std.testing.expectEqual(@as(u32, 4), deque.pop().?);

    // Remaining: 2, 3. Steal gets 2.
    try std.testing.expectEqual(@as(u32, 2), deque.steal().?);

    // Pop gets 3.
    try std.testing.expectEqual(@as(u32, 3), deque.pop().?);

    // Empty now.
    try std.testing.expect(deque.pop() == null);
    try std.testing.expect(deque.steal() == null);
}

test "len reflects push and pop" {
    const Deque = ChaseLevDeque(u32);
    var deque = try Deque.init(std.testing.allocator, Deque.DEFAULT_CAPACITY);
    defer deque.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), deque.len());

    deque.push(1);
    try std.testing.expectEqual(@as(usize, 1), deque.len());

    deque.push(2);
    try std.testing.expectEqual(@as(usize, 2), deque.len());

    _ = deque.pop();
    try std.testing.expectEqual(@as(usize, 1), deque.len());

    _ = deque.steal();
    try std.testing.expectEqual(@as(usize, 0), deque.len());
}

test "single element race between pop and steal" {
    const Deque = ChaseLevDeque(u32);
    var deque = try Deque.init(std.testing.allocator, Deque.DEFAULT_CAPACITY);
    defer deque.deinit(std.testing.allocator);

    deque.push(42);

    // Pop should succeed (owner has priority with CAS).
    const result = deque.pop();
    try std.testing.expectEqual(@as(u32, 42), result.?);

    // Deque should be empty.
    try std.testing.expect(deque.steal() == null);
}
