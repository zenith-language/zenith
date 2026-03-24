/// Channel data structure for inter-fiber communication.
///
/// Supports both buffered (ring buffer) and unbuffered (rendezvous) modes.
/// Channels are the primary communication primitive between fibers.
///
/// - Buffered: chan(N) creates a channel with N-slot ring buffer.
/// - Unbuffered: chan() creates a rendezvous channel (direct handoff).
///
/// Channels use per-channel mutexes (not lock-free) because channel operations
/// are infrequent relative to computation.

const std = @import("std");
const Allocator = std.mem.Allocator;
const obj_mod = @import("obj");
const Obj = obj_mod.Obj;
const value_mod = @import("value");
const Value = value_mod.Value;
const fiber_mod = @import("fiber");
const ObjFiber = fiber_mod.ObjFiber;

// ── Fiber Queue ────────────────────────────────────────────────────────

/// Intrusive linked list of parked fibers.
/// Used for senders/receivers waiting on a channel.
/// Links through ObjFiber.next_waiter field.
pub const FiberQueue = struct {
    head: ?*ObjFiber = null,
    tail: ?*ObjFiber = null,

    /// Add a fiber to the back of the queue.
    pub fn push(self: *FiberQueue, fiber: *ObjFiber) void {
        fiber.next_waiter = null;
        if (self.tail) |t| {
            t.next_waiter = fiber;
        } else {
            self.head = fiber;
        }
        self.tail = fiber;
    }

    /// Remove and return the fiber at the front of the queue.
    pub fn pop(self: *FiberQueue) ?*ObjFiber {
        const fiber = self.head orelse return null;
        self.head = fiber.next_waiter;
        if (self.head == null) {
            self.tail = null;
        }
        fiber.next_waiter = null;
        return fiber;
    }

    /// Check if the queue is empty.
    pub fn isEmpty(self: *const FiberQueue) bool {
        return self.head == null;
    }
};

// ── Channel Result Types ───────────────────────────────────────────────

/// Result of a send operation.
pub const SendResult = enum {
    /// Value was sent successfully (buffered: space available, unbuffered: receiver took it).
    sent,
    /// Channel is closed; cannot send.
    closed,
    /// Channel is full (buffered) or no receiver available (unbuffered); caller should park.
    would_block,
};

/// Result of a recv operation.
pub const RecvResult = union(enum) {
    /// Received a value.
    value: Value,
    /// Channel is closed and empty; no more values.
    closed,
    /// Channel is empty and open; caller should park.
    would_block,
};

// ── ObjChannel ─────────────────────────────────────────────────────────

/// Heap-allocated channel object for inter-fiber communication.
///
/// Embeds `Obj` as first field following the standard heap object pattern.
pub const ObjChannel = struct {
    /// GC object header (must be first field for @fieldParentPtr).
    obj: Obj,
    /// Ring buffer for buffered channels; null for unbuffered.
    buffer: ?[]Value,
    /// Maximum number of items in buffer. 0 for unbuffered.
    capacity: u32,
    /// Read position in ring buffer.
    head: u32,
    /// Write position in ring buffer.
    tail: u32,
    /// Current number of items in buffer.
    count: u32,
    /// Whether the channel has been closed.
    closed: bool,
    /// Per-channel mutex for synchronization.
    mutex: std.Thread.Mutex,
    /// Fibers blocked on send (channel full or unbuffered with no receiver).
    senders: FiberQueue,
    /// Fibers blocked on recv (channel empty or unbuffered with no sender).
    receivers: FiberQueue,
    /// Direct handoff slot for unbuffered channels.
    handoff: ?Value,

    /// Create a new channel with the given capacity.
    /// capacity > 0: buffered channel with ring buffer.
    /// capacity == 0: unbuffered (rendezvous) channel.
    pub fn create(allocator: Allocator, capacity: u32) !*ObjChannel {
        const buffer = if (capacity > 0)
            try allocator.alloc(Value, capacity)
        else
            null;

        const ch = try allocator.create(ObjChannel);
        ch.* = .{
            .obj = .{ .obj_type = .channel },
            .buffer = buffer,
            .capacity = capacity,
            .head = 0,
            .tail = 0,
            .count = 0,
            .closed = false,
            .mutex = .{},
            .senders = .{},
            .receivers = .{},
            .handoff = null,
        };
        return ch;
    }

    /// Attempt to send a value on the channel.
    ///
    /// For buffered channels:
    /// - If closed: returns .closed
    /// - If space available: writes to buffer, wakes a waiting receiver if any, returns .sent
    /// - If full: returns .would_block (caller should park)
    ///
    /// For unbuffered channels:
    /// - If closed: returns .closed
    /// - If receiver waiting: direct handoff, returns .sent
    /// - If no receiver: returns .would_block (caller should park after storing handoff)
    pub fn send(self: *ObjChannel, val: Value) SendResult {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.closed) return .closed;

        if (self.capacity > 0) {
            // Buffered mode.
            if (self.count < self.capacity) {
                // Space available: write to ring buffer.
                self.buffer.?[self.tail] = val;
                self.tail = (self.tail + 1) % self.capacity;
                self.count += 1;
                return .sent;
            }
            // Buffer full: caller should park.
            return .would_block;
        } else {
            // Unbuffered mode.
            if (!self.receivers.isEmpty()) {
                // Receiver waiting: direct handoff.
                self.handoff = val;
                return .sent;
            }
            // No receiver: store handoff and caller should park.
            self.handoff = val;
            return .would_block;
        }
    }

    /// Attempt to receive a value from the channel.
    ///
    /// For buffered channels:
    /// - If items available: reads from buffer, returns .{.value = val}
    /// - If empty and closed: returns .closed
    /// - If empty and open: returns .would_block (caller should park)
    ///
    /// For unbuffered channels:
    /// - If sender waiting with handoff: takes handoff, returns .{.value = val}
    /// - If no sender and closed: returns .closed
    /// - If no sender and open: returns .would_block (caller should park)
    pub fn recv(self: *ObjChannel) RecvResult {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.capacity > 0) {
            // Buffered mode.
            if (self.count > 0) {
                const val = self.buffer.?[self.head];
                self.head = (self.head + 1) % self.capacity;
                self.count -= 1;
                return .{ .value = val };
            }
            if (self.closed) return .closed;
            return .would_block;
        } else {
            // Unbuffered mode.
            if (self.handoff) |val| {
                self.handoff = null;
                return .{ .value = val };
            }
            if (self.closed) return .closed;
            return .would_block;
        }
    }

    /// Close the channel.
    ///
    /// Sets closed flag. After closing:
    /// - No more sends are allowed (returns .closed)
    /// - Existing buffered values can still be received
    /// - Once empty, recv returns .closed
    ///
    /// Returns lists of waiting fibers that need to be unparked.
    /// The caller is responsible for actually unparking them.
    pub fn close(self: *ObjChannel) struct { senders: FiberQueue, receivers: FiberQueue } {
        self.mutex.lock();
        defer self.mutex.unlock();

        self.closed = true;

        // Collect all waiting fibers to be unparked after unlocking.
        const waiting_senders = self.senders;
        const waiting_receivers = self.receivers;
        self.senders = .{};
        self.receivers = .{};

        return .{
            .senders = waiting_senders,
            .receivers = waiting_receivers,
        };
    }

    /// Recover the containing ObjChannel from an *Obj pointer.
    pub fn fromObj(obj_ptr: *Obj) *ObjChannel {
        return @fieldParentPtr("obj", obj_ptr);
    }

    /// Free the channel's buffer (if any) and deallocate.
    pub fn destroy(self: *ObjChannel, allocator: Allocator) void {
        if (self.buffer) |buf| {
            allocator.free(buf);
        }
        allocator.destroy(self);
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

test "FiberQueue push and pop" {
    const allocator = std.testing.allocator;
    const func = try obj_mod.ObjFunction.create(allocator);
    defer func.obj.destroy(allocator);
    const closure = try obj_mod.ObjClosure.create(allocator, func);
    defer closure.obj.destroy(allocator);

    const f1 = try ObjFiber.create(allocator, closure, 1, "f1");
    defer f1.destroy(allocator);
    const f2 = try ObjFiber.create(allocator, closure, 2, "f2");
    defer f2.destroy(allocator);

    var q = FiberQueue{};
    try std.testing.expect(q.isEmpty());

    q.push(f1);
    try std.testing.expect(!q.isEmpty());

    q.push(f2);
    try std.testing.expect(!q.isEmpty());

    // FIFO order.
    const popped1 = q.pop();
    try std.testing.expect(popped1 != null);
    try std.testing.expectEqual(@as(u64, 1), popped1.?.id);

    const popped2 = q.pop();
    try std.testing.expect(popped2 != null);
    try std.testing.expectEqual(@as(u64, 2), popped2.?.id);

    try std.testing.expect(q.isEmpty());
    try std.testing.expect(q.pop() == null);
}

test "ObjChannel buffered: send then recv" {
    const allocator = std.testing.allocator;
    const ch = try ObjChannel.create(allocator, 4);
    defer ch.destroy(allocator);

    // Send values.
    try std.testing.expectEqual(SendResult.sent, ch.send(Value.fromInt(10)));
    try std.testing.expectEqual(SendResult.sent, ch.send(Value.fromInt(20)));
    try std.testing.expectEqual(SendResult.sent, ch.send(Value.fromInt(30)));

    // Recv values (FIFO order).
    const r1 = ch.recv();
    try std.testing.expectEqual(@as(i32, 10), r1.value.asInt());
    const r2 = ch.recv();
    try std.testing.expectEqual(@as(i32, 20), r2.value.asInt());
    const r3 = ch.recv();
    try std.testing.expectEqual(@as(i32, 30), r3.value.asInt());

    // Empty channel, not closed.
    try std.testing.expectEqual(RecvResult.would_block, ch.recv());
}

test "ObjChannel buffered: ring buffer wraparound" {
    const allocator = std.testing.allocator;
    const ch = try ObjChannel.create(allocator, 2);
    defer ch.destroy(allocator);

    // Fill buffer.
    try std.testing.expectEqual(SendResult.sent, ch.send(Value.fromInt(1)));
    try std.testing.expectEqual(SendResult.sent, ch.send(Value.fromInt(2)));
    // Full.
    try std.testing.expectEqual(SendResult.would_block, ch.send(Value.fromInt(3)));

    // Recv one to make space.
    try std.testing.expectEqual(@as(i32, 1), ch.recv().value.asInt());

    // Send wraps around.
    try std.testing.expectEqual(SendResult.sent, ch.send(Value.fromInt(4)));

    // Recv remaining in order.
    try std.testing.expectEqual(@as(i32, 2), ch.recv().value.asInt());
    try std.testing.expectEqual(@as(i32, 4), ch.recv().value.asInt());
}

test "ObjChannel close: recv after close returns closed when empty" {
    const allocator = std.testing.allocator;
    const ch = try ObjChannel.create(allocator, 4);
    defer ch.destroy(allocator);

    // Send a value and close.
    try std.testing.expectEqual(SendResult.sent, ch.send(Value.fromInt(42)));
    _ = ch.close();

    // Can still recv buffered value.
    try std.testing.expectEqual(@as(i32, 42), ch.recv().value.asInt());

    // Now empty and closed.
    try std.testing.expectEqual(RecvResult.closed, ch.recv());
}

test "ObjChannel close: send after close returns closed" {
    const allocator = std.testing.allocator;
    const ch = try ObjChannel.create(allocator, 4);
    defer ch.destroy(allocator);

    _ = ch.close();
    try std.testing.expectEqual(SendResult.closed, ch.send(Value.fromInt(1)));
}

test "ObjChannel unbuffered: no receiver returns would_block" {
    const allocator = std.testing.allocator;
    const ch = try ObjChannel.create(allocator, 0);
    defer ch.destroy(allocator);

    // No receiver available.
    try std.testing.expectEqual(SendResult.would_block, ch.send(Value.fromInt(1)));
}

test "ObjChannel unbuffered: close returns closed on recv" {
    const allocator = std.testing.allocator;
    const ch = try ObjChannel.create(allocator, 0);
    defer ch.destroy(allocator);

    _ = ch.close();
    try std.testing.expectEqual(RecvResult.closed, ch.recv());
}

test "ObjChannel fromObj recovers original" {
    const allocator = std.testing.allocator;
    const ch = try ObjChannel.create(allocator, 4);
    defer ch.destroy(allocator);

    const obj_ptr: *Obj = &ch.obj;
    const recovered = ObjChannel.fromObj(obj_ptr);
    try std.testing.expectEqual(@as(u32, 4), recovered.capacity);
    try std.testing.expect(!recovered.closed);
}

test "ObjChannel create and destroy lifecycle" {
    const allocator = std.testing.allocator;
    // Buffered.
    const ch1 = try ObjChannel.create(allocator, 8);
    ch1.destroy(allocator);
    // Unbuffered.
    const ch2 = try ObjChannel.create(allocator, 0);
    ch2.destroy(allocator);
}
