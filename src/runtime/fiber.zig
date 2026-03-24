/// Fiber data structures for the concurrent runtime.
///
/// Defines ObjFiber (per-fiber VM state), FiberStack (guard-page-protected
/// native stack), FiberState, and PlatformContext.

const std = @import("std");
const Allocator = std.mem.Allocator;
const obj_mod = @import("obj");
const Obj = obj_mod.Obj;
const ObjClosure = obj_mod.ObjClosure;
const ObjUpvalue = obj_mod.ObjUpvalue;
const value_mod = @import("value");
const Value = value_mod.Value;
const chunk_mod = @import("chunk");

// ── Fiber State ────────────────────────────────────────────────────────

/// Lifecycle states for a fiber.
pub const FiberState = enum(u8) {
    /// Fiber has been created but not yet started.
    created,
    /// Fiber is currently executing on a worker thread.
    running,
    /// Fiber is ready to run (in the scheduler's run queue).
    runnable,
    /// Fiber is blocked waiting on another fiber or channel.
    waiting,
    /// Fiber has completed (successfully or via panic).
    dead,
};

// ── Fiber Stack (guard-page protected) ─────────────────────────────────

/// A native stack region allocated via mmap with a guard page at the bottom
/// to detect stack overflow via hardware fault (SIGSEGV/SIGBUS) rather
/// than silent memory corruption.
pub const FiberStack = struct {
    /// Full mmap'd region including guard page.
    memory: []align(std.heap.page_size_min) u8,
    /// Pointer to start of usable region (after guard page).
    usable: [*]u8,
    /// Usable size in bytes.
    size: usize,

    /// Allocate a new fiber stack with one guard page and one usable page.
    /// The guard page is PROT_NONE to catch overflow; the usable page is
    /// PROT_READ|PROT_WRITE.
    ///
    /// On macOS ARM64, page_size is 16KB. On x86-64 Linux, it is 4KB.
    /// We use std.heap.page_size_min for both guard and usable regions.
    pub fn alloc() !FiberStack {
        const page_size = std.heap.page_size_min;
        const total_size = page_size * 2; // 1 guard + 1 usable

        const memory = std.posix.mmap(
            null,
            total_size,
            std.posix.PROT.READ | std.posix.PROT.WRITE,
            .{ .TYPE = .PRIVATE, .ANONYMOUS = true },
            -1,
            0,
        ) catch return error.OutOfMemory;

        // Set the first page as guard (PROT_NONE).
        std.posix.mprotect(memory[0..page_size], std.posix.PROT.NONE) catch {
            std.posix.munmap(memory);
            return error.OutOfMemory;
        };

        return FiberStack{
            .memory = memory,
            .usable = @ptrCast(memory.ptr + page_size),
            .size = page_size,
        };
    }

    /// Free the entire mmap'd region (guard + usable).
    pub fn free(self: *FiberStack) void {
        std.posix.munmap(self.memory);
        self.memory = &.{};
        self.usable = undefined;
        self.size = 0;
    }
};

// ── Platform Context ───────────────────────────────────────────────────

/// Platform-specific saved register context for fiber switching.
/// The `sp` field stores the saved stack pointer that the assembly
/// `fiber_switch` routine saves/restores.
pub const PlatformContext = struct {
    sp: usize = 0,
};

// ── Constants ──────────────────────────────────────────────────────────

/// Per-fiber value stack size (number of Value slots).
/// 512 slots is adequate for stream workloads. Each Value is 8 bytes,
/// so this is 4096 bytes on all platforms.
pub const FIBER_STACK_SIZE: u32 = 512;

/// Per-fiber call frame limit. Fibers don't need the full 256 frames
/// that the main VM has; 32 is sufficient for typical fiber workloads.
pub const FIBER_FRAMES_MAX: u32 = 32;

// ── Call Frame (fiber-local) ───────────────────────────────────────────

/// Call frame for fiber-local function calls.
/// Structurally identical to the VM's CallFrame.
pub const CallFrame = struct {
    closure: *ObjClosure,
    ip: u32,
    base_slot: u32,
};

// ── ObjFiber ───────────────────────────────────────────────────────────

/// Global monotonic fiber ID counter.
var next_fiber_id: u64 = 1;

/// A fiber object: lightweight green thread with its own value stack,
/// call frames, and platform context for cooperative switching.
///
/// Embeds `Obj` as first field following the standard heap object pattern.
pub const ObjFiber = struct {
    /// GC object header (must be first field for @fieldParentPtr).
    obj: Obj,
    /// Current lifecycle state.
    state: FiberState,
    /// Per-fiber value stack.
    stack: [FIBER_STACK_SIZE]Value,
    /// Current stack top index.
    stack_top: u32,
    /// Per-fiber call frames.
    frames: [FIBER_FRAMES_MAX]CallFrame,
    /// Current frame count.
    frame_count: u32,
    /// Per-fiber open upvalue list head (VM-05 compatible).
    open_upvalues: ?*ObjUpvalue,
    /// Unique fiber ID (auto-incremented).
    id: u64,
    /// Optional debug name for this fiber.
    name: ?[]const u8,
    /// Return value when fiber completes successfully.
    result: ?Value,
    /// Panic message if fiber panicked.
    panic_message: ?[]const u8,
    /// Fiber we are blocked waiting on (for join).
    waiting_on: ?*ObjFiber,
    /// Head of intrusive linked list of fibers waiting to join us.
    waiters: ?*ObjFiber,
    /// Next pointer in the waiter linked list.
    next_waiter: ?*ObjFiber,
    /// Saved register context for switching.
    context: PlatformContext,
    /// Guard-page-protected native stack (null for main fiber which uses OS stack).
    native_stack: ?FiberStack,

    /// Allocate and initialize a new fiber for the given closure.
    ///
    /// The fiber starts in `created` state with the closure set up
    /// as the first call frame.
    pub fn create(allocator: Allocator, closure: *ObjClosure, id_override: ?u64, fiber_name: ?[]const u8) !*ObjFiber {
        const fiber = try allocator.create(ObjFiber);
        const fid = id_override orelse blk: {
            const current = next_fiber_id;
            next_fiber_id += 1;
            break :blk current;
        };

        fiber.* = ObjFiber{
            .obj = .{ .obj_type = .fiber },
            .state = .created,
            .stack = undefined,
            .stack_top = 0,
            .frames = undefined,
            .frame_count = 0,
            .open_upvalues = null,
            .id = fid,
            .name = fiber_name,
            .result = null,
            .panic_message = null,
            .waiting_on = null,
            .waiters = null,
            .next_waiter = null,
            .context = .{},
            .native_stack = null,
        };

        // Initialize the first call frame with the closure.
        // Push the closure as slot 0 (self-reference, same as VM convention).
        fiber.stack[0] = Value.fromObj(&closure.obj);
        fiber.stack_top = 1;
        fiber.frames[0] = CallFrame{
            .closure = closure,
            .ip = 0,
            .base_slot = 0,
        };
        fiber.frame_count = 1;

        return fiber;
    }

    /// Recover the containing ObjFiber from an *Obj pointer.
    pub fn fromObj(obj_ptr: *Obj) *ObjFiber {
        return @fieldParentPtr("obj", obj_ptr);
    }

    /// Free the fiber's native stack (if any) and deallocate.
    pub fn destroy(self: *ObjFiber, allocator: Allocator) void {
        if (self.native_stack) |*ns| {
            ns.free();
        }
        allocator.destroy(self);
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

test "FiberStack.alloc creates valid memory region" {
    var stack = try FiberStack.alloc();
    defer stack.free();

    const page_size = std.heap.page_size_min;

    // Total region is 2 pages.
    try std.testing.expectEqual(page_size * 2, stack.memory.len);

    // Usable region starts after the guard page.
    try std.testing.expectEqual(page_size, stack.size);

    // Usable pointer is offset by one page from the memory start.
    try std.testing.expectEqual(@intFromPtr(stack.memory.ptr) + page_size, @intFromPtr(stack.usable));
}

test "FiberStack.alloc usable region is writable" {
    var stack = try FiberStack.alloc();
    defer stack.free();

    // Write to the usable region -- should not fault.
    const usable_slice = stack.usable[0..stack.size];
    usable_slice[0] = 0xAB;
    usable_slice[stack.size - 1] = 0xCD;

    try std.testing.expectEqual(@as(u8, 0xAB), usable_slice[0]);
    try std.testing.expectEqual(@as(u8, 0xCD), usable_slice[stack.size - 1]);
}

test "FiberStack.free does not crash" {
    var stack = try FiberStack.alloc();
    // Free should not crash.
    stack.free();
}

test "ObjFiber.create initializes all fields correctly" {
    const allocator = std.testing.allocator;

    // Create a minimal closure for the fiber.
    const func = try obj_mod.ObjFunction.create(allocator);
    defer func.obj.destroy(allocator);
    const closure = try ObjClosure.create(allocator, func);
    defer closure.obj.destroy(allocator);

    const fiber = try ObjFiber.create(allocator, closure, 42, "test-fiber");
    defer fiber.destroy(allocator);

    try std.testing.expectEqual(obj_mod.ObjType.fiber, fiber.obj.obj_type);
    try std.testing.expectEqual(FiberState.created, fiber.state);
    try std.testing.expectEqual(@as(u64, 42), fiber.id);
    try std.testing.expectEqualStrings("test-fiber", fiber.name.?);
    try std.testing.expectEqual(@as(u32, 1), fiber.stack_top);
    try std.testing.expectEqual(@as(u32, 1), fiber.frame_count);
    try std.testing.expect(fiber.result == null);
    try std.testing.expect(fiber.panic_message == null);
    try std.testing.expect(fiber.waiting_on == null);
    try std.testing.expect(fiber.waiters == null);
    try std.testing.expect(fiber.next_waiter == null);
    try std.testing.expect(fiber.open_upvalues == null);
    try std.testing.expect(fiber.native_stack == null);

    // First call frame should reference the closure.
    try std.testing.expectEqual(closure, fiber.frames[0].closure);
    try std.testing.expectEqual(@as(u32, 0), fiber.frames[0].ip);
    try std.testing.expectEqual(@as(u32, 0), fiber.frames[0].base_slot);
}

test "FiberState transitions" {
    // Simple state machine test -- states are just enums so transitions
    // are assignments, but we verify the enum values are distinct.
    var state: FiberState = .created;
    try std.testing.expectEqual(FiberState.created, state);

    state = .running;
    try std.testing.expectEqual(FiberState.running, state);

    state = .runnable;
    try std.testing.expectEqual(FiberState.runnable, state);

    state = .waiting;
    try std.testing.expectEqual(FiberState.waiting, state);

    state = .dead;
    try std.testing.expectEqual(FiberState.dead, state);
}

test "ObjFiber.fromObj recovers original" {
    const allocator = std.testing.allocator;

    const func = try obj_mod.ObjFunction.create(allocator);
    defer func.obj.destroy(allocator);
    const closure = try ObjClosure.create(allocator, func);
    defer closure.obj.destroy(allocator);

    const fiber = try ObjFiber.create(allocator, closure, 99, null);
    defer fiber.destroy(allocator);

    const obj_ptr: *Obj = &fiber.obj;
    const recovered = ObjFiber.fromObj(obj_ptr);
    try std.testing.expectEqual(@as(u64, 99), recovered.id);
}

test "ObjFiber auto-increment ID" {
    const allocator = std.testing.allocator;

    const func = try obj_mod.ObjFunction.create(allocator);
    defer func.obj.destroy(allocator);
    const closure = try ObjClosure.create(allocator, func);
    defer closure.obj.destroy(allocator);

    // Create two fibers with auto-assigned IDs (no override).
    const f1 = try ObjFiber.create(allocator, closure, null, null);
    defer f1.destroy(allocator);
    const f2 = try ObjFiber.create(allocator, closure, null, null);
    defer f2.destroy(allocator);

    // IDs should be sequential.
    try std.testing.expect(f2.id == f1.id + 1);
}
