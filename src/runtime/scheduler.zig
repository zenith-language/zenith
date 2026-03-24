/// M:N fiber scheduler with work-stealing and safepoint protocol.
///
/// Distributes runnable fibers across OS worker threads. Each worker
/// has a local Chase-Lev deque for LIFO cache-friendly scheduling.
/// When a worker runs out of local work, it tries to steal from other
/// workers (FIFO) or pull from the global overflow queue.
///
/// GC coordination uses a safepoint protocol: the GC thread sets
/// `safepoint_requested`, workers check it at loop top and park until
/// the GC finishes.

const std = @import("std");
const Allocator = std.mem.Allocator;
const deque_mod = @import("deque");
const ChaseLevDeque = deque_mod.ChaseLevDeque;
const fiber_mod = @import("fiber");
const ObjFiber = fiber_mod.ObjFiber;

// ── Global Queue ────────────────────────────────────────────────────

/// Mutex-protected FIFO queue for overflow and cross-thread scheduling.
pub const GlobalQueue = struct {
    items: std.ArrayListUnmanaged(*ObjFiber),
    mutex: std.Thread.Mutex,

    pub fn init() GlobalQueue {
        return .{
            .items = .empty,
            .mutex = .{},
        };
    }

    pub fn deinit(self: *GlobalQueue, allocator: Allocator) void {
        self.items.deinit(allocator);
    }

    pub fn push(self: *GlobalQueue, fiber: *ObjFiber, allocator: Allocator) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.items.append(allocator, fiber) catch {};
    }

    pub fn pop(self: *GlobalQueue) ?*ObjFiber {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.items.items.len == 0) return null;
        // FIFO: remove from front.
        const fiber = self.items.items[0];
        // Shift remaining items down.
        if (self.items.items.len > 1) {
            std.mem.copyForwards(
                *ObjFiber,
                self.items.items[0 .. self.items.items.len - 1],
                self.items.items[1..self.items.items.len],
            );
        }
        self.items.items.len -= 1;
        return fiber;
    }

    pub fn pushBatch(self: *GlobalQueue, fibers: []*ObjFiber, allocator: Allocator) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        for (fibers) |f| {
            self.items.append(allocator, f) catch {};
        }
    }

    pub fn len(self: *const GlobalQueue) usize {
        // Not locked; approximate.
        return self.items.items.len;
    }
};

// ── Worker ──────────────────────────────────────────────────────────

/// A worker thread in the scheduler's thread pool.
pub const Worker = struct {
    id: u32,
    deque: ChaseLevDeque(*ObjFiber),
    current_fiber: ?*ObjFiber,
    scheduler: *Scheduler,
    rng: std.Random.DefaultPrng,
    thread: ?std.Thread,
    /// Counter for periodic global queue checks (every 61st attempt).
    tick: u64,

    pub fn init(id: u32, allocator: Allocator) !Worker {
        return .{
            .id = id,
            .deque = try ChaseLevDeque(*ObjFiber).init(allocator, ChaseLevDeque(*ObjFiber).DEFAULT_CAPACITY),
            .current_fiber = null,
            .scheduler = undefined,
            .rng = std.Random.DefaultPrng.init(@as(u64, id) *% 0x9E3779B97F4A7C15 +% 1),
            .thread = null,
            .tick = 0,
        };
    }

    pub fn deinit(self: *Worker, allocator: Allocator) void {
        self.deque.deinit(allocator);
    }

    /// Find work using priority: local deque > global queue > steal.
    /// Every 61st attempt, checks global queue first (Go's pattern for
    /// preventing global queue starvation).
    pub fn findWork(self: *Worker) ?*ObjFiber {
        self.tick +%= 1;

        // Every 61st attempt, check global queue first.
        if (self.tick % 61 == 0) {
            if (self.scheduler.global_queue.pop()) |f| return f;
        }

        // 1. Pop from own deque (LIFO for cache locality).
        if (self.deque.pop()) |f| return f;

        // 2. Check global queue.
        if (self.scheduler.global_queue.pop()) |f| return f;

        // 3. Try stealing from a random other worker's deque (FIFO steal).
        return self.trySteal();
    }

    /// Attempt to steal from a random other worker.
    fn trySteal(self: *Worker) ?*ObjFiber {
        const num_workers = self.scheduler.num_workers;
        if (num_workers <= 1) return null;

        // Random victim selection.
        var start = self.rng.random().int(u32) % num_workers;
        var attempts: u32 = 0;
        while (attempts < num_workers) : (attempts += 1) {
            if (start != self.id) {
                if (self.scheduler.workers[start].deque.steal()) |f| {
                    return f;
                }
            }
            start = (start + 1) % num_workers;
        }
        return null;
    }

    /// Main worker loop: find work, run fibers, check safepoints.
    pub fn workerLoop(self: *Worker) void {
        while (self.scheduler.running.load(.acquire)) {
            // Check safepoint at loop top (cooperative GC coordination).
            self.scheduler.checkSafepoint(self);

            if (self.findWork()) |fiber| {
                self.current_fiber = fiber;
                fiber.state = .running;

                // Run the fiber's bytecode.
                // In the current plan, this is a placeholder -- actual VM
                // integration happens in Plan 04. For now, mark as dead.
                self.runFiber(fiber);

                self.current_fiber = null;
            } else {
                // No work found -- wait on condition variable.
                self.scheduler.work_mutex.lock();
                defer self.scheduler.work_mutex.unlock();
                // Double-check running flag while holding lock.
                if (!self.scheduler.running.load(.acquire)) break;
                // Wait with a timeout to periodically re-check.
                self.scheduler.work_available.timedWait(
                    &self.scheduler.work_mutex,
                    100_000_000, // 100ms
                ) catch {};
            }
        }
    }

    /// Run a fiber until it yields, completes, or parks.
    /// Placeholder for Plan 04 (VM integration).
    fn runFiber(self: *Worker, fiber: *ObjFiber) void {
        _ = self;
        // Currently a no-op: fiber goes to dead state.
        // Plan 04 will integrate VM dispatch here.
        fiber.state = .dead;
    }
};

// ── Scheduler ───────────────────────────────────────────────────────

/// M:N fiber scheduler: distributes fibers across OS worker threads.
pub const Scheduler = struct {
    workers: []Worker,
    global_queue: GlobalQueue,
    all_fibers: std.ArrayListUnmanaged(*ObjFiber),
    all_fibers_mutex: std.Thread.Mutex,
    running: std.atomic.Value(bool),
    safepoint_requested: std.atomic.Value(bool),
    threads_at_safepoint: std.atomic.Value(u32),
    safepoint_mutex: std.Thread.Mutex,
    safepoint_cond: std.Thread.Condition,
    work_available: std.Thread.Condition,
    work_mutex: std.Thread.Mutex,
    num_workers: u32,
    fiber_id_counter: std.atomic.Value(u64),
    allocator: Allocator,

    /// Initialize the scheduler with the given number of workers.
    /// Default num_workers = CPU count (clamped to 1 minimum).
    pub fn init(num_workers_opt: ?u32, allocator: Allocator) !Scheduler {
        const num_workers = num_workers_opt orelse blk: {
            const cpu_count = std.Thread.getCpuCount() catch 4;
            break :blk @as(u32, @intCast(@min(cpu_count, std.math.maxInt(u32))));
        };
        const actual = if (num_workers == 0) 1 else num_workers;

        const workers = try allocator.alloc(Worker, actual);
        for (workers, 0..) |*w, i| {
            w.* = try Worker.init(@intCast(i), allocator);
        }

        const sched = Scheduler{
            .workers = workers,
            .global_queue = GlobalQueue.init(),
            .all_fibers = .empty,
            .all_fibers_mutex = .{},
            .running = std.atomic.Value(bool).init(false),
            .safepoint_requested = std.atomic.Value(bool).init(false),
            .threads_at_safepoint = std.atomic.Value(u32).init(0),
            .safepoint_mutex = .{},
            .safepoint_cond = .{},
            .work_available = .{},
            .work_mutex = .{},
            .num_workers = actual,
            .fiber_id_counter = std.atomic.Value(u64).init(1),
            .allocator = allocator,
        };

        // NOTE: Back-pointers are NOT set here because `sched` is a local
        // variable returned by value. The caller must call fixWorkerPointers()
        // after the scheduler has settled in its final memory location.
        return sched;
    }

    /// Fix worker back-pointers to this scheduler instance.
    /// Must be called after the Scheduler has settled in its final
    /// memory location (e.g., after assignment to a local var).
    pub fn fixWorkerPointers(self: *Scheduler) void {
        for (self.workers) |*w| {
            w.scheduler = self;
        }
    }

    /// Free scheduler resources.
    pub fn deinit(self: *Scheduler) void {
        for (self.workers) |*w| {
            w.deinit(self.allocator);
        }
        self.allocator.free(self.workers);
        self.global_queue.deinit(self.allocator);
        self.all_fibers.deinit(self.allocator);
    }

    /// Start the scheduler: spawn OS threads for workers 1..N-1.
    /// Worker 0 runs on the calling thread.
    pub fn start(self: *Scheduler) !void {
        self.running.store(true, .release);

        // Fix scheduler back-pointers (init returned by value).
        self.fixWorkerPointers();

        // Spawn OS threads for workers 1..N-1.
        for (self.workers[1..]) |*w| {
            w.thread = try std.Thread.spawn(.{}, workerThreadEntry, .{w});
        }
    }

    /// Stop the scheduler: signal all workers, join all threads.
    pub fn stop(self: *Scheduler) void {
        self.running.store(false, .release);

        // Wake all workers.
        self.work_available.broadcast();
        // Also wake any workers parked at safepoint.
        self.safepoint_cond.broadcast();

        // Join OS threads (skip worker 0 which is the calling thread).
        for (self.workers[1..]) |*w| {
            if (w.thread) |t| {
                t.join();
                w.thread = null;
            }
        }
    }

    /// Generate the next unique fiber ID (atomic monotonic counter).
    pub fn nextFiberId(self: *Scheduler) u64 {
        return self.fiber_id_counter.fetchAdd(1, .monotonic);
    }

    /// Register a fiber in the global all_fibers list for GC scanning.
    pub fn registerFiber(self: *Scheduler, fiber: *ObjFiber) void {
        self.all_fibers_mutex.lock();
        defer self.all_fibers_mutex.unlock();
        self.all_fibers.append(self.allocator, fiber) catch {};
    }

    /// Unregister a fiber from the global all_fibers list.
    pub fn unregisterFiber(self: *Scheduler, fiber: *ObjFiber) void {
        self.all_fibers_mutex.lock();
        defer self.all_fibers_mutex.unlock();
        for (self.all_fibers.items, 0..) |f, i| {
            if (f == fiber) {
                _ = self.all_fibers.swapRemove(i);
                return;
            }
        }
    }

    /// Schedule a fiber for execution.
    /// Pushes to the global queue and wakes a sleeping worker.
    pub fn schedule(self: *Scheduler, fiber: *ObjFiber) void {
        fiber.state = .runnable;
        self.global_queue.push(fiber, self.allocator);
        self.work_available.signal();
    }

    /// Park a fiber: sets it to waiting state.
    /// The fiber will be unparked when its wait condition is satisfied.
    pub fn parkFiber(self: *Scheduler, fiber: *ObjFiber) void {
        _ = self;
        fiber.state = .waiting;
    }

    /// Unpark a fiber: set to runnable and push to global queue.
    pub fn unparkFiber(self: *Scheduler, fiber: *ObjFiber) void {
        fiber.state = .runnable;
        self.global_queue.push(fiber, self.allocator);
        self.work_available.signal();
    }

    // ── Safepoint protocol ──────────────────────────────────────────

    /// Request a safepoint: GC thread calls this before collection.
    pub fn requestSafepoint(self: *Scheduler) void {
        self.safepoint_requested.store(true, .release);
    }

    /// Check and enter safepoint: called by workers at loop top.
    /// If safepoint_requested, increment counter, signal GC, wait.
    pub fn checkSafepoint(self: *Scheduler, worker: *Worker) void {
        _ = worker;
        if (!self.safepoint_requested.load(.acquire)) return;

        self.safepoint_mutex.lock();
        defer self.safepoint_mutex.unlock();

        // Re-check under lock.
        if (!self.safepoint_requested.load(.acquire)) return;

        // Announce arrival at safepoint.
        _ = self.threads_at_safepoint.fetchAdd(1, .release);
        self.safepoint_cond.signal(); // Wake GC thread.

        // Wait until safepoint is released.
        while (self.safepoint_requested.load(.acquire)) {
            self.safepoint_cond.wait(&self.safepoint_mutex);
        }

        // Leaving safepoint.
        _ = self.threads_at_safepoint.fetchSub(1, .release);
    }

    /// Wait until all workers have reached the safepoint.
    /// Called by the GC thread after requestSafepoint().
    pub fn waitForSafepoint(self: *Scheduler) void {
        // Wait until all workers except the GC-triggering one are at safepoint.
        const expected = self.num_workers - 1;

        self.safepoint_mutex.lock();
        defer self.safepoint_mutex.unlock();

        while (self.threads_at_safepoint.load(.acquire) < expected) {
            self.safepoint_cond.wait(&self.safepoint_mutex);
        }
    }

    /// Release safepoint: let workers resume execution.
    pub fn releaseSafepoint(self: *Scheduler) void {
        self.safepoint_requested.store(false, .release);
        self.safepoint_cond.broadcast();
    }

    /// OS thread entry point for worker threads.
    fn workerThreadEntry(worker: *Worker) void {
        worker.workerLoop();
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

test "GlobalQueue push/pop FIFO ordering" {
    const allocator = std.testing.allocator;
    var gq = GlobalQueue.init();
    defer gq.deinit(allocator);

    // Create mock fibers (just need unique pointers).
    const obj_mod = @import("obj");
    const ObjFunction = obj_mod.ObjFunction;
    const ObjClosure = obj_mod.ObjClosure;

    const func = try ObjFunction.create(allocator);
    defer func.obj.destroy(allocator);
    const closure = try ObjClosure.create(allocator, func);
    defer closure.obj.destroy(allocator);

    const f1 = try ObjFiber.create(allocator, closure, 1, "f1");
    defer f1.destroy(allocator);
    const f2 = try ObjFiber.create(allocator, closure, 2, "f2");
    defer f2.destroy(allocator);
    const f3 = try ObjFiber.create(allocator, closure, 3, "f3");
    defer f3.destroy(allocator);

    gq.push(f1, allocator);
    gq.push(f2, allocator);
    gq.push(f3, allocator);

    // Pop should return FIFO order: f1, f2, f3.
    try std.testing.expectEqual(f1, gq.pop().?);
    try std.testing.expectEqual(f2, gq.pop().?);
    try std.testing.expectEqual(f3, gq.pop().?);
    try std.testing.expect(gq.pop() == null);
}

test "GlobalQueue empty returns null" {
    const allocator = std.testing.allocator;
    var gq = GlobalQueue.init();
    defer gq.deinit(allocator);

    try std.testing.expect(gq.pop() == null);
}

test "Worker.findWork priority: local deque first" {
    const allocator = std.testing.allocator;
    var sched = try Scheduler.init(1, allocator);
    defer sched.deinit();
    sched.fixWorkerPointers();

    const obj_mod = @import("obj");
    const ObjFunction = obj_mod.ObjFunction;
    const ObjClosure = obj_mod.ObjClosure;

    const func = try ObjFunction.create(allocator);
    defer func.obj.destroy(allocator);
    const closure = try ObjClosure.create(allocator, func);
    defer closure.obj.destroy(allocator);

    const f_local = try ObjFiber.create(allocator, closure, 10, "local");
    defer f_local.destroy(allocator);
    const f_global = try ObjFiber.create(allocator, closure, 20, "global");
    defer f_global.destroy(allocator);

    // Push one to local deque, one to global queue.
    sched.workers[0].deque.push(f_local);
    sched.global_queue.push(f_global, allocator);

    // findWork should get local first (LIFO from deque).
    const first = sched.workers[0].findWork();
    try std.testing.expectEqual(f_local, first.?);

    // Next should get global.
    const second = sched.workers[0].findWork();
    try std.testing.expectEqual(f_global, second.?);

    // Empty now.
    try std.testing.expect(sched.workers[0].findWork() == null);
}

test "Scheduler.init creates correct number of workers" {
    const allocator = std.testing.allocator;

    var sched2 = try Scheduler.init(2, allocator);
    defer sched2.deinit();
    try std.testing.expectEqual(@as(u32, 2), sched2.num_workers);
    try std.testing.expectEqual(@as(usize, 2), sched2.workers.len);

    var sched4 = try Scheduler.init(4, allocator);
    defer sched4.deinit();
    try std.testing.expectEqual(@as(u32, 4), sched4.num_workers);

    // Zero defaults to 1.
    var sched0 = try Scheduler.init(0, allocator);
    defer sched0.deinit();
    try std.testing.expectEqual(@as(u32, 1), sched0.num_workers);
}

test "Scheduler.nextFiberId is monotonic" {
    const allocator = std.testing.allocator;
    var sched = try Scheduler.init(1, allocator);
    defer sched.deinit();
    sched.fixWorkerPointers();

    const id1 = sched.nextFiberId();
    const id2 = sched.nextFiberId();
    const id3 = sched.nextFiberId();

    try std.testing.expect(id2 == id1 + 1);
    try std.testing.expect(id3 == id2 + 1);
}

test "Scheduler register/unregister fiber" {
    const allocator = std.testing.allocator;
    var sched = try Scheduler.init(1, allocator);
    defer sched.deinit();
    sched.fixWorkerPointers();

    const obj_mod = @import("obj");
    const ObjFunction = obj_mod.ObjFunction;
    const ObjClosure = obj_mod.ObjClosure;

    const func = try ObjFunction.create(allocator);
    defer func.obj.destroy(allocator);
    const closure = try ObjClosure.create(allocator, func);
    defer closure.obj.destroy(allocator);

    const fiber = try ObjFiber.create(allocator, closure, 1, "test");
    defer fiber.destroy(allocator);

    sched.registerFiber(fiber);
    try std.testing.expectEqual(@as(usize, 1), sched.all_fibers.items.len);

    sched.unregisterFiber(fiber);
    try std.testing.expectEqual(@as(usize, 0), sched.all_fibers.items.len);
}

test "Scheduler schedule sets fiber runnable" {
    const allocator = std.testing.allocator;
    var sched = try Scheduler.init(1, allocator);
    defer sched.deinit();
    sched.fixWorkerPointers();

    const obj_mod = @import("obj");
    const ObjFunction = obj_mod.ObjFunction;
    const ObjClosure = obj_mod.ObjClosure;

    const func = try ObjFunction.create(allocator);
    defer func.obj.destroy(allocator);
    const closure = try ObjClosure.create(allocator, func);
    defer closure.obj.destroy(allocator);

    const fiber = try ObjFiber.create(allocator, closure, 1, "test");
    defer fiber.destroy(allocator);

    sched.schedule(fiber);
    try std.testing.expectEqual(fiber_mod.FiberState.runnable, fiber.state);

    // Should be in global queue.
    const popped = sched.global_queue.pop();
    try std.testing.expectEqual(fiber, popped.?);
}
