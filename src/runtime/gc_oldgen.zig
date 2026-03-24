const std = @import("std");
const Allocator = std.mem.Allocator;
const obj_mod = @import("obj");
const Obj = obj_mod.Obj;
const ObjString = obj_mod.ObjString;
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
const gc_mod = @import("gc");
const GC = gc_mod.GC;
const gc_nursery_mod = @import("gc_nursery");
const NurseryCollector = gc_nursery_mod.NurseryCollector;

/// Write barrier using a remembered set.
///
/// Since the VM allocates objects individually through the Zig allocator
/// (not from a contiguous heap region), a card table over address ranges
/// would be impractical (addresses are scattered). Instead, the write
/// barrier uses a remembered set: a list of old-gen objects that have had
/// nursery references stored into them.
///
/// Per Claude's discretion (04-CONTEXT.md: "Card table vs remembered set
/// implementation for write barriers" is Claude's choice). The user
/// specified "card table" as the requirement name, not the exact
/// implementation. A remembered set is the correct choice for
/// scatter-allocated objects.
pub const WriteBarrier = struct {
    /// Old-gen objects that have had nursery references stored in them.
    dirty_objects: std.ArrayListUnmanaged(*Obj),

    pub fn init() WriteBarrier {
        return .{
            .dirty_objects = .empty,
        };
    }

    pub fn deinit(self: *WriteBarrier, allocator: Allocator) void {
        self.dirty_objects.deinit(allocator);
    }

    /// Record that an old-gen object has stored a reference to a nursery object.
    pub inline fn recordStore(self: *WriteBarrier, container: *Obj, allocator: Allocator) !void {
        try self.dirty_objects.append(allocator, container);
    }

    /// Scan all dirty old-gen objects for nursery references.
    /// Called during nursery collection to find additional roots.
    /// For each nursery object referenced by a dirty old-gen object,
    /// promotes it via the nursery collector.
    pub fn scanDirtyObjects(self: *WriteBarrier, nursery: *NurseryCollector, gc: *GC) !void {
        for (self.dirty_objects.items) |obj| {
            try scanObjectForNurseryRefs(obj, nursery, gc);
        }
    }

    /// Clear the dirty set after scanning.
    pub fn clear(self: *WriteBarrier) void {
        self.dirty_objects.clearRetainingCapacity();
    }

    /// Scan a single old-gen object's references. For each referenced
    /// nursery object, promote it via the nursery collector.
    fn scanObjectForNurseryRefs(obj: *Obj, nursery: *NurseryCollector, gc: *GC) !void {
        switch (obj.obj_type) {
            // Leaf objects: no outgoing references.
            .string, .bytes, .int_big, .range => {},

            .function => {
                const func = ObjFunction.fromObj(obj);
                for (func.chunk.constants.items) |*val| {
                    try nursery.processValue(val, gc);
                }
            },

            .closure => {
                const clos = ObjClosure.fromObj(obj);
                if (!clos.function.obj.isOldGen()) {
                    try nursery.markNurseryObj(&clos.function.obj, gc);
                }
                for (clos.upvalues) |maybe_uv| {
                    if (maybe_uv) |uv| {
                        if (!uv.obj.isOldGen()) {
                            try nursery.markNurseryObj(&uv.obj, gc);
                        }
                    }
                }
            },

            .upvalue => {
                const uv = ObjUpvalue.fromObj(obj);
                try nursery.processValue(&uv.closed, gc);
            },

            .list => {
                const lst = ObjList.fromObj(obj);
                for (lst.items.items) |*val| {
                    try nursery.processValue(val, gc);
                }
            },

            .map => {
                const m = ObjMap.fromObj(obj);
                var it = m.entries.iterator();
                while (it.next()) |entry| {
                    try nursery.processValue(entry.key_ptr, gc);
                    try nursery.processValue(entry.value_ptr, gc);
                }
            },

            .tuple => {
                const t = ObjTuple.fromObj(obj);
                for (t.fields) |*val| {
                    try nursery.processValue(val, gc);
                }
            },

            .record => {
                const rec = ObjRecord.fromObj(obj);
                for (rec.field_values) |*val| {
                    try nursery.processValue(val, gc);
                }
            },

            .adt => {
                const a = ObjAdt.fromObj(obj);
                for (a.payload) |*val| {
                    try nursery.processValue(val, gc);
                }
            },
            .stream => {
                const s = ObjStream.fromObj(obj);
                try s.state.traceGCRefs(nursery, gc);
            },
            .fiber => {
                // Fiber GC scanning will be implemented in Plan 02.
            },
            .channel => {
                // Channel GC scanning will be implemented in Plan 03.
            },
        }
    }
};

/// Old-generation mark-sweep collector.
///
/// Performs a full mark-sweep of old-gen objects:
/// 1. Mark phase: trace from roots through all reachable old-gen objects.
/// 2. Sweep intern table: remove entries for dead interned strings
///    (MUST happen before sweeping old-gen objects to prevent dangling pointers).
/// 3. Sweep phase: free unmarked old-gen objects, clear marks on survivors.
pub const OldGenCollector = struct {
    gray_stack: std.ArrayListUnmanaged(*Obj),

    pub fn init() OldGenCollector {
        return .{
            .gray_stack = .empty,
        };
    }

    pub fn deinit(self: *OldGenCollector, allocator: Allocator) void {
        self.gray_stack.deinit(allocator);
    }

    /// Full mark-sweep of old generation. Returns bytes freed.
    pub fn collect(self: *OldGenCollector, gc: *GC, vm: anytype) !usize {
        // 1. Mark phase: trace from roots.
        try self.mark(gc, vm);

        // 2. Process gray stack: scan marked objects' references.
        try self.traceReferences(gc);

        // 3. Sweep intern table BEFORE sweeping old-gen objects.
        //    This prevents dangling pointers in the intern table.
        _ = gc.intern_table.removeUnmarked();

        // 4. Sweep phase: free unmarked old-gen objects.
        const freed = sweep(gc);

        return freed;
    }

    /// Mark phase: scan roots and mark reachable old-gen objects.
    fn mark(self: *OldGenCollector, gc: *GC, vm: anytype) !void {
        // Import gc_roots here to scan same root set as nursery.
        const gc_roots_mod = @import("gc_roots");
        try gc_roots_mod.scanRootsForOldGen(self, gc, vm);
    }

    /// Mark a single old-gen object and push to gray stack for scanning.
    /// Only marks objects that are in old-gen and not already marked.
    pub fn markObj(self: *OldGenCollector, obj: *Obj, gc: *GC) !void {
        // Only mark old-gen objects; nursery objects are handled by nursery collector.
        if (!obj.isOldGen()) return;
        // Already marked -- skip.
        if (obj.isMarked()) return;

        obj.setMarked(true);
        try self.gray_stack.append(gc.backing_allocator, obj);
    }

    /// Process a value: if it references an old-gen object, mark it.
    pub fn processValue(self: *OldGenCollector, val: *Value, gc: *GC) !void {
        if (val.isObj()) {
            try self.markObj(val.asObj(), gc);
        }
    }

    /// Process gray stack: scan each marked object's outgoing references
    /// and mark any old-gen objects they point to.
    fn traceReferences(self: *OldGenCollector, gc: *GC) !void {
        while (self.gray_stack.items.len > 0) {
            const obj = self.gray_stack.pop().?;
            try self.scanObject(obj, gc);
        }
    }

    /// Scan an object's outgoing references and mark old-gen referents.
    fn scanObject(self: *OldGenCollector, obj: *Obj, gc: *GC) !void {
        switch (obj.obj_type) {
            // Leaf objects: no outgoing references.
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
                    if (maybe_uv) |uv| {
                        try self.markObj(&uv.obj, gc);
                    }
                }
            },

            .upvalue => {
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
                try s.state.traceGCRefsOldGen(self, gc);
            },
            .fiber => {
                // Fiber GC scanning will be implemented in Plan 02.
            },
            .channel => {
                // Channel GC scanning will be implemented in Plan 03.
            },
        }
    }

    /// Sweep phase: walk old_objects list, free unmarked objects,
    /// clear mark bits on survivors. Returns bytes freed (object count).
    fn sweep(gc: *GC) usize {
        var freed: usize = 0;
        var prev: ?*Obj = null;
        var current = gc.old_objects;
        while (current) |obj| {
            const next = obj.next;
            if (!obj.isMarked()) {
                // Unreachable: unlink from old_objects list and destroy.
                if (prev) |p| {
                    p.next = next;
                } else {
                    gc.old_objects = next;
                }
                obj.destroy(gc.backing_allocator);
                freed += 1;
                // Decrement old_gen_size (estimate: 1 unit per object).
                gc.old_gen_size -|= 1;
            } else {
                // Reachable: clear mark bit for next cycle.
                obj.setMarked(false);
                prev = obj;
            }
            current = next;
        }
        return freed;
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

test "OldGenCollector markObj sets GC_MARK flag on old-gen objects" {
    const allocator = std.testing.allocator;
    var gc = try GC.init(allocator);
    defer gc.deinit();

    var oldgen = OldGenCollector.init();
    defer oldgen.deinit(allocator);

    // Create a string and make it old-gen.
    const str = try ObjString.create(allocator, "old", null);
    gc.trackOldObject(&str.obj);

    // Should start unmarked.
    try std.testing.expect(!str.obj.isMarked());
    try std.testing.expect(str.obj.isOldGen());

    // Mark it.
    try oldgen.markObj(&str.obj, &gc);

    // Should now be marked and on the gray stack.
    try std.testing.expect(str.obj.isMarked());
    try std.testing.expectEqual(@as(usize, 1), oldgen.gray_stack.items.len);
}

test "OldGenCollector markObj is idempotent for already-marked objects" {
    const allocator = std.testing.allocator;
    var gc = try GC.init(allocator);
    defer gc.deinit();

    var oldgen = OldGenCollector.init();
    defer oldgen.deinit(allocator);

    const str = try ObjString.create(allocator, "old", null);
    gc.trackOldObject(&str.obj);

    try oldgen.markObj(&str.obj, &gc);
    try oldgen.markObj(&str.obj, &gc); // Second call should be no-op.

    try std.testing.expectEqual(@as(usize, 1), oldgen.gray_stack.items.len);
}

test "OldGenCollector markObj ignores nursery objects" {
    const allocator = std.testing.allocator;
    var gc = try GC.init(allocator);
    defer gc.deinit();

    var oldgen = OldGenCollector.init();
    defer oldgen.deinit(allocator);

    // Create a nursery object (not old-gen).
    const str = try ObjString.create(allocator, "young", null);
    gc.trackObject(&str.obj);

    try oldgen.markObj(&str.obj, &gc);

    // Should not be marked or on gray stack (nursery objects skip).
    try std.testing.expect(!str.obj.isMarked());
    try std.testing.expectEqual(@as(usize, 0), oldgen.gray_stack.items.len);
}

test "OldGenCollector sweep destroys unmarked and preserves marked" {
    const allocator = std.testing.allocator;
    var gc = try GC.init(allocator);

    // Create two old-gen objects.
    const alive = try ObjString.create(allocator, "alive", null);
    gc.trackOldObject(&alive.obj);
    gc.old_gen_size += 1;
    const dead = try ObjString.create(allocator, "dead", null);
    gc.trackOldObject(&dead.obj);
    gc.old_gen_size += 1;

    // Mark 'alive' as reachable.
    alive.obj.setMarked(true);

    // Sweep should destroy 'dead' and preserve 'alive'.
    const freed = OldGenCollector.sweep(&gc);
    try std.testing.expectEqual(@as(usize, 1), freed);

    // 'alive' should still be in old_objects with mark cleared.
    try std.testing.expect(gc.old_objects != null);
    try std.testing.expect(!alive.obj.isMarked()); // Mark cleared after sweep.

    gc.deinit();
}

test "WriteBarrier recordStore adds to dirty list" {
    const allocator = std.testing.allocator;
    var gc = try GC.init(allocator);
    defer gc.deinit();

    var wb = WriteBarrier.init();
    defer wb.deinit(allocator);

    const str = try ObjString.create(allocator, "container", null);
    gc.trackOldObject(&str.obj);

    try wb.recordStore(&str.obj, allocator);
    try std.testing.expectEqual(@as(usize, 1), wb.dirty_objects.items.len);
    try std.testing.expectEqual(&str.obj, wb.dirty_objects.items[0]);
}

test "WriteBarrier clear empties dirty list" {
    const allocator = std.testing.allocator;
    var gc = try GC.init(allocator);
    defer gc.deinit();

    var wb = WriteBarrier.init();
    defer wb.deinit(allocator);

    const str = try ObjString.create(allocator, "container", null);
    gc.trackOldObject(&str.obj);

    try wb.recordStore(&str.obj, allocator);
    wb.clear();
    try std.testing.expectEqual(@as(usize, 0), wb.dirty_objects.items.len);
}

test "WriteBarrier scanDirtyObjects marks nursery objects referenced by dirty old-gen objects" {
    const allocator = std.testing.allocator;
    var gc = try GC.init(allocator);
    defer gc.deinit();

    var nursery = NurseryCollector.init();
    defer nursery.deinit(allocator);

    var wb = WriteBarrier.init();
    defer wb.deinit(allocator);

    // Create an old-gen upvalue that has closed over a nursery string.
    const young_str = try ObjString.create(allocator, "young_val", null);
    gc.trackObject(&young_str.obj);

    var slot = Value.nil;
    const uv = try ObjUpvalue.create(allocator, &slot);
    gc.trackOldObject(&uv.obj);
    // Close the upvalue with the nursery string as value.
    uv.closed = Value.fromObj(&young_str.obj);
    uv.location = &uv.closed;

    // Record the upvalue as dirty.
    try wb.recordStore(&uv.obj, allocator);

    // Scan dirty objects should promote the nursery string.
    try wb.scanDirtyObjects(&nursery, &gc);
    try nursery.processGrayStack(&gc);

    try std.testing.expect(young_str.obj.isOldGen());
}
