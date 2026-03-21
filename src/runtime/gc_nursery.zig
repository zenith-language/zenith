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
const value_mod = @import("value");
const Value = value_mod.Value;
const gc_mod = @import("gc");
const GC = gc_mod.GC;

/// Nursery collector using Cheney's-style promotion.
///
/// Since objects are individually heap-allocated (not bump-allocated
/// in a contiguous semi-space), the collector adapts Cheney's algorithm
/// to work as a promotion-based collector:
///
/// 1. Walk roots; for each nursery object found, promote it to old-gen
///    (set GC_OLD flag, move from nursery_objects list to old_objects list).
/// 2. Scan promoted objects' references via a gray stack (worklist) --
///    if they point to nursery objects, promote those too.
/// 3. After the gray stack drains, sweep the remaining nursery_objects
///    list (unreachable) -- destroy them and count freed bytes.
pub const NurseryCollector = struct {
    gray_stack: std.ArrayListUnmanaged(*Obj),

    pub fn init() NurseryCollector {
        return .{
            .gray_stack = .empty,
        };
    }

    pub fn deinit(self: *NurseryCollector, allocator: Allocator) void {
        self.gray_stack.deinit(allocator);
    }

    /// Collect nursery: promote live objects to old-gen, destroy dead ones.
    /// Returns the number of objects freed.
    ///
    /// The collection sequence is:
    /// 1. Root scanning (done by caller before this function)
    /// 2. Process gray stack (promote transitive references)
    /// 3. Sweep remaining nursery objects (unreachable -> destroy)
    pub fn processGrayStack(self: *NurseryCollector, gc: *GC) !void {
        while (self.gray_stack.items.len > 0) {
            const obj = self.gray_stack.pop().?;
            try self.scanObject(obj, gc);
        }
    }

    /// Sweep remaining nursery objects (unreachable) and destroy them.
    /// Returns the number of objects destroyed.
    pub fn sweepNursery(gc: *GC) usize {
        var freed: usize = 0;
        var obj = gc.nursery_objects;
        while (obj) |o| {
            const next = o.next;
            o.destroy(gc.backing_allocator);
            freed += 1;
            obj = next;
        }
        gc.nursery_objects = null;
        return freed;
    }

    /// Mark a nursery object as live: promote it to old-gen and push
    /// to the gray stack for reference scanning.
    ///
    /// If the object is already old-gen, this is a no-op (already promoted).
    pub fn markNurseryObj(self: *NurseryCollector, obj: *Obj, gc: *GC) !void {
        // Already promoted or already old-gen -- skip.
        if (obj.isOldGen()) return;

        // Promote: set old-gen flag.
        obj.promoteToOld();

        // Remove from nursery_objects list by unlinking.
        // We do this by marking with GC_OLD; the sweep phase
        // will skip old objects when walking nursery_objects.
        // Actually, we need to physically move the object to the
        // old_objects list. We defer this to after gray stack processing
        // to avoid corrupting the nursery list during iteration.
        // Instead, we use the GC_OLD flag as the indicator: after
        // gray stack processing, we walk nursery_objects and move
        // promoted objects to old_objects.

        // Push to gray stack for reference scanning.
        try self.gray_stack.append(gc.backing_allocator, obj);
    }

    /// Process a value: if it references a nursery object, mark it.
    pub fn processValue(self: *NurseryCollector, val: *Value, gc: *GC) !void {
        if (val.isObj()) {
            const obj = val.asObj();
            if (!obj.isOldGen()) {
                try self.markNurseryObj(obj, gc);
            }
        }
    }

    /// Scan an object's outgoing references. For each referenced object
    /// that is still in the nursery, promote it.
    ///
    /// Handles all 12 object types:
    /// - string, bytes, int_big, range: leaf objects (no references)
    /// - function: scan constant pool values
    /// - closure: scan function + upvalues
    /// - upvalue: scan closed value
    /// - list: scan all items
    /// - map: scan all keys and values
    /// - tuple: scan all fields
    /// - record: scan all field_values
    /// - adt: scan all payload values
    fn scanObject(self: *NurseryCollector, obj: *Obj, gc: *GC) !void {
        switch (obj.obj_type) {
            // Leaf objects: no outgoing references to other GC objects.
            .string, .bytes, .int_big, .range => {},

            .function => {
                const func = ObjFunction.fromObj(obj);
                // Scan constant pool for object references.
                for (func.chunk.constants.items) |*val| {
                    try self.processValue(val, gc);
                }
            },

            .closure => {
                const clos = ObjClosure.fromObj(obj);
                // Scan the underlying function.
                if (!clos.function.obj.isOldGen()) {
                    try self.markNurseryObj(&clos.function.obj, gc);
                }
                // Scan captured upvalues.
                for (clos.upvalues) |maybe_uv| {
                    if (maybe_uv) |uv| {
                        if (!uv.obj.isOldGen()) {
                            try self.markNurseryObj(&uv.obj, gc);
                        }
                    }
                }
            },

            .upvalue => {
                const uv = ObjUpvalue.fromObj(obj);
                // Scan the closed value (if the upvalue is closed, its value
                // may reference a GC object).
                try self.processValue(&uv.closed, gc);
                // Note: open upvalues point to stack slots which are
                // scanned as roots -- no need to scan location here.
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
        }
    }

    /// After gray stack processing, physically move promoted objects
    /// from the nursery_objects list to old_objects list.
    /// Objects with GC_OLD set have been promoted; the rest remain
    /// in nursery for sweeping.
    pub fn migratePromoted(gc: *GC) void {
        var prev: ?*Obj = null;
        var current = gc.nursery_objects;
        while (current) |obj| {
            const next = obj.next;
            if (obj.isOldGen()) {
                // Unlink from nursery list.
                if (prev) |p| {
                    p.next = next;
                } else {
                    gc.nursery_objects = next;
                }
                // Prepend to old_objects list.
                obj.next = gc.old_objects;
                gc.old_objects = obj;
                // prev stays the same (we removed current).
            } else {
                prev = obj;
            }
            current = next;
        }
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

test "markNurseryObj promotes object and adds to gray stack" {
    const allocator = std.testing.allocator;
    var gc = try GC.init(allocator);
    defer gc.deinit();

    var nursery = NurseryCollector.init();
    defer nursery.deinit(allocator);

    // Create a nursery object (string).
    const str = try ObjString.create(allocator, "hello", null);
    gc.trackObject(&str.obj);

    // Verify it starts as nursery (not old-gen).
    try std.testing.expect(!str.obj.isOldGen());

    // Mark it.
    try nursery.markNurseryObj(&str.obj, &gc);

    // Should now be old-gen.
    try std.testing.expect(str.obj.isOldGen());
    // Should be on the gray stack.
    try std.testing.expectEqual(@as(usize, 1), nursery.gray_stack.items.len);
}

test "markNurseryObj is idempotent for already-promoted objects" {
    const allocator = std.testing.allocator;
    var gc = try GC.init(allocator);
    defer gc.deinit();

    var nursery = NurseryCollector.init();
    defer nursery.deinit(allocator);

    const str = try ObjString.create(allocator, "test", null);
    gc.trackObject(&str.obj);

    try nursery.markNurseryObj(&str.obj, &gc);
    try nursery.markNurseryObj(&str.obj, &gc); // Second call should be no-op.

    try std.testing.expectEqual(@as(usize, 1), nursery.gray_stack.items.len);
}

test "sweepNursery destroys unreachable objects" {
    const allocator = std.testing.allocator;
    var gc = try GC.init(allocator);

    // Create two nursery objects -- do NOT defer deinit because sweep will free them.
    const str1 = try ObjString.create(allocator, "alive", null);
    gc.trackObject(&str1.obj);
    const str2 = try ObjString.create(allocator, "dead", null);
    gc.trackObject(&str2.obj);

    // Promote str1 to old-gen (simulating it being reachable).
    str1.obj.promoteToOld();
    NurseryCollector.migratePromoted(&gc);

    // Sweep should destroy str2 (still in nursery = unreachable).
    const freed = NurseryCollector.sweepNursery(&gc);
    try std.testing.expectEqual(@as(usize, 1), freed);
    try std.testing.expect(gc.nursery_objects == null);

    // str1 should be in old_objects.
    try std.testing.expect(gc.old_objects != null);
    try std.testing.expectEqual(&str1.obj, gc.old_objects.?);

    // Clean up old-gen manually.
    gc.deinit();
}

test "processValue marks nursery object values" {
    const allocator = std.testing.allocator;
    var gc = try GC.init(allocator);
    defer gc.deinit();

    var nursery = NurseryCollector.init();
    defer nursery.deinit(allocator);

    const str = try ObjString.create(allocator, "ref", null);
    gc.trackObject(&str.obj);

    var val = Value.fromObj(&str.obj);
    try nursery.processValue(&val, &gc);

    try std.testing.expect(str.obj.isOldGen());
}

test "processValue ignores non-object values" {
    const allocator = std.testing.allocator;
    var gc = try GC.init(allocator);
    defer gc.deinit();

    var nursery = NurseryCollector.init();
    defer nursery.deinit(allocator);

    var val = Value.fromInt(42);
    try nursery.processValue(&val, &gc);

    // Gray stack should remain empty.
    try std.testing.expectEqual(@as(usize, 0), nursery.gray_stack.items.len);
}

test "scanObject traverses closure -> function -> constants" {
    const allocator = std.testing.allocator;
    var gc = try GC.init(allocator);
    defer gc.deinit();

    var nursery = NurseryCollector.init();
    defer nursery.deinit(allocator);

    // Create a function with a string constant.
    const func = try ObjFunction.create(allocator);
    gc.trackObject(&func.obj);
    const str = try ObjString.create(allocator, "constant", null);
    gc.trackObject(&str.obj);
    _ = try func.chunk.addConstant(Value.fromObj(&str.obj), allocator);

    // Create a closure wrapping the function.
    const clos = try ObjClosure.create(allocator, func);
    gc.trackObject(&clos.obj);

    // Mark the closure as old-gen (simulating root scanning found it).
    clos.obj.promoteToOld();

    // Scan the closure -- should promote the function and the constant string.
    try nursery.scanObject(&clos.obj, &gc);
    try nursery.processGrayStack(&gc);

    try std.testing.expect(func.obj.isOldGen());
    try std.testing.expect(str.obj.isOldGen());
}

test "scanObject handles list items" {
    const allocator = std.testing.allocator;
    var gc = try GC.init(allocator);
    defer gc.deinit();

    var nursery = NurseryCollector.init();
    defer nursery.deinit(allocator);

    const str = try ObjString.create(allocator, "item", null);
    gc.trackObject(&str.obj);

    const lst = try ObjList.create(allocator);
    gc.trackObject(&lst.obj);
    try lst.items.append(allocator, Value.fromObj(&str.obj));
    try lst.items.append(allocator, Value.fromInt(42)); // non-object

    lst.obj.promoteToOld();
    try nursery.scanObject(&lst.obj, &gc);
    try nursery.processGrayStack(&gc);

    try std.testing.expect(str.obj.isOldGen());
}
