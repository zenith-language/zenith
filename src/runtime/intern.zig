const std = @import("std");
const Allocator = std.mem.Allocator;
const obj_mod = @import("obj");
const ObjString = obj_mod.ObjString;
const Obj = obj_mod.Obj;

/// Open-addressing hash table for string interning.
/// Stores weak references to ObjString instances. Equal strings
/// (by byte content) are deduplicated to share a single pointer,
/// enabling O(1) pointer-equality checks after interning.
pub const InternTable = struct {
    entries: []?*ObjString,
    count: usize,
    capacity: usize,
    allocator: Allocator,

    /// Sentinel value used to mark deleted (tombstone) slots.
    /// Uses an impossible pointer value so it cannot collide with
    /// any real ObjString pointer or null. The address equals ObjString
    /// alignment to satisfy Zig's pointer alignment requirements.
    const TOMBSTONE: ?*ObjString = @ptrFromInt(@alignOf(ObjString));

    pub fn init(allocator: Allocator, initial_capacity: usize) !InternTable {
        const cap = if (initial_capacity < 8) 8 else initial_capacity;
        const entries = try allocator.alloc(?*ObjString, cap);
        @memset(entries, null);
        return .{
            .entries = entries,
            .count = 0,
            .capacity = cap,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *InternTable) void {
        self.allocator.free(self.entries);
        self.entries = &.{};
        self.count = 0;
        self.capacity = 0;
    }

    /// Look up an interned string by its byte content and precomputed hash.
    /// Returns the existing ObjString if found, or null if not interned.
    pub fn findByContent(self: *const InternTable, bytes: []const u8, hash: u32) ?*ObjString {
        if (self.capacity == 0) return null;

        var index = hash & @as(u32, @intCast(self.capacity - 1));
        while (true) {
            const entry = self.entries[index];
            if (entry == null) {
                // Empty slot -- string not found.
                return null;
            }
            if (entry != TOMBSTONE) {
                const str = entry.?;
                if (str.hash == hash and str.bytes.len == bytes.len and
                    std.mem.eql(u8, str.bytes, bytes))
                {
                    return str;
                }
            }
            // Skip tombstones and hash collisions.
            index = (index + 1) & @as(u32, @intCast(self.capacity - 1));
        }
    }

    /// Insert a string into the intern table. The string must not already
    /// be present (caller should check findByContent first).
    /// Grows the table if load factor exceeds 0.75.
    pub fn insert(self: *InternTable, str: *ObjString) !void {
        // Grow if load > 0.75
        if ((self.count + 1) * 4 > self.capacity * 3) {
            try self.grow();
        }

        var index = str.hash & @as(u32, @intCast(self.capacity - 1));
        while (true) {
            const entry = self.entries[index];
            if (entry == null or entry == TOMBSTONE) {
                self.entries[index] = str;
                self.count += 1;
                return;
            }
            index = (index + 1) & @as(u32, @intCast(self.capacity - 1));
        }
    }

    /// Sweep the table: remove entries whose ObjString has the mark bit
    /// unset (i.e., not reachable). Returns the number of entries removed.
    pub fn removeUnmarked(self: *InternTable) usize {
        var removed: usize = 0;
        for (self.entries) |*entry| {
            if (entry.* != null and entry.* != TOMBSTONE) {
                const str = entry.*.?;
                if (!str.obj.isMarked()) {
                    entry.* = TOMBSTONE;
                    self.count -= 1;
                    removed += 1;
                }
            }
        }
        return removed;
    }

    /// Double the table capacity and rehash all non-null, non-tombstone entries.
    fn grow(self: *InternTable) !void {
        const new_capacity = self.capacity * 2;
        const new_entries = try self.allocator.alloc(?*ObjString, new_capacity);
        @memset(new_entries, null);

        // Rehash all live entries into the new table.
        var migrated: usize = 0;
        for (self.entries) |entry| {
            if (entry != null and entry != TOMBSTONE) {
                const str = entry.?;
                var index = str.hash & @as(u32, @intCast(new_capacity - 1));
                while (new_entries[index] != null) {
                    index = (index + 1) & @as(u32, @intCast(new_capacity - 1));
                }
                new_entries[index] = str;
                migrated += 1;
            }
        }

        self.allocator.free(self.entries);
        self.entries = new_entries;
        self.capacity = new_capacity;
        // count stays the same (tombstones are dropped during rehash)
        self.count = migrated;
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

test "InternTable insert and find by content" {
    const allocator = std.testing.allocator;
    var table = try InternTable.init(allocator, 8);
    defer table.deinit();

    // Create a string and insert it.
    const str = try ObjString.create(allocator, "hello", null);
    defer str.obj.destroy(allocator);
    try table.insert(str);

    // Should find it by content.
    const found = table.findByContent("hello", ObjString.hashBytes("hello"));
    try std.testing.expect(found != null);
    try std.testing.expectEqual(str, found.?);

    // Should not find a different string.
    const not_found = table.findByContent("world", ObjString.hashBytes("world"));
    try std.testing.expect(not_found == null);
}

test "InternTable duplicate detection returns same pointer" {
    const allocator = std.testing.allocator;
    var table = try InternTable.init(allocator, 8);
    defer table.deinit();

    // Create and intern first string.
    const str1 = try ObjString.create(allocator, "dup", null);
    defer str1.obj.destroy(allocator);
    try table.insert(str1);

    // Creating via intern table should return same pointer.
    const str2 = try ObjString.create(allocator, "dup", &table);
    // str2 should be the same pointer as str1.
    try std.testing.expectEqual(str1, str2);
    // No extra cleanup needed since str2 == str1.
}

test "InternTable removeUnmarked removes entries with mark bit unset" {
    const allocator = std.testing.allocator;
    var table = try InternTable.init(allocator, 8);
    defer table.deinit();

    // Create two strings.
    const str1 = try ObjString.create(allocator, "keep", null);
    defer str1.obj.destroy(allocator);
    const str2 = try ObjString.create(allocator, "remove", null);
    defer str2.obj.destroy(allocator);

    try table.insert(str1);
    try table.insert(str2);
    try std.testing.expectEqual(@as(usize, 2), table.count);

    // Mark str1 as reachable.
    str1.obj.setMarked(true);

    const removed = table.removeUnmarked();
    try std.testing.expectEqual(@as(usize, 1), removed);
    try std.testing.expectEqual(@as(usize, 1), table.count);

    // str1 should still be findable.
    try std.testing.expect(table.findByContent("keep", ObjString.hashBytes("keep")) != null);
    // str2 should be gone.
    try std.testing.expect(table.findByContent("remove", ObjString.hashBytes("remove")) == null);

    // Clean up mark.
    str1.obj.setMarked(false);
}

test "InternTable grow/rehash preserves all entries" {
    const allocator = std.testing.allocator;
    // Start with very small capacity to force growth.
    var table = try InternTable.init(allocator, 8);
    defer table.deinit();

    // Insert enough strings to trigger at least one grow.
    const labels = [_][]const u8{ "s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10", "s11" };
    var strings: [12]*ObjString = undefined;
    for (labels, 0..) |label, i| {
        strings[i] = try ObjString.create(allocator, label, null);
        try table.insert(strings[i]);
    }
    defer for (&strings) |str| {
        str.obj.destroy(allocator);
    };

    // All strings should still be findable after growth.
    for (labels, 0..) |label, i| {
        const found = table.findByContent(label, ObjString.hashBytes(label));
        try std.testing.expect(found != null);
        try std.testing.expectEqual(strings[i], found.?);
    }
}
