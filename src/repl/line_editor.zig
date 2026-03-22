/// Raw terminal line editor with history and escape sequence handling.
const std = @import("std");

pub const LineEditor = struct {
    allocator: std.mem.Allocator,

    // Implemented in Task 2

    pub fn init(allocator: std.mem.Allocator) !LineEditor {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *LineEditor) void {
        _ = self;
    }
};
