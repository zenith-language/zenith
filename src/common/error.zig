// Error diagnostic types -- implemented in Task 2.

const std = @import("std");

pub const Diagnostic = struct {
    message: []const u8,
};

pub const ErrorCode = enum(u16) {
    E001 = 1,
};

test "placeholder" {
    try std.testing.expect(true);
}
