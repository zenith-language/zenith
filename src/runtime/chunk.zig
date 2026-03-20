// Bytecode chunk container -- implemented in Task 2.

const std = @import("std");

pub const OpCode = enum(u8) {
    op_return,
};

pub const Chunk = struct {
    code: std.ArrayListUnmanaged(u8),
};

test "placeholder" {
    try std.testing.expect(true);
}
