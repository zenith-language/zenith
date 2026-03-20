// NaN-boxed value representation -- implemented in Task 2.

const std = @import("std");

pub const Value = struct {
    bits: u64,
};

test "placeholder" {
    try std.testing.expect(true);
}
