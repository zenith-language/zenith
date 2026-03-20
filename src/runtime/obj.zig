// Heap object types -- implemented in Task 2.

const std = @import("std");

pub const Obj = struct {
    obj_type: ObjType,
    next: ?*Obj,
};

pub const ObjType = enum {
    string,
    bytes,
    int_big,
};

test "placeholder" {
    try std.testing.expect(true);
}
