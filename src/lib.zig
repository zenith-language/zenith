// Zenith library root -- re-exports public API for consumers.
//
// All submodules are imported here so that `zig build test` can discover
// their inline test blocks through transitive reference analysis.

pub const token = @import("compiler/token.zig");
pub const memory = @import("runtime/memory.zig");
pub const value = @import("runtime/value.zig");
pub const obj = @import("runtime/obj.zig");
pub const chunk = @import("runtime/chunk.zig");
pub const err = @import("common/error.zig");
pub const debug = @import("common/debug.zig");

test {
    // Force the test runner to analyse all transitive dependencies so
    // that inline tests in every imported module are discovered.
    @import("std").testing.refAllDeclsRecursive(@This());
}
