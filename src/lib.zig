// Zenith library root -- re-exports public API for consumers.
//
// All submodules are provided via named module imports from the build system.
// This avoids cross-directory import issues in Zig 0.15+.

pub const token = @import("token");
pub const memory = @import("memory");
pub const value = @import("value");
pub const obj = @import("obj");
pub const chunk = @import("chunk");
pub const err = @import("error");
pub const debug = @import("debug");
pub const lexer = @import("lexer");

test {
    // Force the test runner to analyse all transitive dependencies so
    // that inline tests in every imported module are discovered.
    @import("std").testing.refAllDeclsRecursive(@This());
}
