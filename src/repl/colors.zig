/// ANSI color codes and colorized value formatting for REPL output.
const std = @import("std");
const value_mod = @import("value");
const Value = value_mod.Value;
const obj_mod = @import("obj");

pub const Color = struct {
    pub const reset = "\x1b[0m";
    pub const bold = "\x1b[1m";
    pub const red = "\x1b[31m";
    pub const green = "\x1b[32m"; // strings
    pub const yellow = "\x1b[33m"; // atoms
    pub const blue = "\x1b[34m"; // functions
    pub const cyan = "\x1b[36m"; // numbers (int, float)
    pub const magenta = "\x1b[35m"; // booleans
    pub const dim = "\x1b[2m"; // nil, structural chars
};

/// Write a pre-formatted value string with ANSI color codes based on value type.
/// When `use_color` is false, writes the formatted string verbatim.
pub fn formatColorValue(val: Value, formatted: []const u8, writer: anytype, use_color: bool) !void {
    if (!use_color) {
        try writer.writeAll(formatted);
        return;
    }

    const color = getColorForValue(val);
    try writer.writeAll(color);
    try writer.writeAll(formatted);
    try writer.writeAll(Color.reset);
}

/// Determine the ANSI color code for a given value type.
fn getColorForValue(val: Value) []const u8 {
    if (val.isFloat() or val.isInt()) return Color.cyan;
    if (val.isBool()) return Color.magenta;
    if (val.isNil()) return Color.dim;
    if (val.isAtom()) return Color.yellow;
    if (val.isObj()) {
        const obj_ptr = val.asObj();
        return switch (obj_ptr.obj_type) {
            .string => Color.green,
            .function, .closure => Color.blue,
            .int_big => Color.cyan,
            .range => Color.cyan,
            .stream => Color.blue,
            .list, .map, .tuple, .record, .adt => Color.reset,
            .bytes, .upvalue => Color.dim,
        };
    }
    return Color.reset;
}
