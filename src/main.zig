const std = @import("std");

pub fn main() !void {
    const file = std.fs.File.stdout();
    try file.writeAll("Zenith v0.1.0\n");
}
