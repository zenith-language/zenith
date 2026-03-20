const std = @import("std");
const Allocator = std.mem.Allocator;

/// Structured error codes for all Zenith diagnostics.
pub const ErrorCode = enum(u16) {
    E001 = 1, // type mismatch
    E002 = 2, // undefined variable
    E003 = 3, // integer overflow
    E004 = 4, // division by zero
    E005 = 5, // unexpected token
    E006 = 6, // unterminated string
    E007 = 7, // invalid number literal
    E008 = 8, // too many constants
    E009 = 9, // too many locals
    E010 = 10, // break outside loop
    E011 = 11, // undefined atom
    E012 = 12, // arity mismatch

    pub fn name(self: ErrorCode) []const u8 {
        return switch (self) {
            .E001 => "type mismatch",
            .E002 => "undefined variable",
            .E003 => "integer overflow",
            .E004 => "division by zero",
            .E005 => "unexpected token",
            .E006 => "unterminated string",
            .E007 => "invalid number literal",
            .E008 => "too many constants",
            .E009 => "too many locals",
            .E010 => "break outside loop",
            .E011 => "undefined atom",
            .E012 => "arity mismatch",
        };
    }

    /// Format as "EXXX" for display.
    pub fn code(self: ErrorCode) [4]u8 {
        var buf: [4]u8 = .{ 'E', '0', '0', '0' };
        const num: u16 = @intFromEnum(self);
        buf[1] = @intCast('0' + (num / 100) % 10);
        buf[2] = @intCast('0' + (num / 10) % 10);
        buf[3] = @intCast('0' + num % 10);
        return buf;
    }
};

/// Diagnostic severity level.
pub const Severity = enum {
    @"error",
    warning,
    note,

    pub fn label(self: Severity) []const u8 {
        return switch (self) {
            .@"error" => "error",
            .warning => "warning",
            .note => "note",
        };
    }

    pub fn colorCode(self: Severity) []const u8 {
        return switch (self) {
            .@"error" => "\x1b[1;31m", // bold red
            .warning => "\x1b[1;33m", // bold yellow
            .note => "\x1b[1;36m", // bold cyan
        };
    }
};

/// Byte-offset span in the source text.
pub const Span = struct {
    start: u32,
    end: u32,

    pub fn len(self: Span) u32 {
        return self.end - self.start;
    }
};

/// A secondary annotation label on a source span.
pub const Label = struct {
    span: Span,
    message: []const u8,
    style: LabelStyle,

    pub const LabelStyle = enum { primary, secondary };
};

/// A single diagnostic message with structured context.
pub const Diagnostic = struct {
    error_code: ErrorCode,
    severity: Severity,
    message: []const u8,
    span: Span,
    labels: []const Label,
    help: ?[]const u8,

    /// Render this diagnostic in Rust-style format.
    ///
    /// Example output:
    /// ```
    /// error[E001]: type mismatch
    ///   --> file.zen:5:12
    ///    |
    ///  5 |   let x = 1 + "hello"
    ///    |            ^^^^^^^^^^^ expected Int, got String
    ///    |
    ///    = help: use str() to convert
    /// ```
    pub fn render(
        diag: Diagnostic,
        source: []const u8,
        file_name: []const u8,
        writer: anytype,
        use_color: bool,
    ) !void {
        const bold = if (use_color) "\x1b[1m" else "";
        const reset = if (use_color) "\x1b[0m" else "";
        const sev_color = if (use_color) diag.severity.colorCode() else "";
        const blue = if (use_color) "\x1b[1;34m" else "";

        // Header: error[E001]: message
        const code_str = diag.error_code.code();
        try writer.print("{s}{s}[{s}]{s}: {s}{s}{s}\n", .{
            sev_color,
            diag.severity.label(),
            &code_str,
            reset,
            bold,
            diag.message,
            reset,
        });

        // Location: --> file.zen:line:col
        const loc = lineCol(source, diag.span.start);
        try writer.print("  {s}-->{s} {s}:{d}:{d}\n", .{
            blue, reset,
            file_name,
            loc.line,
            loc.col,
        });

        // Source context
        const source_line = getSourceLine(source, diag.span.start);
        const line_num_width = digitCount(loc.line);
        const pad = line_num_width + 2;

        // Empty gutter line
        try writeSpaces(writer, pad);
        try writer.print("{s}|{s}\n", .{ blue, reset });

        // Source line
        try writer.print(" {s}{d}{s} {s}|{s} {s}\n", .{
            blue,
            loc.line,
            reset,
            blue,
            reset,
            source_line,
        });

        // Caret line -- point to the span
        try writeSpaces(writer, pad);
        try writer.print("{s}|{s} ", .{ blue, reset });
        // Spaces to reach column
        try writeSpaces(writer, loc.col - 1);
        // Carets
        const caret_len = @max(1, caretWidth(source, diag.span, loc));
        const caret_color = if (use_color) sev_color else "";
        for (0..caret_len) |_| {
            try writer.print("{s}^{s}", .{ caret_color, reset });
        }
        // Primary label message (first label)
        if (diag.labels.len > 0) {
            try writer.print(" {s}{s}{s}", .{
                caret_color,
                diag.labels[0].message,
                reset,
            });
        }
        try writer.writeAll("\n");

        // Empty gutter
        try writeSpaces(writer, pad);
        try writer.print("{s}|{s}\n", .{ blue, reset });

        // Help line
        if (diag.help) |help| {
            try writeSpaces(writer, pad);
            try writer.print("{s}={s} {s}help{s}: {s}\n", .{
                blue, reset, bold, reset, help,
            });
        }
    }
};

/// Accumulator for all diagnostics (report all, no early bail).
pub const DiagnosticList = struct {
    items: std.ArrayListUnmanaged(Diagnostic) = .empty,

    pub fn append(self: *DiagnosticList, diag: Diagnostic, allocator: Allocator) !void {
        try self.items.append(allocator, diag);
    }

    pub fn hasErrors(self: *const DiagnosticList) bool {
        for (self.items.items) |d| {
            if (d.severity == .@"error") return true;
        }
        return false;
    }

    pub fn deinit(self: *DiagnosticList, allocator: Allocator) void {
        self.items.deinit(allocator);
    }
};

// ── Utility functions ──────────────────────────────────────────────────

const LineCol = struct {
    line: u32,
    col: u32,
};

fn lineCol(source: []const u8, byte_offset: u32) LineCol {
    var line: u32 = 1;
    var col: u32 = 1;
    for (source[0..@min(byte_offset, @as(u32, @intCast(source.len)))]) |ch| {
        if (ch == '\n') {
            line += 1;
            col = 1;
        } else {
            col += 1;
        }
    }
    return .{ .line = line, .col = col };
}

fn getSourceLine(source: []const u8, byte_offset: u32) []const u8 {
    const offset = @min(byte_offset, @as(u32, @intCast(source.len)));

    // Find the start of this line.
    var start: usize = offset;
    while (start > 0 and source[start - 1] != '\n') {
        start -= 1;
    }

    // Find the end of this line.
    var end: usize = offset;
    while (end < source.len and source[end] != '\n') {
        end += 1;
    }

    return source[start..end];
}

fn digitCount(n: u32) u32 {
    if (n == 0) return 1;
    var count: u32 = 0;
    var v = n;
    while (v > 0) : (v /= 10) {
        count += 1;
    }
    return count;
}

fn writeSpaces(writer: anytype, count: u32) !void {
    for (0..count) |_| {
        try writer.writeByte(' ');
    }
}

fn caretWidth(source: []const u8, span: Span, loc: LineCol) u32 {
    _ = loc;
    if (span.end <= span.start) return 1;

    // Find line start for span.start
    var line_start: usize = span.start;
    while (line_start > 0 and source[line_start - 1] != '\n') {
        line_start -= 1;
    }

    // Caret width is limited to current line
    var end: usize = span.end;
    // Don't extend past newline
    var i: usize = span.start;
    while (i < end and i < source.len and source[i] != '\n') {
        i += 1;
    }
    end = i;

    const width = end - span.start;
    return if (width == 0) 1 else @intCast(width);
}

// ── Tests ──────────────────────────────────────────────────────────────

test "ErrorCode.code formats correctly" {
    try std.testing.expectEqualStrings("E001", &ErrorCode.E001.code());
    try std.testing.expectEqualStrings("E012", &ErrorCode.E012.code());
}

test "ErrorCode.name returns description" {
    try std.testing.expectEqualStrings("type mismatch", ErrorCode.E001.name());
    try std.testing.expectEqualStrings("division by zero", ErrorCode.E004.name());
}

test "Span.len calculates correctly" {
    const span = Span{ .start = 10, .end = 20 };
    try std.testing.expectEqual(@as(u32, 10), span.len());
}

test "lineCol computes correct line and column" {
    const source = "line1\nline2\nline3";
    //               0123456789...
    const loc = lineCol(source, 6); // 'l' of "line2"
    try std.testing.expectEqual(@as(u32, 2), loc.line);
    try std.testing.expectEqual(@as(u32, 1), loc.col);
}

test "Diagnostic.render produces Rust-style output" {
    const source = "  let x = 1 + \"hello\"";
    const diag = Diagnostic{
        .error_code = .E001,
        .severity = .@"error",
        .message = "type mismatch",
        .span = .{ .start = 12, .end = 21 },
        .labels = &[_]Label{
            .{
                .span = .{ .start = 12, .end = 21 },
                .message = "expected Int, got String",
                .style = .primary,
            },
        },
        .help = "use str() to convert",
    };

    var buf: [1024]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buf);
    const writer = stream.writer();

    try diag.render(source, "file.zen", writer, false);

    const output = stream.getWritten();

    // Verify key parts of the Rust-style format are present.
    try std.testing.expect(std.mem.indexOf(u8, output, "error[E001]") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "type mismatch") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "file.zen:1:13") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "^") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "expected Int, got String") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "help: use str() to convert") != null);
}

test "Diagnostic.render with color disabled has no ANSI codes" {
    const source = "let x = 42";
    const diag = Diagnostic{
        .error_code = .E002,
        .severity = .@"error",
        .message = "undefined variable",
        .span = .{ .start = 4, .end = 5 },
        .labels = &[_]Label{},
        .help = null,
    };

    var buf: [512]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buf);
    const writer = stream.writer();

    try diag.render(source, "test.zen", writer, false);

    const output = stream.getWritten();
    try std.testing.expect(std.mem.indexOf(u8, output, "\x1b") == null);
}

test "DiagnosticList accumulates errors" {
    const allocator = std.testing.allocator;
    var list: DiagnosticList = .{};
    defer list.deinit(allocator);

    try list.append(.{
        .error_code = .E001,
        .severity = .@"error",
        .message = "first",
        .span = .{ .start = 0, .end = 1 },
        .labels = &[_]Label{},
        .help = null,
    }, allocator);

    try list.append(.{
        .error_code = .E002,
        .severity = .warning,
        .message = "second",
        .span = .{ .start = 5, .end = 6 },
        .labels = &[_]Label{},
        .help = null,
    }, allocator);

    try std.testing.expectEqual(@as(usize, 2), list.items.items.len);
    try std.testing.expect(list.hasErrors());
}
